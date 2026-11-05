import json
import time
import os
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ==============================================================================
# 第一部分：擴充監控標的 (輿情集中：圖文、短影音、短劇、社群)
# ==============================================================================

TARGET_SERVICES = [
    {
        "id": "threads_ig",
        "name": "Threads & IG 圖片 CDN",
        "host": "scontent.cdninstagram.com",
        "category": "Meta 核心",
        "desc": "Threads/IG 貼文大圖破圖與看圖轉圈"
    },
    {
        "id": "meta_reels",
        "name": "Instagram Reels",
        "host": "video.xx.fbcdn.net",
        "category": "Meta 核心",
        "desc": "短影音分段載入、看2秒卡住緩衝"
    },
    {
        "id": "fb_static",
        "name": "Facebook 動態圖文",
        "host": "static.xx.fbcdn.net",
        "category": "Meta 核心",
        "desc": "臉書首頁貼文刷不出、社團圖片延遲"
    },
    {
        "id": "hongguo_drama",
        "name": "紅果短劇 (字節跳動)",
        "host": "v3-dy-y.zjcdn.com",
        "category": "爆款短劇",
        "desc": "熱門短劇起播延遲、換集卡頓"
    },
    {
        "id": "tiktok_cdn",
        "name": "TikTok 媒體串流",
        "host": "v16-webapp-prime.tiktokcdn.com",
        "category": "熱門短影音",
        "desc": "跨國短影音回程吞吐與畫質卡頓"
    },
    {
        "id": "x_media",
        "name": "X (Twitter) 圖片",
        "host": "pbs.twimg.com",
        "category": "Fastly CDN",
        "desc": "Fastly 機房看大圖轉圈延遲"
    },
    {
        "id": "youtube_stream",
        "name": "YouTube Shorts / 串流",
        "host": "googlevideo.com",
        "category": "Google 直連",
        "desc": "國際海纜直連標準對照基準線"
    },
    {
        "id": "line_obs",
        "name": "LINE 圖片與貼圖",
        "host": "obs.line-scdn.net",
        "category": "通訊社群",
        "desc": "晚間傳送照片、社群大圖轉圈"
    },
    {
        "id": "github_fastly",
        "name": "GitHub 檔案下載",
        "host": "objects.githubusercontent.com",
        "category": "Fastly 骨幹",
        "desc": "Release 與大型資產檔案下載掉速"
    },
    {
        "id": "discord_media",
        "name": "Discord 媒體傳輸",
        "host": "cdn.discordapp.com",
        "category": "Cloudflare",
        "desc": "海外圖床與語音連線品質"
    },
    {
        "id": "netflix_edge",
        "name": "Netflix 邊緣節點",
        "host": "ipv4-c001-tpe001-ix.1.oca.nflxvideo.net",
        "category": "串流影音",
        "desc": "追劇起播與本地快取對照"
    }
]

STATE_FILE = "egress_state.json"
tz = timezone(timedelta(hours=8))
now_dt = datetime.now(tz)
now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
time_short = now_dt.strftime('%H:%M')

# 節點座標對應表 (用於 SVG 拓撲地圖繪製)
# 畫布比例：寬 1000 x 高 420 (台灣、東亞、東南亞、美西)
GEO_COORDS = {
    "TW": {"name": "台灣 (台中起點)", "x": 380, "y": 240},
    "TPE": {"name": "台北本地節點", "x": 395, "y": 225},
    "TYO": {"name": "日本 (東京 NRT/HND)", "x": 620, "y": 140},
    "OSA": {"name": "日本 (大阪 KIX)", "x": 560, "y": 160},
    "HKG": {"name": "香港 (HKG POP)", "x": 330, "y": 255},
    "SIN": {"name": "新加坡 (SIN POP)", "x": 230, "y": 360},
    "US_WEST": {"name": "美國西岸 (SJC/LAX)", "x": 920, "y": 170}
}

def determine_geo(resolved_ip, headers_str):
    """依據 Header 或 IP 粗估 CDN 落地點"""
    h_lower = headers_str.lower()
    if "nrt" in h_lower or "hnd" in h_lower or "tyo" in h_lower:
        return "TYO"
    if "kix" in h_lower or "osa" in h_lower:
        return "OSA"
    if "hkg" in h_lower or "hongkong" in h_lower:
        return "HKG"
    if "sin" in h_lower or "singapore" in h_lower:
        return "SIN"
    if "sjc" in h_lower or "lax" in h_lower or "sea" in h_lower:
        return "US_WEST"
    if "tpe" in h_lower or "tw" in h_lower:
        return "TPE"
    
    # 預設依 IP 與通用經驗推估
    if resolved_ip.startswith("199.232.") or resolved_ip.startswith("151.101."):
        return "TYO" # Fastly 常態走東京
    if resolved_ip.startswith("157.240.") or resolved_ip.startswith("31.13."):
        return "HKG" # Meta 亞洲多走香港/台灣
    return "TYO"

# ==============================================================================
# 第二部分：Globalping 探測 (支援單次重試，剔除 0 值)
# ==============================================================================

def execute_single_probe(host):
    """執行一次 Globalping HTTPS 探測"""
    api_url = "https://api.globalping.io/v1/measurements"
    payload = json.dumps({
        "type": "http",
        "target": host,
        "locations": [{"country": "TW", "asn": 3462}],
        "measurementOptions": {"protocol": "HTTPS"},
        "limit": 1
    }).encode("utf-8")

    req = urllib.request.Request(
        api_url, data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "HiNet-Egress-Probe"}
    )

    measurement_id = None
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            measurement_id = json.loads(resp.read().decode("utf-8")).get("id")
    except Exception:
        return None

    if not measurement_id:
        return None

    poll_url = f"{api_url}/{measurement_id}"
    for _ in range(10):
        time.sleep(1)
        try:
            req_poll = urllib.request.Request(poll_url, headers={"User-Agent": "HiNet-Egress-Probe"})
            with urllib.request.urlopen(req_poll, timeout=8) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("status") == "finished":
                    results = data.get("results", [])
                    if results:
                        res = results[0].get("result", {})
                        timings = res.get("timings", {})
                        total_ms = round(timings.get("total", 0), 1)
                        tcp_ms = round(timings.get("tcp", 0), 1)
                        resolved_ip = res.get("resolvedAddress", "")
                        raw_headers = res.get("rawHeaders", "")
                        if total_ms > 0:
                            return {
                                "total_ms": total_ms,
                                "tcp_ms": tcp_ms,
                                "resolved_ip": resolved_ip,
                                "headers": raw_headers
                            }
        except Exception:
            pass
    return None

def probe_service_with_retry(service):
    """帶重試機制探測：若為 0 則在 2 秒內重試一次，取有效值"""
    sid = service["id"]
    res = execute_single_probe(service["host"])
    
    # 第一次若為 0 或逾時，重試第二次
    if not res or res["total_ms"] <= 0:
        time.sleep(2)
        res = execute_single_probe(service["host"])

    if res and res["total_ms"] > 0:
        geo_key = determine_geo(res["resolved_ip"], res["headers"])
        return sid, {
            "status": "OK",
            "total_ms": res["total_ms"],
            "tcp_ms": res["tcp_ms"],
            "resolved_ip": res["resolved_ip"],
            "geo_key": geo_key,
            "geo_name": GEO_COORDS.get(geo_key, {}).get("name", "海外節點")
        }
    else:
        return sid, {
            "status": "FAIL",
            "total_ms": 0,
            "tcp_ms": 0,
            "resolved_ip": "--",
            "geo_key": "TYO",
            "geo_name": "線路逾時",
            "err": "重試後依然逾時"
        }

# ==============================================================================
# 第三部分：平行發起巡檢
# ==============================================================================

print(f"[{now_str}] 啟動 HiNet 出口體感巡檢 (11 款輿情熱門服務)...")

results = {}
with ThreadPoolExecutor(max_workers=3) as executor:
    future_to_svc = {executor.submit(probe_service_with_retry, s): s["id"] for s in TARGET_SERVICES}
    for future in as_completed(future_to_svc):
        svc_id, res_data = future.result()
        results[svc_id] = res_data

# ==============================================================================
# 第四部分：狀態處理與歷史記錄
# ==============================================================================

history_state = []
if os.path.exists(STATE_FILE):
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            history_state = json.load(f)
    except Exception:
        history_state = []

current_snapshot = {
    "timestamp": now_str,
    "time_short": time_short,
    "data": {}
}

for s in TARGET_SERVICES:
    sid = s["id"]
    res = results.get(sid, {"status": "FAIL", "total_ms": 0, "tcp_ms": 0, "geo_key": "TYO"})
    total_ms = res.get("total_ms", 0)

    # 體感標準：<150ms 綠 (秒開), 150-350ms 黃 (稍等), >=350ms 紅 (轉圈卡頓)
    if res.get("status") != "OK":
        status_text = "🚨 連線逾時"
        status_color = "#f87171"
    elif total_ms >= 350:
        status_text = f"🚨 轉圈卡頓 ({total_ms}ms)"
        status_color = "#f87171"
    elif total_ms >= 150:
        status_text = f"稍微延遲 ({total_ms}ms)"
        status_color = "#fde047"
    else:
        status_text = "順暢良好"
        status_color = "#34d399"

    current_snapshot["data"][sid] = {
        "name": s["name"],
        "category": s["category"],
        "desc": s["desc"],
        "total_ms": total_ms,
        "tcp_ms": res.get("tcp_ms", 0),
        "status": res.get("status"),
        "status_text": status_text,
        "status_color": status_color,
        "geo_key": res.get("geo_key", "TYO"),
        "geo_name": res.get("geo_name", "海外節點"),
        "ip": res.get("resolved_ip", "--")
    }

history_state.insert(0, current_snapshot)
history_state = history_state[:72]  # 保留 24 小時

with open(STATE_FILE, "w", encoding="utf-8") as f:
    json.dump(history_state, f, ensure_ascii=False, indent=2)

# ==============================================================================
# 第五部分：前端 HTML 組裝 (包含 SVG 動態雙向拓撲圖與 HUD 懸浮)
# ==============================================================================

chronological_data = list(reversed(history_state))
chart_labels = json.dumps([item["time_short"] for item in chronological_data])

# 產生地圖上的 SVG 連線路徑
svg_paths_html = ""
tw_pt = GEO_COORDS["TW"]

for s in TARGET_SERVICES:
    sid = s["id"]
    curr = current_snapshot["data"].get(sid, {})
    geo = GEO_COORDS.get(curr.get("geo_key"), GEO_COORDS["TYO"])
    color = curr.get("status_color", "#34d399")
    
    # 畫貝茲曲線 (Bezier Curve)
    mid_x = (tw_pt["x"] + geo["x"]) / 2
    mid_y = min(tw_pt["y"], geo["y"]) - 40
    d_path = f"M {tw_pt['x']} {tw_pt['y']} Q {mid_x} {mid_y} {geo['x']} {geo['y']}"

    svg_paths_html += f"""
    <g class="path-group" id="grp-{sid}" data-sid="{sid}">
        <path d="{d_path}" class="probe-path-bg" />
        <path d="{d_path}" class="probe-path" stroke="{color}" id="path-{sid}" />
        <!-- 去程流動光點 -->
        <circle r="3" fill="#ffffff" class="flow-dot flow-forward">
            <animateMotion dur="2.5s" repeatCount="indefinite" path="{d_path}" />
        </circle>
        <!-- 回程流動光點 (下載體感) -->
        <circle r="3.5" fill="{color}" class="flow-dot flow-backward">
            <animateMotion dur="2.5s" repeatCount="indefinite" path="{d_path}" keyPoints="1;0" keyTimes="0;1" />
        </circle>
    </g>
    """

# 產生下方緊湊型卡片
compact_cards_html = ""
for s in TARGET_SERVICES:
    sid = s["id"]
    curr = current_snapshot["data"].get(sid, {})
    total_str = f"{curr['total_ms']} ms" if curr['status'] == "OK" else "逾時"
    tcp_str = f"{curr['tcp_ms']} ms" if curr['status'] == "OK" else "--"

    compact_cards_html += f"""
    <div class="compact-card" id="card-{sid}" data-sid="{sid}" onmouseenter="focusApp('{sid}')" onmouseleave="unfocusApp()">
        <div style="display:flex; justify-content:space-between; align-items:center;">
            <div>
                <span class="badge">{curr['category']}</span>
                <span style="font-weight:bold; font-size:14px; color:#f8fafc; margin-left:6px;">{curr['name']}</span>
            </div>
            <span style="font-size:12px; font-weight:bold; color:{curr['status_color']};">{curr['status_text']}</span>
        </div>
        <div style="display:flex; justify-content:space-between; align-items:flex-end; margin-top:8px;">
            <div>
                <div style="font-size:11px; color:#94a3b8;">總體感 (Total)</div>
                <div style="font-size:18px; font-weight:800; color:{'#f87171' if curr['total_ms']>350 else '#38bdf8'};">{total_str}</div>
            </div>
            <div style="text-align:right;">
                <div style="font-size:11px; color:#94a3b8;">節點: <span style="color:#e2e8f0;">{curr['geo_name']}</span></div>
                <div style="font-size:11px; color:#a855f7;">TCP: {tcp_str}</div>
            </div>
        </div>
    </div>
    """

# 彙整歷史數據供圖表使用
chart_datasets = {}
for s in TARGET_SERVICES:
    sid = s["id"]
    chart_datasets[sid] = [item.get("data", {}).get(sid, {}).get("total_ms", 0) for item in chronological_data]

html_content = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HiNet 海外出口體感戰情室</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:#070d18; color:#e2e8f0; margin:0; padding:18px 24px; font-size:14px; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}

        /* 導航切換 */
        .system-tabs {{ display: flex; gap: 8px; margin-bottom: 14px; border-bottom: 2px solid #1e293b; padding-bottom: 8px; }}
        .tab-btn {{ padding: 6px 16px; font-size: 13px; font-weight: bold; border-radius: 6px; text-decoration: none; color: #94a3b8; background: #0f172a; border: 1px solid #1e293b; }}
        .tab-active {{ background: #0284c7; color: #ffffff; border: none; }}

        /* 頂部戰情標題 */
        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:12px; }}
        
        /* 戰情地圖容器 */
        .map-section {{ position:relative; background:#0b1324; border:1px solid #1e293b; border-radius:12px; padding:12px; margin-bottom:18px; box-shadow:0 8px 24px rgba(0,0,0,0.4); }}
        svg {{ width:100%; height:auto; display:block; }}
        
        .probe-path-bg {{ fill:none; stroke:#1e293b; stroke-width:3; stroke-linecap:round; opacity:0.6; }}
        .probe-path {{ fill:none; stroke-width:2.2; stroke-linecap:round; opacity:0.85; transition:all 0.3s; }}
        
        /* 節點圓點 */
        .geo-node {{ fill:#38bdf8; stroke:#070d18; stroke-width:2; }}
        .node-tw {{ fill:#f43f5e; r:7; }}
        .node-label {{ fill:#94a3b8; font-size:11px; font-weight:bold; }}

        /* 聚焦效果 (Hover) */
        .dimmed {{ opacity: 0.12 !important; }}
        .highlight {{ stroke-width: 4.5 !important; opacity: 1 !important; filter: drop-shadow(0 0 8px rgba(56, 189, 248, 0.8)); }}

        /* HUD 懸浮窗 */
        #hud-box {{ position:absolute; top:20px; right:20px; width:280px; background:rgba(15, 23, 42, 0.92); border:1px solid #38bdf8; border-radius:8px; padding:12px; backdrop-filter:blur(8px); display:none; pointer-events:none; box-shadow:0 6px 20px rgba(0,0,0,0.6); }}
        
        /* 下方清單網格 */
        .compact-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 12px; }}
        .compact-card {{ background:#0f172a; border:1px solid #1e293b; border-radius:8px; padding:12px; transition:all 0.2s; cursor:pointer; }}
        .compact-card:hover {{ border-color:#38bdf8; transform:translateY(-2px); }}
        .badge {{ background:#1e293b; color:#94a3b8; font-size:10px; font-weight:bold; padding:2px 6px; border-radius:4px; }}
    </style>
</head>
<body>
    <div class="container">
        <!-- 導航列 -->
        <div class="system-tabs">
            <a href="index.html" class="tab-btn">🌐 IPv4 核心巡檢</a>
            <a href="index.html" class="tab-btn">🚀 IPv6 核心巡檢</a>
            <a href="egress.html" class="tab-btn tab-active">⚡ 海外出口動態拓撲戰情室</a>
        </div>

        <div class="header">
            <div>
                <h2 style="margin:0; font-size:20px; font-weight:800; color:#f8fafc;">
                    ⚡ 中華電信 (HiNet AS3462) 國際出口動態拓撲
                </h2>
                <div style="color:#64748b; font-size:12px; margin-top:4px;">
                    起點：台中實體用戶端探針 ➔ 即時海纜走向與雙向體感延遲 | 更新：{now_str}
                </div>
            </div>
            <div>
                <button onclick="window.location.reload()" style="background:#0284c7; color:#fff; border:none; border-radius:6px; padding:6px 14px; cursor:pointer; font-weight:bold; font-size:12px;">
                    🔄 刷新
                </button>
            </div>
        </div>

        <!-- 頂部拓撲地圖 -->
        <div class="map-section">
            <!-- HUD 資訊框 -->
            <div id="hud-box">
                <div id="hud-title" style="font-size:14px; font-weight:bold; color:#f8fafc; margin-bottom:4px;">--</div>
                <div id="hud-desc" style="font-size:11px; color:#94a3b8; margin-bottom:8px;">--</div>
                <div style="display:flex; justify-content:space-between; margin-bottom:6px;">
                    <span style="font-size:12px; color:#cbd5e1;">總體感耗時: <strong id="hud-total" style="color:#38bdf8;">--</strong></span>
                    <span style="font-size:12px; color:#cbd5e1;">TCP 握手: <strong id="hud-tcp" style="color:#a855f7;">--</strong></span>
                </div>
                <div style="font-size:11px; color:#94a3b8; margin-bottom:6px;">落地機房：<span id="hud-geo" style="color:#e2e8f0;">--</span></div>
                <div style="height:60px;">
                    <canvas id="hud-chart"></canvas>
                </div>
            </div>

            <svg viewBox="0 0 1000 420">
                <!-- 地理節點背景錨點 -->
                <g>
                    <!-- 台灣起點 -->
                    <circle cx="{GEO_COORDS['TW']['x']}" cy="{GEO_COORDS['TW']['y']}" class="node-tw" />
                    <text x="{GEO_COORDS['TW']['x']-45}" y="{GEO_COORDS['TW']['y']+20}" class="node-label" fill="#f43f5e">🇹🇼 台中 (起點)</text>
                    
                    <!-- 海外目標節點 -->
                    <circle cx="{GEO_COORDS['TYO']['x']}" cy="{GEO_COORDS['TYO']['y']}" r="5" class="geo-node" />
                    <text x="{GEO_COORDS['TYO']['x']+10}" y="{GEO_COORDS['TYO']['y']+4}" class="node-label">🇯🇵 日本東京</text>

                    <circle cx="{GEO_COORDS['HKG']['x']}" cy="{GEO_COORDS['HKG']['y']}" r="5" class="geo-node" />
                    <text x="{GEO_COORDS['HKG']['x']-65}" y="{GEO_COORDS['HKG']['y']+4}" class="node-label">🇭🇰 香港 POP</text>

                    <circle cx="{GEO_COORDS['SIN']['x']}" cy="{GEO_COORDS['SIN']['y']}" r="5" class="geo-node" />
                    <text x="{GEO_COORDS['SIN']['x']-65}" y="{GEO_COORDS['SIN']['y']+4}" class="node-label">🇸🇬 新加坡</text>

                    <circle cx="{GEO_COORDS['US_WEST']['x']}" cy="{GEO_COORDS['US_WEST']['y']}" r="5" class="geo-node" />
                    <text x="{GEO_COORDS['US_WEST']['x']-75}" y="{GEO_COORDS['US_WEST']['y']-10}" class="node-label">🇺🇸 美西 SJC</text>
                </g>

                <!-- 動態線路 -->
                {svg_paths_html}
            </svg>
        </div>

        <!-- 下方緊湊卡片網格 -->
        <div class="compact-grid">
            {compact_cards_html}
        </div>
    </div>

    <script>
        const appData = {json.dumps(current_snapshot['data'])};
        const chartHistory = {json.dumps(chart_datasets)};
        const chartLabels = {chart_labels};
        let hudChart = null;

        function initHudChart() {{
            const ctx = document.getElementById('hud-chart').getContext('2d');
            hudChart = new Chart(ctx, {{
                type: 'line',
                data: {{
                    labels: chartLabels,
                    datasets: [{{
                        data: [],
                        borderColor: '#38bdf8',
                        borderWidth: 1.5,
                        pointRadius: 0,
                        tension: 0.3
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{ legend: {{ display: false }} }},
                    scales: {{ x: {{ display: false }}, y: {{ display: false }} }}
                }}
            }});
        }}
        initHudChart();

        function focusApp(sid) {{
            const info = appData[sid];
            if (!info) return;

            // 線條高亮
            document.querySelectorAll('.path-group').forEach(el => {{
                if (el.getAttribute('data-sid') === sid) {{
                    el.classList.remove('dimmed');
                    el.querySelector('.probe-path').classList.add('highlight');
                }} else {{
                    el.classList.add('dimmed');
                }}
            }});

            // 顯示 HUD
            const hud = document.getElementById('hud-box');
            document.getElementById('hud-title').innerText = info.name;
            document.getElementById('hud-desc').innerText = info.desc;
            document.getElementById('hud-total').innerText = info.total_ms + ' ms';
            document.getElementById('hud-tcp').innerText = info.tcp_ms + ' ms';
            document.getElementById('hud-geo').innerText = info.geo_name;

            hud.style.display = 'block';

            // 更新迷你圖表
            if (hudChart && chartHistory[sid]) {{
                hudChart.data.datasets[0].data = chartHistory[sid];
                hudChart.data.datasets[0].borderColor = info.status_color;
                hudChart.update();
            }}
        }}

        function unfocusApp() {{
            document.querySelectorAll('.path-group').forEach(el => {{
                el.classList.remove('dimmed');
                el.querySelector('.probe-path').classList.remove('highlight');
            }});
            document.getElementById('hud-box').style.display = 'none';
        }}
    </script>
</body>
</html>
"""

with open("egress.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 成功產出具備動態拓撲地圖之體感戰情室。")
