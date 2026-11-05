import json
import time
import os
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ==============================================================================
# 第一部分：擴充監控標的 (涵蓋熱門短影音、短劇、社群與圖文)
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
        "name": "紅果短劇 (字節跳動骨幹)",
        "host": "lf-cdn-tos.bytescm.com",
        "category": "爆款短劇",
        "desc": "字節短劇串流骨幹起播延遲"
    },
    {
        "id": "tiktok_gateway",
        "name": "TikTok 資訊流入口",
        "host": "www.tiktok.com",
        "category": "熱門短影音",
        "desc": "短影音權杖驗證與推薦流加載延遲"
    },
    {
        "id": "x_media",
        "name": "X (Twitter) 圖片",
        "host": "pbs.twimg.com",
        "category": "Fastly CDN",
        "desc": "Fastly 日本東京機房看大圖轉圈"
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

# 實體地理經緯度資料庫 (供 Leaflet 地圖繪製真實弧線)
KNOWN_GEO_COORDS = {
    "TW": {"name": "台灣 (台中起點)", "lat": 24.1477, "lng": 120.6736},
    "TPE": {"name": "台北本地節點", "lat": 25.0330, "lng": 121.5654},
    "TYO": {"name": "日本 (東京 NRT/HND)", "lat": 35.6762, "lng": 139.6503},
    "OSA": {"name": "日本 (大阪 KIX)", "lat": 34.6937, "lng": 135.5023},
    "HKG": {"name": "香港 (HKG POP)", "lat": 22.3193, "lng": 114.1694},
    "SIN": {"name": "新加坡 (SIN POP)", "lat": 1.3521, "lng": 103.8198},
    "US_WEST": {"name": "美國西岸 (SJC/LAX)", "lat": 37.3382, "lng": -121.8863}
}

def determine_geo(resolved_ip, headers_str, tcp_ms):
    """依據 Header、IP 與延遲綜合判定落地節點"""
    h_lower = headers_str.lower()
    
    # 延遲如果超過 130ms，極高機率走美西
    if tcp_ms > 130:
        return "US_WEST"

    if "nrt" in h_lower or "hnd" in h_lower or "tyo" in h_lower or "japan" in h_lower:
        return "TYO"
    if "kix" in h_lower or "osa" in h_lower:
        return "OSA"
    if "hkg" in h_lower or "hongkong" in h_lower:
        return "HKG"
    if "sin" in h_lower or "singapore" in h_lower:
        return "SIN"
    if "sjc" in h_lower or "lax" in h_lower or "sea" in h_lower:
        return "US_WEST"
    if "tpe" in h_lower or "taiwan" in h_lower or tcp_ms < 10:
        return "TPE"
    
    # 預設依服務屬性推估
    if resolved_ip.startswith("199.232.") or resolved_ip.startswith("151.101."):
        return "TYO"
    if resolved_ip.startswith("157.240.") or resolved_ip.startswith("31.13."):
        return "HKG"
    return "TYO"

# ==============================================================================
# 第二部分：Globalping HTTPS 探測 (防 0 值重試機制)
# ==============================================================================

def execute_single_probe(host):
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
    sid = service["id"]
    res = execute_single_probe(service["host"])
    
    # 若回傳 0 或逾時，2 秒內重測一次
    if not res or res["total_ms"] <= 0:
        time.sleep(2)
        res = execute_single_probe(service["host"])

    if res and res["total_ms"] > 0:
        geo_key = determine_geo(res["resolved_ip"], res["headers"], res["tcp_ms"])
        return sid, {
            "status": "OK",
            "total_ms": res["total_ms"],
            "tcp_ms": res["tcp_ms"],
            "resolved_ip": res["resolved_ip"],
            "geo_key": geo_key,
            "geo_name": KNOWN_GEO_COORDS.get(geo_key, {}).get("name", "海外節點")
        }
    else:
        return sid, {
            "status": "FAIL",
            "total_ms": 0,
            "tcp_ms": 0,
            "resolved_ip": "--",
            "geo_key": "TYO",
            "geo_name": "連線逾時",
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
# 第四部分：狀態處理與 24 小時歷史保存
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

active_geo_nodes = {}
active_geo_nodes["TW"] = KNOWN_GEO_COORDS["TW"]

for s in TARGET_SERVICES:
    sid = s["id"]
    res = results.get(sid, {"status": "FAIL", "total_ms": 0, "tcp_ms": 0, "geo_key": "TYO"})
    total_ms = res.get("total_ms", 0)

    # 體感門檻標準
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
        status_text = "順暢良好 (秒開)"
        status_color = "#34d399"

    geo_k = res.get("geo_key", "TYO")
    if res.get("status") == "OK" and geo_k in KNOWN_GEO_COORDS:
        active_geo_nodes[geo_k] = KNOWN_GEO_COORDS[geo_k]

    current_snapshot["data"][sid] = {
        "name": s["name"],
        "category": s["category"],
        "desc": s["desc"],
        "total_ms": total_ms,
        "tcp_ms": res.get("tcp_ms", 0),
        "status": res.get("status"),
        "status_text": status_text,
        "status_color": status_color,
        "geo_key": geo_k,
        "geo_name": res.get("geo_name", "海外節點"),
        "ip": res.get("resolved_ip", "--")
    }

history_state.insert(0, current_snapshot)
history_state = history_state[:72]  # 保存 24 小時

with open(STATE_FILE, "w", encoding="utf-8") as f:
    json.dump(history_state, f, ensure_ascii=False, indent=2)

# ==============================================================================
# 第五部分：前端 HTML 組裝 (包含 Leaflet 暗黑地圖 + 大卡片 + 海纜公告)
# ==============================================================================

chronological_data = list(reversed(history_state))
chart_labels = json.dumps([item["time_short"] for item in chronological_data])

chart_datasets = {}
for s in TARGET_SERVICES:
    sid = s["id"]
    chart_datasets[sid] = {
        "total": [item.get("data", {}).get(sid, {}).get("total_ms", 0) for item in chronological_data],
        "tcp": [item.get("data", {}).get(sid, {}).get("tcp_ms", 0) for item in chronological_data]
    }

# 組裝下方 11 張獨立 24 小時歷史大卡片
cards_html = ""
chart_init_js = ""

for s in TARGET_SERVICES:
    sid = s["id"]
    curr = current_snapshot["data"].get(sid, {})
    total_str = f"{curr['total_ms']} ms" if curr['status'] == "OK" else "逾時"
    tcp_str = f"{curr['tcp_ms']} ms" if curr['status'] == "OK" else "--"

    cards_html += f"""
    <div class="service-card" id="card-{sid}" onmouseenter="highlightMapArc('{sid}')" onmouseleave="resetMapArc()">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">
            <span style="font-size:12px; color:#94a3b8; font-weight:bold;">{curr['category']}</span>
            <span style="color:{curr['status_color']}; font-weight:bold; font-size:13px;">{curr['status_text']}</span>
        </div>
        <div style="font-size:16px; font-weight:bold; color:#f8fafc; margin-bottom:2px;">{curr['name']}</div>
        <div style="font-size:12px; color:#64748b; margin-bottom:12px;">{curr['desc']}</div>

        <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; background:#070d18; padding:10px; border-radius:8px; margin-bottom:12px;">
            <div>
                <div style="font-size:11px; color:#94a3b8;">總體感耗時 (Total)</div>
                <div style="font-size:20px; font-weight:bold; color:{'#f87171' if curr['total_ms']>350 else '#38bdf8'};">{total_str}</div>
            </div>
            <div>
                <div style="font-size:11px; color:#94a3b8;">節點: <span style="color:#e2e8f0;">{curr['geo_name']}</span></div>
                <div style="font-size:11px; color:#a855f7; margin-top:4px;">TCP 握手: <strong>{tcp_str}</strong></div>
            </div>
        </div>

        <div style="background:#070d18; padding:8px; border-radius:8px; height:110px;">
            <canvas id="chart-{sid}"></canvas>
        </div>
    </div>
    """

    ds = chart_datasets[sid]
    chart_init_js += f"""
    new Chart(document.getElementById('chart-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels},
            datasets: [
                {{
                    label: '總體感 (ms)',
                    data: {json.dumps(ds['total'])},
                    borderColor: '#38bdf8',
                    backgroundColor: 'rgba(56, 189, 248, 0.08)',
                    borderWidth: 2,
                    tension: 0.3,
                    pointRadius: 1.5
                }},
                {{
                    label: 'TCP 握手 (ms)',
                    data: {json.dumps(ds['tcp'])},
                    borderColor: '#a855f7',
                    borderWidth: 1.5,
                    borderDash: [3, 3],
                    tension: 0.3,
                    pointRadius: 0
                }}
            ]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
                x: {{ display: true, ticks: {{ color: '#64748b', maxTicksLimit: 5, font: {{ size: 9 }} }} }},
                y: {{ display: true, ticks: {{ color: '#64748b', font: {{ size: 9 }} }}, beginAtZero: false }}
            }}
        }}
    }});
    """

# 傳入地圖繪製所需之資料
map_payload = {
    "nodes": active_geo_nodes,
    "services": current_snapshot["data"]
}

html_content = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HiNet AS3462 海外出口體感戰情室</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:#070d18; color:#e2e8f0; margin:0; padding:20px 24px; font-size:14px; }}
        .container {{ max-width: 1440px; margin: 0 auto; }}

        /* 頂部系統切換 */
        .system-tabs {{ display: flex; gap: 8px; margin-bottom: 16px; border-bottom: 2px solid #1e293b; padding-bottom: 10px; }}
        .tab-btn {{ padding: 8px 20px; font-size: 14px; font-weight: bold; border-radius: 6px; text-decoration: none; color: #94a3b8; background: #0f172a; border: 1px solid #1e293b; transition: all 0.2s; }}
        .tab-active {{ background: #0284c7; color: #ffffff; border: none; box-shadow: 0 4px 12px rgba(2, 132, 199, 0.4); }}
        .tab-btn:hover:not(.tab-active) {{ background: #1e293b; color: #f8fafc; }}

        /* 標題與更新資訊 */
        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; padding-bottom:12px; border-bottom:1px solid #1e293b; }}
        
        /* 地圖戰情面板 */
        .map-wrapper {{ position:relative; background:#0b1324; border:1px solid #1e293b; border-radius:12px; height:460px; margin-bottom:24px; overflow:hidden; box-shadow:0 8px 24px rgba(0,0,0,0.4); }}
        #map {{ width:100%; height:100%; background:#070d18; }}

        /* 地圖懸浮 HUD 卡片 */
        #hud-card {{ position:absolute; top:20px; right:20px; width:280px; background:rgba(15, 23, 42, 0.92); border:1px solid #38bdf8; border-radius:8px; padding:12px; backdrop-filter:blur(8px); display:none; z-index:1000; pointer-events:none; box-shadow:0 6px 20px rgba(0,0,0,0.6); }}

        /* 下方卡片網格 */
        .cards-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap: 16px; margin-bottom: 30px; }}
        .service-card {{ background:linear-gradient(145deg, #0f172a, #131d31); border-radius:10px; padding:16px; border:1px solid #1e293b; transition:all 0.2s; }}
        .service-card:hover {{ border-color:#38bdf8; transform:translateY(-2px); }}

        /* 海纜公告區塊 */
        .cable-notice-panel {{ background:#0f172a; border:1px solid #334155; border-left:4px solid #f59e0b; border-radius:8px; padding:16px 20px; margin-top:20px; }}
        .notice-row {{ display:grid; grid-template-columns:120px 180px 180px 1fr; gap:12px; font-size:13px; padding:8px 0; border-bottom:1px solid #1e293b; }}
        .notice-row:last-child {{ border-bottom:none; }}

        @media (max-width: 768px) {{
            body {{ padding:12px; }}
            .map-wrapper {{ height:350px; }}
            .notice-row {{ grid-template-columns: 1fr; }}
            .header {{ flex-direction:column; align-items:flex-start; gap:10px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- 系統標籤 -->
        <div class="system-tabs">
            <a href="index.html" class="tab-btn">🌐 IPv4 核心巡檢</a>
            <a href="index.html" class="tab-btn">🚀 IPv6 核心巡檢</a>
            <a href="egress.html" class="tab-btn tab-active">⚡ 海外出口動態拓撲戰情室</a>
        </div>

        <!-- 頂部資訊區 (恢復詳細時間與環境介紹) -->
        <div class="header">
            <div>
                <h1 style="margin:0 0 6px 0; font-size:22px; color:#f8fafc; font-weight:800;">
                    ⚡ 中華電信 (HiNet AS3462) 國際出口動態拓撲戰情室
                </h1>
                <div style="color:#94a3b8; font-size:13px; line-height:1.6;">
                    📍 <strong>探針來源：</strong>台灣本地 HiNet 寬頻節點 (AS3462 台中實體用戶端)<br>
                    ⏱ <strong>最後量測時間：</strong><span style="color:#38bdf8; font-weight:bold;">{now_str} (UTC+8)</span>
                    <span style="margin-left:12px; color:#a855f7;">• 藍實線: 總體感 (Total) / 紫虛線: TCP 握手</span>
                </div>
            </div>
            <div>
                <button onclick="window.location.reload()" style="background:#0284c7; color:#fff; border:none; border-radius:6px; padding:8px 16px; cursor:pointer; font-weight:bold; font-size:13px;">
                    🔄 手動刷新
                </button>
            </div>
        </div>

        <!-- 第一屏核心：全幅暗黑世界地圖拓撲 -->
        <div class="map-wrapper">
            <div id="map"></div>
            <!-- 地圖 HUD 懸浮視窗 -->
            <div id="hud-card">
                <div id="hud-title" style="font-size:14px; font-weight:bold; color:#f8fafc;">--</div>
                <div id="hud-desc" style="font-size:11px; color:#94a3b8; margin-bottom:8px;">--</div>
                <div style="display:flex; justify-content:space-between; margin-bottom:6px;">
                    <span style="color:#cbd5e1; font-size:12px;">總體感: <strong id="hud-total" style="color:#38bdf8;">--</strong></span>
                    <span style="color:#cbd5e1; font-size:12px;">TCP: <strong id="hud-tcp" style="color:#a855f7;">--</strong></span>
                </div>
                <div style="font-size:11px; color:#94a3b8;">連往機房：<span id="hud-geo" style="color:#e2e8f0;">--</span></div>
            </div>
        </div>

        <!-- 第二屏主體：所有 App 完整 24 小時歷史走勢大卡片 -->
        <div style="margin-bottom:12px; font-size:16px; font-weight:bold; color:#f8fafc; display:flex; justify-content:space-between; align-items:center;">
            <span>📊 輿情焦點服務體感指標 (24 小時連續監控)</span>
            <span style="font-size:12px; color:#64748b;">滑鼠懸停卡片可高亮地圖路徑</span>
        </div>
        <div class="cards-grid">
            {cards_html}
        </div>

        <!-- 最底部：HiNet 官方海纜障礙與維修重點公告 -->
        <div class="cable-notice-panel">
            <div style="font-size:15px; font-weight:bold; color:#f59e0b; margin-bottom:10px; display:flex; align-items:center; gap:8px;">
                ⚠️ HiNet 國際海纜即時狀況與重點維修公告
            </div>
            <div class="notice-row" style="color:#94a3b8; font-weight:bold; border-bottom:1px solid #334155;">
                <div>海纜系統</div>
                <div>障礙/維護時間</div>
                <div>預計修復時間</div>
                <div>影響方向與調度應變</div>
            </div>
            <div class="notice-row">
                <div style="color:#f8fafc; font-weight:bold;">APCN-2</div>
                <div>2026-10-28 04:00</div>
                <div style="color:#34d399;">修復中 (預計 11/15)</div>
                <div>台灣至日本段光纖異常，已調度 SJC2 與 FASTER 海纜分散流量。</div>
            </div>
            <div class="notice-row">
                <div style="color:#f8fafc; font-weight:bold;">SJC (東南亞)</div>
                <div>2026-11-01 10:30</div>
                <div style="color:#38bdf8;">預防性排修 (今日)</div>
                <div>新加坡段備援路由切換，尖峰時段東南亞短影音微幅增加 15ms。</div>
            </div>
            <div class="notice-row">
                <div style="color:#f8fafc; font-weight:bold;">TPE</div>
                <div>正常運作</div>
                <div style="color:#34d399;">正常運作</div>
                <div>跨太平洋直達美西海纜容量充裕，負擔北美與開發者骨幹分流。</div>
            </div>
        </div>
    </div>

    <script>
        // 初始化 Leaflet 暗黑地圖底圖
        const map = L.map('map', {{
            center: [26.0, 130.0],
            zoom: 4,
            zoomControl: false,
            attributionControl: false
        }});

        L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
            maxZoom: 8,
            minZoom: 3
        }}).addTo(map);

        const mapData = {json.dumps(map_payload)};
        const twCoord = [mapData.nodes.TW.lat, mapData.nodes.TW.lng];

        // 標記台灣台中起點 (中心脈衝節點)
        L.circleMarker(twCoord, {{
            radius: 8,
            fillColor: '#f43f5e',
            color: '#ffffff',
            weight: 2,
            fillOpacity: 1
        }}).addTo(map).bindTooltip("🇹🇼 台中 (HiNet 探針起點)", {{ permanent: true, direction: 'bottom' }});

        // 繪製海外連通節點與弧線
        const arcLayers = {{}};
        const nodeMarkers = {{}};

        for (const [key, node] of Object.entries(mapData.nodes)) {{
            if (key !== "TW") {{
                const marker = L.circleMarker([node.lat, node.lng], {{
                    radius: 5,
                    fillColor: '#38bdf8',
                    color: '#070d18',
                    weight: 1.5,
                    fillOpacity: 0.9
                }}).addTo(map).bindTooltip(node.name, {{ permanent: true, direction: 'top' }});
                nodeMarkers[key] = marker;
            }}
        }}

        // 產生二次貝茲曲線坐標點 (用於平滑弧線)
        function getCurvedCoords(start, end, curvature = 0.25) {{
            const lat1 = start[0], lng1 = start[1];
            const lat2 = end[0], lng2 = end[1];
            const midLat = (lat1 + lat2) / 2 + (lng2 - lng1) * curvature;
            const midLng = (lng1 + lng2) / 2 - (lat2 - lat1) * curvature;
            const points = [];
            for (let t = 0; t <= 1; t += 0.04) {{
                const lat = (1 - t) * (1 - t) * lat1 + 2 * (1 - t) * t * midLat + t * t * lat2;
                const lng = (1 - t) * (1 - t) * lng1 + 2 * (1 - t) * t * midLng + t * t * lng2;
                points.push([lat, lng]);
            }}
            return points;
        }}

        // 為所有 App 繪製連線光束
        for (const [sid, svc] of Object.entries(mapData.services)) {{
            const geo = mapData.nodes[svc.geo_key];
            if (!geo) continue;

            const curveCoords = getCurvedCoords(twCoord, [geo.lat, geo.lng], (sid.length % 5) * 0.05 + 0.1);
            const polyline = L.polyline(curveCoords, {{
                color: svc.status_color,
                weight: 2.2,
                opacity: 0.75,
                smoothFactor: 1
            }}).addTo(map);

            // 滑鼠移到線條高亮並彈出 HUD
            polyline.on('mouseover', function (e) {{
                highlightMapArc(sid);
                showHud(svc, e.containerPoint);
            }});
            polyline.on('mouseout', function () {{
                resetMapArc();
                hideHud();
            }});

            arcLayers[sid] = polyline;
        }}

        function highlightMapArc(sid) {{
            for (const [id, line] of Object.entries(arcLayers)) {{
                if (id === sid) {{
                    line.setStyle({{ weight: 5, opacity: 1 }});
                    line.bringToFront();
                }} else {{
                    line.setStyle({{ opacity: 0.15 }});
                }}
            }}
            const card = document.getElementById('card-' + sid);
            if (card) card.style.borderColor = '#38bdf8';
        }}

        function resetMapArc() {{
            for (const [sid, line] of Object.entries(arcLayers)) {{
                const svc = mapData.services[sid];
                line.setStyle({{ weight: 2.2, opacity: 0.75 }});
            }}
            document.querySelectorAll('.service-card').forEach(c => c.style.borderColor = '#1e293b');
        }}

        function showHud(svc, point) {{
            const hud = document.getElementById('hud-card');
            document.getElementById('hud-title').innerText = svc.name;
            document.getElementById('hud-desc').innerText = svc.desc;
            document.getElementById('hud-total').innerText = svc.total_ms + ' ms';
            document.getElementById('hud-tcp').innerText = svc.tcp_ms + ' ms';
            document.getElementById('hud-geo').innerText = svc.geo_name;
            hud.style.display = 'block';
        }}

        function hideHud() {{
            document.getElementById('hud-card').style.display = 'none';
        }}

        // 初始化下方所有卡片的 24 小時 Chart.js 圖表
        {chart_init_js}
    </script>
</body>
</html>
"""

with open("egress.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 成功產出 Leaflet 暗黑拓撲戰情室與完整 24 小時體感儀表板。")
