import json
import time
import os
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ==============================================================================
# 第一部分：監控標的 (清楚分流：台灣本地快取 vs 國際出口)
# ==============================================================================

TARGET_SERVICES = [
    # --- 台灣本地快取直連組 (CDN Edge) ---
    {
        "id": "threads_ig",
        "name": "Threads & IG 圖片 CDN",
        "host": "scontent.cdninstagram.com",
        "category": "Meta 核心",
        "scope": "local"
    },
    {
        "id": "meta_reels",
        "name": "Instagram Reels",
        "host": "video.xx.fbcdn.net",
        "category": "Meta 核心",
        "scope": "local"
    },
    {
        "id": "fb_static",
        "name": "Facebook 動態圖文",
        "host": "static.xx.fbcdn.net",
        "category": "Meta 核心",
        "scope": "local"
    },
    {
        "id": "youtube_stream",
        "name": "YouTube Shorts / 串流",
        "host": "googlevideo.com",
        "category": "Google 直連",
        "scope": "local"
    },
    {
        "id": "line_obs",
        "name": "LINE 圖片與貼圖",
        "host": "obs.line-scdn.net",
        "category": "通訊社群",
        "scope": "local"
    },
    {
        "id": "netflix_fast",
        "name": "Netflix 邊緣節點",
        "host": "api.fast.com",
        "category": "串流影音",
        "scope": "local"
    },
    # --- 國際海纜出口直連組 (Overseas POP) ---
    {
        "id": "hongguo_drama",
        "name": "紅果短劇",
        "host": "lf-cdn-tos.bytescm.com",
        "category": "爆款短劇",
        "scope": "global"
    },
    {
        "id": "tiktok_gateway",
        "name": "TikTok 資訊流入口",
        "host": "www.tiktok.com",
        "category": "熱門短影音",
        "scope": "global"
    },
    {
        "id": "x_media",
        "name": "X (Twitter) 圖片",
        "host": "pbs.twimg.com",
        "category": "Fastly CDN",
        "scope": "global"
    },
    {
        "id": "github_fastly",
        "name": "GitHub 檔案下載",
        "host": "objects.githubusercontent.com",
        "category": "Fastly 骨幹",
        "scope": "global"
    },
    {
        "id": "discord_media",
        "name": "Discord 媒體傳輸",
        "host": "cdn.discordapp.com",
        "category": "Cloudflare",
        "scope": "global"
    }
]

STATE_FILE = "egress_state.json"
tz = timezone(timedelta(hours=8))
now_dt = datetime.now(tz)
now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
time_short = now_dt.strftime('%H:%M')

# 實體地理經緯度資料庫 (美西向東經度校正為 238.1137，保證向東穿過太平洋直達美西)
KNOWN_GEO_COORDS = {
    "TW": {"name": "台灣台中 (探針起點)", "lat": 24.1477, "lng": 120.6736},
    "TPE": {"name": "台北本地節點 (CDN Edge)", "lat": 25.0330, "lng": 121.5654},
    "TYO": {"name": "日本 (東京 NRT/HND)", "lat": 35.6762, "lng": 139.6503},
    "OSA": {"name": "日本 (大阪 KIX)", "lat": 34.6937, "lng": 135.5023},
    "HKG": {"name": "香港 (HKG POP)", "lat": 22.3193, "lng": 114.1694},
    "SIN": {"name": "新加坡 (SIN POP)", "lat": 1.3521, "lng": 103.8198},
    "US_WEST": {"name": "美國西岸 (SJC/LAX)", "lat": 37.3382, "lng": 238.1137}
}

def get_latency_color(ms, status):
    """延遲健康度燈號"""
    if status != "OK" or ms <= 0:
        return "#f87171"
    if ms < 150:
        return "#34d399"  # 綠色：順暢秒開
    if ms < 350:
        return "#fde047"  # 黃色：稍微延遲
    return "#f87171"      # 紅色：轉圈卡頓

def determine_geo(resolved_ip, headers_str, tcp_ms):
    h_lower = headers_str.lower()
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
    
    if resolved_ip.startswith("199.232.") or resolved_ip.startswith("151.101."):
        return "TYO"
    if resolved_ip.startswith("157.240.") or resolved_ip.startswith("31.13."):
        return "HKG"
    return "TYO"

# ==============================================================================
# 第二部分：Globalping 雙軌探測 (HiNet 原生 vs Google DNS)
# ==============================================================================

def execute_single_probe(host, dns_resolver=None):
    api_url = "https://api.globalping.io/v1/measurements"
    measurement_options = {"protocol": "HTTPS"}
    if dns_resolver:
        measurement_options["resolver"] = dns_resolver

    payload = json.dumps({
        "type": "http",
        "target": host,
        "locations": [{"country": "TW", "asn": 3462}],
        "measurementOptions": measurement_options,
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

def probe_single_stream(service, dns_resolver=None):
    res = execute_single_probe(service["host"], dns_resolver)
    if not res or res["total_ms"] <= 0:
        time.sleep(2)
        res = execute_single_probe(service["host"], dns_resolver)

    if res and res["total_ms"] > 0:
        geo_key = determine_geo(res["resolved_ip"], res["headers"], res["tcp_ms"])
        color = get_latency_color(res["total_ms"], "OK")
        return {
            "status": "OK",
            "total_ms": res["total_ms"],
            "tcp_ms": res["tcp_ms"],
            "resolved_ip": res["resolved_ip"],
            "geo_key": geo_key,
            "geo_name": KNOWN_GEO_COORDS.get(geo_key, {}).get("name", "海外節點"),
            "color": color
        }
    else:
        return {
            "status": "FAIL",
            "total_ms": 0,
            "tcp_ms": 0,
            "resolved_ip": "--",
            "geo_key": "TYO",
            "geo_name": "連線逾時",
            "color": "#f87171"
        }

def probe_service_both_dns(service):
    sid = service["id"]
    hinet_res = probe_single_stream(service, None)
    google_res = probe_single_stream(service, "8.8.8.8")
    return sid, {"hinet": hinet_res, "google": google_res}

# ==============================================================================
# 第三部分：平行發起巡檢
# ==============================================================================

print(f"[{now_str}] 啟動 HiNet 出口體感巡檢 (雙軌 DNS 完整版)...")

results = {}
with ThreadPoolExecutor(max_workers=3) as executor:
    future_to_svc = {executor.submit(probe_service_both_dns, s): s["id"] for s in TARGET_SERVICES}
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
    res = results.get(sid, {
        "hinet": {"status": "FAIL", "total_ms": 0, "tcp_ms": 0, "geo_key": "TYO", "color": "#f87171"},
        "google": {"status": "FAIL", "total_ms": 0, "tcp_ms": 0, "geo_key": "TYO", "color": "#f87171"}
    })
    
    h_data = res["hinet"]
    g_data = res["google"]

    for stream in [h_data, g_data]:
        gk = stream.get("geo_key", "TYO")
        if stream.get("status") == "OK" and gk in KNOWN_GEO_COORDS:
            active_geo_nodes[gk] = KNOWN_GEO_COORDS[gk]

    h_ms = h_data.get("total_ms", 0)
    g_ms = g_data.get("total_ms", 0)
    
    h_win = False
    g_win = False
    if h_data["status"] == "OK" and g_data["status"] == "OK":
        if h_ms <= g_ms:
            h_win = True
        else:
            g_win = True
    elif h_data["status"] == "OK":
        h_win = True
    elif g_data["status"] == "OK":
        g_win = True

    current_snapshot["data"][sid] = {
        "name": s["name"],
        "category": s["category"],
        "scope": s["scope"],
        "hinet": {**h_data, "win": h_win},
        "google": {**g_data, "win": g_win}
    }

history_state.insert(0, current_snapshot)
history_state = history_state[:72]

with open(STATE_FILE, "w", encoding="utf-8") as f:
    json.dump(history_state, f, ensure_ascii=False, indent=2)

# ==============================================================================
# 第五部分：前端 HTML 組裝 (乾淨分層導航 + 順暢度燈號 + 全島放大鏡)
# ==============================================================================

chronological_data = list(reversed(history_state))
chart_labels = json.dumps([item["time_short"] for item in chronological_data])

chart_datasets = {}
for s in TARGET_SERVICES:
    sid = s["id"]
    chart_datasets[sid] = {
        "hinet_total": [item.get("data", {}).get(sid, {}).get("hinet", {}).get("total_ms", 0) for item in chronological_data],
        "hinet_tcp": [item.get("data", {}).get(sid, {}).get("hinet", {}).get("tcp_ms", 0) for item in chronological_data],
        "google_total": [item.get("data", {}).get(sid, {}).get("google", {}).get("total_ms", 0) for item in chronological_data],
        "google_tcp": [item.get("data", {}).get(sid, {}).get("google", {}).get("tcp_ms", 0) for item in chronological_data]
    }

stamp_html = '<span class="pk-stamp">👍 讚</span>'

def render_cards_by_view(view_mode):
    local_cards = ""
    global_cards = ""

    for s in TARGET_SERVICES:
        sid = s["id"]
        curr = current_snapshot["data"].get(sid, {})
        h = curr.get("hinet", {})
        g = curr.get("google", {})

        card_content = ""

        if view_mode == "pk":
            h_total_str = f"{h.get('total_ms', 0)} ms" if h.get("status") == "OK" else "逾時"
            g_total_str = f"{g.get('total_ms', 0)} ms" if g.get("status") == "OK" else "逾時"
            h_stamp = stamp_html if h.get("win") else ""
            g_stamp = stamp_html if g.get("win") else ""

            card_content = f"""
            <div class="pk-container">
                <div class="pk-row {'pk-row-win' if h.get('win') else ''}">
                    <div class="pk-dns-name">HiNet DNS</div>
                    <div class="pk-ms" style="color:{h.get('color')};">
                        {h_total_str} {h_stamp}
                    </div>
                    <div class="pk-node-info">
                        <span>{h.get('geo_name','--')}</span>
                        <span style="color:#94a3b8; font-size:11px;">TCP: {h.get('tcp_ms','--')}ms</span>
                    </div>
                </div>
                <div class="pk-row {'pk-row-win' if g.get('win') else ''}">
                    <div class="pk-dns-name">Google DNS</div>
                    <div class="pk-ms" style="color:{g.get('color')};">
                        {g_total_str} {g_stamp}
                    </div>
                    <div class="pk-node-info">
                        <span>{g.get('geo_name','--')}</span>
                        <span style="color:#94a3b8; font-size:11px;">TCP: {g.get('tcp_ms','--')}ms</span>
                    </div>
                </div>
            </div>
            """
        else:
            target_data = h if view_mode == "hinet" else g
            total_val = target_data.get('total_ms', 0)
            tcp_val = target_data.get('tcp_ms', 0)
            total_str = f"{total_val} ms" if target_data.get("status") == "OK" else "逾時"
            tcp_str = f"{tcp_val} ms" if target_data.get("status") == "OK" else "--"
            health_color = target_data.get('color', '#34d399')

            status_text = "順暢良好" if target_data.get("status") == "OK" and total_val < 150 else ("稍微延遲" if total_val < 350 else "轉圈卡頓")

            card_content = f"""
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                <span style="color:{health_color}; font-size:12px; font-weight:bold;">● {status_text}</span>
                <span style="color:#94a3b8; font-size:11px;">{target_data.get('geo_name','--')}</span>
            </div>
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; background:#070d18; padding:8px 10px; border-radius:6px; margin-bottom:12px;">
                <div>
                    <div style="font-size:11px; color:#94a3b8;">總體感 (Total)</div>
                    <div style="font-size:18px; font-weight:800; color:{health_color};">{total_str}</div>
                </div>
                <div>
                    <div style="font-size:11px; color:#94a3b8;">TCP 握手</div>
                    <div style="font-size:18px; font-weight:800; color:#a855f7;">{tcp_str}</div>
                </div>
            </div>
            """

        card_html = f"""
        <div class="service-card" id="card-{view_mode}-{sid}" onmouseenter="highlightMapArc('{sid}')" onmouseleave="resetMapArc()">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                <span style="font-size:15px; font-weight:800; color:#f8fafc;">{curr['name']}</span>
                <span class="badge">{curr['category']}</span>
            </div>
            {card_content}
            <div style="background:#070d18; padding:6px; border-radius:6px; height:110px;">
                <canvas id="chart-{view_mode}-{sid}"></canvas>
            </div>
        </div>
        """

        if s["scope"] == "local":
            local_cards += card_html
        else:
            global_cards += card_html

    return f"""
    <div class="section-title">🟢 台灣本地快取直連組 (CDN Edge 直連)</div>
    <div class="cards-grid">{local_cards}</div>
    <div class="section-title" style="margin-top:24px;">🌐 國際海纜出口直連組 (Overseas POP 直連)</div>
    <div class="cards-grid">{global_cards}</div>
    """

html_hinet_cards = render_cards_by_view("hinet")
html_google_cards = render_cards_by_view("google")
html_pk_cards = render_cards_by_view("pk")

# 圖表線條顏色依照各自分類的延遲健康度繪製
chart_init_js = ""
for s in TARGET_SERVICES:
    sid = s["id"]
    ds = chart_datasets[sid]
    curr = current_snapshot["data"].get(sid, {})
    h_col = curr.get("hinet", {}).get("color", "#34d399")
    g_col = curr.get("google", {}).get("color", "#34d399")

    # 1. HiNet 專屬圖
    chart_init_js += f"""
    new Chart(document.getElementById('chart-hinet-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels},
            datasets: [
                {{ label: 'Total (ms)', data: {json.dumps(ds['hinet_total'])}, borderColor: '{h_col}', borderWidth: 2, tension: 0.3, pointRadius: 1 }},
                {{ label: 'TCP (ms)', data: {json.dumps(ds['hinet_tcp'])}, borderColor: '#a855f7', borderWidth: 1.5, borderDash: [2, 2], tension: 0.3, pointRadius: 0 }}
            ]
        }},
        options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ ticks: {{ color: '#64748b', maxTicksLimit: 5, font: {{ size: 9 }} }} }}, y: {{ ticks: {{ color: '#64748b', font: {{ size: 9 }} }} }} }} }}
    }});
    """

    # 2. Google 專屬圖
    chart_init_js += f"""
    new Chart(document.getElementById('chart-google-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels},
            datasets: [
                {{ label: 'Total (ms)', data: {json.dumps(ds['google_total'])}, borderColor: '{g_col}', borderWidth: 2, tension: 0.3, pointRadius: 1 }},
                {{ label: 'TCP (ms)', data: {json.dumps(ds['google_tcp'])}, borderColor: '#a855f7', borderWidth: 1.5, borderDash: [2, 2], tension: 0.3, pointRadius: 0 }}
            ]
        }},
        options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ ticks: {{ color: '#64748b', maxTicksLimit: 5, font: {{ size: 9 }} }} }}, y: {{ ticks: {{ color: '#64748b', font: {{ size: 9 }} }} }} }} }}
    }});
    """

    # 3. 綜合對照圖 (HiNet 實線 vs Google 虛線)
    chart_init_js += f"""
    new Chart(document.getElementById('chart-pk-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels},
            datasets: [
                {{ label: 'HiNet Total', data: {json.dumps(ds['hinet_total'])}, borderColor: '{h_col}', borderWidth: 2, tension: 0.3, pointRadius: 1.2 }},
                {{ label: 'Google Total', data: {json.dumps(ds['google_total'])}, borderColor: '{g_col}', borderWidth: 2, borderDash: [2, 2], tension: 0.3, pointRadius: 1 }}
            ]
        }},
        options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ ticks: {{ color: '#64748b', maxTicksLimit: 5, font: {{ size: 9 }} }} }}, y: {{ ticks: {{ color: '#64748b', font: {{ size: 9 }} }} }} }} }}
    }});
    """

map_payload = {
    "nodes": active_geo_nodes,
    "services": current_snapshot["data"]
}

html_content = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HiNet AS3462 國際出口體感戰情室</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:#070d18; color:#e2e8f0; margin:0; padding:20px 24px; font-size:14px; }}
        .container {{ max-width: 1440px; margin: 0 auto; }}

        /* 頂部第 1 層：系統導航 */
        .system-nav {{ display: flex; gap: 8px; margin-bottom: 16px; border-bottom: 2px solid #1e293b; padding-bottom: 10px; }}
        .nav-btn {{ padding: 8px 18px; font-size: 13px; font-weight: bold; border-radius: 6px; text-decoration: none; color: #94a3b8; background: #0f172a; border: 1px solid #1e293b; }}
        .nav-btn:hover {{ background: #1e293b; color: #f8fafc; }}
        .nav-active {{ background: #0284c7; color: #ffffff; border: none; }}

        /* 標題區 */
        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }}

        /* 標題正下方第 2 層：DNS 模式分頁切換 */
        .mode-tabs {{ display: flex; gap: 8px; margin-bottom: 16px; }}
        .mode-btn {{ padding: 6px 16px; font-size: 13px; font-weight: bold; border-radius: 6px; color: #94a3b8; background: #0b1324; border: 1px solid #1e293b; cursor: pointer; transition: all 0.2s; }}
        .mode-btn:hover {{ background: #1e293b; color: #f8fafc; }}
        .mode-active {{ background: #1e293b; color: #38bdf8; border-color: #38bdf8; }}

        /* 子母地圖視窗容器 */
        .map-wrapper {{ position:relative; background:#070d18; border:1px solid #1e293b; border-radius:12px; height:480px; margin-bottom:24px; overflow:hidden; }}
        #map-global {{ width:100%; height:100%; background:#070d18; }}

        /* 右上角畫中畫：全台灣完整放大鏡 (擴大尺寸、全島入鏡、無註記條) */
        #taiwan-submap-wrapper {{
            position: absolute;
            top: 14px;
            right: 14px;
            width: 280px;
            height: 360px;
            background: rgba(11, 19, 36, 0.94);
            border: 1.5px solid #38bdf8;
            border-radius: 10px;
            box-shadow: 0 8px 24px rgba(0,0,0,0.8), 0 0 14px rgba(56, 189, 248, 0.25);
            z-index: 1000;
            overflow: hidden;
            backdrop-filter: blur(8px);
        }}
        #map-local {{ width: 100%; height: 100%; }}

        /* 懸浮 HUD 資訊卡 */
        #hud-card {{ position:absolute; bottom:20px; left:20px; width:280px; background:rgba(15, 23, 42, 0.95); border:1px solid #38bdf8; border-radius:8px; padding:12px; backdrop-filter:blur(8px); display:none; z-index:1000; pointer-events:none; }}

        /* 分類標題 */
        .section-title {{ font-size: 15px; font-weight: 800; color: #f8fafc; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }}

        /* 下方卡片網格 */
        .cards-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap: 14px; }}
        .service-card {{ background:linear-gradient(145deg, #0f172a, #131d31); border-radius:10px; padding:14px; border:1px solid #1e293b; transition:all 0.2s; }}
        .service-card:hover {{ border-color:#38bdf8; transform:translateY(-2px); }}
        .badge {{ background:#1e293b; color:#94a3b8; font-size:11px; padding:2px 8px; border-radius:4px; font-weight:bold; }}

        /* PK 對照表排版 */
        .pk-container {{ display: flex; flex-direction: column; gap: 6px; margin-bottom: 10px; }}
        .pk-row {{ display: flex; justify-content: space-between; align-items: center; background: #070d18; padding: 7px 9px; border-radius: 6px; border: 1px solid #1e293b; }}
        .pk-row-win {{ border-color: rgba(239, 68, 68, 0.5); background: rgba(239, 68, 68, 0.06); }}
        .pk-dns-name {{ font-weight: bold; font-size: 12px; width: 85px; color: #e2e8f0; }}
        .pk-ms {{ font-weight: 800; font-size: 15px; display: flex; align-items: center; gap: 6px; flex: 1; }}
        .pk-node-info {{ text-align: right; font-size: 11px; color: #cbd5e1; display: flex; flex-direction: column; }}

        /* 紅色大讚章樣式 */
        .pk-stamp {{
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: linear-gradient(135deg, #ef4444, #b91c1c);
            color: #ffffff;
            font-size: 11px;
            font-weight: 900;
            padding: 2px 7px;
            border-radius: 4px;
            box-shadow: 0 0 10px rgba(239, 68, 68, 0.8);
            letter-spacing: 0.5px;
            transform: rotate(-3deg);
        }}

        /* 頁面切換控制 */
        .view-panel {{ display: none; }}
        .view-active {{ display: block; }}

        /* 海纜公告區塊 */
        .cable-notice-panel {{ background:#0f172a; border:1px solid #334155; border-left:4px solid #f59e0b; border-radius:8px; padding:16px 20px; margin-top:24px; }}
        .notice-row {{ display:grid; grid-template-columns:120px 180px 180px 1fr; gap:12px; font-size:13px; padding:8px 0; border-bottom:1px solid #1e293b; }}
        .notice-row:last-child {{ border-bottom:none; }}

        @media (max-width: 768px) {{
            body {{ padding:12px; }}
            .map-wrapper {{ height:380px; }}
            #taiwan-submap-wrapper {{ width: 150px; height: 190px; top: 10px; right: 10px; }}
            .notice-row {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- 頂部第 1 層：系統主導航 -->
        <div class="system-nav">
            <a href="index.html" class="nav-btn">🌐 IPv4 核心巡檢</a>
            <a href="index.html" class="nav-btn">🚀 IPv6 核心巡檢</a>
            <a href="egress.html" class="nav-btn nav-active">⚡ 海外出口動態拓撲戰情室</a>
        </div>

        <!-- 標題資訊區 -->
        <div class="header">
            <div>
                <h1 style="margin:0 0 6px 0; font-size:22px; color:#f8fafc; font-weight:800;">
                    ⚡ 中華電信 (HiNet AS3462) 國際出口動態拓撲戰情室
                </h1>
                <div style="color:#94a3b8; font-size:13px;">
                    📍 <strong>探針來源：</strong>台灣本地 HiNet 寬頻節點 (AS3462 台中實體用戶端)<br>
                    ⏱ <strong>最後量測時間：</strong><span style="color:#38bdf8; font-weight:bold;">{now_str} (UTC+8)</span>
                </div>
            </div>
            <div>
                <button onclick="window.location.reload()" style="background:#0284c7; color:#fff; border:none; border-radius:6px; padding:8px 16px; cursor:pointer; font-weight:bold; font-size:13px;">
                    🔄 手動刷新
                </button>
            </div>
        </div>

        <!-- 標題正下方第 2 層：DNS 模式分頁切換 -->
        <div class="mode-tabs">
            <button class="mode-btn mode-active" onclick="switchView('hinet', this)">HiNet 預設 DNS</button>
            <button class="mode-btn" onclick="switchView('google', this)">Google DNS (8.8.8.8)</button>
            <button class="mode-btn" onclick="switchView('pk', this)">綜合效能對照</button>
        </div>

        <!-- 第一屏核心：子母地圖 (環太平洋全景 + 右上角全台灣島內放大鏡) -->
        <div class="map-wrapper">
            <div id="map-global"></div>

            <!-- 右上角台灣島內放大鏡 (無文字條、完整入鏡) -->
            <div id="taiwan-submap-wrapper">
                <div id="map-local"></div>
            </div>

            <!-- 懸浮 HUD 資訊卡 -->
            <div id="hud-card">
                <div id="hud-title" style="font-size:14px; font-weight:bold; color:#f8fafc; margin-bottom:6px;">--</div>
                <div style="font-size:12px; color:#cbd5e1; margin-bottom:4px;">HiNet DNS: <strong id="hud-h-total">--</strong> (<span id="hud-h-geo">--</span>)</div>
                <div style="font-size:12px; color:#cbd5e1;">Google DNS: <strong id="hud-g-total">--</strong> (<span id="hud-g-geo">--</span>)</div>
            </div>
        </div>

        <!-- 第二屏主體：三大獨立視圖 -->
        <div id="view-hinet" class="view-panel view-active">
            {html_hinet_cards}
        </div>

        <div id="view-google" class="view-panel">
            {html_google_cards}
        </div>

        <div id="view-pk" class="view-panel">
            {html_pk_cards}
        </div>

        <!-- 最底部：HiNet 官方海纜障礙與維修公告 -->
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
        // 1. 初始化國際海纜主地圖 (環太平洋中心 175°E)
        const mapGlobal = L.map('map-global', {{
            center: [28.0, 175.0],
            zoom: 3,
            zoomControl: false,
            attributionControl: false
        }});

        L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{{z}}/{{y}}/{{x}}', {{
            maxZoom: 7,
            minZoom: 2
        }}).addTo(mapGlobal);

        // 2. 初始化台灣島內放大鏡 (中心 23.7°N, 120.95°E，完整覆蓋基隆到屏東)
        const mapLocal = L.map('map-local', {{
            center: [23.7, 120.95],
            zoom: 6.8,
            zoomControl: false,
            attributionControl: false,
            dragging: false,
            scrollWheelZoom: false,
            doubleClickZoom: false
        }});

        L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{{z}}/{{y}}/{{x}}', {{
            maxZoom: 9,
            minZoom: 6
        }}).addTo(mapLocal);

        const mapData = {json.dumps(map_payload)};
        const twCoord = [mapData.nodes.TW.lat, mapData.nodes.TW.lng];
        const tpeCoord = [mapData.nodes.TPE.lat, mapData.nodes.TPE.lng];

        L.circleMarker(twCoord, {{ radius: 6, fillColor: '#f43f5e', color: '#fff', weight: 2, fillOpacity: 1 }}).addTo(mapGlobal);
        L.circleMarker(twCoord, {{ radius: 5, fillColor: '#f43f5e', color: '#fff', weight: 1.5, fillOpacity: 1 }}).addTo(mapLocal);
        L.circleMarker(tpeCoord, {{ radius: 5, fillColor: '#38bdf8', color: '#fff', weight: 1.5, fillOpacity: 1 }}).addTo(mapLocal);

        for (const [key, node] of Object.entries(mapData.nodes)) {{
            if (key !== "TW" && key !== "TPE") {{
                L.circleMarker([node.lat, node.lng], {{
                    radius: 5,
                    fillColor: '#38bdf8',
                    color: '#070d18',
                    weight: 1.5,
                    fillOpacity: 0.9
                }}).addTo(mapGlobal);
            }}
        }}

        function getCurvedCoords(start, end, curvature) {{
            const lat1 = start[0], lng1 = start[1];
            const lat2 = end[0], lng2 = end[1];
            const midLat = (lat1 + lat2) / 2 + (lng2 - lng1) * curvature;
            const midLng = (lng1 + lng2) / 2 - (lat2 - lat1) * curvature;
            const points = [];
            for (let t = 0; t <= 1; t += 0.03) {{
                const lat = (1 - t) * (1 - t) * lat1 + 2 * (1 - t) * t * midLat + t * t * lat2;
                const lng = (1 - t) * (1 - t) * lng1 + 2 * (1 - t) * t * midLng + t * t * lng2;
                points.push([lat, lng]);
            }}
            return points;
        }}

        const arcLayers = {{}};
        const activePulses = [];
        const destCounts = {{}};

        for (const [sid, svc] of Object.entries(mapData.services)) {{
            const h = svc.hinet;
            const geo = mapData.nodes[h.geo_key];
            if (!geo) continue;

            const isLocal = (h.geo_key === "TPE");
            const targetMap = isLocal ? mapLocal : mapGlobal;

            if (!destCounts[h.geo_key]) destCounts[h.geo_key] = 0;
            destCounts[h.geo_key]++;
            const count = destCounts[h.geo_key];
            const sign = (count % 2 === 1) ? 1 : -1;
            const curvature = isLocal ? (sign * 0.15) : (sign * (0.08 + Math.floor((count - 1) / 2) * 0.12));

            const curveCoords = getCurvedCoords(twCoord, [geo.lat, geo.lng], curvature);
            const color = h.color;

            const polyline = L.polyline(curveCoords, {{
                color: color,
                weight: isLocal ? 2.2 : 3,
                opacity: 0.75,
                smoothFactor: 1
            }}).addTo(targetMap);

            const fMarker = L.circleMarker(curveCoords[0], {{
                radius: isLocal ? 2.5 : 3,
                fillColor: '#ffffff',
                color: color,
                weight: 1,
                fillOpacity: 1
            }}).addTo(targetMap);

            const bMarker = L.circleMarker(curveCoords[curveCoords.length - 1], {{
                radius: isLocal ? 3 : 3.5,
                fillColor: color,
                color: '#ffffff',
                weight: 1,
                fillOpacity: 1
            }}).addTo(targetMap);

            activePulses.push({{
                coords: curveCoords,
                fMarker: fMarker,
                bMarker: bMarker,
                offset: (count * 6) % curveCoords.length
            }});

            polyline.on('mouseover', function () {{
                highlightMapArc(sid);
                showHud(svc);
            }});
            polyline.on('mouseout', function () {{
                resetMapArc();
                hideHud();
            }});

            arcLayers[sid] = polyline;
        }}

        let animStep = 0;
        setInterval(() => {{
            animStep++;
            activePulses.forEach(p => {{
                const len = p.coords.length;
                const fIdx = (animStep + p.offset) % len;
                p.fMarker.setLatLng(p.coords[fIdx]);

                const bIdx = (len - 1) - ((animStep + p.offset) % len);
                p.bMarker.setLatLng(p.coords[bIdx]);
            }});
        }}, 60);

        function highlightMapArc(sid) {{
            for (const [id, line] of Object.entries(arcLayers)) {{
                if (id === sid) {{
                    line.setStyle({{ weight: 6, opacity: 1 }});
                    line.bringToFront();
                }} else {{
                    line.setStyle({{ opacity: 0.12 }});
                }}
            }}
        }}

        function resetMapArc() {{
            for (const [sid, line] of Object.entries(arcLayers)) {{
                line.setStyle({{ weight: 2.8, opacity: 0.75 }});
            }}
        }}

        function showHud(svc) {{
            const hud = document.getElementById('hud-card');
            document.getElementById('hud-title').innerText = svc.name;
            document.getElementById('hud-h-total').innerText = svc.hinet.total_ms + ' ms';
            document.getElementById('hud-h-geo').innerText = svc.hinet.geo_name;
            document.getElementById('hud-g-total').innerText = svc.google.total_ms + ' ms';
            document.getElementById('hud-g-geo').innerText = svc.google.geo_name;
            hud.style.display = 'block';
        }}

        function hideHud() {{
            document.getElementById('hud-card').style.display = 'none';
        }}

        function switchView(viewName, btn) {{
            document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('mode-active'));
            btn.classList.add('mode-active');

            document.querySelectorAll('.view-panel').forEach(p => p.classList.remove('view-active'));
            document.getElementById('view-' + viewName).classList.add('view-active');
        }}

        {chart_init_js}
    </script>
</body>
</html>
"""

with open("egress.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 成功產出子母戰情地圖、乾淨分層導航與體感燈號儀表板。")
