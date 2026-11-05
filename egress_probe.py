import json
import time
import os
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ==============================================================================
# 第一部分：監控標的 (修正 FB 影音端點，依真實業務屬性分組)
# ==============================================================================

TARGET_SERVICES = [
    # --- 區塊 A：主流串流與社群平台 (邊緣解析觀測) ---
    {
        "id": "threads_ig",
        "name": "Threads & IG 圖片 CDN",
        "host": "scontent.cdninstagram.com",
        "group": "edge_services"
    },
    {
        "id": "meta_reels",
        "name": "Instagram Reels 短影音",
        "host": "video.xx.fbcdn.net",
        "group": "edge_services"
    },
    {
        "id": "fb_static",
        "name": "Facebook 塗鴉牆圖文",
        "host": "static.xx.fbcdn.net",
        "group": "edge_services"
    },
    {
        "id": "fb_live",
        "name": "Facebook 直播與動態影音",
        "host": "scontent.xx.fbcdn.net",  # 修正：改用開放 Edge 探測之 Meta 影音節點
        "group": "edge_services"
    },
    {
        "id": "youtube_stream",
        "name": "YouTube 影音串流 / Shorts",
        "host": "googlevideo.com",
        "group": "edge_services"
    },
    {
        "id": "line_obs",
        "name": "LINE 圖片與貼圖",
        "host": "obs.line-scdn.net",
        "group": "edge_services"
    },
    {
        "id": "netflix_fast",
        "name": "Netflix 邊緣節點",
        "host": "api.fast.com",
        "group": "edge_services"
    },
    # --- 區塊 B：跨境平台與海外骨幹 ---
    {
        "id": "hongguo_drama",
        "name": "紅果短劇",
        "host": "lf-cdn-tos.bytescm.com",
        "group": "cross_border"
    },
    {
        "id": "tiktok_gateway",
        "name": "TikTok 資訊流入口",
        "host": "www.tiktok.com",
        "group": "cross_border"
    },
    {
        "id": "x_media",
        "name": "X (Twitter) 圖片",
        "host": "pbs.twimg.com",
        "group": "cross_border"
    },
    {
        "id": "github_fastly",
        "name": "GitHub 檔案下載",
        "host": "objects.githubusercontent.com",
        "group": "cross_border"
    },
    {
        "id": "discord_media",
        "name": "Discord 媒體傳輸",
        "host": "cdn.discordapp.com",
        "group": "cross_border"
    }
]

STATE_FILE = "egress_state.json"
tz = timezone(timedelta(hours=8))
now_dt = datetime.now(tz)
now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
time_short = now_dt.strftime('%H:%M')

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
    if status != "OK" or ms <= 0:
        return "#f87171"
    if ms < 150:
        return "#34d399"
    if ms < 350:
        return "#fde047"
    return "#f87171"

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
# 第二部分：Globalping 雙軌探測
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

print(f"[{now_str}] 啟動 HiNet 出口與 CDN 路由巡檢...")

results = {}
with ThreadPoolExecutor(max_workers=3) as executor:
    future_to_svc = {executor.submit(probe_service_both_dns, s): s["id"] for s in TARGET_SERVICES}
    for future in as_completed(future_to_svc):
        svc_id, res_data = future.result()
        results[svc_id] = res_data

# ==============================================================================
# 第四部分：狀態處理、真・路由異動偵測與 24 小時歷史保存
# ==============================================================================

history_state = []
if os.path.exists(STATE_FILE):
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            history_state = json.load(f)
    except Exception:
        history_state = []

def detect_route_drift(sid, dns_type, current_geo_key, current_time_str):
    """
    真・路由異動判定：
    只有當該服務在歷史紀錄中曾穩定落地於 TPE (台北本地)，而當前卻漂移至海外 (非 TPE) 時，才判定為漂移異動。
    若該服務原本即常態在境外 (如 Netflix 始終在 US_WEST)，則不觸發異動告警。
    """
    if current_geo_key == "TPE" or current_geo_key == "FAIL":
        return None

    had_tpe_baseline = False
    earliest_drift_time = current_time_str

    for snapshot in history_state:
        svc_data = snapshot.get("data", {}).get(sid, {}).get(dns_type, {})
        geo = svc_data.get("geo_key")
        if geo == "TPE":
            had_tpe_baseline = True
            break
        elif geo and geo != "TPE":
            earliest_drift_time = snapshot.get("time_short", earliest_drift_time)

    if had_tpe_baseline:
        return earliest_drift_time
    return None

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

    h_drift = detect_route_drift(sid, "hinet", h_data.get("geo_key"), time_short)
    g_drift = detect_route_drift(sid, "google", g_data.get("geo_key"), time_short)

    current_snapshot["data"][sid] = {
        "name": s["name"],
        "group": s["group"],
        "hinet": {**h_data, "win": h_win, "drift_since": h_drift},
        "google": {**g_data, "win": g_win, "drift_since": g_drift}
    }

history_state.insert(0, current_snapshot)
history_state = history_state[:72]

with open(STATE_FILE, "w", encoding="utf-8") as f:
    json.dump(history_state, f, ensure_ascii=False, indent=2)

# ==============================================================================
# 第五部分：前端 HTML 組裝
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
    edge_cards = ""
    cross_cards = ""

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

            h_drift_badge = f'<div class="drift-badge">⚠️ 路由漂移：台北 ➔ {h.get("geo_name")} (自 {h.get("drift_since")} 至今)</div>' if h.get("drift_since") else ""
            g_drift_badge = f'<div class="drift-badge">⚠️ 路由漂移：台北 ➔ {g.get("geo_name")} (自 {g.get("drift_since")} 至今)</div>' if g.get("drift_since") else ""

            card_content = f"""
            <div class="pk-container">
                <div class="pk-row {'pk-row-win' if h.get('win') else ''}">
                    <div class="pk-dns-name" style="color:#38bdf8;">HiNet DNS</div>
                    <div class="pk-ms" style="color:{h.get('color')};">
                        {h_total_str} {h_stamp}
                    </div>
                    <div class="pk-node-info">
                        <span>{h.get('geo_name','--')}</span>
                        <span style="color:#94a3b8; font-size:11px;">TCP: {h.get('tcp_ms','--')}ms</span>
                    </div>
                </div>
                {h_drift_badge}
                <div class="pk-row {'pk-row-win' if g.get('win') else ''}">
                    <div class="pk-dns-name" style="color:#fb923c;">Google DNS</div>
                    <div class="pk-ms" style="color:{g.get('color')};">
                        {g_total_str} {g_stamp}
                    </div>
                    <div class="pk-node-info">
                        <span>{g.get('geo_name','--')}</span>
                        <span style="color:#94a3b8; font-size:11px;">TCP: {g.get('tcp_ms','--')}ms</span>
                    </div>
                </div>
                {g_drift_badge}
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
            drift_badge = f'<div class="drift-badge">⚠️ 路由漂移：台北 ➔ {target_data.get("geo_name")} (自 {target_data.get("drift_since")} 至今)</div>' if target_data.get("drift_since") else ""

            card_content = f"""
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                <span style="color:{health_color}; font-size:12px; font-weight:bold;">● {status_text}</span>
                <span style="color:#94a3b8; font-size:11px;">{target_data.get('geo_name','--')}</span>
            </div>
            {drift_badge}
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
            </div>
            {card_content}
            <div style="background:#070d18; padding:6px; border-radius:6px; height:110px;">
                <canvas id="chart-{view_mode}-{sid}"></canvas>
            </div>
        </div>
        """

        if s["group"] == "edge_services":
            edge_cards += card_html
        else:
            cross_cards += card_html

    return f"""
    <div class="section-title">⚡ 主流串流與社群平台 (邊緣解析觀測)</div>
    <div class="cards-grid">{edge_cards}</div>
    <div class="section-title" style="margin-top:24px;">🌐 跨境平台與海外骨幹</div>
    <div class="cards-grid">{cross_cards}</div>
    """

html_hinet_cards = render_cards_by_view("hinet")
html_google_cards = render_cards_by_view("google")
html_pk_cards = render_cards_by_view("pk")

# 圖表渲染配置 (曲線自適應延遲燈號顏色，PK 視圖維持經典藍橘)
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

    # 3. 綜合對照圖 (HiNet 藍色實線 vs Google 橘色虛線)
    chart_init_js += f"""
    new Chart(document.getElementById('chart-pk-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels},
            datasets: [
                {{ label: 'HiNet Total', data: {json.dumps(ds['hinet_total'])}, borderColor: '#38bdf8', borderWidth: 2, tension: 0.3, pointRadius: 1.2 }},
                {{ label: 'Google Total', data: {json.dumps(ds['google_total'])}, borderColor: '#fb923c', borderWidth: 2, borderDash: [3, 3], tension: 0.3, pointRadius: 1 }}
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
    <title>HiNet AS3462 國際出口與 CDN 路由動態拓撲</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:#070d18; color:#e2e8f0; margin:0; padding:20px 24px; font-size:14px; }}
        .container {{ max-width: 1440px; margin: 0 auto; }}

        /* 頂部第 1 層：系統主導航 */
        .system-nav {{ display: flex; gap: 8px; margin-bottom: 16px; border-bottom: 2px solid #1e293b; padding-bottom: 10px; }}
        .nav-btn {{ padding: 8px 18px; font-size: 13px; font-weight: bold; border-radius: 6px; text-decoration: none; color: #94a3b8; background: #0f172a; border: 1px solid #1e293b; }}
        .nav-btn:hover {{ background: #1e293b; color: #f8fafc; }}
        .nav-active {{ background: #0284c7; color: #ffffff; border: none; }}

        /* 標題區 (去戰情室中二化) */
        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }}

        /* 子母地圖視窗容器 */
        .map-wrapper {{ position:relative; background:#070d18; border:1px solid #1e293b; border-radius:12px; height:480px; margin-bottom:16px; overflow:hidden; }}
        #map-global {{ width:100%; height:100%; background:#070d18; }}

        /* 右上角畫中畫：全台灣完整放大鏡 (280x360px、全島入鏡、無任何白字條) */
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

        /* 地圖正下方：DNS 模式分頁切換 */
        .mode-tabs {{ display: flex; gap: 8px; margin-bottom: 20px; }}
        .mode-btn {{ padding: 7px 18px; font-size: 13px; font-weight: bold; border-radius: 6px; color: #94a3b8; background: #0b1324; border: 1px solid #1e293b; cursor: pointer; transition: all 0.2s; }}
        .mode-btn:hover {{ background: #1e293b; color: #f8fafc; }}
        .mode-active {{ background: #1e293b; color: #38bdf8; border-color: #38bdf8; }}

        /* 分類標題 */
        .section-title {{ font-size: 15px; font-weight: 800; color: #f8fafc; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }}

        /* 下方卡片網格 */
        .cards-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap: 14px; }}
        .service-card {{ background:linear-gradient(145deg, #0f172a, #131d31); border-radius:10px; padding:14px; border:1px solid #1e293b; transition:all 0.2s; }}
        .service-card:hover {{ border-color:#38bdf8; transform:translateY(-2px); }}

        /* 真・路由漂移告警標籤 */
        .drift-badge {{
            background: rgba(245, 158, 11, 0.15);
            border: 1px solid #f59e0b;
            color: #fbbf24;
            font-size: 11px;
            font-weight: bold;
            padding: 3px 8px;
            border-radius: 4px;
            margin-bottom: 8px;
            display: inline-block;
        }}

        /* PK 對照表排版 */
        .pk-container {{ display: flex; flex-direction: column; gap: 6px; margin-bottom: 10px; }}
        .pk-row {{ display: flex; justify-content: space-between; align-items: center; background: #070d18; padding: 7px 9px; border-radius: 6px; border: 1px solid #1e293b; }}
        .pk-row-win {{ border-color: rgba(239, 68, 68, 0.5); background: rgba(239, 68, 68, 0.06); }}
        .pk-dns-name {{ font-weight: bold; font-size: 12px; width: 85px; }}
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

        /* 海纜公告區塊 (包含實際修復/排程進度) */
        .cable-notice-panel {{ background:#0f172a; border:1px solid #1e293b; border-radius:10px; padding:16px 20px; margin-top:28px; }}
        .cable-table {{ width:100%; border-collapse:collapse; margin-top:10px; }}
        .cable-table th {{ text-align:left; color:#94a3b8; font-size:12px; padding:8px 10px; border-bottom:1px solid #1e293b; }}
        .cable-table td {{ padding:12px 10px; border-bottom:1px solid #1e293b; font-size:13px; vertical-align:top; }}
        .cable-badge-down {{ background:rgba(239, 68, 68, 0.2); color:#f87171; border:1px solid #ef4444; font-size:11px; font-weight:bold; padding:2px 6px; border-radius:4px; }}
        .cable-badge-warn {{ background:rgba(251, 146, 60, 0.2); color:#fb923c; border:1px solid #fb923c; font-size:11px; font-weight:bold; padding:2px 6px; border-radius:4px; }}
        .cable-badge-fixed {{ background:rgba(52, 211, 153, 0.2); color:#34d399; border:1px solid #34d399; font-size:11px; font-weight:bold; padding:2px 6px; border-radius:4px; }}

        @media (max-width: 768px) {{
            body {{ padding:12px; }}
            .map-wrapper {{ height:380px; }}
            #taiwan-submap-wrapper {{ width: 150px; height: 190px; top: 10px; right: 10px; }}
            .cable-table {{ display:block; overflow-x:auto; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- 頂部第 1 層：系統主導航 -->
        <div class="system-nav">
            <a href="index.html" class="nav-btn">🌐 IPv4 核心巡檢</a>
            <a href="index.html" class="nav-btn">🚀 IPv6 核心巡檢</a>
            <a href="egress.html" class="nav-btn nav-active">⚡ 海外出口動態拓撲</a>
        </div>

        <!-- 標題資訊區 -->
        <div class="header">
            <div>
                <h1 style="margin:0 0 6px 0; font-size:22px; color:#f8fafc; font-weight:800;">
                    HiNet (AS3462) 國際出口與 CDN 路由動態拓撲
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

        <!-- 第一屏核心：子母地圖 (全面鎖定滾輪縮放) -->
        <div class="map-wrapper">
            <div id="map-global"></div>

            <div id="taiwan-submap-wrapper">
                <div id="map-local"></div>
            </div>

            <div id="hud-card">
                <div id="hud-title" style="font-size:14px; font-weight:bold; color:#f8fafc; margin-bottom:6px;">--</div>
                <div style="font-size:12px; color:#cbd5e1; margin-bottom:4px;">HiNet DNS: <strong id="hud-h-total">--</strong> (<span id="hud-h-geo">--</span>)</div>
                <div style="font-size:12px; color:#cbd5e1;">Google DNS: <strong id="hud-g-total">--</strong> (<span id="hud-g-geo">--</span>)</div>
            </div>
        </div>

        <!-- 地圖正下方：DNS 模式分頁切換 -->
        <div class="mode-tabs">
            <button class="mode-btn mode-active" onclick="switchView('hinet', this)">HiNet 預設 DNS</button>
            <button class="mode-btn" onclick="switchView('google', this)">Google DNS (8.8.8.8)</button>
            <button class="mode-btn" onclick="switchView('pk', this)">綜合效能對照</button>
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

        <!-- 最底部：真實國際海纜障礙與維修公告專區 (融合數發部通報與 HiNet 維運進度) -->
        <div class="cable-notice-panel">
            <div style="font-size:16px; font-weight:bold; color:#f8fafc; margin-bottom:8px; display:flex; align-items:center; gap:8px;">
                ⚠️ 台灣周邊國際海纜即時障礙通報 (資料來源：數位發展部海纜通報 & 中華電信網路維運公告)
            </div>
            <table class="cable-table">
                <thead>
                    <tr>
                        <th style="width:180px;">海纜系統 / 狀態</th>
                        <th style="width:110px;">障礙天數</th>
                        <th>受損情資與訊務疏轉說明</th>
                        <th style="width:140px;">預計修復 (ETR)</th>
                        <th style="width:160px;">實際修復 / 當前進度</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>
                            <strong style="color:#f8fafc;">SJC2 香港方向</strong><br>
                            <span class="cable-badge-down">斷線</span>
                        </td>
                        <td style="font-size:18px; font-weight:800; color:#f8fafc;">21 天</td>
                        <td>
                            <strong>中華電信公告：</strong>10 月 16 日芯線障礙，訊務已於第一時間疏轉至其他國際海纜；往香港尖峰時段可能有輕微延遲。<br>
                            <span style="color:#94a3b8; font-size:12px;">數位發展部：距香港春坎角約 109 公里處斷纜。</span>
                        </td>
                        <td style="color:#cbd5e1;">2026/12/30</td>
                        <td style="color:#38bdf8;">搶修中 (維修船排程)</td>
                    </tr>
                    <tr>
                        <td>
                            <strong style="color:#f8fafc;">台馬二號東引方向</strong><br>
                            <span class="cable-badge-down">斷線</span>
                        </td>
                        <td style="font-size:18px; font-weight:800; color:#f8fafc;">31 天</td>
                        <td>
                            <strong>中華電信公告：</strong>10 月 6 日芯線障礙，訊務已第一時間疏轉至台馬第三海纜與微波備援。<br>
                            <span style="color:#94a3b8; font-size:12px;">數位發展部：距馬祖東引約 45.3 公里處斷纜。</span>
                        </td>
                        <td style="color:#cbd5e1;">2026/11/30</td>
                        <td style="color:#38bdf8;">搶修中 (備援微波運作)</td>
                    </tr>
                    <tr>
                        <td>
                            <strong style="color:#f8fafc;">TPE 淡水沙崙出海段</strong><br>
                            <span class="cable-badge-down">斷線</span>
                        </td>
                        <td style="font-size:18px; font-weight:800; color:#f8fafc;">34 天</td>
                        <td>
                            <strong>數位發展部通報：</strong>距淡水沙崙約 1.6 公里處斷纜。<br>
                            <span style="color:#94a3b8; font-size:12px;">中華電信公告：芯線障礙，訊務已疏轉至其他國際海纜，跨太平洋直達美西容量維持調度。</span>
                        </td>
                        <td style="color:#cbd5e1;">2026/11/30</td>
                        <td style="color:#38bdf8;">搶修中 (近岸搶修)</td>
                    </tr>
                    <tr>
                        <td>
                            <strong style="color:#f8fafc;">EAC1 香港方向</strong><br>
                            <span class="cable-badge-warn">部分斷線</span>
                        </td>
                        <td style="font-size:18px; font-weight:800; color:#f8fafc;">71 天</td>
                        <td>
                            <strong>數位發展部通報：</strong>距新北八里約 567 公里處發生部分芯線受損障礙，已切換至備援路由運行。
                        </td>
                        <td style="color:#cbd5e1;">2026/11/15</td>
                        <td style="color:#fb923c;">調度運行中</td>
                    </tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        // 1. 初始化國際海纜主地圖 (鎖定滾輪縮放)
        const mapGlobal = L.map('map-global', {{
            center: [28.0, 175.0],
            zoom: 3,
            zoomControl: false,
            scrollWheelZoom: false,
            attributionControl: false
        }});

        L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{{z}}/{{y}}/{{x}}', {{
            maxZoom: 7,
            minZoom: 2
        }}).addTo(mapGlobal);

        // 2. 初始化台灣全島放大鏡 (鎖定縮放與拖曳)
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

        let currentActiveView = 'hinet';
        const arcLayers = {{}};
        const activePulses = [];
        let arcCount = 0;

        function createArc(sid, dnsType, geoKey, color, isDashed) {{
            const geo = mapData.nodes[geoKey];
            if (!geo) return;

            const isLocal = (geoKey === "TPE");
            const targetMap = isLocal ? mapLocal : mapGlobal;

            arcCount++;
            const sign = (arcCount % 2 === 1) ? 1 : -1;
            const curvature = isLocal ? (sign * 0.15) : (sign * (0.08 + Math.floor((arcCount - 1) / 2) * 0.12));
            const curveCoords = getCurvedCoords(twCoord, [geo.lat, geo.lng], curvature);

            const polyline = L.polyline(curveCoords, {{
                color: color,
                weight: isLocal ? 2.2 : 3,
                opacity: 0.75,
                dashArray: isDashed ? '4, 4' : null,
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
                offset: (arcCount * 6) % curveCoords.length,
                dnsType: dnsType
            }});

            polyline.on('mouseover', function () {{
                highlightMapArc(sid);
                showHud(mapData.services[sid]);
            }});
            polyline.on('mouseout', function () {{
                resetMapArc();
                hideHud();
            }});

            if (!arcLayers[sid]) arcLayers[sid] = [];
            arcLayers[sid].push({{ line: polyline, dnsType: dnsType }});
        }}

        // 繪製 HiNet 與 Google 雙軌流光
        for (const [sid, svc] of Object.entries(mapData.services)) {{
            // HiNet 光束
            createArc(sid, 'hinet', svc.hinet.geo_key, svc.hinet.color, false);
            // Google 光束 (PK 視圖下為活力橘)
            createArc(sid, 'google', svc.google.geo_key, '#fb923c', true);
        }}

        function updateArcVisibility() {{
            for (const [sid, items] of Object.entries(arcLayers)) {{
                items.forEach(item => {{
                    if (currentActiveView === 'pk') {{
                        item.line.setStyle({{ opacity: 0.75 }});
                    }} else if (currentActiveView === 'hinet') {{
                        item.line.setStyle({{ opacity: item.dnsType === 'hinet' ? 0.75 : 0 }});
                    }} else if (currentActiveView === 'google') {{
                        item.line.setStyle({{ opacity: item.dnsType === 'google' ? 0.75 : 0 }});
                    }}
                }});
            }}
        }}
        updateArcVisibility();

        let animStep = 0;
        setInterval(() => {{
            animStep++;
            activePulses.forEach(p => {{
                if (currentActiveView !== 'pk' && p.dnsType !== currentActiveView) {{
                    p.fMarker.setStyle({{ opacity: 0, fillOpacity: 0 }});
                    p.bMarker.setStyle({{ opacity: 0, fillOpacity: 0 }});
                    return;
                }}
                p.fMarker.setStyle({{ opacity: 1, fillOpacity: 1 }});
                p.bMarker.setStyle({{ opacity: 1, fillOpacity: 1 }});

                const len = p.coords.length;
                const fIdx = (animStep + p.offset) % len;
                p.fMarker.setLatLng(p.coords[fIdx]);

                const bIdx = (len - 1) - ((animStep + p.offset) % len);
                p.bMarker.setLatLng(p.coords[bIdx]);
            }});
        }}, 60);

        function highlightMapArc(sid) {{
            for (const [id, items] of Object.entries(arcLayers)) {{
                items.forEach(item => {{
                    if (id === sid) {{
                        if (currentActiveView === 'pk' || item.dnsType === currentActiveView) {{
                            item.line.setStyle({{ weight: 6, opacity: 1 }});
                            item.line.bringToFront();
                        }}
                    }} else {{
                        item.line.setStyle({{ opacity: 0.1 }});
                    }}
                }});
            }}
        }}

        function resetMapArc() {{
            updateArcVisibility();
            for (const [sid, items] of Object.entries(arcLayers)) {{
                items.forEach(item => {{
                    item.line.setStyle({{ weight: 2.8 }});
                }});
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
            currentActiveView = viewName;
            document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('mode-active'));
            btn.classList.add('mode-active');

            document.querySelectorAll('.view-panel').forEach(p => p.classList.remove('view-active'));
            document.getElementById('view-' + viewName).classList.add('view-active');

            updateArcVisibility();
        }}

        {chart_init_js}
    </script>
</body>
</html>
"""

with open("egress.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 成功更新：真・路由異動偵測、FB 探針修復、PK 地圖雙軌光束連動與修復進度擴充。")
