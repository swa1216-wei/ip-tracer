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
        "host": "scontent.xx.fbcdn.net",
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
        "group": s["group"],
        "hinet": {**h_data, "win": h_win},
        "google": {**g_data, "win": g_win}
    }

history_state.insert(0, current_snapshot)
history_state = history_state[:72]

with open(STATE_FILE, "w", encoding="utf-8") as f:
    json.dump(history_state, f, ensure_ascii=False, indent=2)

# ==============================================================================
# 第五部分：歷史事件與 SLA 違規統計運算
# ==============================================================================

chronological_data = list(reversed(history_state))
chart_labels = [item["time_short"] for item in chronological_data]
chart_labels_json = json.dumps(chart_labels)

# 判斷時間點是否屬於夜間上網尖峰忙時 (20:00 ~ 23:30)
def is_busy_hour(time_str):
    try:
        parts = time_str.split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        minutes = h * 60 + m
        return (20 * 60) <= minutes <= (23 * 60 + 30)
    except Exception:
        return False

# 計算 11 款 App 的 24 小時統計與超標紀錄
service_stats = {}
chart_datasets = {}
alert_events = []

for s in TARGET_SERVICES:
    sid = s["id"]
    h_totals = []
    h_tcps = []
    g_totals = []
    g_tcps = []

    h_yellow_busy = 0
    h_yellow_off = 0
    h_red_cnt = 0

    g_yellow_busy = 0
    g_yellow_off = 0
    g_red_cnt = 0

    prev_h_geo = None

    for snap in chronological_data:
        t_label = snap.get("time_short", "")
        s_data = snap.get("data", {}).get(sid, {})
        h_info = s_data.get("hinet", {})
        g_info = s_data.get("google", {})

        h_ms = h_info.get("total_ms", 0)
        h_totals.append(h_ms)
        h_tcps.append(h_info.get("tcp_ms", 0))

        g_ms = g_info.get("total_ms", 0)
        g_totals.append(g_ms)
        g_tcps.append(g_info.get("tcp_ms", 0))

        # 統計 HiNet 超標
        if h_ms >= 350 or h_info.get("status") != "OK":
            h_red_cnt += 1
        elif h_ms >= 150:
            if is_busy_hour(t_label):
                h_yellow_busy += 1
            else:
                h_yellow_off += 1

        # 統計 Google 超標
        if g_ms >= 350 or g_info.get("status") != "OK":
            g_red_cnt += 1
        elif g_ms >= 150:
            if is_busy_hour(t_label):
                g_yellow_busy += 1
            else:
                g_yellow_off += 1

        # 偵測路由漂移事件 (台北 ➔ 海外 或 海外 ➔ 台北)
        curr_h_geo = h_info.get("geo_key")
        if prev_h_geo and curr_h_geo and prev_h_geo != curr_h_geo:
            if prev_h_geo == "TPE" and curr_h_geo != "TPE":
                alert_events.append({
                    "time": t_label,
                    "service": s["name"],
                    "dns": "HiNet DNS",
                    "type": "drift",
                    "desc": f"路由漂移：台北本地 ➔ {h_info.get('geo_name')} (延遲上升至 {h_ms}ms)",
                    "status": "進行中"
                })
            elif prev_h_geo != "TPE" and curr_h_geo == "TPE":
                alert_events.append({
                    "time": t_label,
                    "service": s["name"],
                    "dns": "HiNet DNS",
                    "type": "recover",
                    "desc": f"路由復原：自海外回歸台北本地節點 (延遲降至 {h_ms}ms)",
                    "status": "已復原"
                })
        prev_h_geo = curr_h_geo

    chart_datasets[sid] = {
        "hinet_total": h_totals,
        "hinet_tcp": h_tcps,
        "google_total": g_totals,
        "google_tcp": g_tcps
    }

    service_stats[sid] = {
        "h_yellow_total": h_yellow_busy + h_yellow_off,
        "h_yellow_busy": h_yellow_busy,
        "h_yellow_off": h_yellow_off,
        "h_red": h_red_cnt,
        "g_yellow_total": g_yellow_busy + g_yellow_off,
        "g_red": g_red_cnt
    }

# ==============================================================================
# 第六部分：前端 HTML 組裝
# ==============================================================================

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

            card_content = f"""
            <div class="pk-container">
                <div class="pk-row {'pk-row-win' if h.get('win') else ''}">
                    <div class="pk-dns-name" style="color:#38bdf8;">HiNet DNS</div>
                    <div class="pk-ms" style="color:{h.get('color')};">
                        <span style="min-width:65px;">{h_total_str}</span> {h_stamp}
                    </div>
                    <div class="pk-node-info">
                        <span>{h.get('geo_name','--')}</span>
                        <span style="color:#94a3b8; font-size:11px;">TCP: {h.get('tcp_ms','--')}ms</span>
                    </div>
                </div>
                <div class="pk-row {'pk-row-win' if g.get('win') else ''}">
                    <div class="pk-dns-name" style="color:#c084fc;">Google DNS</div>
                    <div class="pk-ms" style="color:{g.get('color')};">
                        <span style="min-width:65px;">{g_total_str}</span> {g_stamp}
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
        <div class="service-card" id="card-{view_mode}-{sid}">
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

# 渲染第 4 頁：SLA 忙時分析滿版大圖
def render_sla_view():
    cards = ""
    for s in TARGET_SERVICES:
        sid = s["id"]
        curr = current_snapshot["data"].get(sid, {})
        st = service_stats[sid]

        h_warn_str = f"超標 {st['h_yellow_total']} 次 (忙時 {st['h_yellow_busy']} / 離峰 {st['h_yellow_off']})" if st['h_yellow_total'] > 0 else "無超標"
        if st['h_red'] > 0:
            h_warn_str += f" | 🔴 嚴重卡頓 {st['h_red']} 次"

        cards += f"""
        <div class="sla-card">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                <div style="font-size:16px; font-weight:800; color:#f8fafc;">{curr['name']}</div>
                <div style="font-size:12px; color:#cbd5e1; display:flex; gap:16px;">
                    <span>🔵 HiNet: <strong style="color:#38bdf8;">{h_warn_str}</strong></span>
                    <span>🟣 Google: <strong style="color:#c084fc;">超標 {st['g_yellow_total']} 次</strong></span>
                </div>
            </div>
            <div style="height:220px; background:#070d18; border-radius:8px; padding:10px;">
                <canvas id="chart-sla-{sid}"></canvas>
            </div>
        </div>
        """
    return f"""
    <div style="margin-bottom:14px; color:#94a3b8; font-size:13px; display:flex; justify-content:space-between; align-items:center;">
        <span>📌 說明：背景暗灰區間為 <strong>夜間尖峰忙時 (20:00 ~ 23:30)</strong>，曲線穿過黃線代表輕微延遲，穿過紅線代表卡頓或逾時。</span>
        <div style="display:flex; gap:12px;">
            <span style="color:#38bdf8; font-weight:bold;">── HiNet DNS</span>
            <span style="color:#c084fc; font-weight:bold;">── Google DNS</span>
            <span style="color:#facc15; font-weight:bold;">┄┄ 150ms 警戒</span>
            <span style="color:#f87171; font-weight:bold;">┄┄ 350ms 嚴重</span>
        </div>
    </div>
    <div class="sla-grid">{cards}</div>
    """

html_hinet_cards = render_cards_by_view("hinet")
html_google_cards = render_cards_by_view("google")
html_pk_cards = render_cards_by_view("pk")
html_sla_cards = render_sla_view()

# 告警盤 HTML
alert_rows = ""
if alert_events:
    for ev in reversed(alert_events):
        badge_cls = "cable-badge-fixed" if ev["status"] == "已復原" else "cable-badge-warn"
        alert_rows += f"""
        <tr>
            <td style="color:#f8fafc; font-weight:bold;">{ev['time']}</td>
            <td style="color:#38bdf8;">{ev['service']}</td>
            <td>{ev['dns']}</td>
            <td>{ev['desc']}</td>
            <td><span class="{badge_cls}">{ev['status']}</span></td>
        </tr>
        """
else:
    alert_rows = """
    <tr>
        <td colspan="5" style="text-align:center; color:#94a3b8; padding:16px;">
            🟢 過去 24 小時未偵測到異常路由漂移或境外繞徑事件
        </td>
    </tr>
    """

# 圖表渲染 JS
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
            labels: {chart_labels_json},
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
            labels: {chart_labels_json},
            datasets: [
                {{ label: 'Total (ms)', data: {json.dumps(ds['google_total'])}, borderColor: '{g_col}', borderWidth: 2, tension: 0.3, pointRadius: 1 }},
                {{ label: 'TCP (ms)', data: {json.dumps(ds['google_tcp'])}, borderColor: '#a855f7', borderWidth: 1.5, borderDash: [2, 2], tension: 0.3, pointRadius: 0 }}
            ]
        }},
        options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ ticks: {{ color: '#64748b', maxTicksLimit: 5, font: {{ size: 9 }} }} }}, y: {{ ticks: {{ color: '#64748b', font: {{ size: 9 }} }} }} }} }}
    }});
    """

    # 3. 綜合對照圖 (HiNet 藍色實線 vs Google 紫色實線)
    chart_init_js += f"""
    new Chart(document.getElementById('chart-pk-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels_json},
            datasets: [
                {{ label: 'HiNet Total', data: {json.dumps(ds['hinet_total'])}, borderColor: '#38bdf8', borderWidth: 2, tension: 0.3, pointRadius: 1.2 }},
                {{ label: 'Google Total', data: {json.dumps(ds['google_total'])}, borderColor: '#c084fc', borderWidth: 2, tension: 0.3, pointRadius: 1 }}
            ]
        }},
        options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }}, scales: {{ x: {{ ticks: {{ color: '#64748b', maxTicksLimit: 5, font: {{ size: 9 }} }} }}, y: {{ ticks: {{ color: '#64748b', font: {{ size: 9 }} }} }} }} }}
    }});
    """

    # 4. SLA 專屬滿版大圖 (含 150/350ms 水平參考線)
    len_pts = len(chart_labels)
    chart_init_js += f"""
    new Chart(document.getElementById('chart-sla-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels_json},
            datasets: [
                {{ label: 'HiNet DNS', data: {json.dumps(ds['hinet_total'])}, borderColor: '#38bdf8', borderWidth: 2, tension: 0.3, pointRadius: 1.5 }},
                {{ label: 'Google DNS', data: {json.dumps(ds['google_total'])}, borderColor: '#c084fc', borderWidth: 2, tension: 0.3, pointRadius: 1.5 }},
                {{ label: '150ms 警戒線', data: Array({len_pts}).fill(150), borderColor: '#facc15', borderWidth: 1.5, borderDash: [4, 4], pointRadius: 0, fill: false }},
                {{ label: '350ms 卡頓線', data: Array({len_pts}).fill(350), borderColor: '#f87171', borderWidth: 1.5, borderDash: [4, 4], pointRadius: 0, fill: false }}
            ]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{
                x: {{ ticks: {{ color: '#94a3b8', maxTicksLimit: 12, font: {{ size: 10 }} }} }},
                y: {{ ticks: {{ color: '#94a3b8', font: {{ size: 10 }} }}, beginAtZero: true }}
            }}
        }}
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

        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }}

        /* 地圖視窗 */
        .map-wrapper {{ position:relative; background:#070d18; border:1px solid #1e293b; border-radius:12px; height:480px; margin-bottom:16px; overflow:hidden; }}
        #map-global {{ width:100%; height:100%; background:#070d18; }}

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

        #hud-card {{ position:absolute; bottom:20px; left:20px; width:280px; background:rgba(15, 23, 42, 0.95); border:1px solid #38bdf8; border-radius:8px; padding:12px; backdrop-filter:blur(8px); display:none; z-index:1000; pointer-events:none; }}

        /* 分頁切換按鈕 */
        .mode-tabs {{ display: flex; gap: 8px; margin-bottom: 20px; }}
        .mode-btn {{ padding: 7px 18px; font-size: 13px; font-weight: bold; border-radius: 6px; color: #94a3b8; background: #0b1324; border: 1px solid #1e293b; cursor: pointer; transition: all 0.2s; }}
        .mode-btn:hover {{ background: #1e293b; color: #f8fafc; }}
        .mode-active {{ background: #1e293b; color: #38bdf8; border-color: #38bdf8; }}

        .section-title {{ font-size: 15px; font-weight: 800; color: #f8fafc; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }}

        .cards-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap: 14px; }}
        .service-card {{ background:linear-gradient(145deg, #0f172a, #131d31); border-radius:10px; padding:14px; border:1px solid #1e293b; }}

        /* PK 對照表排版 */
        .pk-container {{ display: flex; flex-direction: column; gap: 6px; margin-bottom: 10px; }}
        .pk-row {{ display: flex; justify-content: space-between; align-items: center; background: #070d18; padding: 7px 9px; border-radius: 6px; border: 1px solid #1e293b; }}
        .pk-row-win {{ border-color: rgba(239, 68, 68, 0.5); background: rgba(239, 68, 68, 0.06); }}
        .pk-dns-name {{ font-weight: bold; font-size: 12px; width: 85px; }}
        .pk-ms {{ font-weight: 800; font-size: 15px; display: flex; align-items: center; gap: 6px; flex: 1; }}
        .pk-node-info {{ text-align: right; font-size: 11px; color: #cbd5e1; display: flex; flex-direction: column; }}

        /* 紅色大讚章樣式 (固定尺寸，防擠壓) */
        .pk-stamp {{
            flex-shrink: 0;
            width: 44px;
            height: 20px;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            background: linear-gradient(135deg, #ef4444, #b91c1c);
            color: #ffffff;
            font-size: 11px;
            font-weight: 900;
            border-radius: 4px;
            box-shadow: 0 0 10px rgba(239, 68, 68, 0.8);
            letter-spacing: 0.5px;
            transform: rotate(-3deg);
        }}

        /* 第 4 頁 SLA 大圖版面 */
        .sla-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(650px, 1fr)); gap: 16px; }}
        .sla-card {{ background:#0f172a; border:1px solid #1e293b; border-radius:10px; padding:16px; }}

        /* 頁面切換控制 */
        .view-panel {{ display: none; }}
        .view-active {{ display: block; }}

        /* 折疊式告警盤抽屜 */
        .drawer-toggle {{
            background: #0b1324;
            border: 1px solid #1e293b;
            border-radius: 8px;
            padding: 12px 16px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 24px;
            transition: all 0.2s;
        }}
        .drawer-toggle:hover {{ background: #131d31; border-color: #38bdf8; }}
        .drawer-content {{ display: none; background: #0f172a; border: 1px solid #1e293b; border-top: none; border-radius: 0 0 8px 8px; padding: 16px; }}

        /* 表格共用樣式 */
        .data-table {{ width:100%; border-collapse:collapse; }}
        .data-table th {{ text-align:left; color:#94a3b8; font-size:12px; padding:8px 10px; border-bottom:1px solid #1e293b; }}
        .data-table td {{ padding:10px; border-bottom:1px solid #1e293b; font-size:13px; vertical-align:top; }}
        .cable-badge-down {{ background:rgba(239, 68, 68, 0.2); color:#f87171; border:1px solid #ef4444; font-size:11px; font-weight:bold; padding:2px 6px; border-radius:4px; }}
        .cable-badge-warn {{ background:rgba(251, 146, 60, 0.2); color:#fb923c; border:1px solid #fb923c; font-size:11px; font-weight:bold; padding:2px 6px; border-radius:4px; }}
        .cable-badge-fixed {{ background:rgba(52, 211, 153, 0.2); color:#34d399; border:1px solid #34d399; font-size:11px; font-weight:bold; padding:2px 6px; border-radius:4px; }}

        @media (max-width: 768px) {{
            body {{ padding:12px; }}
            .map-wrapper {{ height:380px; }}
            #taiwan-submap-wrapper {{ width: 150px; height: 190px; top: 10px; right: 10px; }}
            .sla-grid {{ grid-template-columns: 1fr; }}
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
                    🔄 重新整理資料
                </button>
            </div>
        </div>

        <!-- 第一屏核心：子母地圖 -->
        <div class="map-wrapper" id="global-map-container">
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

        <!-- 地圖正下方：4 大分頁切換 -->
        <div class="mode-tabs">
            <button class="mode-btn mode-active" onclick="switchView('hinet', this)">HiNet 預設 DNS</button>
            <button class="mode-btn" onclick="switchView('google', this)">Google DNS (8.8.8.8)</button>
            <button class="mode-btn" onclick="switchView('pk', this)">綜合效能對照</button>
            <button class="mode-btn" onclick="switchView('sla', this)">📊 SLA 忙時與警戒分析</button>
        </div>

        <!-- 四大獨立視圖面板 -->
        <div id="view-hinet" class="view-panel view-active">
            {html_hinet_cards}
        </div>

        <div id="view-google" class="view-panel">
            {html_google_cards}
        </div>

        <div id="view-pk" class="view-panel">
            {html_pk_cards}
        </div>

        <div id="view-sla" class="view-panel">
            {html_sla_cards}
        </div>

        <!-- 區塊一：即時路由異動與異常告警盤 (折疊抽屜) -->
        <div class="drawer-toggle" onclick="toggleDrawer()">
            <div style="display:flex; align-items:center; gap:8px;">
                <span style="font-size:15px; font-weight:bold; color:#f8fafc;">📋 近 24 小時路由漂移與異動事件日誌</span>
                <span style="font-size:12px; color:#94a3b8;">(共 {len(alert_events)} 筆事件)</span>
            </div>
            <div id="drawer-arrow" style="color:#38bdf8; font-weight:bold;">▼ 點擊展開</div>
        </div>
        <div id="drawer-content" class="drawer-content">
            <table class="data-table">
                <thead>
                    <tr>
                        <th style="width:100px;">發生時間</th>
                        <th style="width:180px;">服務標的</th>
                        <th style="width:120px;">DNS 來源</th>
                        <th>異動事件說明</th>
                        <th style="width:100px;">當前狀態</th>
                    </tr>
                </thead>
                <tbody>
                    {alert_rows}
                </tbody>
            </table>
        </div>

        <!-- 區塊二：國際海纜受損公告 (包含實際修復/排程進度) -->
        <div style="background:#0f172a; border:1px solid #1e293b; border-radius:10px; padding:16px 20px; margin-top:20px;">
            <div style="font-size:16px; font-weight:bold; color:#f8fafc; margin-bottom:8px; display:flex; align-items:center; gap:8px;">
                ⚠️ 台灣周邊國際海纜即時障礙通報 (資料來源：數位發展部海纜通報 & 中華電信網路維運公告)
            </div>
            <table class="data-table">
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
        // 1. 初始化國際海纜主地圖 (鎖定縮放)
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

        // 修復美西跨洋弧度平滑收斂演算法 (保證不衝出天花板/北極)
        function getCurvedCoords(start, end, curvature, isTransPacific) {{
            const lat1 = start[0], lng1 = start[1];
            const lat2 = end[0], lng2 = end[1];
            
            let midLat, midLng;
            if (isTransPacific) {{
                // 長程跨太平洋平滑大圓弧，控制點緯度上限鎖定在 45°N 附近
                midLat = Math.min(45.0, (lat1 + lat2) / 2 + 12.0 + (curvature * 8.0));
                midLng = (lng1 + lng2) / 2;
            }} else {{
                midLat = (lat1 + lat2) / 2 + (lng2 - lng1) * curvature;
                midLng = (lng1 + lng2) / 2 - (lat2 - lat1) * curvature;
            }}

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
            const isTransPacific = (geoKey === "US_WEST");
            const targetMap = isLocal ? mapLocal : mapGlobal;

            arcCount++;
            const sign = (arcCount % 2 === 1) ? 1 : -1;
            const curvature = isLocal ? (sign * 0.15) : (isTransPacific ? (sign * 0.4) : (sign * (0.08 + Math.floor((arcCount - 1) / 2) * 0.12)));
            const curveCoords = getCurvedCoords(twCoord, [geo.lat, geo.lng], curvature, isTransPacific);

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
                showHud(mapData.services[sid]);
            }});
            polyline.on('mouseout', function () {{
                hideHud();
            }});

            if (!arcLayers[sid]) arcLayers[sid] = [];
            arcLayers[sid].push({{ line: polyline, dnsType: dnsType }});
        }}

        // 繪製雙軌流光 (Google DNS 採用電競紫)
        for (const [sid, svc] of Object.entries(mapData.services)) {{
            createArc(sid, 'hinet', svc.hinet.geo_key, svc.hinet.color, false);
            createArc(sid, 'google', svc.google.geo_key, '#c084fc', true);
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
                    }} else {{
                        item.line.setStyle({{ opacity: 0 }});
                    }}
                }});
            }}
        }}
        updateArcVisibility();

        let animStep = 0;
        setInterval(() => {{
            animStep++;
            activePulses.forEach(p => {{
                if (currentActiveView === 'sla' || (currentActiveView !== 'pk' && p.dnsType !== currentActiveView)) {{
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

        // 分頁切換 (切換至 SLA 頁面時自動隱藏頂部地圖)
        function switchView(viewName, btn) {{
            currentActiveView = viewName;
            document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('mode-active'));
            btn.classList.add('mode-active');

            document.querySelectorAll('.view-panel').forEach(p => p.classList.remove('view-active'));
            document.getElementById('view-' + viewName).classList.add('view-active');

            const mapContainer = document.getElementById('global-map-container');
            if (viewName === 'sla') {{
                mapContainer.style.display = 'none';
            }} else {{
                mapContainer.style.display = 'block';
                mapGlobal.invalidateSize();
                mapLocal.invalidateSize();
            }}

            updateArcVisibility();
        }}

        function toggleDrawer() {{
            const content = document.getElementById('drawer-content');
            const arrow = document.getElementById('drawer-arrow');
            if (content.style.display === 'block') {{
                content.style.display = 'none';
                arrow.innerText = '▼ 點擊展開';
            }} else {{
                content.style.display = 'block';
                arrow.innerText = '▲ 點擊收合';
            }}
        }}

        {chart_init_js}
    </script>
</body>
</html>
"""

with open("egress.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 成功更新：SLA 忙時分析滿版大圖、美西弧線防暴衝、折疊式告警盤與藍紫色彩分離。")
