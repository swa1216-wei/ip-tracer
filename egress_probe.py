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

KNOWN_GEO_NAMES = {
    "TPE": "台北本地節點 (CDN Edge)",
    "TYO": "日本 (東京 NRT/HND)",
    "OSA": "日本 (大阪 KIX)",
    "HKG": "香港 (HKG POP)",
    "SIN": "新加坡 (SIN POP)",
    "US_WEST": "美國西岸 (SJC/LAX)"
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
# 第二部分：Globalping 探測 (防抖動連續 3 次複測驗證)
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
    
    is_abnormal = (not res) or (res.get("total_ms", 0) <= 0) or (res.get("total_ms", 0) >= 150)
    if is_abnormal:
        for attempt in range(2):
            time.sleep(2)
            retry_res = execute_single_probe(service["host"], dns_resolver)
            if retry_res and retry_res.get("total_ms", 0) > 0:
                res = retry_res
                if res["total_ms"] < 150:
                    break

    if res and res["total_ms"] > 0:
        geo_key = determine_geo(res["resolved_ip"], res["headers"], res["tcp_ms"])
        color = get_latency_color(res["total_ms"], "OK")
        return {
            "status": "OK",
            "total_ms": res["total_ms"],
            "tcp_ms": res["tcp_ms"],
            "resolved_ip": res["resolved_ip"],
            "geo_key": geo_key,
            "geo_name": KNOWN_GEO_NAMES.get(geo_key, "海外節點"),
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

print(f"[{now_str}] 啟動 HiNet 出口與 CDN 路由巡檢 (含 3 次異常複測防抖機制)...")

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

for s in TARGET_SERVICES:
    sid = s["id"]
    res = results.get(sid, {
        "hinet": {"status": "FAIL", "total_ms": 0, "tcp_ms": 0, "geo_key": "TYO", "color": "#f87171"},
        "google": {"status": "FAIL", "total_ms": 0, "tcp_ms": 0, "geo_key": "TYO", "color": "#f87171"}
    })
    
    h_data = res["hinet"]
    g_data = res["google"]

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
        "host": s["host"],
        "group": s["group"],
        "hinet": {**h_data, "win": h_win},
        "google": {**g_data, "win": g_win}
    }

history_state.insert(0, current_snapshot)
history_state = history_state[:72]

with open(STATE_FILE, "w", encoding="utf-8") as f:
    json.dump(history_state, f, ensure_ascii=False, indent=2)

# ==============================================================================
# 第五部分：歷史事件與節點切換紀錄運算
# ==============================================================================

chronological_data = list(reversed(history_state))
chart_labels = [item["time_short"] for item in chronological_data]
chart_labels_json = json.dumps(chart_labels)

chart_datasets = {}
node_switch_logs = []

for s in TARGET_SERVICES:
    sid = s["id"]
    h_totals = []
    h_tcps = []
    g_totals = []
    g_tcps = []

    prev_h_geo = None
    prev_g_geo = None

    for snap in chronological_data:
        t_label = snap.get("time_short", "")
        s_data = snap.get("data", {}).get(sid, {})
        h_info = s_data.get("hinet", {})
        g_info = s_data.get("google", {})

        h_totals.append(h_info.get("total_ms", 0))
        h_tcps.append(h_info.get("tcp_ms", 0))

        g_totals.append(g_info.get("total_ms", 0))
        g_tcps.append(g_info.get("tcp_ms", 0))

        # HiNet 節點切換紀錄
        curr_h_geo = h_info.get("geo_key")
        if prev_h_geo and curr_h_geo and prev_h_geo != curr_h_geo:
            prev_name = KNOWN_GEO_NAMES.get(prev_h_geo, prev_h_geo)
            curr_name = KNOWN_GEO_NAMES.get(curr_h_geo, curr_h_geo)
            node_switch_logs.append({
                "time": t_label,
                "service": s["name"],
                "dns": "HiNet DNS",
                "trajectory": f"{prev_name} ➔ {curr_name}",
                "current_node": curr_name
            })
        if curr_h_geo:
            prev_h_geo = curr_h_geo

        # Google 節點切換紀錄
        curr_g_geo = g_info.get("geo_key")
        if prev_g_geo and curr_g_geo and prev_g_geo != curr_g_geo:
            prev_name = KNOWN_GEO_NAMES.get(prev_g_geo, prev_g_geo)
            curr_name = KNOWN_GEO_NAMES.get(curr_g_geo, curr_g_geo)
            node_switch_logs.append({
                "time": t_label,
                "service": s["name"],
                "dns": "Google DNS",
                "trajectory": f"{prev_name} ➔ {curr_name}",
                "current_node": curr_name
            })
        if curr_g_geo:
            prev_g_geo = curr_g_geo

    chart_datasets[sid] = {
        "hinet_total": h_totals,
        "hinet_tcp": h_tcps,
        "google_total": g_totals,
        "google_tcp": g_tcps
    }

# ==============================================================================
# 第六部分：HTML 畫面渲染 (含即時單點複測按鈕)
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
        host_domain = s["host"]

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
            retest_btn = f"""
            <button onclick="triggerLiveProbe('{sid}', '{curr['name']}', '{host_domain}', 'both', this)" class="retest-chip retest-chip-neutral">
                ⚡ 雙向複測
            </button>
            """
        else:
            target_data = h if view_mode == "hinet" else g
            total_val = target_data.get('total_ms', 0)
            tcp_val = target_data.get('tcp_ms', 0)
            total_str = f"{total_val} ms" if target_data.get("status") == "OK" else "逾時"
            tcp_str = f"{tcp_val} ms" if target_data.get("status") == "OK" else "--"
            health_color = target_data.get('color', '#34d399')

            status_text = "順暢良好" if target_data.get("status") == "OK" and total_val < 150 else ("稍微延遲" if total_val < 350 else "轉圈卡頓")

            # 依延遲快慢給予不同顏色的複測晶片按鈕
            dns_param = "hinet" if view_mode == "hinet" else "google"
            chip_class = "retest-chip-alert" if total_val >= 350 or target_data.get("status") != "OK" else ("retest-chip-warn" if total_val >= 150 else "retest-chip-neutral")
            chip_label = "⚡ 立即重測" if total_val >= 150 or target_data.get("status") != "OK" else "🔄 抽查"

            retest_btn = f"""
            <button onclick="triggerLiveProbe('{sid}', '{curr['name']}', '{host_domain}', '{dns_param}', this)" class="retest-chip {chip_class}">
                {chip_label}
            </button>
            """

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
                <div>{retest_btn}</div>
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
        ds = chart_datasets[sid]

        valid_h = [v for v in ds["hinet_total"] if v > 0]
        valid_g = [v for v in ds["google_total"] if v > 0]
        h_avg = round(sum(valid_h) / len(valid_h), 1) if valid_h else 0
        g_avg = round(sum(valid_g) / len(valid_g), 1) if valid_g else 0

        diff = abs(h_avg - g_avg)
        if diff <= 10:
            badge_html = f'<span class="verdict-badge" style="background:#1e293b; color:#94a3b8;">⚖️ 表現相當 (差距 {diff:.1f}ms)</span>'
        elif h_avg < g_avg:
            badge_html = f'<span class="verdict-badge" style="background:rgba(56,189,248,0.2); color:#38bdf8; border:1px solid #38bdf8;">🏆 HiNet 均值快 {diff:.1f}ms</span>'
        else:
            badge_html = f'<span class="verdict-badge" style="background:rgba(192,132,252,0.2); color:#c084fc; border:1px solid #c084fc;">🏆 Google 均值快 {diff:.1f}ms</span>'

        cards += f"""
        <div class="sla-card">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                <div style="font-size:16px; font-weight:800; color:#f8fafc;">{curr['name']}</div>
                <div style="display:flex; align-items:center; gap:12px;">
                    <div style="font-size:12px; color:#94a3b8;">
                        HiNet: <strong style="color:#38bdf8;">{h_avg}ms</strong> ｜ Google: <strong style="color:#c084fc;">{g_avg}ms</strong>
                    </div>
                    {badge_html}
                </div>
            </div>
            <div style="height:220px; background:#070d18; border-radius:8px; padding:10px;">
                <canvas id="chart-sla-{sid}"></canvas>
            </div>
        </div>
        """
    return f"""
    <div style="margin-bottom:14px; color:#94a3b8; font-size:13px; display:flex; justify-content:space-between; align-items:center;">
        <span>📌 說明：曲線穿過黃線代表輕微延遲，穿過紅線代表卡頓或逾時。</span>
        <div style="display:flex; gap:14px;">
            <span style="color:#38bdf8; font-weight:bold;">── HiNet DNS</span>
            <span style="color:#c084fc; font-weight:bold;">── Google DNS</span>
            <span style="color:#facc15; font-weight:bold;">┄┄ 150ms 警戒</span>
            <span style="color:#f87171; font-weight:bold;">┄┄ 350ms 卡頓</span>
        </div>
    </div>
    <div class="sla-grid">{cards}</div>
    """

html_hinet_cards = render_cards_by_view("hinet")
html_google_cards = render_cards_by_view("google")
html_pk_cards = render_cards_by_view("pk")
html_sla_cards = render_sla_view()

# 節點切換紀錄表格
log_rows = ""
unique_services = sorted(list(set([s["name"] for s in TARGET_SERVICES])))
service_options = "".join([f'<option value="{svc}">{svc}</option>' for svc in unique_services])

if node_switch_logs:
    for log in reversed(node_switch_logs):
        log_rows += f"""
        <tr class="log-row" data-dns="{log['dns']}" data-service="{log['service']}">
            <td style="color:#f8fafc; font-weight:bold;">{log['time']}</td>
            <td style="color:#38bdf8;">{log['service']}</td>
            <td>{log['dns']}</td>
            <td style="color:#e2e8f0;">{log['trajectory']}</td>
            <td style="color:#cbd5e1;">{log['current_node']}</td>
        </tr>
        """
else:
    log_rows = """
    <tr id="empty-log-row">
        <td colspan="5" style="text-align:center; color:#94a3b8; padding:16px;">
            過去 24 小時節點位置穩定，未發生跨節點變更
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

    # 3. 綜合對照圖
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

    # 4. SLA 專屬滿版大圖
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

html_content = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HiNet AS3462 國際出口與 CDN 路由監測</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:#070d18; color:#e2e8f0; margin:0; padding:20px 24px; font-size:14px; }}
        .container {{ max-width: 1440px; margin: 0 auto; }}

        /* 頂部固定系統導航 Tab 列 (完全對齊 index.html 樣式) */
        .system-tabs {{ display: flex; gap: 8px; margin-bottom: 20px; border-bottom: 2px solid #334155; padding-bottom: 12px; }}
        .tab-btn {{ padding: 10px 22px; font-size: 15px; font-weight: bold; border-radius: 8px 8px 0 0; text-decoration: none; display: inline-flex; align-items: center; gap: 8px; cursor: pointer; transition: all 0.2s; }}
        .tab-active {{ background: #0284c7; color: #ffffff; box-shadow: 0 4px 14px rgba(2, 132, 199, 0.4); border: none; }}
        .tab-inactive {{ background: #1e293b; color: #94a3b8; border: 1px solid #334155; border-bottom: none; }}
        .tab-inactive:hover {{ background: #334155; color: #f8fafc; }}

        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; }}

        /* 分頁切換列 (釘選頂部) */
        .mode-tabs {{ display: flex; gap: 8px; margin-bottom: 20px; }}
        .mode-btn {{ padding: 8px 20px; font-size: 13px; font-weight: bold; border-radius: 6px; color: #94a3b8; background: #0b1324; border: 1px solid #1e293b; cursor: pointer; transition: all 0.2s; }}
        .mode-btn:hover {{ background: #1e293b; color: #f8fafc; }}
        .mode-active {{ background: #1e293b; color: #38bdf8; border-color: #38bdf8; }}

        .section-title {{ font-size: 15px; font-weight: 800; color: #f8fafc; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }}

        .cards-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap: 14px; }}
        .service-card {{ background:linear-gradient(145deg, #0f172a, #131d31); border-radius:10px; padding:14px; border:1px solid #1e293b; }}

        /* 複測微型按鈕 */
        .retest-chip {{ font-size:11px; font-weight:bold; border-radius:4px; padding:3px 8px; border:none; cursor:pointer; transition:all 0.2s; }}
        .retest-chip-neutral {{ background:#1e293b; color:#94a3b8; border:1px solid #334155; }}
        .retest-chip-neutral:hover {{ background:#334155; color:#fff; }}
        .retest-chip-warn {{ background:#ea580c; color:#fff; }}
        .retest-chip-alert {{ background:#dc2626; color:#fff; }}

        /* PK 對照表排版 */
        .pk-container {{ display: flex; flex-direction: column; gap: 6px; margin-bottom: 10px; }}
        .pk-row {{ display: flex; justify-content: space-between; align-items: center; background: #070d18; padding: 7px 9px; border-radius: 6px; border: 1px solid #1e293b; }}
        .pk-row-win {{ border-color: rgba(239, 68, 68, 0.5); background: rgba(239, 68, 68, 0.06); }}
        .pk-dns-name {{ font-weight: bold; font-size: 12px; width: 85px; }}
        .pk-ms {{ font-weight: 800; font-size: 15px; display: flex; align-items: center; gap: 6px; flex: 1; }}
        .pk-node-info {{ text-align: right; font-size: 11px; color: #cbd5e1; display: flex; flex-direction: column; }}

        /* 紅色大讚章樣式 */
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
        .verdict-badge {{ font-size:11px; font-weight:bold; padding:3px 8px; border-radius:4px; }}

        /* 頁面切換控制 */
        .view-panel {{ display: none; }}
        .view-active {{ display: block; }}

        /* 折疊式節點切換紀錄抽屜 */
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

        /* 篩選列排版 */
        .filter-bar {{ display:flex; gap:12px; margin-bottom:14px; align-items:center; flex-wrap:wrap; }}
        .filter-select {{ background:#070d18; border:1px solid #334155; color:#f8fafc; font-size:12px; padding:6px 12px; border-radius:6px; outline:none; }}

        /* 表格共用樣式 */
        .data-table {{ width:100%; border-collapse:collapse; }}
        .data-table th {{ text-align:left; color:#94a3b8; font-size:12px; padding:8px 10px; border-bottom:1px solid #1e293b; }}
        .data-table td {{ padding:10px; border-bottom:1px solid #1e293b; font-size:13px; vertical-align:top; }}
        .cable-badge-down {{ background:rgba(239, 68, 68, 0.2); color:#f87171; border:1px solid #ef4444; font-size:11px; font-weight:bold; padding:2px 6px; border-radius:4px; }}
        .cable-badge-warn {{ background:rgba(251, 146, 60, 0.2); color:#fb923c; border:1px solid #fb923c; font-size:11px; font-weight:bold; padding:2px 6px; border-radius:4px; }}

        /* 複測彈窗 Modal 樣式 */
        .modal-overlay {{
            display: none;
            position: fixed;
            top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(0, 0, 0, 0.75);
            backdrop-filter: blur(4px);
            z-index: 9999;
            justify-content: center;
            align-items: center;
        }}
        .modal-box {{
            background: #0f172a;
            border: 1px solid #38bdf8;
            border-radius: 12px;
            width: 90%;
            max-width: 780px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.6);
            overflow: hidden;
            display: flex;
            flex-direction: column;
            max-height: 85vh;
        }}
        .modal-header {{
            background: #1e293b;
            padding: 14px 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid #334155;
        }}
        .modal-body {{
            padding: 20px;
            overflow-y: auto;
            color: #e2e8f0;
        }}

        @media (max-width: 768px) {{
            body {{ padding:12px; }}
            .sla-grid {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- 頂部固定系統導航 Tab 切換列 (對齊 index.html) -->
        <div class="system-tabs">
            <a href="index.html" class="tab-btn tab-inactive">
                🌐 IPv4 核心巡檢
            </a>
            <a href="index.html#v6" class="tab-btn tab-inactive">
                🚀 IPv6 核心巡檢
            </a>
            <a href="egress.html" class="tab-btn tab-active">
                ⚡ 海外出口監測
            </a>
        </div>

        <div class="header">
            <div>
                <h1 style="margin:0 0 6px 0; font-size:22px; color:#f8fafc; font-weight:800;">
                    HiNet (AS3462) 國際出口與 CDN 路由監測
                </h1>
                <div style="color:#94a3b8; font-size:13px;">
                    📍 <strong>探針來源：</strong>台灣本地 HiNet 寬頻節點 (AS3462 台中實體用戶端)<br>
                    ⏱ <strong>最後量測時間：</strong><span style="color:#38bdf8; font-weight:bold;">{now_str} (UTC+8)</span>
                </div>
            </div>
            <div>
                <button onclick="window.location.reload()" title="重整取得 GitHub 最新排程數據" style="background:#0284c7; color:#fff; border:none; border-radius:6px; padding:8px 16px; cursor:pointer; font-weight:bold; font-size:13px;">
                    🔄 畫面更新 (排程運作中)
                </button>
            </div>
        </div>

        <!-- 4 大分頁切換 (釘選頂部) -->
        <div class="mode-tabs">
            <button class="mode-btn mode-active" onclick="switchView('hinet', this)">HiNet 預設 DNS</button>
            <button class="mode-btn" onclick="switchView('google', this)">Google DNS (8.8.8.8)</button>
            <button class="mode-btn" onclick="switchView('pk', this)">綜合效能對照</button>
            <button class="mode-btn" onclick="switchView('sla', this)">SLA 忙時與警戒分析</button>
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

        <!-- 區塊一：24 小時節點切換紀錄 (折疊抽屜 + 下拉篩選) -->
        <div class="drawer-toggle" onclick="toggleDrawer()">
            <div style="display:flex; align-items:center; gap:8px;">
                <span style="font-size:15px; font-weight:bold; color:#f8fafc;">📋 近 24 小時節點切換紀錄</span>
                <span style="font-size:12px; color:#94a3b8;">(共 {len(node_switch_logs)} 筆異動)</span>
            </div>
            <div id="drawer-arrow" style="color:#38bdf8; font-weight:bold;">▼ 點擊展開</div>
        </div>
        <div id="drawer-content" class="drawer-content">
            <div class="filter-bar">
                <span style="color:#94a3b8; font-size:12px;">條件過濾：</span>
                <select id="filter-dns" class="filter-select" onchange="applyLogFilters()">
                    <option value="ALL">全部 DNS 來源</option>
                    <option value="HiNet DNS">僅看 HiNet DNS</option>
                    <option value="Google DNS">僅看 Google DNS</option>
                </select>
                <select id="filter-service" class="filter-select" onchange="applyLogFilters()">
                    <option value="ALL">全部服務標的 (11款)</option>
                    {service_options}
                </select>
                <span id="filter-count" style="color:#64748b; font-size:12px;">顯示 {len(node_switch_logs)} 筆</span>
            </div>
            <table class="data-table">
                <thead>
                    <tr>
                        <th style="width:110px;">時間</th>
                        <th style="width:190px;">服務標的</th>
                        <th style="width:130px;">DNS 來源</th>
                        <th>節點切換軌跡</th>
                        <th style="width:190px;">當前落地點</th>
                    </tr>
                </thead>
                <tbody id="log-table-body">
                    {log_rows}
                </tbody>
            </table>
        </div>

        <!-- 區塊二：國際海纜受損公告 -->
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

    <!-- 即時複測彈出視窗 (Modal) -->
    <div id="live-probe-modal" class="modal-overlay">
        <div class="modal-box">
            <div class="modal-header">
                <span id="modal-probe-title" style="font-weight:bold;color:#38bdf8;font-size:16px;">⚡ 即時 CDN 路由複測診斷</span>
                <button onclick="closeProbeModal()" style="background:transparent;border:none;color:#94a3b8;font-size:22px;cursor:pointer;font-weight:bold;">&times;</button>
            </div>
            <div id="modal-probe-content" class="modal-body">
                <!-- 動態注入複測結果 -->
            </div>
            <div style="background:#1e293b;padding:12px 20px;text-align:right;border-top:1px solid #334155;">
                <button onclick="closeProbeModal()" style="background:#0284c7;color:#fff;border:none;border-radius:6px;padding:8px 18px;cursor:pointer;font-weight:bold;font-size:14px;">關閉視窗</button>
            </div>
        </div>
    </div>

    <script>
        function switchView(viewName, btn) {{
            document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('mode-active'));
            btn.classList.add('mode-active');

            document.querySelectorAll('.view-panel').forEach(p => p.classList.remove('view-active'));
            document.getElementById('view-' + viewName).classList.add('view-active');
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

        function applyLogFilters() {{
            const dnsFilter = document.getElementById('filter-dns').value;
            const serviceFilter = document.getElementById('filter-service').value;
            const rows = document.querySelectorAll('.log-row');
            let visibleCount = 0;

            rows.forEach(row => {{
                const rDns = row.getAttribute('data-dns');
                const rService = row.getAttribute('data-service');

                const matchDns = (dnsFilter === 'ALL' || rDns === dnsFilter);
                const matchService = (serviceFilter === 'ALL' || rService === serviceFilter);

                if (matchDns && matchService) {{
                    row.style.display = '';
                    visibleCount++;
                }} else {{
                    row.style.display = 'none';
                }}
            }});

            const countEl = document.getElementById('filter-count');
            if (countEl) {{
                countEl.innerText = `顯示 ${{visibleCount}} 筆`;
            }}
        }}

        // 前端即時單點複測 (Live Probe)
        function closeProbeModal() {{
            document.getElementById('live-probe-modal').style.display = 'none';
        }}

        function determineClientGeo(headers, tcpMs) {{
            const h = (headers || '').toLowerCase();
            if (tcpMs > 130) return '美國西岸 (SJC/LAX)';
            if (h.includes('nrt') || h.includes('hnd') || h.includes('tyo') || h.includes('japan')) return '日本 (東京 NRT/HND)';
            if (h.includes('kix') || h.includes('osa')) return '日本 (大阪 KIX)';
            if (h.includes('hkg') || h.includes('hongkong')) return '香港 (HKG POP)';
            if (h.includes('sin') || h.includes('singapore')) return '新加坡 (SIN POP)';
            if (h.includes('tpe') || h.includes('taiwan') || tcpMs < 10) return '台北本地節點 (CDN Edge)';
            return '海外節點';
        }}

        async function runSingleHttpProbe(host, resolver) {{
            const mOptions = {{ protocol: "HTTPS" }};
            if (resolver) mOptions.resolver = resolver;

            const res = await fetch("https://api.globalping.io/v1/measurements", {{
                method: "POST",
                headers: {{ "Content-Type": "application/json" }},
                body: JSON.stringify({{
                    type: "http",
                    target: host,
                    locations: [{{ country: "TW", asn: 3462 }}],
                    measurementOptions: mOptions,
                    limit: 1
                }})
            }});
            const data = await res.json();
            const mId = data.id;

            for (let i = 0; i < 12; i++) {{
                await new Promise(r => setTimeout(r, 1000));
                const pollRes = await fetch("https://api.globalping.io/v1/measurements/" + mId);
                const pollData = await pollRes.json();
                if (pollData.status === "finished") {{
                    if (pollData.results && pollData.results.length > 0) {{
                        const r = pollData.results[0].result;
                        return {{
                            total: r.timings?.total || 0,
                            tcp: r.timings?.tcp || 0,
                            ip: r.resolvedAddress || '--',
                            headers: r.rawHeaders || ''
                        }};
                    }}
                    break;
                }}
            }}
            return null;
        }}

        async function triggerLiveProbe(sid, name, host, dnsMode, btn) {{
            const originText = btn.innerText;
            btn.innerText = "⏳ 測量中...";
            btn.disabled = true;

            const modal = document.getElementById('live-probe-modal');
            const titleEl = document.getElementById('modal-probe-title');
            const contentEl = document.getElementById('modal-probe-content');

            titleEl.innerText = `⚡ 即時 CDN 路由複測：${{name}} (${{host}})`;
            contentEl.innerHTML = '<div style="padding:20px;text-align:center;color:#38bdf8;font-size:15px;">🌐 正在向台灣 HiNet 實體節點發起即時 HTTPS 交握探測，請稍候約 3~8 秒...</div>';
            modal.style.display = 'flex';

            const tasks = [];
            if (dnsMode === 'both') {{
                tasks.push({{ label: 'HiNet DNS (預設)', resolver: null, color: '#38bdf8' }});
                tasks.push({{ label: 'Google DNS (8.8.8.8)', resolver: '8.8.8.8', color: '#c084fc' }});
            }} else if (dnsMode === 'hinet') {{
                tasks.push({{ label: 'HiNet DNS (預設)', resolver: null, color: '#38bdf8' }});
            }} else {{
                tasks.push({{ label: 'Google DNS (8.8.8.8)', resolver: '8.8.8.8', color: '#c084fc' }});
            }}

            let outHtml = '';
            for (const t of tasks) {{
                try {{
                    const r = await runSingleHttpProbe(host, t.resolver);
                    if (r && r.total > 0) {{
                        const geoName = determineClientGeo(r.headers, r.tcp);
                        const statusColor = r.total < 150 ? '#34d399' : (r.total < 350 ? '#fde047' : '#f87171');
                        const statusText = r.total < 150 ? '✔ 順暢良好' : (r.total < 350 ? '⚠️ 輕微延遲' : '🚨 嚴重卡頓');

                        outHtml += `
                        <div style="margin-bottom:16px;background:#090d16;padding:14px;border-radius:8px;border:1px solid #334155;">
                            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                                <strong style="color:${{t.color}};font-size:15px;">📡 ${{t.label}}</strong>
                                <span style="color:${{statusColor}};font-weight:bold;font-size:14px;">${{statusText}} (總體感 ${{r.total}} ms)</span>
                            </div>
                            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;font-size:12px;color:#cbd5e1;margin-bottom:10px;">
                                <div>落地節點：<strong style="color:#f8fafc;">${{geoName}}</strong></div>
                                <div>TCP 握手：<strong style="color:#a855f7;">${{r.tcp}} ms</strong></div>
                                <div>解析 IP：<span style="font-family:monospace;color:#38bdf8;">${{r.ip}}</span></div>
                            </div>
                            <pre style="margin:0;color:#94a3b8;font-size:12px;line-height:1.4;white-space:pre-wrap;word-break:break-all;background:#020617;padding:10px;border-radius:6px;max-height:160px;overflow-y:auto;">${{r.headers || '無標頭輸出'}}</pre>
                        </div>
                        `;
                    }} else {{
                        outHtml += `<div style="color:#fb7185;padding:12px;background:#090d16;border-radius:8px;margin-bottom:12px;">❌ [${{t.label}}] 探測逾時或連線失敗</div>`;
                    }}
                }} catch(err) {{
                    outHtml += `<div style="color:#fb7185;padding:12px;background:#090d16;border-radius:8px;margin-bottom:12px;">❌ [${{t.label}}] 探測錯誤: ${{err.message}}</div>`;
                }}
            }}

            contentEl.innerHTML = outHtml;
            btn.innerText = originText;
            btn.disabled = false;
        }}

        {chart_init_js}
    </script>
</body>
</html>
"""

with open("egress.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 成功更新：對齊 index 頂部導航、加入卡片即時單點複測 Modal、修正重新整理按鈕說明。")
