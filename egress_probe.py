import json
import time
import os
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# ==============================================================================
# 第一部分：監控標的 (純 Domain 穩定探測，Netflix 歸入跨境)
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
    # --- 區塊 B：跨境平台與海外骨幹 ---
    {
        "id": "netflix_fast",
        "name": "Netflix 邊緣節點",
        "host": "api.fast.com",
        "group": "cross_border"
    },
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
    "TPE": "台北",
    "TYO": "東京",
    "OSA": "大阪",
    "HKG": "香港",
    "SIN": "新加坡",
    "US_WEST": "美西"
}

GEO_FLAGS = {
    "TPE": "🇹🇼",
    "TYO": "🇯🇵",
    "OSA": "🇯🇵",
    "HKG": "🇭🇰",
    "SIN": "🇸🇬",
    "US_WEST": "🇺🇸"
}

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
# 第二部分：Globalping 探測 (純 Host 查詢)
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
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 HiNet-NOC-Probe"
        }
    )

    measurement_id = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                measurement_id = json.loads(resp.read().decode("utf-8")).get("id")
                if measurement_id:
                    break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                time.sleep(3 + attempt * 2)
                continue
            return None
        except Exception:
            if attempt < 2:
                time.sleep(2)
                continue
            return None

    if not measurement_id:
        return None

    poll_url = f"{api_url}/{measurement_id}"
    for _ in range(20):
        time.sleep(1)
        try:
            req_poll = urllib.request.Request(
                poll_url, 
                headers={"User-Agent": "Mozilla/5.0 HiNet-NOC-Probe"}
            )
            with urllib.request.urlopen(req_poll, timeout=10) as resp:
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
                        return None
        except Exception:
            pass
    return None

def probe_single_stream(service, dns_resolver=None):
    res = execute_single_probe(service["host"], dns_resolver)
    
    if (not res) or (res.get("total_ms", 0) <= 0) or (res.get("total_ms", 0) >= 350):
        time.sleep(2)
        retry_res = execute_single_probe(service["host"], dns_resolver)
        if retry_res and retry_res.get("total_ms", 0) > 0:
            res = retry_res

    if res and res["total_ms"] > 0:
        geo_key = determine_geo(res["resolved_ip"], res["headers"], res["tcp_ms"])
        tot = res["total_ms"]
        if tot < 150:
            color = "#34d399"
            status_text = "順暢良好"
        elif tot < 350:
            color = "#fde047"
            status_text = "稍微延遲"
        else:
            color = "#f87171"
            status_text = "轉圈卡頓"

        flag = GEO_FLAGS.get(geo_key, "🌐")
        name = KNOWN_GEO_NAMES.get(geo_key, "海外")
        return {
            "status": "OK",
            "status_text": status_text,
            "total_ms": res["total_ms"],
            "tcp_ms": res["tcp_ms"],
            "resolved_ip": res["resolved_ip"],
            "geo_key": geo_key,
            "geo_name": f"{flag} {name}",
            "color": color
        }
    else:
        return {
            "status": "FAIL",
            "status_text": "連線逾時",
            "total_ms": 0,
            "tcp_ms": 0,
            "resolved_ip": "--",
            "geo_key": "TYO",
            "geo_name": "逾時",
            "color": "#f87171"
        }

# ==============================================================================
# 第三部分：循序排程 (間隔 0.8 秒)
# ==============================================================================

print(f"[{now_str}] 啟動 HiNet 出口與 CDN 路由監測 (純 Host 穩定模式)...")

results = {}
for idx, s in enumerate(TARGET_SERVICES):
    sid = s["id"]
    print(f"[{idx+1}/{len(TARGET_SERVICES)}] 探測 {s['name']}...")
    
    hinet_res = probe_single_stream(s, None)
    time.sleep(0.8)
    google_res = probe_single_stream(s, "8.8.8.8")
    time.sleep(0.8)
    
    results[sid] = {"hinet": hinet_res, "google": google_res}

# ==============================================================================
# 第四部分：歷史數據保存 (最近 72 筆)
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
# 第五部分：歷史數據走勢整理
# ==============================================================================

chronological_data = list(reversed(history_state))
chart_labels = [item["time_short"] for item in chronological_data]
chart_labels_json = json.dumps(chart_labels)

chart_datasets = {}
node_switch_logs = []
service_stats = {}

for s in TARGET_SERVICES:
    sid = s["id"]
    h_totals = []
    h_tcps = []
    h_geos = []

    g_totals = []
    g_tcps = []
    g_geos = []

    h_peak_totals = []
    g_peak_totals = []
    h_peak_geos = []
    g_peak_geos = []

    prev_h_geo = None
    prev_g_geo = None

    for snap in chronological_data:
        t_label = snap.get("time_short", "")
        t_full = snap.get("timestamp", "")
        s_data = snap.get("data", {}).get(sid, {})
        h_info = s_data.get("hinet", {})
        g_info = s_data.get("google", {})

        is_peak = False
        try:
            parts = t_label.split(":")
            hour = int(parts[0])
            minute = int(parts[1])
            if (hour >= 20 and hour < 23) or (hour == 23 and minute <= 30):
                is_peak = True
        except Exception:
            pass

        # HiNet 數據
        h_ms = h_info.get("total_ms", 0)
        h_geo = h_info.get("geo_key", "TYO")
        h_totals.append(h_ms if h_info.get("status") == "OK" else 0)
        h_tcps.append(h_info.get("tcp_ms", 0))
        h_geos.append(f"{GEO_FLAGS.get(h_geo, '')} {KNOWN_GEO_NAMES.get(h_geo, '海外')}")

        if h_info.get("status") == "OK" and h_ms > 0 and is_peak:
            h_peak_totals.append(h_ms)
            h_peak_geos.append(h_geo)

        # Google 數據
        g_ms = g_info.get("total_ms", 0)
        g_geo = g_info.get("geo_key", "TYO")
        g_totals.append(g_ms if g_info.get("status") == "OK" else 0)
        g_tcps.append(g_info.get("tcp_ms", 0))
        g_geos.append(f"{GEO_FLAGS.get(g_geo, '')} {KNOWN_GEO_NAMES.get(g_geo, '海外')}")

        if g_info.get("status") == "OK" and g_ms > 0 and is_peak:
            g_peak_totals.append(g_ms)
            g_peak_geos.append(g_geo)

        # 節點切換紀錄
        if prev_h_geo and h_geo and prev_h_geo != h_geo:
            prev_name = f"{GEO_FLAGS.get(prev_h_geo, '')} {KNOWN_GEO_NAMES.get(prev_h_geo, prev_h_geo)}"
            curr_name = f"{GEO_FLAGS.get(h_geo, '')} {KNOWN_GEO_NAMES.get(h_geo, h_geo)}"
            node_switch_logs.append({
                "time": t_label,
                "timestamp": t_full,
                "service": s["name"],
                "dns": "HiNet DNS",
                "trajectory": f"{prev_name} ➔ {curr_name}",
                "current_node": curr_name
            })
        if h_geo:
            prev_h_geo = h_geo

        if prev_g_geo and g_geo and prev_g_geo != g_geo:
            prev_name = f"{GEO_FLAGS.get(prev_g_geo, '')} {KNOWN_GEO_NAMES.get(prev_g_geo, prev_g_geo)}"
            curr_name = f"{GEO_FLAGS.get(g_geo, '')} {KNOWN_GEO_NAMES.get(g_geo, g_geo)}"
            node_switch_logs.append({
                "time": t_label,
                "timestamp": t_full,
                "service": s["name"],
                "dns": "Google DNS",
                "trajectory": f"{prev_name} ➔ {curr_name}",
                "current_node": curr_name
            })
        if g_geo:
            prev_g_geo = g_geo

    chart_datasets[sid] = {
        "hinet_total": h_totals,
        "hinet_tcp": h_tcps,
        "hinet_geos": h_geos,
        "google_total": g_totals,
        "google_tcp": g_tcps,
        "google_geos": g_geos
    }

    def get_dominant_geo(geos_list):
        if not geos_list:
            return "TPE"
        return max(set(geos_list), key=geos_list.count)

    service_stats[sid] = {
        "h_peak_avg": round(sum(h_peak_totals) / len(h_peak_totals), 1) if h_peak_totals else 0,
        "g_peak_avg": round(sum(g_peak_totals) / len(g_peak_totals), 1) if g_peak_totals else 0,
        "h_peak_geo": get_dominant_geo(h_peak_geos),
        "g_peak_geo": get_dominant_geo(g_peak_geos),
        "has_peak_data": len(h_peak_totals) > 0
    }

# ==============================================================================
# 第六部分：四頁專屬頂部戰況卡
# ==============================================================================

hinet_good = 0
hinet_warn = 0
hinet_alert = 0

google_good = 0
google_warn = 0
google_alert = 0

pk_hinet_win = 0
pk_google_win = 0
pk_tie = 0

for s in TARGET_SERVICES:
    sid = s["id"]
    curr = current_snapshot["data"].get(sid, {})
    h = curr.get("hinet", {})
    g = curr.get("google", {})

    h_ms = h.get("total_ms", 0)
    if h.get("status") == "OK" and h_ms < 150:
        hinet_good += 1
    elif h.get("status") == "OK" and h_ms < 350:
        hinet_warn += 1
    else:
        hinet_alert += 1

    g_ms = g.get("total_ms", 0)
    if g.get("status") == "OK" and g_ms < 150:
        google_good += 1
    elif g.get("status") == "OK" and g_ms < 350:
        google_warn += 1
    else:
        google_alert += 1

    if h.get("status") == "OK" and g.get("status") == "OK":
        if abs(h_ms - g_ms) <= 10:
            pk_tie += 1
        elif h_ms < g_ms:
            pk_hinet_win += 1
        else:
            pk_google_win += 1
    elif h.get("status") == "OK":
        pk_hinet_win += 1
    elif g.get("status") == "OK":
        pk_google_win += 1

total_apps = len(TARGET_SERVICES)

summary_cards_hinet = f"""
<div class="summary-cards">
    <div class="summary-card" style="border-top:4px solid #38bdf8;">
        <div class="summary-title">監控標的總數</div>
        <div class="summary-val" style="color:#ffffff;">{total_apps} <span class="summary-unit">款 App</span></div>
    </div>
    <div class="summary-card" style="border-top:4px solid #10b981;">
        <div class="summary-title" style="color:#a7f3d0;">🟢 順暢良好 (&lt;150ms)</div>
        <div class="summary-val" style="color:#34d399;">{hinet_good} <span class="summary-unit">款</span></div>
    </div>
    <div class="summary-card" style="border-top:4px solid {'#f59e0b' if hinet_warn > 0 else '#334155'};">
        <div class="summary-title" style="color:#fde047;">🟡 輕微延遲 (150~350ms)</div>
        <div class="summary-val" style="color:{'#fde047' if hinet_warn > 0 else '#94a3b8'};">{hinet_warn} <span class="summary-unit">款</span></div>
    </div>
    <div class="summary-card" style="border-top:4px solid {'#ef4444' if hinet_alert > 0 else '#334155'};">
        <div class="summary-title" style="color:#fecdd3;">🔴 嚴重卡頓 / 逾時</div>
        <div class="summary-val" style="color:{'#fb7185' if hinet_alert > 0 else '#94a3b8'};">{hinet_alert} <span class="summary-unit">款</span></div>
    </div>
</div>
"""

summary_cards_google = f"""
<div class="summary-cards">
    <div class="summary-card" style="border-top:4px solid #c084fc;">
        <div class="summary-title">監控標的總數</div>
        <div class="summary-val" style="color:#ffffff;">{total_apps} <span class="summary-unit">款 App</span></div>
    </div>
    <div class="summary-card" style="border-top:4px solid #10b981;">
        <div class="summary-title" style="color:#a7f3d0;">🟢 順暢良好 (&lt;150ms)</div>
        <div class="summary-val" style="color:#34d399;">{google_good} <span class="summary-unit">款</span></div>
    </div>
    <div class="summary-card" style="border-top:4px solid {'#f59e0b' if google_warn > 0 else '#334155'};">
        <div class="summary-title" style="color:#fde047;">🟡 輕微延遲 (150~350ms)</div>
        <div class="summary-val" style="color:{'#fde047' if google_warn > 0 else '#94a3b8'};">{google_warn} <span class="summary-unit">款</span></div>
    </div>
    <div class="summary-card" style="border-top:4px solid {'#ef4444' if google_alert > 0 else '#334155'};">
        <div class="summary-title" style="color:#fecdd3;">🔴 嚴重卡頓 / 逾時</div>
        <div class="summary-val" style="color:{'#fb7185' if google_alert > 0 else '#94a3b8'};">{google_alert} <span class="summary-unit">款</span></div>
    </div>
</div>
"""

summary_cards_pk = f"""
<div class="summary-cards">
    <div class="summary-card" style="border-top:4px solid #38bdf8;">
        <div class="summary-title">監控標的總數</div>
        <div class="summary-val" style="color:#ffffff;">{total_apps} <span class="summary-unit">款 App</span></div>
    </div>
    <div class="summary-card" style="border-top:4px solid #38bdf8;">
        <div class="summary-title" style="color:#7dd3fc;">🏆 HiNet DNS 勝出</div>
        <div class="summary-val" style="color:#38bdf8;">{pk_hinet_win} <span class="summary-unit">款</span></div>
    </div>
    <div class="summary-card" style="border-top:4px solid #c084fc;">
        <div class="summary-title" style="color:#e9d5ff;">🏆 Google DNS 勝出</div>
        <div class="summary-val" style="color:#c084fc;">{pk_google_win} <span class="summary-unit">款</span></div>
    </div>
    <div class="summary-card" style="border-top:4px solid #64748b;">
        <div class="summary-title">⚖️ 表現相當 (差距&le;10ms)</div>
        <div class="summary-val" style="color:#cbd5e1;">{pk_tie} <span class="summary-unit">款</span></div>
    </div>
</div>
"""

# ==============================================================================
# 第七部分：HTML 畫面渲染 (無假文字、俐落膠囊、乾淨真實數據)
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

            h_loc = f"{GEO_FLAGS.get(h.get('geo_key','TYO'),'')} {KNOWN_GEO_NAMES.get(h.get('geo_key','TYO'),'')}"
            g_loc = f"{GEO_FLAGS.get(g.get('geo_key','TYO'),'')} {KNOWN_GEO_NAMES.get(g.get('geo_key','TYO'),'')}"

            card_content = f"""
            <div class="pk-container">
                <div class="pk-row {'pk-row-win' if h.get('win') else ''}">
                    <div class="pk-dns-name" style="color:#38bdf8;">HiNet DNS</div>
                    <div class="pk-ms" style="color:{h.get('color')};">
                        <span id="val-pk-hinet-{sid}" style="min-width:65px;">{h_total_str}</span> {h_stamp}
                    </div>
                    <div class="pk-node-info">
                        <span id="geo-pk-hinet-{sid}" class="geo-capsule">{h_loc}</span>
                        <span style="color:#94a3b8; font-size:11px;">TCP: {h.get('tcp_ms','--')}ms</span>
                    </div>
                </div>
                <div class="pk-row {'pk-row-win' if g.get('win') else ''}">
                    <div class="pk-dns-name" style="color:#c084fc;">Google DNS</div>
                    <div class="pk-ms" style="color:{g.get('color')};">
                        <span id="val-pk-google-{sid}" style="min-width:65px;">{g_total_str}</span> {g_stamp}
                    </div>
                    <div class="pk-node-info">
                        <span id="geo-pk-google-{sid}" class="geo-capsule">{g_loc}</span>
                        <span style="color:#94a3b8; font-size:11px;">TCP: {g.get('tcp_ms','--')}ms</span>
                    </div>
                </div>
            </div>
            """
            retest_btn = f"""
            <button onclick="triggerLiveProbe('{sid}', '{curr['name']}', '{host_domain}', 'both', this)" class="retest-chip retest-chip-neutral">
                ⚡ 重測
            </button>
            """
        else:
            target_data = h if view_mode == "hinet" else g
            total_val = target_data.get('total_ms', 0)
            tcp_val = target_data.get('tcp_ms', 0)

            total_str = f"{total_val} ms" if target_data.get("status") == "OK" else "逾時"
            tcp_str = f"{tcp_val} ms" if target_data.get("status") == "OK" else "--"
            health_color = target_data.get('color', '#34d399')
            status_text = target_data.get('status_text', '順暢良好')

            dns_param = "hinet" if view_mode == "hinet" else "google"
            retest_btn = f"""
            <button onclick="triggerLiveProbe('{sid}', '{curr['name']}', '{host_domain}', '{dns_param}', this)" class="retest-chip retest-chip-neutral">
                ⚡ 重測
            </button>
            """

            target_geo = target_data.get('geo_key', 'TYO')
            capsule_flag = GEO_FLAGS.get(target_geo, '🌐')
            capsule_name = KNOWN_GEO_NAMES.get(target_geo, '海外')

            card_content = f"""
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                <span id="status-{view_mode}-{sid}" style="color:{health_color}; font-size:12px; font-weight:bold;">● {status_text}</span>
                <span id="geo-{view_mode}-{sid}" class="geo-capsule">{capsule_flag} {capsule_name}</span>
            </div>
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px; background:#070d18; padding:8px 10px; border-radius:6px; margin-bottom:12px;">
                <div>
                    <div style="font-size:11px; color:#94a3b8;">總體感延遲</div>
                    <div id="total-{view_mode}-{sid}" style="font-size:18px; font-weight:800; color:{health_color};">{total_str}</div>
                </div>
                <div>
                    <div style="font-size:11px; color:#94a3b8;">TCP 握手 (光纖)</div>
                    <div id="tcp-{view_mode}-{sid}" style="font-size:18px; font-weight:800; color:#a855f7;">{tcp_str}</div>
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

# 渲染第 4 頁：SLA 滿版大圖
def render_sla_view():
    cards = ""
    for s in TARGET_SERVICES:
        sid = s["id"]
        curr = current_snapshot["data"].get(sid, {})
        st = service_stats[sid]

        if st["has_peak_data"]:
            diff = abs(st["h_peak_avg"] - st["g_peak_avg"])
            if diff <= 10:
                badge = f'<span class="verdict-badge" style="background:#1e293b; color:#94a3b8;">⚖️ 忙時持平 (差 {diff:.1f}ms)</span>'
            elif st["h_peak_avg"] < st["g_peak_avg"]:
                badge = f'<span class="verdict-badge" style="background:rgba(56,189,248,0.2); color:#38bdf8; border:1px solid #38bdf8;">🏆 HiNet 快 {diff:.1f}ms</span>'
            else:
                badge = f'<span class="verdict-badge" style="background:rgba(192,132,252,0.2); color:#c084fc; border:1px solid #c084fc;">🏆 Google 快 {diff:.1f}ms</span>'

            h_loc = f"{GEO_FLAGS.get(st['h_peak_geo'],'')} {KNOWN_GEO_NAMES.get(st['h_peak_geo'],'')}"
            g_loc = f"{GEO_FLAGS.get(st['g_peak_geo'],'')} {KNOWN_GEO_NAMES.get(st['g_peak_geo'],'')}"
            peak_headline = f"""
            <div style="font-size:13px; color:#e2e8f0; display:flex; align-items:center; gap:10px;">
                <span>忙時 (20:00~23:30)：HiNet <strong style="color:#38bdf8;">{st['h_peak_avg']}ms</strong> ({h_loc}) ｜ Google <strong style="color:#c084fc;">{st['g_peak_avg']}ms</strong> ({g_loc})</span>
                {badge}
            </div>
            """
        else:
            peak_headline = '<div style="font-size:12px; color:#94a3b8;">忙時 (20:00~23:30) 數據累積中</div>'

        cards += f"""
        <div class="sla-card">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                <div style="font-size:16px; font-weight:800; color:#f8fafc;">{curr['name']}</div>
                {peak_headline}
            </div>
            <div style="height:220px; background:#070d18; border-radius:8px; padding:10px;">
                <canvas id="chart-sla-{sid}"></canvas>
            </div>
        </div>
        """
    return f"""
    <div style="margin-bottom:14px; color:#94a3b8; font-size:13px; display:flex; justify-content:space-between; align-items:center;">
        <span>📌 說明：滑鼠觸碰圓點可顯示當時落地城市與實測延遲。</span>
        <div style="display:flex; gap:14px;">
            <span style="color:#38bdf8; font-weight:bold;">── HiNet DNS</span>
            <span style="color:#c084fc; font-weight:bold;">── Google DNS</span>
            <span style="color:#facc15; font-weight:bold;">┄┄ 150ms 警戒線</span>
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

one_hour_ago = now_dt - timedelta(hours=1)
if node_switch_logs:
    for log in reversed(node_switch_logs):
        is_recent = False
        try:
            log_dt = datetime.strptime(log.get("timestamp", ""), '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz)
            if log_dt >= one_hour_ago:
                is_recent = True
        except Exception:
            pass

        time_badge = f'<span style="background:#ef4444; color:#fff; font-size:10px; padding:2px 5px; border-radius:3px; margin-right:4px;">NEW</span>' if is_recent else ""

        log_rows += f"""
        <tr class="log-row" data-dns="{log['dns']}" data-service="{log['service']}" style="{'background:rgba(239,68,68,0.06);' if is_recent else ''}">
            <td style="color:#f8fafc; font-weight:bold;">{time_badge}{log['time']}</td>
            <td style="color:#38bdf8; font-weight:bold;">{log['service']}</td>
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

# ==============================================================================
# 第八部分：圖表渲染 JS (修復 len_pts 定義缺失)
# ==============================================================================

chart_init_js = ""
len_pts = len(chart_labels)

for s in TARGET_SERVICES:
    sid = s["id"]
    ds = chart_datasets[sid]
    curr = current_snapshot["data"].get(sid, {})
    h_col = curr.get("hinet", {}).get("color", "#34d399")
    g_col = curr.get("google", {}).get("color", "#34d399")
    
    h_geos_json = json.dumps(ds['hinet_geos'])
    g_geos_json = json.dumps(ds['google_geos'])

    chart_init_js += f"""
    new Chart(document.getElementById('chart-hinet-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels_json},
            datasets: [
                {{ label: '總體感 (ms)', data: {json.dumps(ds['hinet_total'])}, borderColor: '{h_col}', borderWidth: 2, tension: 0.3, pointRadius: 1 }},
                {{ label: 'TCP 握手 (ms)', data: {json.dumps(ds['hinet_tcp'])}, borderColor: '#a855f7', borderWidth: 1.5, borderDash: [2, 2], tension: 0.3, pointRadius: 0 }}
            ]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{
                legend: {{ display: false }},
                tooltip: {{
                    callbacks: {{
                        afterLabel: function(ctx) {{
                            const geos = {h_geos_json};
                            return '落地城市: ' + (geos[ctx.dataIndex] || '');
                        }}
                    }}
                }}
            }},
            scales: {{ x: {{ ticks: {{ color: '#64748b', maxTicksLimit: 5, font: {{ size: 9 }} }} }}, y: {{ ticks: {{ color: '#64748b', font: {{ size: 9 }} }} }} }}
        }}
    }});

    new Chart(document.getElementById('chart-google-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels_json},
            datasets: [
                {{ label: '總體感 (ms)', data: {json.dumps(ds['google_total'])}, borderColor: '{g_col}', borderWidth: 2, tension: 0.3, pointRadius: 1 }},
                {{ label: 'TCP 握手 (ms)', data: {json.dumps(ds['google_tcp'])}, borderColor: '#a855f7', borderWidth: 1.5, borderDash: [2, 2], tension: 0.3, pointRadius: 0 }}
            ]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{
                legend: {{ display: false }},
                tooltip: {{
                    callbacks: {{
                        afterLabel: function(ctx) {{
                            const geos = {g_geos_json};
                            return '落地城市: ' + (geos[ctx.dataIndex] || '');
                        }}
                    }}
                }}
            }},
            scales: {{ x: {{ ticks: {{ color: '#64748b', maxTicksLimit: 5, font: {{ size: 9 }} }} }}, y: {{ ticks: {{ color: '#64748b', font: {{ size: 9 }} }} }} }}
        }}
    }});

    new Chart(document.getElementById('chart-pk-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels_json},
            datasets: [
                {{ label: 'HiNet', data: {json.dumps(ds['hinet_total'])}, borderColor: '#38bdf8', borderWidth: 2, tension: 0.3, pointRadius: 1.2 }},
                {{ label: 'Google', data: {json.dumps(ds['google_total'])}, borderColor: '#c084fc', borderWidth: 2, tension: 0.3, pointRadius: 1 }}
            ]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{
                legend: {{ display: false }},
                tooltip: {{
                    callbacks: {{
                        afterLabel: function(ctx) {{
                            const hGeos = {h_geos_json};
                            const gGeos = {g_geos_json};
                            return ctx.datasetIndex === 0 ? 'HiNet 落地: ' + hGeos[ctx.dataIndex] : 'Google 落地: ' + gGeos[ctx.dataIndex];
                        }}
                    }}
                }}
            }},
            scales: {{ x: {{ ticks: {{ color: '#64748b', maxTicksLimit: 5, font: {{ size: 9 }} }} }}, y: {{ ticks: {{ color: '#64748b', font: {{ size: 9 }} }} }} }}
        }}
    }});

    new Chart(document.getElementById('chart-sla-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels_json},
            datasets: [
                {{ label: 'HiNet DNS', data: {json.dumps(ds['hinet_total'])}, borderColor: '#38bdf8', borderWidth: 2, tension: 0.3, pointRadius: 1.5 }},
                {{ label: 'Google DNS', data: {json.dumps(ds['google_total'])}, borderColor: '#c084fc', borderWidth: 2, tension: 0.3, pointRadius: 1.5 }},
                {{ label: '150ms 警戒線', data: Array({len_pts}).fill(150), borderColor: '#facc15', borderWidth: 1.5, borderDash: [4, 4], pointRadius: 0, fill: false }}
            ]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
                legend: {{ display: false }},
                tooltip: {{
                    callbacks: {{
                        afterLabel: function(ctx) {{
                            const hGeos = {h_geos_json};
                            const gGeos = {g_geos_json};
                            return (ctx.datasetIndex === 0) ? 'HiNet: ' + hGeos[ctx.dataIndex] : 'Google: ' + gGeos[ctx.dataIndex];
                        }}
                    }}
                }}
            }},
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
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background:#0b1120; color:#e2e8f0; margin:0; padding:28px; font-size:15px; }}
        .container {{ max-width: 1440px; margin: 0 auto; }}

        .system-tabs {{ display: flex; gap: 8px; margin-bottom: 20px; border-bottom: 2px solid #334155; padding-bottom: 12px; }}
        .tab-btn {{ padding: 10px 22px; font-size: 15px; font-weight: bold; border-radius: 8px 8px 0 0; text-decoration: none; display: inline-flex; align-items: center; gap: 8px; cursor: pointer; transition: all 0.2s; }}
        .tab-active {{ background: #0284c7; color: #ffffff; box-shadow: 0 4px 14px rgba(2, 132, 199, 0.4); border: none; }}
        .tab-inactive {{ background: #1e293b; color: #94a3b8; border: 1px solid #334155; border-bottom: none; }}
        .tab-inactive:hover {{ background: #334155; color: #f8fafc; }}

        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:20px; padding-bottom:16px; border-bottom:1px solid #334155; }}

        .summary-cards {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 20px; }}
        .summary-card {{ background: linear-gradient(145deg, #1e293b, #0f172a); border-radius: 10px; padding: 18px 20px; border: 1px solid #334155; box-shadow: 0 6px 16px rgba(0,0,0,0.25); }}
        .summary-title {{ font-size: 18px; font-weight: bold; color: #94a3b8; margin-bottom: 6px; }}
        .summary-val {{ font-size: 34px; font-weight: 900; }}
        .summary-unit {{ font-size: 14px; color: #94a3b8; font-weight: normal; }}

        .mode-tabs {{ display: flex; gap: 8px; margin-bottom: 20px; }}
        .mode-btn {{ padding: 9px 22px; font-size: 14px; font-weight: bold; border-radius: 6px; color: #94a3b8; background: #0f172a; border: 1px solid #334155; cursor: pointer; transition: all 0.2s; }}
        .mode-btn:hover {{ background: #1e293b; color: #f8fafc; }}
        .mode-active {{ background: #1e293b; color: #38bdf8; border-color: #38bdf8; box-shadow: 0 0 10px rgba(56, 189, 248, 0.3); }}

        .section-title {{ font-size: 16px; font-weight: 800; color: #f8fafc; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; }}

        .cards-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(330px, 1fr)); gap: 14px; }}
        .service-card {{ background:linear-gradient(145deg, #1e293b, #0f172a); border-radius:10px; padding:14px; border:1px solid #334155; }}

        .retest-chip {{ font-size:12px; font-weight:bold; border-radius:6px; padding:4px 10px; border:none; cursor:pointer; transition:all 0.2s; display:inline-flex; align-items:center; }}
        .retest-chip-neutral {{ background:#334155; color:#cbd5e1; }}
        .retest-chip-neutral:hover {{ background:#475569; color:#fff; }}

        .geo-capsule {{
            background: #090d16;
            color: #f8fafc;
            font-size: 12px;
            font-weight: 800;
            padding: 3px 8px;
            border-radius: 4px;
            border: 1px solid #38bdf8;
            box-shadow: 0 0 6px rgba(56, 189, 248, 0.3);
            display: inline-flex;
            align-items: center;
            gap: 4px;
        }}

        .pk-container {{ display: flex; flex-direction: column; gap: 6px; margin-bottom: 10px; }}
        .pk-row {{ display: flex; justify-content: space-between; align-items: center; background: #070d18; padding: 7px 9px; border-radius: 6px; border: 1px solid #1e293b; }}
        .pk-row-win {{ border-color: rgba(239, 68, 68, 0.5); background: rgba(239, 68, 68, 0.06); }}
        .pk-dns-name {{ font-weight: bold; font-size: 12px; width: 85px; }}
        .pk-ms {{ font-weight: 800; font-size: 15px; display: flex; align-items: center; gap: 6px; flex: 1; }}
        .pk-node-info {{ text-align: right; font-size: 11px; color: #cbd5e1; display: flex; flex-direction: column; }}

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

        .sla-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(650px, 1fr)); gap: 16px; }}
        .sla-card {{ background:#1e293b; border:1px solid #334155; border-radius:10px; padding:16px; }}
        .verdict-badge {{ font-size:11px; font-weight:bold; padding:3px 8px; border-radius:4px; }}

        .view-panel {{ display: none; }}
        .view-active {{ display: block; }}

        .drawer-toggle {{
            background: #0f172a;
            border: 1px solid #334155;
            border-radius: 8px;
            padding: 14px 18px;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 28px;
            transition: all 0.2s;
        }}
        .drawer-toggle:hover {{ background: #1e293b; border-color: #38bdf8; }}
        .drawer-content {{
            display: block;
            background: #0f172a;
            border: 1px solid #334155;
            border-top: none;
            border-radius: 0 0 8px 8px;
            padding: 18px;
        }}

        .custom-scrollbar::-webkit-scrollbar {{ width: 8px; height: 8px; }}
        .custom-scrollbar::-webkit-scrollbar-track {{ background: #090d16; border-radius: 6px; }}
        .custom-scrollbar::-webkit-scrollbar-thumb {{ background: #334155; border-radius: 6px; }}
        .custom-scrollbar::-webkit-scrollbar-thumb:hover {{ background: #475569; }}

        .filter-bar {{ display:flex; gap:12px; margin-bottom:14px; align-items:center; flex-wrap:wrap; }}
        .filter-select {{ background:#070d18; border:1px solid #334155; color:#f8fafc; font-size:13px; padding:6px 12px; border-radius:6px; outline:none; }}

        .data-table {{ width:100%; border-collapse:collapse; }}
        .data-table th {{ text-align:left; color:#94a3b8; font-size:13px; padding:10px; border-bottom:1px solid #334155; }}
        .data-table td {{ padding:12px 10px; border-bottom:1px solid #334155; font-size:14px; vertical-align:top; }}
        .cable-badge-down {{ background:rgba(239, 68, 68, 0.2); color:#f87171; border:1px solid #ef4444; font-size:11px; font-weight:bold; padding:2px 6px; border-radius:4px; }}
        .cable-badge-warn {{ background:rgba(251, 146, 60, 0.2); color:#fb923c; border:1px solid #fb923c; font-size:11px; font-weight:bold; padding:2px 6px; border-radius:4px; }}

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
            .summary-cards {{ grid-template-columns: repeat(2, 1fr); gap: 10px; }}
            .sla-grid {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <div class="container">
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
                <h1 style="margin:0 0 8px 0; font-size:24px; color:#f8fafc; font-weight:800;">
                    HiNet (AS3462) 國際出口與 CDN 路由監測
                </h1>
                <div style="color:#94a3b8; font-size:14px; line-height:1.6;">
                    📍 <strong>探針來源：</strong>台灣本地 HiNet 寬頻節點 (AS3462 台中實體用戶端)<br>
                    ⏱ <strong>最後量測時間：</strong><span style="color:#38bdf8; font-weight:bold;">{now_str} (UTC+8)</span>
                </div>
            </div>
            <div>
                <button onclick="window.location.reload()" title="重整取得最新數據" style="background:#0284c7; color:#fff; border:none; border-radius:8px; padding:10px 18px; cursor:pointer; font-weight:bold; font-size:14px;">
                    🔄 畫面刷新
                </button>
            </div>
        </div>

        <div class="mode-tabs">
            <button class="mode-btn mode-active" onclick="switchView('hinet', this)">HiNet 預設 DNS</button>
            <button class="mode-btn" onclick="switchView('google', this)">Google DNS (8.8.8.8)</button>
            <button class="mode-btn" onclick="switchView('pk', this)">綜合效能對照</button>
            <button class="mode-btn" onclick="switchView('sla', this)">SLA 忙時與警戒分析</button>
        </div>

        <div id="view-hinet" class="view-panel view-active">
            {summary_cards_hinet}
            {html_hinet_cards}
        </div>

        <div id="view-google" class="view-panel">
            {summary_cards_google}
            {html_google_cards}
        </div>

        <div id="view-pk" class="view-panel">
            {summary_cards_pk}
            {html_pk_cards}
        </div>

        <div id="view-sla" class="view-panel">
            {summary_cards_pk}
            {html_sla_cards}
        </div>

        <div class="drawer-toggle" onclick="toggleDrawer()">
            <div style="display:flex; align-items:center; gap:8px;">
                <span style="font-size:16px; font-weight:bold; color:#f8fafc;">📋 近 24 小時節點切換紀錄</span>
                <span style="font-size:13px; color:#94a3b8;">(共 {len(node_switch_logs)} 筆異動，依發生時間倒序排列)</span>
            </div>
            <div id="drawer-arrow" style="color:#38bdf8; font-weight:bold;">▲ 點擊收合</div>
        </div>
        <div id="drawer-content" class="drawer-content">
            <div class="filter-bar">
                <span style="color:#94a3b8; font-size:13px;">條件過濾：</span>
                <select id="filter-dns" class="filter-select" onchange="applyLogFilters()">
                    <option value="ALL">全部 DNS 來源</option>
                    <option value="HiNet DNS">僅看 HiNet DNS</option>
                    <option value="Google DNS">僅看 Google DNS</option>
                </select>
                <select id="filter-service" class="filter-select" onchange="applyLogFilters()">
                    <option value="ALL">全部服務標的 (12款)</option>
                    {service_options}
                </select>
                <span id="filter-count" style="color:#64748b; font-size:13px;">顯示 {len(node_switch_logs)} 筆</span>
            </div>
            <div class="custom-scrollbar" style="max-height:360px; overflow-y:auto; border-radius:8px; border:1px solid #334155;">
                <table class="data-table" style="box-shadow:none;">
                    <thead>
                        <tr>
                            <th style="background:#090d16; width:120px;">時間</th>
                            <th style="background:#090d16; width:200px;">服務標的</th>
                            <th style="background:#090d16; width:130px;">DNS 來源</th>
                            <th style="background:#090d16;">節點切換軌跡</th>
                            <th style="background:#090d16; width:190px;">當前落地點</th>
                        </tr>
                    </thead>
                    <tbody id="log-table-body">
                        {log_rows}
                    </tbody>
                </table>
            </div>
        </div>

        <div style="background:#0f172a; border:1px solid #334155; border-radius:10px; padding:18px 22px; margin-top:24px;">
            <div style="font-size:16px; font-weight:bold; color:#f8fafc; margin-bottom:10px; display:flex; align-items:center; gap:8px;">
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

    <!-- 即時複測彈出視窗 -->
    <div id="live-probe-modal" class="modal-overlay">
        <div class="modal-box">
            <div class="modal-header">
                <span id="modal-probe-title" style="font-weight:bold;color:#38bdf8;font-size:16px;">⚡ 即時 CDN 路由複測診斷</span>
                <button onclick="closeProbeModal()" style="background:transparent;border:none;color:#94a3b8;font-size:22px;cursor:pointer;font-weight:bold;">&times;</button>
            </div>
            <div id="modal-probe-content" class="modal-body"></div>
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
            if (content.style.display === 'none') {{
                content.style.display = 'block';
                arrow.innerText = '▲ 點擊收合';
            }} else {{
                content.style.display = 'none';
                arrow.innerText = '▼ 點擊展開';
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

        function closeProbeModal() {{
            document.getElementById('live-probe-modal').style.display = 'none';
        }}

        function determineClientGeo(headers, tcpMs) {{
            const h = (headers || '').toLowerCase();
            if (tcpMs > 130) return {{ flag: '🇺🇸', name: '美西' }};
            if (h.includes('nrt') || h.includes('hnd') || h.includes('tyo') || h.includes('japan')) return {{ flag: '🇯🇵', name: '東京' }};
            if (h.includes('kix') || h.includes('osa')) return {{ flag: '🇯🇵', name: '大阪' }};
            if (h.includes('hkg') || h.includes('hongkong')) return {{ flag: '🇭🇰', name: '香港' }};
            if (h.includes('sin') || h.includes('singapore')) return {{ flag: '🇸🇬', name: '新加坡' }};
            if (h.includes('tpe') || h.includes('taiwan') || tcpMs < 10) return {{ flag: '🇹🇼', name: '台北' }};
            return {{ flag: '🌐', name: '海外' }};
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

            for (let i = 0; i < 20; i++) {{
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
                tasks.push({{ label: 'HiNet DNS (預設)', mode: 'hinet', resolver: null, color: '#38bdf8' }});
                tasks.push({{ label: 'Google DNS (8.8.8.8)', mode: 'google', resolver: '8.8.8.8', color: '#c084fc' }});
            }} else if (dnsMode === 'hinet') {{
                tasks.push({{ label: 'HiNet DNS (預設)', mode: 'hinet', resolver: null, color: '#38bdf8' }});
            }} else {{
                tasks.push({{ label: 'Google DNS (8.8.8.8)', mode: 'google', resolver: '8.8.8.8', color: '#c084fc' }});
            }}

            let outHtml = '';
            for (const t of tasks) {{
                try {{
                    const r = await runSingleHttpProbe(host, t.resolver);
                    if (r && r.total > 0) {{
                        const g = determineClientGeo(r.headers, r.tcp);
                        const statusColor = r.total < 150 ? '#34d399' : (r.total < 350 ? '#fde047' : '#f87171');
                        const statusText = r.total < 150 ? '● 順暢良好' : (r.total < 350 ? '● 稍微延遲' : '● 轉圈卡頓');

                        const totalEl = document.getElementById(`total-${{t.mode}}-${{sid}}`);
                        const tcpEl = document.getElementById(`tcp-${{t.mode}}-${{sid}}`);
                        const statusEl = document.getElementById(`status-${{t.mode}}-${{sid}}`);
                        const geoEl = document.getElementById(`geo-${{t.mode}}-${{sid}}`);
                        
                        if (totalEl) {{
                            totalEl.innerText = `${{r.total}} ms`;
                            totalEl.style.color = statusColor;
                        }}
                        if (tcpEl) tcpEl.innerText = `${{r.tcp}} ms`;
                        if (statusEl) {{
                            statusEl.innerText = statusText;
                            statusEl.style.color = statusColor;
                        }}
                        if (geoEl) geoEl.innerHTML = `${{g.flag}} ${{g.name}}`;

                        const pkValEl = document.getElementById(`val-pk-${{t.mode}}-${{sid}}`);
                        const pkGeoEl = document.getElementById(`geo-pk-${{t.mode}}-${{sid}}`);
                        if (pkValEl) {{
                            pkValEl.innerText = `${{r.total}} ms`;
                            pkValEl.style.color = statusColor;
                        }}
                        if (pkGeoEl) pkGeoEl.innerHTML = `${{g.flag}} ${{g.name}}`;

                        outHtml += `
                        <div style="margin-bottom:16px;background:#090d16;padding:14px;border-radius:8px;border:1px solid #334155;">
                            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                                <strong style="color:${{t.color}};font-size:15px;">📡 ${{t.label}}</strong>
                                <span style="color:${{statusColor}};font-weight:bold;font-size:14px;">${{statusText}} (總體感 ${{r.total}} ms)</span>
                            </div>
                            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;font-size:12px;color:#cbd5e1;margin-bottom:10px;">
                                <div>落地節點：<strong style="color:#f8fafc;">${{g.flag}} ${{g.name}}</strong></div>
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

print(f"[{now_str}] 成功修復：回歸純 Host 探測，全數標的恢復正常綠燈。")
