import json
import time
import os
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ==============================================================================
# 第一部分：監控標的設定 (覆蓋主要塞車點、CDN、OTT 與遊戲平台)
# ==============================================================================

TARGET_SERVICES = [
    {
        "id": "fastly",
        "name": "Fastly CDN (X圖片 / GitHub / Reddit)",
        "target": "fastly.com",
        "category": "近期主要塞車點"
    },
    {
        "id": "cloudflare",
        "name": "Cloudflare (Discord / 遊戲API / 1.1.1.1)",
        "target": "1.1.1.1",
        "category": "主流 CDN 骨幹"
    },
    {
        "id": "google",
        "name": "Google / YouTube (8.8.8.8)",
        "target": "8.8.8.8",
        "category": "國際直連指標"
    },
    {
        "id": "akamai",
        "name": "Akamai CDN (Apple AppStore / 微軟更新)",
        "target": "www.akamai.com",
        "category": "主流 CDN 骨幹"
    },
    {
        "id": "aws",
        "name": "AWS CloudFront (海外手遊 / 串流影音)",
        "target": "aws.amazon.com",
        "category": "跨國雲端節點"
    },
    {
        "id": "steam",
        "name": "Steam 遊戲節點 (Valve 海外連線)",
        "target": "store.steampowered.com",
        "category": "跨國遊戲連線"
    }
]

STATE_FILE = "egress_state.json"
tz = timezone(timedelta(hours=8))
now_dt = datetime.now(tz)
now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
time_short = now_dt.strftime('%H:%M')

# ==============================================================================
# 第二部分：Globalping 探測函式 (鎖定 TW 且指定 HiNet ASN 3462 / 9294)
# ==============================================================================

def ping_service(service, ip_version):
    """
    透過 Globalping API 發起 Ping 量測，取得 RTT (延遲) 與 掉包率
    指定來源必須在台灣 (TW) 且 ASN 包含 3462 (HiNet) 或 9294 (行動核心出口)
    """
    api_url = "https://api.globalping.io/v1/measurements"
    
    payload = json.dumps({
        "type": "ping",
        "target": service["target"],
        "locations": [
            {
                "country": "TW",
                "asn": 3462
            }
        ],
        "measurementOptions": {
            "packets": 5,
            "ipVersion": ip_version
        },
        "limit": 1
    }).encode("utf-8")

    req = urllib.request.Request(
        api_url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "HiNet-Egress-Probe"}
    )

    measurement_id = None
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                measurement_id = json.loads(resp.read().decode("utf-8")).get("id")
                break
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(3)
                continue
            return service["id"], ip_version, {"status": "FAIL", "rtt": 0, "loss": 100, "probe": "無回應"}
        except Exception:
            return service["id"], ip_version, {"status": "FAIL", "rtt": 0, "loss": 100, "probe": "連線異常"}

    if not measurement_id:
        return service["id"], ip_version, {"status": "FAIL", "rtt": 0, "loss": 100, "probe": "無測量ID"}

    poll_url = f"{api_url}/{measurement_id}"
    for _ in range(12):
        time.sleep(1)
        try:
            req_poll = urllib.request.Request(poll_url, headers={"User-Agent": "HiNet-Egress-Probe"})
            with urllib.request.urlopen(req_poll, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if data.get("status") == "finished":
                    results = data.get("results", [])
                    if results:
                        res = results[0].get("result", {})
                        probe_info = results[0].get("probe", {})
                        probe_network = f"{probe_info.get('city', 'TW')} (AS{probe_info.get('asn', '3462')})"
                        
                        stats = res.get("stats", {})
                        avg_rtt = stats.get("avg", 0)
                        loss = stats.get("loss", 0)
                        
                        return service["id"], ip_version, {
                            "status": "OK" if loss < 100 else "FAIL",
                            "rtt": round(avg_rtt, 1),
                            "loss": round(loss, 1),
                            "probe": probe_network
                        }
        except Exception:
            pass

    return service["id"], ip_version, {"status": "TIMEOUT", "rtt": 0, "loss": 100, "probe": "逾時"}

# ==============================================================================
# 第三部分：平行執行探測 (IPv4 與 IPv6 雙棧)
# ==============================================================================

print(f"[{now_str}] 啟動 HiNet 出口體感探測 (指定 ASN 3462)...")

tasks = []
for s in TARGET_SERVICES:
    tasks.append((s, 4))
    tasks.append((s, 6))

results = {}
with ThreadPoolExecutor(max_workers=4) as executor:
    future_to_task = {executor.submit(ping_service, t[0], t[1]): (t[0]["id"], t[1]) for t in tasks}
    for future in as_completed(future_to_task):
        svc_id, ip_v, res_data = future.result()
        if svc_id not in results:
            results[svc_id] = {}
        results[svc_id][ip_v] = res_data

# ==============================================================================
# 第四部分：歷史資料滾動管理 (保留 24 小時，約 72 筆)
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
    v4 = results.get(sid, {}).get(4, {"status": "FAIL", "rtt": 0, "loss": 100})
    v6 = results.get(sid, {}).get(6, {"status": "FAIL", "rtt": 0, "loss": 100})

    # 體感狀態判定
    is_congested = False
    status_text = "順暢良好"
    status_color = "#34d399"

    v4_rtt = v4.get("rtt", 0)
    v6_rtt = v6.get("rtt", 0)

    if v4.get("loss", 0) >= 20:
        is_congested = True
        status_text = f"🚨 掉包嚴重 ({v4['loss']}%)"
        status_color = "#f87171"
    elif v4_rtt >= 200:
        is_congested = True
        status_text = f"🚨 延遲暴增 ({v4_rtt}ms)"
        status_color = "#f87171"
    elif v6_rtt > 0 and v4_rtt > (v6_rtt * 2.5) and v4_rtt > 90:
        is_congested = True
        status_text = f"⚠️ IPv4 塞車 (v4:{v4_rtt}ms / v6:{v6_rtt}ms)"
        status_color = "#fbbf24"
    elif v4_rtt >= 80:
        status_text = f"微幅延遲 ({v4_rtt}ms)"
        status_color = "#fde047"

    current_snapshot["data"][sid] = {
        "name": s["name"],
        "category": s["category"],
        "v4_rtt": v4_rtt,
        "v4_loss": v4.get("loss", 0),
        "v6_rtt": v6_rtt,
        "v6_loss": v6.get("loss", 0),
        "probe": v4.get("probe", "HiNet"),
        "status_text": status_text,
        "status_color": status_color,
        "is_congested": is_congested
    }

# 存入滾動歷史 (保留最近 72 筆，以 20 分鐘一筆計算剛好 24 小時)
history_state.insert(0, current_snapshot)
history_state = history_state[:72]

with open(STATE_FILE, "w", encoding="utf-8") as f:
    json.dump(history_state, f, ensure_ascii=False, indent=2)

# ==============================================================================
# 第五部分：前端 HTML 組裝 (整合 Chart.js 繪製 24 小時雙軌折線圖)
# ==============================================================================

# 準備折線圖的時間軸標籤與各服務的歷史數列 (由舊到新排序)
chronological_data = list(reversed(history_state))
chart_labels = json.dumps([item["time_short"] for item in chronological_data])

chart_datasets = {}
for s in TARGET_SERVICES:
    sid = s["id"]
    chart_datasets[sid] = {
        "v4": [item.get("data", {}).get(sid, {}).get("v4_rtt", 0) for item in chronological_data],
        "v6": [item.get("data", {}).get(sid, {}).get("v6_rtt", 0) for item in chronological_data]
    }

cards_html = ""
for s in TARGET_SERVICES:
    sid = s["id"]
    curr = current_snapshot["data"].get(sid, {})
    v4_str = f"{curr['v4_rtt']} ms" if curr['v4_loss'] < 100 else "無法連通"
    v6_str = f"{curr['v6_rtt']} ms" if curr['v6_loss'] < 100 else "無 / 斷線"
    loss_tag = f"<span style='color:#f87171;'> (掉包 {curr['v4_loss']}%)</span>" if curr['v4_loss'] > 0 else ""

    cards_html += f"""
    <div style="background:linear-gradient(145deg, #1e293b, #0f172a);border-radius:12px;padding:20px;border:1px solid #334155;border-top:4px solid {curr['status_color']};box-shadow:0 6px 16px rgba(0,0,0,0.25);">
        <div style="font-size:12px;color:#94a3b8;font-weight:bold;margin-bottom:4px;">{curr['category']}</div>
        <div style="font-size:18px;font-weight:bold;color:#f8fafc;margin-bottom:14px;">{curr['name']}</div>
        
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;background:#090d16;padding:12px;border-radius:8px;margin-bottom:14px;">
            <div>
                <div style="font-size:12px;color:#94a3b8;">IPv4 延遲 (HiNet)</div>
                <div style="font-size:22px;font-weight:bold;color:{'#f87171' if curr['v4_rtt']>180 else '#38bdf8'};">{v4_str}{loss_tag}</div>
            </div>
            <div>
                <div style="font-size:12px;color:#94a3b8;">IPv6 延遲</div>
                <div style="font-size:22px;font-weight:bold;color:{'#f87171' if curr['v6_rtt']>180 else '#a855f7'};">{v6_str}</div>
            </div>
        </div>

        <div style="display:flex;justify-content:space-between;align-items:center;font-size:13px;margin-bottom:14px;">
            <span style="color:#94a3b8;">即時體感：</span>
            <span style="color:{curr['status_color']};font-weight:bold;">{curr['status_text']}</span>
        </div>

        <div style="background:#090d16;padding:10px;border-radius:8px;height:120px;">
            <canvas id="chart-{sid}"></canvas>
        </div>
    </div>
    """

# 注入 Chart.js 畫圖指令
chart_init_js = ""
for s in TARGET_SERVICES:
    sid = s["id"]
    ds = chart_datasets[sid]
    chart_init_js += f"""
    new Chart(document.getElementById('chart-{sid}'), {{
        type: 'line',
        data: {{
            labels: {chart_labels},
            datasets: [
                {{
                    label: 'IPv4 (ms)',
                    data: {json.dumps(ds['v4'])},
                    borderColor: '#38bdf8',
                    backgroundColor: 'rgba(56, 189, 248, 0.1)',
                    borderWidth: 2,
                    tension: 0.3,
                    pointRadius: 1
                }},
                {{
                    label: 'IPv6 (ms)',
                    data: {json.dumps(ds['v6'])},
                    borderColor: '#a855f7',
                    backgroundColor: 'transparent',
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
                x: {{ display: true, ticks: {{ color: '#64748b', maxTicksLimit: 6, font: {{ size: 10 }} }} }},
                y: {{ display: true, ticks: {{ color: '#64748b', font: {{ size: 10 }} }}, beginAtZero: false }}
            }}
        }}
    }});
    """

html_content = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>海外出口體感監控 (HiNet AS3462)</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background:#0b1120; color:#e2e8f0; margin:0; padding:28px; font-size:15px; }}
        .container {{ max-width: 1440px; margin: 0 auto; }}

        /* 頂部固定系統導航 Tab 列 */
        .system-tabs {{ display: flex; gap: 8px; margin-bottom: 20px; border-bottom: 2px solid #334155; padding-bottom: 12px; }}
        .tab-btn {{ padding: 10px 22px; font-size: 15px; font-weight: bold; border-radius: 8px 8px 0 0; text-decoration: none; display: inline-flex; align-items: center; gap: 8px; cursor: pointer; transition: all 0.2s; }}
        .tab-active {{ background: #0284c7; color: #ffffff; box-shadow: 0 4px 14px rgba(2, 132, 199, 0.4); border: none; }}
        .tab-inactive {{ background: #1e293b; color: #94a3b8; border: 1px solid #334155; border-bottom: none; }}
        .tab-inactive:hover {{ background: #334155; color: #f8fafc; }}

        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:24px; padding-bottom:18px; border-bottom:1px solid #334155; }}
        .cards-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 18px; margin-bottom: 30px; }}

        @media (max-width: 768px) {{
            body {{ padding: 12px; }}
            .cards-grid {{ grid-template-columns: 1fr; }}
            .header {{ flex-direction: column; align-items: flex-start; gap: 12px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- 頂部系統導航 (三 Tab 互相跳轉) -->
        <div class="system-tabs">
            <a href="index.html" class="tab-btn tab-inactive">🌐 IPv4 核心巡檢</a>
            <a href="index.html" class="tab-btn tab-inactive">🚀 IPv6 核心巡檢</a>
            <a href="egress.html" class="tab-btn tab-active">⚡ 海外出口體感監控</a>
        </div>

        <div class="header">
            <div>
                <h1 style="margin:0 0 8px 0; font-size:24px; color:#f8fafc; font-weight:800; display:flex; align-items:center; gap:10px;">
                    ⚡ 中華電信 (HiNet AS3462) 海外出口體感監控
                </h1>
                <div style="color:#94a3b8; font-size:14px; line-height:1.6;">
                    📍 <strong>探針來源：</strong>台灣本地 HiNet 寬頻節點 (AS3462/AS9294)<br>
                    ⏱ <strong>最後量測時間：</strong><span style="color:#38bdf8; font-weight:bold;">{now_str} (UTC+8)</span>
                    <span style="margin-left:12px; color:#a855f7;">• 藍實線: IPv4 / 紫虛線: IPv6 (過去 24 小時數線趨勢)</span>
                </div>
            </div>
            <div>
                <button onclick="window.location.reload()" style="background:#0284c7; color:#fff; border:none; border-radius:8px; padding:10px 18px; cursor:pointer; font-weight:bold; font-size:14px;">
                    🔄 畫面刷新
                </button>
            </div>
        </div>

        <!-- 6 大指標服務卡片與即時折線圖 -->
        <div class="cards-grid">
            {cards_html}
        </div>
    </div>

    <script>
        {chart_init_js}
    </script>
</body>
</html>
"""

with open("egress.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 出口體感巡檢完成，已產出 egress.html 與 egress_state.json。")
