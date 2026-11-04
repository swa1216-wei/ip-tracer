import json
import time
import os
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ==============================================================================
# 第一部分：監控標的設定 (6 大精選指標，包含 Threads/IG 與 X 重災區)
# ==============================================================================

TARGET_SERVICES = [
    {
        "id": "threads_ig",
        "name": "Threads & IG 圖片 CDN",
        "host": "scontent.cdninstagram.com",
        "category": "Meta 媒體骨幹 (看圖轉圈/破圖)",
        "desc": "Threads 與 Instagram 照片/靜態內容主要來源"
    },
    {
        "id": "meta_reels",
        "name": "Meta Reels 影音串流",
        "host": "video.xx.fbcdn.net",
        "category": "Meta 影音骨幹 (短影音看一半轉圈)",
        "desc": "Instagram Reels 與 FB 短影音分段載入伺服器"
    },
    {
        "id": "x_media",
        "name": "X (Twitter) 圖片節點",
        "host": "pbs.twimg.com",
        "category": "Fastly CDN (東京 NRT POP 延遲)",
        "desc": "X / Twitter 圖片專用 Fastly 節點"
    },
    {
        "id": "github_fastly",
        "name": "GitHub 檔案下載骨幹",
        "host": "objects.githubusercontent.com",
        "category": "Fastly CDN (Release 下載掉速)",
        "desc": "GitHub Release 與大型資產檔案下載骨幹"
    },
    {
        "id": "discord_media",
        "name": "Discord 媒體傳輸節點",
        "host": "cdn.discordapp.com",
        "category": "Cloudflare CDN (語音/圖床)",
        "desc": "Discord 聊天圖片、語音及附件傳輸節點"
    },
    {
        "id": "youtube_stream",
        "name": "YouTube 影音串流指標",
        "host": "googlevideo.com",
        "category": "Google 國際直連指標",
        "desc": "YouTube 影音封包對照標準基準線"
    }
]

STATE_FILE = "egress_state.json"
tz = timezone(timedelta(hours=8))
now_dt = datetime.now(tz)
now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
time_short = now_dt.strftime('%H:%M')

# ==============================================================================
# 第二部分：Globalping HTTPS 探測 (鎖定 TW + HiNet ASN 3462)
# ==============================================================================

def measure_service(service):
    """
    向 Globalping 發起標準 HTTP 探測 (HTTPS 協定)，
    直接抓取 timings 中的 total (整體耗時) 與 tcp (純握手耗時)。
    """
    api_url = "https://api.globalping.io/v1/measurements"
    
    # 規格：純域名 target、無 method 參數、鎖定台灣 HiNet
    payload = json.dumps({
        "type": "http",
        "target": service["host"],
        "locations": [
            {
                "country": "TW",
                "asn": 3462
            }
        ],
        "measurementOptions": {
            "protocol": "HTTPS"
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
            return service["id"], {"status": "FAIL", "total_ms": 0, "tcp_ms": 0, "err": str(e)}
        except Exception as e:
            return service["id"], {"status": "FAIL", "total_ms": 0, "tcp_ms": 0, "err": str(e)}

    if not measurement_id:
        return service["id"], {"status": "FAIL", "total_ms": 0, "tcp_ms": 0, "err": "無測量ID"}

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
                        timings = res.get("timings", {})
                        total_ms = round(timings.get("total", 0), 1)
                        tcp_ms = round(timings.get("tcp", 0), 1)
                        
                        # 只要有拿到耗時數據，不論 HTTP 狀態碼 (包含 400)，即代表成功測得延遲
                        if total_ms > 0:
                            return service["id"], {
                                "status": "OK",
                                "total_ms": total_ms,
                                "tcp_ms": tcp_ms
                            }
                        else:
                            return service["id"], {
                                "status": "FAIL",
                                "total_ms": 0,
                                "tcp_ms": 0,
                                "err": "未取得耗時"
                            }
        except Exception:
            pass

    return service["id"], {"status": "TIMEOUT", "total_ms": 0, "tcp_ms": 0, "err": "探測逾時"}

# ==============================================================================
# 第三部分：平行發起探測
# ==============================================================================

print(f"[{now_str}] 啟動 HiNet 出口體感巡檢 (鎖定 AS3462 台中探針)...")

results = {}
with ThreadPoolExecutor(max_workers=3) as executor:
    future_to_svc = {executor.submit(measure_service, s): s["id"] for s in TARGET_SERVICES}
    for future in as_completed(future_to_svc):
        svc_id, res_data = future.result()
        results[svc_id] = res_data

# ==============================================================================
# 第四部分：狀態判定與 24 小時歷史滾動記錄
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
    res = results.get(sid, {"status": "FAIL", "total_ms": 0, "tcp_ms": 0})
    total_ms = res.get("total_ms", 0)
    tcp_ms = res.get("tcp_ms", 0)

    # 體感量化標準判定：
    # 綠燈：< 150ms (秒開良好)
    # 黃燈：150ms ~ 350ms (稍微等待)
    # 紅燈：>= 350ms 或 失敗 (明顯轉圈卡頓)
    if res.get("status") != "OK":
        status_text = "🚨 連線逾時 / 異常"
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

    current_snapshot["data"][sid] = {
        "name": s["name"],
        "category": s["category"],
        "desc": s["desc"],
        "total_ms": total_ms,
        "tcp_ms": tcp_ms,
        "status": res.get("status"),
        "status_text": status_text,
        "status_color": status_color
    }

history_state.insert(0, current_snapshot)
history_state = history_state[:72]  # 保留 24 小時

with open(STATE_FILE, "w", encoding="utf-8") as f:
    json.dump(history_state, f, ensure_ascii=False, indent=2)

# ==============================================================================
# 第五部分：前端 HTML 組裝 (包含雙指標與 24 小時折線圖)
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

cards_html = ""
for s in TARGET_SERVICES:
    sid = s["id"]
    curr = current_snapshot["data"].get(sid, {})
    total_str = f"{curr['total_ms']} ms" if curr['status'] == "OK" else "逾時 / 異常"
    tcp_str = f"{curr['tcp_ms']} ms" if curr['status'] == "OK" else "--"

    cards_html += f"""
    <div style="background:linear-gradient(145deg, #1e293b, #0f172a);border-radius:12px;padding:20px;border:1px solid #334155;border-top:4px solid {curr['status_color']};box-shadow:0 6px 16px rgba(0,0,0,0.25);">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">
            <span style="font-size:12px;color:#94a3b8;font-weight:bold;">{curr['category']}</span>
            <span style="color:{curr['status_color']};font-weight:bold;font-size:13px;">{curr['status_text']}</span>
        </div>
        <div style="font-size:18px;font-weight:bold;color:#f8fafc;margin-bottom:2px;">{curr['name']}</div>
        <div style="font-size:12px;color:#64748b;margin-bottom:14px;">{curr['desc']}</div>
        
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;background:#090d16;padding:12px;border-radius:8px;margin-bottom:14px;">
            <div>
                <div style="font-size:12px;color:#94a3b8;">總體感耗時 (Total)</div>
                <div style="font-size:22px;font-weight:bold;color:{'#f87171' if curr['total_ms']>350 else '#38bdf8'};">{total_str}</div>
            </div>
            <div>
                <div style="font-size:12px;color:#94a3b8;">TCP 握手耗時</div>
                <div style="font-size:22px;font-weight:bold;color:#a855f7;">{tcp_str}</div>
            </div>
        </div>

        <div style="background:#090d16;padding:10px;border-radius:8px;height:120px;">
            <canvas id="chart-{sid}"></canvas>
        </div>
    </div>
    """

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
                    label: '總體感 (ms)',
                    data: {json.dumps(ds['total'])},
                    borderColor: '#38bdf8',
                    backgroundColor: 'rgba(56, 189, 248, 0.1)',
                    borderWidth: 2,
                    tension: 0.3,
                    pointRadius: 2
                }},
                {{
                    label: 'TCP 握手 (ms)',
                    data: {json.dumps(ds['tcp'])},
                    borderColor: '#a855f7',
                    backgroundColor: 'transparent',
                    borderWidth: 1.5,
                    borderDash: [3, 3],
                    tension: 0.3,
                    pointRadius: 1
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

        .system-tabs {{ display: flex; gap: 8px; margin-bottom: 20px; border-bottom: 2px solid #334155; padding-bottom: 12px; }}
        .tab-btn {{ padding: 10px 22px; font-size: 15px; font-weight: bold; border-radius: 8px 8px 0 0; text-decoration: none; display: inline-flex; align-items: center; gap: 8px; cursor: pointer; transition: all 0.2s; }}
        .tab-active {{ background: #0284c7; color: #ffffff; box-shadow: 0 4px 14px rgba(2, 132, 199, 0.4); border: none; }}
        .tab-inactive {{ background: #1e293b; color: #94a3b8; border: 1px solid #334155; border-bottom: none; }}
        .tab-inactive:hover {{ background: #334155; color: #f8fafc; }}

        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:20px; padding-bottom:18px; border-bottom:1px solid #334155; }}
        .criteria-box {{ background: rgba(15, 23, 42, 0.8); border: 1px solid #334155; border-radius: 8px; padding: 12px 18px; margin-bottom: 24px; font-size: 13px; line-height: 1.7; }}
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
        <!-- 頂部系統導航 -->
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
                    📍 <strong>探針來源：</strong>台灣本地 HiNet 寬頻節點 (AS3462 台中實體用戶端)<br>
                    ⏱ <strong>最後量測時間：</strong><span style="color:#38bdf8; font-weight:bold;">{now_str} (UTC+8)</span>
                    <span style="margin-left:12px; color:#a855f7;">• 藍實線: 總體感耗時 (Total) / 紫虛線: TCP 握手耗時</span>
                </div>
            </div>
            <div>
                <button onclick="window.location.reload()" style="background:#0284c7; color:#fff; border:none; border-radius:8px; padding:10px 18px; cursor:pointer; font-weight:bold; font-size:14px;">
                    🔄 畫面刷新
                </button>
            </div>
        </div>

        <!-- 體感量化標準說明 -->
        <div class="criteria-box">
            <strong style="color:#f8fafc;">📊 體感量化判定標準說明 (HTTPS Total Timings)：</strong><br>
            • <span style="color:#34d399;font-weight:bold;">順暢良好 (秒開)</span>：耗時 &lt; 150ms，開圖看短影音極度流暢。<br>
            • <span style="color:#fde047;font-weight:bold;">稍微延遲</span>：耗時 150ms ～ 350ms，圖片載入有些微等待感。<br>
            • <span style="color:#f87171;font-weight:bold;">🚨 轉圈卡頓 / 異常</span>：耗時 &ge; 350ms 或連線逾時，短影音播一半轉圈、大圖破圖。
        </div>

        <!-- 6 大指標卡片 -->
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

print(f"[{now_str}] 出口體感巡檢完成，已成功產出真實體感儀表板。")
