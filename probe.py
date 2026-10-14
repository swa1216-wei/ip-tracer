import json
import time
import os
import re
import ipaddress
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ================= 設定區 =================
# 38 個純核心 Internet NAT 網段切半取 .8 (共 76 個目標 IP)
TARGET_IPS = [
    "42.72.0.8", "42.72.64.8", "42.72.128.8", "42.72.192.8",
    "42.73.0.8", "42.73.128.8",
    "42.74.0.8", "42.75.0.8",
    "42.76.0.8", "42.76.64.8", "42.76.128.8", "42.76.192.8",
    "42.77.0.8", "42.77.64.8", "42.77.128.8", "42.77.192.8",
    "42.79.128.8", "42.79.160.8", "42.79.192.8", "42.79.224.8",
    "111.71.0.8", "111.71.64.8", "111.71.212.8", "111.71.213.8", "111.71.214.8", "111.71.215.8",
    "111.82.0.8", "111.82.128.8",
    "111.83.0.8", "111.83.64.8", "111.83.128.8", "111.83.192.8",
    "114.136.0.8", "114.136.64.8", "114.136.128.8", "114.136.192.8",
    "114.137.0.8", "114.137.32.8", "114.137.64.8", "114.137.96.8", "114.137.128.8", "114.137.192.8",
    "223.136.0.8", "223.136.32.8", "223.136.64.8", "223.136.96.8", "223.136.128.8", "223.136.192.8",
    "223.137.0.8", "223.137.64.8", "223.137.128.8", "223.137.192.8",
    "223.138.0.8", "223.138.32.8", "223.138.64.8", "223.138.96.8", "223.138.128.8", "223.138.192.8",
    "223.139.0.8", "223.139.64.8", "223.139.128.8", "223.139.192.8",
    "223.140.0.8", "223.140.64.8", "223.140.128.8", "223.140.192.8",
    "223.141.0.8", "223.141.64.8", "223.141.128.8", "223.141.160.8", "223.141.192.8", "223.141.224.8",
    "223.143.240.8", "223.143.242.8", "223.143.244.8", "223.143.246.8"
]

# 8 台核心邊界路由器 (BR) 白名單
BR_LIST = {
    "210.65.126.209": "3L1BR1",
    "210.65.126.213": "3L1BR2",
    "210.65.126.101": "4L1BR3",
    "210.65.126.105": "4L1BR4",
    "210.65.126.193": "7L2BR1",
    "210.65.126.197": "7L2BR2",
    "210.65.126.93":  "2L3BR3",
    "210.65.126.97":  "2L3BR4",
}

HISTORY_FILE = "monitor_state.json"
# ==========================================

tz = timezone(timedelta(hours=8))
now_str = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

# 載入歷史狀態
state = {"baselines": {}, "last_status": {}, "event_logs": []}
if os.path.exists(HISTORY_FILE):
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        pass

def run_single_trace(target):
    api_url = "https://api.globalping.io/v1/measurements"
    payload = json.dumps({
        "type": "traceroute",
        "target": target,
        "locations": [{"country": "TW"}],
        "limit": 1
    }).encode("utf-8")
    
    req = urllib.request.Request(
        api_url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "GitHub-Network-Probe"}
    )
    
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            measurement_id = json.loads(resp.read().decode("utf-8")).get("id")
    except Exception as e:
        return target, None, f"發起請求失敗: {e}"

    poll_url = f"{api_url}/{measurement_id}"
    for _ in range(15):
        time.sleep(2)
        try:
            req_poll = urllib.request.Request(poll_url, headers={"User-Agent": "GitHub-Network-Probe"})
            with urllib.request.urlopen(req_poll, timeout=15) as resp:
                res_data = json.loads(resp.read().decode("utf-8"))
                if res_data.get("status") == "finished":
                    results = res_data.get("results", [])
                    if results:
                        return target, results[0], results[0].get("result", {}).get("rawOutput", "")
        except Exception:
            pass
    return target, None, "探測逾時"

# 使用 5 個並行線程加速探測
probe_outputs = {}
print(f"[{now_str}] 開始並行巡檢 {len(TARGET_IPS)} 組目標 IP ...")
with ThreadPoolExecutor(max_workers=5) as executor:
    future_to_ip = {executor.submit(run_single_trace, ip): ip for ip in TARGET_IPS}
    for future in as_completed(future_to_ip):
        target, probe_obj, raw_output = future.result()
        probe_outputs[target] = raw_output
        print(f"  -> 完成探測: {target}")

# 依序解析探測結果
current_results = []
for target in TARGET_IPS:
    raw_output = probe_outputs.get(target, "無輸出內容")
    
    last_responded_hop = 0
    last_responded_ip = "無回應"
    has_private_ip = False
    private_ips_found = []
    
    for line in raw_output.splitlines():
        match_hop = re.match(r'^\s*(\d+)\s+', line)
        if match_hop:
            hop_num = int(match_hop.group(1))
            ips_in_line = re.findall(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', line)
            if ips_in_line:
                effective_ips = [ip for ip in ips_in_line if ip != target]
                if effective_ips:
                    last_responded_hop = hop_num
                    last_responded_ip = effective_ips[-1]
                    
                    # 關鍵修正：只檢查第 2 跳之後 (Hop >= 2)，排除第 1 跳探針 Gateway
                    if hop_num >= 2:
                        for ip in effective_ips:
                            try:
                                if ipaddress.ip_address(ip) in ipaddress.ip_network("10.0.0.0/8"):
                                    has_private_ip = True
                                    if ip not in private_ips_found:
                                        private_ips_found.append(ip)
                            except ValueError:
                                pass

    br_name = BR_LIST.get(last_responded_ip, None)
    is_br = (br_name is not None)
    
    # Baseline 基準線設定
    if target not in state["baselines"] and last_responded_hop > 0:
        state["baselines"][target] = last_responded_hop
    baseline_hop = state["baselines"].get(target, last_responded_hop)
    
    # 狀態判定：含內網 IP、未達 BR、或跳數偏離基準
    if has_private_ip or not is_br or (last_responded_hop != baseline_hop):
        current_status = "ALERT"
        status_desc = f"異常 (中途含 {', '.join(private_ips_found)})" if has_private_ip else ("未達 BR" if not is_br else f"跳數偏離 (現有 {last_responded_hop} / 基準 {baseline_hop})")
    else:
        current_status = "OK"
        status_desc = f"正常抵達 {br_name}"

    # 狀態變更偵測 (導入 vs 導出)
    prev_status = state["last_status"].get(target, "OK")
    if prev_status == "OK" and current_status == "ALERT":
        event = f"🔴 {now_str} 【導入告警】目標 {target} 狀態變更為異常！現有跳數: {last_responded_hop} (基準: {baseline_hop})，原因: {status_desc}"
        state["event_logs"].insert(0, event)
    elif prev_status == "ALERT" and current_status == "OK":
        event = f"🟢 {now_str} 【導出恢復】目標 {target} 路由已恢復正常！現有跳數: {last_responded_hop}，到達: {br_name}"
        state["event_logs"].insert(0, event)

    state["last_status"][target] = current_status

    current_results.append({
        "target": target,
        "last_hop": last_responded_hop,
        "baseline_hop": baseline_hop,
        "last_ip": last_responded_ip,
        "br_name": br_name,
        "is_br": is_br,
        "has_private": has_private_ip,
        "private_ips": private_ips_found,
        "status": current_status,
        "desc": status_desc,
        "raw": raw_output
    })

state["event_logs"] = state["event_logs"][:50]

with open(HISTORY_FILE, "w", encoding="utf-8") as f:
    json.dump(state, f, ensure_ascii=False, indent=2)

# ================= 產生展示網頁 =================
alert_count = sum(1 for r in current_results if r["status"] == "ALERT")
ok_count = len(current_results) - alert_count

rows_html = ""
for idx, r in enumerate(current_results):
    bg_color = "#3f1a1a" if r["status"] == "ALERT" else "transparent"
    status_badge = '<span style="color:#f87171;font-weight:bold;">🚨 異常</span>' if r["status"] == "ALERT" else '<span style="color:#4ade80;font-weight:bold;">✔ 正常</span>'
    br_badge = f'<span style="color:#4ade80;font-weight:bold;">✔ {r["br_name"]}</span>' if r["is_br"] else '<span style="color:#f87171;font-weight:bold;">❌ 非BR</span>'
    
    diff = r["last_hop"] - r["baseline_hop"]
    diff_str = f"+{diff}" if diff > 0 else (str(diff) if diff < 0 else "0")
    hop_display = f"{r['last_hop']} <span style='font-size:11px;color:#94a3b8;'>(基:{r['baseline_hop']} / 差:{diff_str})</span>"

    rows_html += f"""
    <tr style="background-color:{bg_color};border-bottom:1px solid #334155;">
        <td style="padding:10px;font-weight:bold;">{r['target']}</td>
        <td style="padding:10px;">{hop_display}</td>
        <td style="padding:10px;font-family:monospace;">{r['last_ip']}</td>
        <td style="padding:10px;">{br_badge}</td>
        <td style="padding:10px;">{r['desc']}</td>
        <td style="padding:10px;">{status_badge}</td>
        <td style="padding:10px;text-align:center;">
            <button onclick="toggleLog('log-{idx}')" style="background:#0284c7;color:#fff;border:none;border-radius:4px;padding:4px 8px;cursor:pointer;font-size:12px;">查看</button>
        </td>
    </tr>
    <tr id="log-{idx}" style="display:none;background:#090d16;">
        <td colspan="7" style="padding:12px;">
            <pre style="margin:0;color:#38bdf8;font-size:12px;white-space:pre-wrap;">{r['raw']}</pre>
        </td>
    </tr>
    """

logs_html = "".join([f"<li style='margin-bottom:6px;'>{log}</li>" for log in state["event_logs"]])
if not logs_html:
    logs_html = "<li style='color:#94a3b8;'>目前尚無狀態變更紀錄（系統正常運行中）</li>"

html_content = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="60">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>4G/5G 行網 Internet 路由巡檢與導入監控</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace; background:#0b1120; color:#e2e8f0; margin:0; padding:24px; }}
        .container {{ max-width: 1250px; margin: 0 auto; }}
        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:20px; }}
        .cards {{ display:flex; gap:16px; margin-bottom:20px; }}
        .card {{ flex:1; background:#1e293b; padding:16px; border-radius:8px; border:1px solid #334155; }}
        .audit-box {{ background:#1e293b; border-left:4px solid #38bdf8; padding:16px; border-radius:6px; margin-bottom:24px; }}
        table {{ width:100%; border-collapse:collapse; background:#1e293b; border-radius:8px; overflow:hidden; font-size:13px; }}
        th {{ background:#0f172a; padding:12px; text-align:left; color:#94a3b8; font-weight:600; }}
    </style>
    <script>
        function toggleLog(id) {{
            var el = document.getElementById(id);
            el.style.display = (el.style.display === 'none') ? 'table-row' : 'none';
        }}
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <h1 style="margin:0;font-size:22px;color:#38bdf8;">🌐 4G/5G 行網 Internet 路由巡檢與導入監控儀表板</h1>
                <div style="font-size:13px;color:#94a3b8;margin-top:8px;line-height:1.6;">
                    📍 <strong>探測來源：</strong>固定 Taiwan 探針節點發起<br>
                    ⏱ <strong>巡檢週期：</strong>GitHub 每 30 分鐘自動 Traceroute 巡檢一次 | <strong>最後巡檢時間：</strong>{now_str} (UTC+8)<br>
                    🔄 <strong>畫面狀態：</strong>瀏覽器每 60 秒自動刷新讀取最新數據
                </div>
            </div>
        </div>

        <div class="cards">
            <div class="card">
                <div style="color:#94a3b8;font-size:12px;">監控目標總數</div>
                <div style="font-size:24px;font-weight:bold;margin-top:4px;">{len(current_results)} 組 IP</div>
            </div>
            <div class="card">
                <div style="color:#94a3b8;font-size:12px;">正常連通 (抵達 8 台 BR)</div>
                <div style="font-size:24px;font-weight:bold;color:#4ade80;margin-top:4px;">{ok_count} 組</div>
            </div>
            <div class="card" style="border-color: {'#ef4444' if alert_count > 0 else '#334155'};">
                <div style="color:#94a3b8;font-size:12px;">異常 / 導入中</div>
                <div style="font-size:24px;font-weight:bold;color: {'#f87171' if alert_count > 0 else '#94a3b8'}; margin-top:4px;">{alert_count} 組</div>
            </div>
        </div>

        <div class="audit-box">
            <h3 style="margin-top:0;margin-bottom:10px;font-size:15px;color:#38bdf8;">📋 路由變更流水帳 (導入 / 導出歷史紀錄)</h3>
            <ul style="margin:0;padding-left:20px;font-size:13px;line-height:1.6;">
                {logs_html}
            </ul>
        </div>

        <table>
            <thead>
                <tr>
                    <th>目標 IP</th>
                    <th>跳數 (基準 / 差值)</th>
                    <th>最終回應節點</th>
                    <th>是否抵達 BR</th>
                    <th>路由狀態摘要</th>
                    <th>判定</th>
                    <th style="text-align:center;">路由日誌</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
</body>
</html>
"""

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 巡檢完成，已產出 76 筆目標 IP 儀表板。")
