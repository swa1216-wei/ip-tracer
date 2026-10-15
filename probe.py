import json
import time
import os
import re
import ipaddress
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ==============================================================================
# 第一部分：系統設定區 (核心 BR 白名單、機房對照表、監控 IP 元數據)
# ==============================================================================

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

# 機房常見代號對照 (自動解析反解域名)
ROUTER_LOCATIONS = {
    "tpdb": "台北東二",
    "tpe4": "台北信義",
    "tpn2": "台北南二",
    "tyfo": "桃園光纖",
    "skc1": "板橋客網",
    "tc":   "台中",
    "tn":   "台南",
    "kh":   "高雄"
}

# 76 個目標 IP 與網段元數據對照表
TARGET_METADATA = {
  "42.72.0.8": {"cidr": "42.72.0.0/17", "part": "前半", "elem": "3L1EPG1", "nat": "NAT5/6"},
  "42.72.64.8": {"cidr": "42.72.0.0/17", "part": "後半", "elem": "3L1EPG1", "nat": "NAT5/6"},
  "42.72.128.8": {"cidr": "42.72.128.0/17", "part": "前半", "elem": "3L1EPG2", "nat": "NAT5/6"},
  "42.72.192.8": {"cidr": "42.72.128.0/17", "part": "後半", "elem": "3L1EPG2", "nat": "NAT5/6"},
  "42.73.0.8": {"cidr": "42.73.0.0/16", "part": "前半", "elem": "3L1EPG3", "nat": "NAT5/6"},
  "42.73.128.8": {"cidr": "42.73.0.0/16", "part": "後半", "elem": "3L1EPG3", "nat": "NAT5/6"},
  "42.74.0.8": {"cidr": "42.74.0.0/15", "part": "前半", "elem": "3L1EPG4", "nat": "NAT5/6"},
  "42.75.0.8": {"cidr": "42.74.0.0/15", "part": "後半", "elem": "3L1EPG4", "nat": "NAT5/6"},
  "42.76.0.8": {"cidr": "42.76.0.0/17", "part": "前半", "elem": "4L1EPG1", "nat": "NAT7/8"},
  "42.76.64.8": {"cidr": "42.76.0.0/17", "part": "後半", "elem": "4L1EPG1", "nat": "NAT7/8"},
  "42.76.128.8": {"cidr": "42.76.128.0/17", "part": "前半", "elem": "4L1EPG1", "nat": "NAT7/8"},
  "42.76.192.8": {"cidr": "42.76.128.0/17", "part": "後半", "elem": "4L1EPG1", "nat": "NAT7/8"},
  "42.77.0.8": {"cidr": "42.77.0.0/17", "part": "前半", "elem": "7L2EPG1", "nat": "NAT7/8"},
  "42.77.64.8": {"cidr": "42.77.0.0/17", "part": "後半", "elem": "7L2EPG1", "nat": "NAT7/8"},
  "42.77.128.8": {"cidr": "42.77.128.0/17", "part": "前半", "elem": "7L2EPG2", "nat": "NAT9/10"},
  "42.77.192.8": {"cidr": "42.77.128.0/17", "part": "後半", "elem": "7L2EPG2", "nat": "NAT9/10"},
  "42.79.128.8": {"cidr": "42.79.128.0/18", "part": "前半", "elem": "7L2EPG2", "nat": "NAT7/8"},
  "42.79.160.8": {"cidr": "42.79.128.0/18", "part": "後半", "elem": "7L2EPG2", "nat": "NAT7/8"},
  "42.79.192.8": {"cidr": "42.79.192.0/18", "part": "前半", "elem": "4L1EPG2", "nat": "NAT9/10"},
  "42.79.224.8": {"cidr": "42.79.192.0/18", "part": "後半", "elem": "4L1EPG2", "nat": "NAT9/10"},
  "111.71.0.8": {"cidr": "111.71.0.0/17", "part": "前半", "elem": "3L1EPG1", "nat": "NAT7/8"},
  "111.71.64.8": {"cidr": "111.71.0.0/17", "part": "後半", "elem": "3L1EPG1", "nat": "NAT7/8"},
  "111.71.212.8": {"cidr": "111.71.212.0/23", "part": "前半", "elem": "2L3EPG1", "nat": "NAT7/8"},
  "111.71.213.8": {"cidr": "111.71.212.0/23", "part": "後半", "elem": "2L3EPG1", "nat": "NAT7/8"},
  "111.71.214.8": {"cidr": "111.71.214.0/23", "part": "前半", "elem": "3L1EPG1", "nat": "NAT7/8"},
  "111.71.215.8": {"cidr": "111.71.214.0/23", "part": "後半", "elem": "3L1EPG1", "nat": "NAT7/8"},
  "111.82.0.8": {"cidr": "111.82.0.0/16", "part": "前半", "elem": "4L1EPG2", "nat": "NAT7/8"},
  "111.82.128.8": {"cidr": "111.82.0.0/16", "part": "後半", "elem": "4L1EPG2", "nat": "NAT7/8"},
  "111.83.0.8": {"cidr": "111.83.0.0/17", "part": "前半", "elem": "4L1EPG2", "nat": "NAT7/8"},
  "111.83.64.8": {"cidr": "111.83.0.0/17", "part": "後半", "elem": "4L1EPG2", "nat": "NAT7/8"},
  "111.83.128.8": {"cidr": "111.83.128.0/17", "part": "前半", "elem": "4L1EPG2", "nat": "NAT9/10"},
  "111.83.192.8": {"cidr": "111.83.128.0/17", "part": "後半", "elem": "4L1EPG2", "nat": "NAT9/10"},
  "114.136.0.8": {"cidr": "114.136.0.0/17", "part": "前半", "elem": "2L3EPG1", "nat": "NAT7/8"},
  "114.136.64.8": {"cidr": "114.136.0.0/17", "part": "後半", "elem": "2L3EPG1", "nat": "NAT7/8"},
  "114.136.128.8": {"cidr": "114.136.128.0/17", "part": "前半", "elem": "2L3EPG2", "nat": "NAT9/10"},
  "114.136.192.8": {"cidr": "114.136.128.0/17", "part": "後半", "elem": "2L3EPG2", "nat": "NAT9/10"},
  "114.137.0.8": {"cidr": "114.137.0.0/18", "part": "前半", "elem": "3L1EPG2", "nat": "NAT7/8"},
  "114.137.32.8": {"cidr": "114.137.0.0/18", "part": "後半", "elem": "3L1EPG2", "nat": "NAT7/8"},
  "114.137.64.8": {"cidr": "114.137.64.0/18", "part": "前半", "elem": "3L1EPG2", "nat": "NAT7/8"},
  "114.137.96.8": {"cidr": "114.137.64.0/18", "part": "後半", "elem": "3L1EPG2", "nat": "NAT7/8"},
  "114.137.128.8": {"cidr": "114.137.128.0/17", "part": "前半", "elem": "3L1EPG2", "nat": "NAT9/10"},
  "114.137.192.8": {"cidr": "114.137.128.0/17", "part": "後半", "elem": "3L1EPG2", "nat": "NAT9/10"},
  "223.136.0.8": {"cidr": "223.136.0.0/18", "part": "前半", "elem": "2L3EPG1", "nat": "NAT7/8"},
  "223.136.32.8": {"cidr": "223.136.0.0/18", "part": "後半", "elem": "2L3EPG1", "nat": "NAT7/8"},
  "223.136.64.8": {"cidr": "223.136.64.0/18", "part": "前半", "elem": "2L3EPG2", "nat": "NAT9/10"},
  "223.136.96.8": {"cidr": "223.136.64.0/18", "part": "後半", "elem": "2L3EPG2", "nat": "NAT9/10"},
  "223.136.128.8": {"cidr": "223.136.128.0/17", "part": "前半", "elem": "2L3EPG2", "nat": "NAT9/10"},
  "223.136.192.8": {"cidr": "223.136.128.0/17", "part": "後半", "elem": "2L3EPG2", "nat": "NAT9/10"},
  "223.137.0.8": {"cidr": "223.137.0.0/17", "part": "前半", "elem": "2L3EPG2", "nat": "NAT7/8"},
  "223.137.64.8": {"cidr": "223.137.0.0/17", "part": "後半", "elem": "2L3EPG2", "nat": "NAT7/8"},
  "223.137.128.8": {"cidr": "223.137.128.0/17", "part": "前半", "elem": "2L3EPG2", "nat": "NAT7/8"},
  "223.137.192.8": {"cidr": "223.137.128.0/17", "part": "後半", "elem": "2L3EPG2", "nat": "NAT7/8"},
  "223.138.0.8": {"cidr": "223.138.0.0/18", "part": "前半", "elem": "7L2EPG1", "nat": "NAT7/8"},
  "223.138.32.8": {"cidr": "223.138.0.0/18", "part": "後半", "elem": "7L2EPG1", "nat": "NAT7/8"},
  "223.138.64.8": {"cidr": "223.138.64.0/18", "part": "前半", "elem": "7L2EPG2", "nat": "NAT9/10"},
  "223.138.96.8": {"cidr": "223.138.64.0/18", "part": "後半", "elem": "7L2EPG2", "nat": "NAT9/10"},
  "223.138.128.8": {"cidr": "223.138.128.0/17", "part": "前半", "elem": "4L1EPG2", "nat": "NAT9/10"},
  "223.138.192.8": {"cidr": "223.138.128.0/17", "part": "後半", "elem": "4L1EPG2", "nat": "NAT9/10"},
  "223.139.0.8": {"cidr": "223.139.0.0/17", "part": "前半", "elem": "7L2EPG1", "nat": "NAT7/8"},
  "223.139.64.8": {"cidr": "223.139.0.0/17", "part": "後半", "elem": "7L2EPG1", "nat": "NAT7/8"},
  "223.139.128.8": {"cidr": "223.139.128.0/17", "part": "前半", "elem": "7L2EPG2", "nat": "NAT9/10"},
  "223.139.192.8": {"cidr": "223.139.128.0/17", "part": "後半", "elem": "7L2EPG2", "nat": "NAT9/10"},
  "223.140.0.8": {"cidr": "223.140.0.0/17", "part": "前半", "elem": "2L3EPG1", "nat": "NAT7/8"},
  "223.140.64.8": {"cidr": "223.140.0.0/17", "part": "後半", "elem": "2L3EPG1", "nat": "NAT7/8"},
  "223.140.128.8": {"cidr": "223.140.128.0/17", "part": "前半", "elem": "2L3EPG2", "nat": "NAT9/10"},
  "223.140.192.8": {"cidr": "223.140.128.0/17", "part": "後半", "elem": "2L3EPG2", "nat": "NAT9/10"},
  "223.141.0.8": {"cidr": "223.141.0.0/17", "part": "前半", "elem": "4L1EPG1", "nat": "NAT7/8"},
  "223.141.64.8": {"cidr": "223.141.0.0/17", "part": "後半", "elem": "4L1EPG1", "nat": "NAT7/8"},
  "223.141.128.8": {"cidr": "223.141.128.0/18", "part": "前半", "elem": "4L1EPG2", "nat": "NAT9/10"},
  "223.141.160.8": {"cidr": "223.141.128.0/18", "part": "後半", "elem": "4L1EPG2", "nat": "NAT9/10"},
  "223.141.192.8": {"cidr": "223.141.192.0/18", "part": "前半", "elem": "4L1EPG2", "nat": "NAT9/10"},
  "223.141.224.8": {"cidr": "223.141.192.0/18", "part": "後半", "elem": "4L1EPG2", "nat": "NAT9/10"},
  "223.143.240.8": {"cidr": "223.143.240.0/22", "part": "前半", "elem": "2L3EPG4", "nat": "NAT9/10"},
  "223.143.242.8": {"cidr": "223.143.240.0/22", "part": "後半", "elem": "2L3EPG4", "nat": "NAT9/10"},
  "223.143.244.8": {"cidr": "223.143.244.0/22", "part": "前半", "elem": "2L3EPG4", "nat": "NAT9/10"},
  "223.143.246.8": {"cidr": "223.143.244.0/22", "part": "後半", "elem": "2L3EPG4", "nat": "NAT9/10"}
}

TARGET_IPS = list(TARGET_METADATA.keys())
HISTORY_FILE = "monitor_state.json"

tz = timezone(timedelta(hours=8))
now_dt = datetime.now(tz)
now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')

state = {"baselines": {}, "last_status": {}, "event_logs": [], "history_snapshots": []}
if os.path.exists(HISTORY_FILE):
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
            if "history_snapshots" not in state:
                state["history_snapshots"] = []
    except Exception:
        pass


# ==============================================================================
# 第二部分：網路探測與路由邏輯運算區 (API 呼叫、重測機制、日誌比對)
# ==============================================================================

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
        return target, f"發起請求失敗: {e}"

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
                        return target, results[0].get("result", {}).get("rawOutput", "")
        except Exception:
            pass
    return target, "探測逾時"

def parse_trace_output(target, raw_output):
    last_responded_hop = 0
    last_responded_ip = "無回應"
    last_responded_domain = ""
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
                    
                    domain_match = re.search(r'([a-zA-Z0-9\-\.]+\.hinet\.net)', line)
                    if domain_match:
                        last_responded_domain = domain_match.group(1)
                    else:
                        last_responded_domain = ""

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
    return {
        "last_hop": last_responded_hop,
        "last_ip": last_responded_ip,
        "domain": last_responded_domain,
        "is_br": is_br,
        "br_name": br_name,
        "has_private": has_private_ip,
        "private_ips": private_ips_found,
        "raw": raw_output
    }

# 第一輪並行探測
probe_outputs = {}
print(f"[{now_str}] 開始第一輪巡檢 {len(TARGET_IPS)} 組目標 IP ...")
with ThreadPoolExecutor(max_workers=8) as executor:
    future_to_ip = {executor.submit(run_single_trace, ip): ip for ip in TARGET_IPS}
    for future in as_completed(future_to_ip):
        target, raw_output = future.result()
        probe_outputs[target] = raw_output

# 解析第一輪結果
parsed_results = {}
targets_need_retry = []
for target in TARGET_IPS:
    parsed = parse_trace_output(target, probe_outputs.get(target, ""))
    parsed_results[target] = parsed
    if not parsed["is_br"] and not parsed["has_private"]:
        targets_need_retry.append(target)

# 第二輪自動複測（最多 2 次）
if targets_need_retry:
    print(f"[{now_str}] 偵測到 {len(targets_need_retry)} 組目標未達 BR，啟動複測重試機制 (最多 2 次)...")
    for retry_round in range(1, 3):
        if not targets_need_retry:
            break
        print(f"  -> 進行第 {retry_round} 次複測，剩餘待測: {len(targets_need_retry)} 組")
        time.sleep(3)
        still_failing = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_ip = {executor.submit(run_single_trace, ip): ip for ip in targets_need_retry}
            for future in as_completed(future_to_ip):
                target, raw_output = future.result()
                retry_parsed = parse_trace_output(target, raw_output)
                if retry_parsed["is_br"]:
                    print(f"     ✔ 目標 {target} 在第 {retry_round} 次複測成功抵達 {retry_parsed['br_name']}！")
                    parsed_results[target] = retry_parsed
                else:
                    still_failing.append(target)
        targets_need_retry = still_failing

# 彙整判定結果
current_results = []
for target in TARGET_IPS:
    parsed = parsed_results[target]
    meta = TARGET_METADATA.get(target, {})
    
    last_responded_hop = parsed["last_hop"]
    last_responded_ip = parsed["last_ip"]
    last_responded_domain = parsed["domain"]
    is_br = parsed["is_br"]
    br_name = parsed["br_name"]
    has_private_ip = parsed["has_private"]
    private_ips_found = parsed["private_ips"]
    raw_output = parsed["raw"]

    location_desc = ""
    if last_responded_domain:
        for code, loc_name in ROUTER_LOCATIONS.items():
            if code in last_responded_domain.lower():
                location_desc = f" ({loc_name})"
                break

    if target not in state["baselines"] and last_responded_hop > 0:
        state["baselines"][target] = last_responded_hop
    baseline_hop = state["baselines"].get(target, last_responded_hop)
    
    if has_private_ip:
        current_status = "ALERT"
        status_desc = f"🚨 異常 (中途含 {', '.join(private_ips_found)})"
    elif not is_br:
        current_status = "ALERT"
        status_desc = f"🚨 未達 BR (重測 2 次仍未抵達)"
    else:
        current_status = "OK"
        diff = last_responded_hop - baseline_hop
        diff_str = f" (+{diff})" if diff > 0 else (f" ({diff})" if diff < 0 else "")
        status_desc = f"正常抵達 {br_name}{diff_str}"

    prev_status = state["last_status"].get(target, "OK")
    target_label = f"{target} [{meta.get('cidr')} {meta.get('part')}]"
    if prev_status == "OK" and current_status == "ALERT":
        event = f"🔴 {now_str} 【導入告警】目標 {target_label} 狀態變更為異常！原因: {status_desc}，最後節點: {last_responded_ip}{location_desc}"
        state["event_logs"].insert(0, event)
    elif prev_status == "ALERT" and current_status == "OK":
        event = f"🟢 {now_str} 【導出恢復】目標 {target_label} 已恢復正常！抵達: {br_name}"
        state["event_logs"].insert(0, event)

    state["last_status"][target] = current_status

    current_results.append({
        "target": target,
        "cidr": meta.get("cidr", ""),
        "part": meta.get("part", ""),
        "elem": meta.get("elem", ""),
        "nat": meta.get("nat", ""),
        "last_hop": last_responded_hop,
        "baseline_hop": baseline_hop,
        "last_ip": last_responded_ip,
        "domain": last_responded_domain,
        "location": location_desc,
        "br_name": br_name,
        "is_br": is_br,
        "has_private": has_private_ip,
        "private_ips": private_ips_found,
        "status": current_status,
        "desc": status_desc,
        "raw": raw_output
    })

# 排序：ALERT (異常) 排在最上方
current_results.sort(key=lambda x: (0 if x["status"] == "ALERT" else 1, x["target"]))

alert_count = sum(1 for r in current_results if r["status"] == "ALERT")
ok_count = len(current_results) - alert_count
total_count = len(current_results)

# 寫入歷史快照（保留最新 7 天，約 700 筆紀錄）
snapshot_entry = {
    "time": now_str,
    "total": total_count,
    "ok": ok_count,
    "alert": alert_count
}
state["history_snapshots"].insert(0, snapshot_entry)
state["history_snapshots"] = state["history_snapshots"][:700]

state["event_logs"] = state["event_logs"][:60]

with open(HISTORY_FILE, "w", encoding="utf-8") as f:
    json.dump(state, f, ensure_ascii=False, indent=2)

# ==============================================================================
# 第三部分：歷史統計指標計算 (近 24 小時健康度 + 7 天歷史時序)
# ==============================================================================

# 計算 24 小時內的狀況 (依時間戳記過濾)
one_day_ago = now_dt - timedelta(hours=24)
snapshots_24h = []
for s in state["history_snapshots"]:
    try:
        s_dt = datetime.strptime(s["time"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz)
        if s_dt >= one_day_ago:
            snapshots_24h.append(s)
    except Exception:
        pass

alert_rounds_24h = [s for s in snapshots_24h if s.get("alert", 0) > 0]
if not alert_rounds_24h:
    stat_24h_html = '<span style="color:#34d399;font-weight:bold;">✔ 過去 24 小時無異常事件（全時段平穩正常抵達 BR）</span>'
else:
    latest_err = alert_rounds_24h[0]
    stat_24h_html = f'<span style="color:#fb7185;font-weight:bold;">⚠️ 過去 24 小時曾出現 {len(alert_rounds_24h)} 次異常巡檢（最近一次於 {latest_err["time"]} 測得 {latest_err["alert"]} 組異常）</span>'

# 生成過去 7 天的歷史三格紀錄列表
history_rows_html = ""
for item in state["history_snapshots"][:100]:  # 畫面上先列出最近 100 筆，其餘存於 JSON
    row_bg = "#3f1a1a" if item.get("alert", 0) > 0 else "transparent"
    alert_badge = f'<span style="color:#fb7185;font-weight:bold;">{item.get("alert", 0)} 組 🚨</span>' if item.get("alert", 0) > 0 else '<span style="color:#94a3b8;">0 組</span>'
    history_rows_html += f"""
    <tr style="background-color:{row_bg};border-bottom:1px solid #334155;">
        <td style="padding:10px 12px;font-family:monospace;color:#f8fafc;">{item.get("time")}</td>
        <td style="padding:10px 12px;font-weight:bold;color:#38bdf8;">{item.get("total", 76)} 組</td>
        <td style="padding:10px 12px;font-weight:bold;color:#34d399;">{item.get("ok", 76)} 組</td>
        <td style="padding:10px 12px;">{alert_badge}</td>
    </tr>
    """

# ==============================================================================
# 第四部分：HTML 前端組裝與聲音/ACK 邏輯
# ==============================================================================

rows_html = ""
for idx, r in enumerate(current_results):
    bg_color = "#3f1a1a" if r["status"] == "ALERT" else "transparent"
    status_badge = '<span style="color:#f87171;font-weight:bold;font-size:16px;">🚨 異常</span>' if r["status"] == "ALERT" else '<span style="color:#4ade80;font-weight:bold;font-size:16px;">✔ 正常</span>'
    
    if r["is_br"]:
        br_badge = f'<span style="color:#4ade80;font-weight:bold;font-size:15px;">✔ {r["br_name"]}</span>'
        last_node_display = f"<span style='color:#38bdf8;font-size:15px;'>{r['last_ip']}</span>"
    else:
        br_badge = '<span style="color:#f87171;font-weight:bold;font-size:15px;">❌ 非BR</span>'
        domain_tip = f"<br><span style='color:#94a3b8;font-size:13px;'>{r['domain']}{r['location']}</span>" if r['domain'] else ""
        last_node_display = f"<span style='color:#f87171;font-weight:bold;font-size:15px;'>{r['last_ip']}</span>{domain_tip}"
    
    diff = r["last_hop"] - r["baseline_hop"]
    diff_str = f"+{diff}" if diff > 0 else (str(diff) if diff < 0 else "0")
    hop_display = f"<span style='font-size:16px;font-weight:bold;'>{r['last_hop']}</span> <span style='font-size:13px;color:#94a3b8;'>(基:{r['baseline_hop']} / 差:{diff_str})</span>"

    rows_html += f"""
    <tr style="background-color:{bg_color};border-bottom:1px solid #334155;">
        <td style="padding:14px 12px;">
            <div style="font-weight:bold;font-size:16px;color:#f8fafc;">{r['target']}</div>
            <div style="font-size:13px;color:#38bdf8;margin-top:2px;">{r['cidr']} ({r['part']})</div>
        </td>
        <td style="padding:14px 12px;">
            <div style="font-weight:bold;font-size:15px;">{r['elem']}</div>
            <div style="font-size:13px;color:#94a3b8;margin-top:2px;">{r['nat']}</div>
        </td>
        <td style="padding:14px 12px;">{hop_display}</td>
        <td style="padding:14px 12px;font-family:monospace;">{last_node_display}</td>
        <td style="padding:14px 12px;">{br_badge}</td>
        <td style="padding:14px 12px;font-size:15px;">{r['desc']}</td>
        <td style="padding:14px 12px;">{status_badge}</td>
        <td style="padding:14px 12px;text-align:center;">
            <button onclick="toggleLog('log-{idx}')" style="background:#0284c7;color:#fff;border:none;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:14px;font-weight:bold;box-shadow:0 2px 6px rgba(2,132,199,0.3);">查看</button>
        </td>
    </tr>
    <tr id="log-{idx}" class="log-detail-row" style="display:none;background:#090d16;">
        <td colspan="8" style="padding:16px;">
            <pre style="margin:0;color:#38bdf8;font-size:14px;line-height:1.6;white-space:pre-wrap;">{r['raw']}</pre>
        </td>
    </tr>
    """

logs_html = "".join([f"<li style='margin-bottom:8px;'>{log}</li>" for log in state["event_logs"]])
if not logs_html:
    logs_html = "<li style='color:#94a3b8;'>目前尚無狀態變更紀錄（系統正常運行中）</li>"

alert_glow = "text-shadow:0 0 14px rgba(251,113,133,0.6);" if alert_count > 0 else ""
alert_color = "#fb7185" if alert_count > 0 else "#94a3b8"

html_content = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>4G/5G 行網 Internet 路由巡檢與導入監控儀表板</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background:#0b1120; color:#e2e8f0; margin:0; padding:28px; font-size:15px; }}
        .container {{ max-width: 1440px; margin: 0 auto; }}
        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:24px; padding-bottom:18px; border-bottom:1px solid #334155; }}
        .cards {{ display:grid; grid-template-columns:repeat(3, 1fr); gap:20px; margin-bottom:20px; }}
        .audit-box {{ background:linear-gradient(145deg, #1e293b, #0f172a); border-left:5px solid #38bdf8; padding:22px; border-radius:10px; margin-top:32px; border:1px solid #334155; box-shadow:0 6px 16px rgba(0,0,0,0.25); }}
        .history-box {{ background:linear-gradient(145deg, #1e293b, #0f172a); border-left:5px solid #a855f7; padding:22px; border-radius:10px; margin-top:32px; border:1px solid #334155; box-shadow:0 6px 16px rgba(0,0,0,0.25); }}
        table {{ width:100%; border-collapse:collapse; background:#1e293b; border-radius:12px; overflow:hidden; font-size:14px; box-shadow:0 8px 24px rgba(0,0,0,0.3); }}
        th {{ background:#0f172a; padding:16px 12px; text-align:left; color:#94a3b8; font-weight:700; font-size:15px; letter-spacing:0.5px; border-bottom:1px solid #334155; }}
    </style>
    <script>
        var anyLogOpened = false;
        var alertCount = {alert_count};
        var audioCtx = null;
        var beepInterval = null;

        // Web Audio API 警報聲音產生器
        function playWarningBeep() {{
            try {{
                if (!audioCtx) {{
                    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
                }}
                var osc = audioCtx.createOscillator();
                var gain = audioCtx.createGain();
                osc.type = 'sine';
                osc.frequency.setValueAtTime(880, audioCtx.currentTime); // 880Hz (A5 高音)
                gain.gain.setValueAtTime(0.15, audioCtx.currentTime);
                gain.gain.exponentialRampToValueAtTime(0.001, audioCtx.currentTime + 0.35);
                osc.connect(gain);
                gain.connect(audioCtx.destination);
                osc.start();
                osc.stop(audioCtx.currentTime + 0.35);
            }} catch(e) {{
                console.log("Audio not allowed yet:", e);
            }}
        }}

        // 檢查警報與 ACK 狀態
        function checkAlertAudio() {{
            var isAcked = sessionStorage.getItem("alarm_acked_{now_str}") === "true";
            var ackBtn = document.getElementById("ack-btn");
            if (alertCount > 0 && !isAcked) {{
                if (ackBtn) ackBtn.style.display = "inline-block";
                // 每隔 2 秒嗶一次
                if (!beepInterval) {{
                    beepInterval = setInterval(playWarningBeep, 2000);
                }}
            }} else {{
                if (ackBtn) ackBtn.style.display = "none";
                if (beepInterval) {{
                    clearInterval(beepInterval);
                    beepInterval = null;
                }}
            }}
        }}

        function ackAlarm() {{
            sessionStorage.setItem("alarm_acked_{now_str}", "true");
            if (beepInterval) {{
                clearInterval(beepInterval);
                beepInterval = null;
            }}
            var ackBtn = document.getElementById("ack-btn");
            if (ackBtn) {{
                ackBtn.innerText = "✔ 警報已消音 (ACK)";
                ackBtn.style.background = "#475569";
                ackBtn.style.cursor = "default";
            }}
        }}

        function toggleLog(id) {{
            var el = document.getElementById(id);
            if (el.style.display === 'none' || el.style.display === '') {{
                el.style.display = 'table-row';
            }} else {{
                el.style.display = 'none';
            }}
            checkOpenedLogs();
        }}

        function checkOpenedLogs() {{
            var rows = document.getElementsByClassName('log-detail-row');
            anyLogOpened = false;
            for (var i = 0; i < rows.length; i++) {{
                if (rows[i].style.display === 'table-row') {{
                    anyLogOpened = true;
                    break;
                }}
            }}
        }}

        // 使用者隨意點擊畫面即可解鎖瀏覽器音頻權限
        window.addEventListener('click', function() {{
            if (!audioCtx) {{
                audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            }}
            checkAlertAudio();
        }}, {{ once: true }});

        window.onload = function() {{
            checkAlertAudio();
        }};

        // 每 15 分鐘 (900秒) 自動重整
        setInterval(function() {{
            if (!anyLogOpened) {{
                window.location.reload();
            }}
        }}, 900000);
    </script>
</head>
<body>
    <div class="container">
        <!-- 頂部戰情室 Header 區塊 -->
        <div class="header">
            <div>
                <h1 style="margin:0 0 10px 0; font-size:26px; color:#f8fafc; font-weight:800; letter-spacing:0.5px; display:flex; align-items:center; gap:10px;">
                    <span style="font-size:28px;">🌐</span> 4G/5G 行網 Internet 路由巡檢與導入監控儀表板
                </h1>
                <div style="font-size:14px; color:#94a3b8; line-height:1.7;">
                    📍 <strong>探測來源：</strong>固定 Taiwan 探針節點發起（未達 BR 自動重測 2 次機制生效中）<br>
                    ⏱ <strong>最後巡檢時間：</strong><span style="color:#38bdf8; font-weight:bold;">{now_str} (UTC+8)</span><br>
                    🔄 <strong>畫面狀態：</strong>每隔15-30分鐘自動重整
                </div>
            </div>
            
            <!-- 右上角按鈕組 -->
            <div style="display:flex; gap:12px; align-items:center;">
                <!-- ACK 消音按鈕 (有異常時才會動態亮出) -->
                <button id="ack-btn" onclick="ackAlarm()" style="display:none; background:#ef4444; color:#fff; border:none; border-radius:8px; padding:11px 18px; cursor:pointer; font-size:15px; font-weight:bold; box-shadow:0 0 15px rgba(239,68,68,0.7); animation:pulse 1.5s infinite;">
                    🔕 靜音警報 (ACK)
                </button>
                <button onclick="window.location.reload()" style="background:#0284c7; color:#fff; border:none; border-radius:8px; padding:11px 20px; cursor:pointer; font-size:15px; font-weight:bold; box-shadow:0 4px 12px rgba(2,132,199,0.3); transition:all 0.2s;" onmouseover="this.style.background='#0369a1'" onmouseout="this.style.background='#0284c7'">
                    🔄 畫面刷新
                </button>
                <a href="https://github.com/swa1216-wei/ip-tracer/actions/workflows/schedule.yml" target="_blank" style="text-decoration:none; background:#10b981; color:#fff; border-radius:8px; padding:11px 20px; font-size:15px; font-weight:bold; display:inline-block; box-shadow:0 4px 12px rgba(16,185,129,0.3); transition:all 0.2s;" onmouseover="this.style.background='#059669'" onmouseout="this.style.background='#10b981'">
                    ⚡ 立即發起探測
                </a>
            </div>
        </div>

        <!-- 長官焦點：核心狀態三格指標 -->
        <div class="cards">
            <!-- 卡片 1：總監控目標 -->
            <div style="background:linear-gradient(145deg, #1e293b, #0f172a); border-radius:12px; padding:22px 24px; border:1px solid #334155; border-top:4px solid #38bdf8; box-shadow:0 8px 20px rgba(0,0,0,0.3);">
                <div style="font-size:14px; font-weight:bold; color:#94a3b8; text-transform:uppercase; letter-spacing:0.8px; margin-bottom:8px;">
                    監控目標總數
                </div>
                <div style="font-size:36px; font-weight:900; color:#ffffff;">
                    {total_count} <span style="font-size:18px; color:#94a3b8; font-weight:normal;">組 IP</span>
                </div>
            </div>

            <!-- 卡片 2：正常連通 -->
            <div style="background:linear-gradient(145deg, #1e293b, #0f172a); border-radius:12px; padding:22px 24px; border:1px solid #1e3a2b; border-top:4px solid #10b981; box-shadow:0 8px 20px rgba(16,185,129,0.15);">
                <div style="font-size:14px; font-weight:bold; color:#a7f3d0; text-transform:uppercase; letter-spacing:0.8px; margin-bottom:8px;">
                    正常連通 (抵達 8 台 BR)
                </div>
                <div style="font-size:36px; font-weight:900; color:#34d399; text-shadow:0 0 12px rgba(52,211,153,0.4);">
                    {ok_count} <span style="font-size:18px; color:#a7f3d0; font-weight:normal;">組</span>
                </div>
            </div>

            <!-- 卡片 3：異常導入 -->
            <div style="background:linear-gradient(145deg, #1e293b, #0f172a); border-radius:12px; padding:22px 24px; border:1px solid #3f1d24; border-top:4px solid #f43f5e; box-shadow:0 8px 20px rgba(244,63,94,0.12);">
                <div style="font-size:14px; font-weight:bold; color:#fecdd3; text-transform:uppercase; letter-spacing:0.8px; margin-bottom:8px;">
                    異常 / 清洗導入中
                </div>
                <div style="font-size:36px; font-weight:900; color:{alert_color}; {alert_glow}">
                    {alert_count} <span style="font-size:18px; font-weight:normal; color:#94a3b8;">組</span>
                </div>
            </div>
        </div>

        <!-- 24小時夜間巡檢健康摘要橫條 -->
        <div style="background:rgba(15,23,42,0.8); border:1px solid #334155; border-radius:8px; padding:12px 18px; margin-bottom:28px; font-size:14px; display:flex; align-items:center; gap:8px;">
            <span style="font-size:16px;">📊</span> <strong>24小時巡檢健康指標：</strong> {stat_24h_html}
        </div>

        <!-- 路由現況表格 -->
        <table>
            <thead>
                <tr>
                    <th>目標 IP / 所屬網段</th>
                    <th>元件 / NAT 群組</th>
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

        <!-- 變更流水帳 LOG (置底歷史記錄) -->
        <div class="audit-box">
            <h3 style="margin-top:0;margin-bottom:12px;font-size:18px;color:#38bdf8;">📋 路由變更流水帳 (導入 / 導出歷史紀錄)</h3>
            <ul style="margin:0;padding-left:24px;font-size:14px;line-height:1.7;">
                {logs_html}
            </ul>
        </div>

        <!-- 過去 7 天排程巡檢歷史三格趨勢紀錄 (跨週末/連假回溯專用) -->
        <div class="history-box">
            <h3 style="margin-top:0;margin-bottom:12px;font-size:18px;color:#c084fc;">📈 過去 7 天排程巡檢歷史快照 (連假/週末不在場回溯)</h3>
            <p style="font-size:13px;color:#94a3b8;margin-top:0;margin-bottom:14px;">此處記錄每一次自動巡檢的三格核心指標，連假或放假回崗時可直接比對是否有出現過異常波峰：</p>
            <div style="max-height:280px; overflow-y:auto; border-radius:8px; border:1px solid #334155;">
                <table style="box-shadow:none;">
                    <thead>
                        <tr>
                            <th style="background:#090d16;padding:10px 12px;">巡檢時間點</th>
                            <th style="background:#090d16;padding:10px 12px;">目標總數</th>
                            <th style="background:#090d16;padding:10px 12px;">正常連通數</th>
                            <th style="background:#090d16;padding:10px 12px;">異常/導入數</th>
                        </tr>
                    </thead>
                    <tbody>
                        {history_rows_html}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
</body>
</html>
"""

# 寫入產出成果
with open("index.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 巡檢完成，已產出完整儀表板。")
