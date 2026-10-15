import json
import time
import os
import re
import ipaddress
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

# ==============================================================================
# 第一部分：系統設定區 (16 台 BR 白名單、海外輪播節點、102 組目標 IP 元數據)
# ==============================================================================

FALLBACK_WEBHOOK_URL = "https://default54eb9440cf0345fe835e61bd4ce515.c8.environment.api.powerplatform.com:443/powerautomate/automations/direct/cu/29/workflows/1d01e593bf3649bea39eba02ca408ecb/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=Gv35_ySE7WCYKGPhw-41NZfHFw7ODncvpvxHvCj4VUc"

BR_LIST = {
    "210.65.126.209": "3L1BR1",
    "210.65.126.213": "3L1BR2",
    "210.65.126.101": "4L1BR3",
    "210.65.126.105": "4L1BR4",
    "210.65.126.193": "7L2BR1",
    "210.65.126.197": "7L2BR2",
    "210.65.126.93":  "2L3BR3",
    "210.65.126.97":  "2L3BR4",
    "211.22.229.189": "2L1-BR1(D)",
    "211.22.229.193": "2L1-BR2(D)",
    "203.74.98.85":   "CHCH-BR1(D)",
    "203.75.166.85":  "CHCH-BR2(D)",
    "203.75.166.81":  "TNE1-BR1(D)",
    "203.74.98.81":   "TNE1-BR2(D)",
    "211.22.229.205": "3L1EPG4U-BR1(D)",
    "211.22.229.201": "3L1EPG4U-BR2(D)"
}

OVERSEAS_NODES = [
    {"country": "JP", "city": "東京", "flag": "🇯🇵"},
    {"country": "HK", "city": "香港", "flag": "🇭🇰"},
    {"country": "SG", "city": "新加坡", "flag": "🇸🇬"}
]

TARGET_METADATA = {
  # --- 3L1 機房 ---
  "42.72.0.8":     {"cidr": "42.72.0.0/17",    "part": "前半", "elem": "3L1EPG1",   "nat": "NAT5/6"},
  "42.72.64.8":    {"cidr": "42.72.0.0/17",    "part": "後半", "elem": "3L1EPG1",   "nat": "NAT5/6"},
  "42.72.128.8":   {"cidr": "42.72.128.0/17",  "part": "前半", "elem": "3L1EPG1",   "nat": "NAT5/6"},
  "42.72.192.8":   {"cidr": "42.72.128.0/17",  "part": "後半", "elem": "3L1EPG1",   "nat": "NAT5/6"},
  "42.73.0.8":     {"cidr": "42.73.0.0/16",    "part": "前半", "elem": "3L1EPG2",   "nat": "NAT5/6"},
  "42.73.128.8":   {"cidr": "42.73.0.0/16",    "part": "後半", "elem": "3L1EPG2",   "nat": "NAT5/6"},
  "114.137.0.8":   {"cidr": "114.137.0.0/18",  "part": "前半", "elem": "3L1EPG3",   "nat": "NAT7/8"},
  "114.137.32.8":  {"cidr": "114.137.0.0/18",  "part": "後半", "elem": "3L1EPG3",   "nat": "NAT7/8"},
  "114.137.64.8":  {"cidr": "114.137.64.0/18", "part": "前半", "elem": "3L1EPG3",   "nat": "NAT7/8"},
  "114.137.96.8":  {"cidr": "114.137.64.0/18", "part": "後半", "elem": "3L1EPG3",   "nat": "NAT7/8"},
  "114.137.128.8": {"cidr": "114.137.128.0/17","part": "前半", "elem": "3L1EPG3",   "nat": "NAT7/8"},
  "114.137.192.8": {"cidr": "114.137.128.0/17","part": "後半", "elem": "3L1EPG3",   "nat": "NAT7/8"},
  "42.79.0.8":     {"cidr": "42.79.0.0/17",    "part": "前半", "elem": "3L1EPG4U",  "nat": "NAT7/8"},
  "42.79.64.8":    {"cidr": "42.79.0.0/17",    "part": "後半", "elem": "3L1EPG4U",  "nat": "NAT7/8"},

  # --- 4L1 機房 ---
  "42.76.0.8":     {"cidr": "42.76.0.0/17",    "part": "前半", "elem": "4L1EPG1",   "nat": "NAT7/8"},
  "42.76.64.8":    {"cidr": "42.76.0.0/17",    "part": "後半", "elem": "4L1EPG1",   "nat": "NAT7/8"},
  "42.76.128.8":   {"cidr": "42.76.128.0/17",  "part": "前半", "elem": "4L1EPG1",   "nat": "NAT7/8"},
  "42.76.192.8":   {"cidr": "42.76.128.0/17",  "part": "後半", "elem": "4L1EPG1",   "nat": "NAT7/8"},
  "111.71.214.8":  {"cidr": "111.71.214.0/23", "part": "前半", "elem": "4L1EPG1",   "nat": "NAT7/8"},
  "111.71.215.8":  {"cidr": "111.71.214.0/23", "part": "後半", "elem": "4L1EPG1",   "nat": "NAT7/8"},
  "223.141.0.8":   {"cidr": "223.141.0.0/17",  "part": "前半", "elem": "4L1EPG1",   "nat": "NAT7/8"},
  "223.141.64.8":  {"cidr": "223.141.0.0/17",  "part": "後半", "elem": "4L1EPG1",   "nat": "NAT7/8"},
  "111.82.0.8":    {"cidr": "111.82.0.0/16",   "part": "前半", "elem": "4L1EPG2",   "nat": "NAT7/8"},
  "111.82.128.8":  {"cidr": "111.82.0.0/16",   "part": "後半", "elem": "4L1EPG2",   "nat": "NAT7/8"},
  "111.83.0.8":    {"cidr": "111.83.0.0/17",   "part": "前半", "elem": "4L1EPG2",   "nat": "NAT7/8"},
  "111.83.64.8":   {"cidr": "111.83.0.0/17",   "part": "後半", "elem": "4L1EPG2",   "nat": "NAT7/8"},
  "42.79.192.8":   {"cidr": "42.79.192.0/18",  "part": "前半", "elem": "4L1EPG3",   "nat": "NAT9/10"},
  "42.79.224.8":   {"cidr": "42.79.192.0/18",  "part": "後半", "elem": "4L1EPG3",   "nat": "NAT9/10"},
  "111.83.128.8":  {"cidr": "111.83.128.0/17", "part": "前半", "elem": "4L1EPG3",   "nat": "NAT9/10"},
  "111.83.192.8":  {"cidr": "111.83.128.0/17", "part": "後半", "elem": "4L1EPG3",   "nat": "NAT9/10"},
  "223.138.128.8": {"cidr": "223.138.128.0/17","part": "前半", "elem": "4L1EPG3",   "nat": "NAT9/10"},
  "223.138.192.8": {"cidr": "223.138.128.0/17","part": "後半", "elem": "4L1EPG3",   "nat": "NAT9/10"},
  "223.141.128.8": {"cidr": "223.141.128.0/18","part": "前半", "elem": "4L1EPG3",   "nat": "NAT9/10"},
  "223.141.160.8": {"cidr": "223.141.128.0/18","part": "後半", "elem": "4L1EPG3",   "nat": "NAT9/10"},
  "223.141.192.8": {"cidr": "223.141.192.0/18","part": "前半", "elem": "4L1EPG3",   "nat": "NAT9/10"},
  "223.141.224.8": {"cidr": "223.141.192.0/18","part": "後半", "elem": "4L1EPG3",   "nat": "NAT9/10"},

  # --- 7L2 機房 ---
  "42.77.0.8":     {"cidr": "42.77.0.0/17",    "part": "前半", "elem": "7L2EPG1",   "nat": "NAT7/8"},
  "42.77.64.8":    {"cidr": "42.77.0.0/17",    "part": "後半", "elem": "7L2EPG1",   "nat": "NAT7/8"},
  "223.138.0.8":   {"cidr": "223.138.0.0/18",  "part": "前半", "elem": "7L2EPG1",   "nat": "NAT7/8"},
  "223.138.32.8":  {"cidr": "223.138.0.0/18",  "part": "後半", "elem": "7L2EPG1",   "nat": "NAT7/8"},
  "223.139.0.8":   {"cidr": "223.139.0.0/17",  "part": "前半", "elem": "7L2EPG1",   "nat": "NAT7/8"},
  "223.139.64.8":  {"cidr": "223.139.0.0/17",  "part": "後半", "elem": "7L2EPG1",   "nat": "NAT7/8"},
  "42.77.128.8":   {"cidr": "42.77.128.0/17",  "part": "前半", "elem": "7L2EPG2",   "nat": "NAT9/10"},
  "42.77.192.8":   {"cidr": "42.77.128.0/17",  "part": "後半", "elem": "7L2EPG2",   "nat": "NAT9/10"},
  "223.138.64.8":  {"cidr": "223.138.64.0/18", "part": "前半", "elem": "7L2EPG2",   "nat": "NAT9/10"},
  "223.138.96.8":  {"cidr": "223.138.64.0/18", "part": "後半", "elem": "7L2EPG2",   "nat": "NAT9/10"},
  "223.139.128.8": {"cidr": "223.139.128.0/17","part": "前半", "elem": "7L2EPG2",   "nat": "NAT9/10"},
  "223.139.192.8": {"cidr": "223.139.128.0/17","part": "後半", "elem": "7L2EPG2",   "nat": "NAT9/10"},
  "42.74.0.8":     {"cidr": "42.74.0.0/15",    "part": "前半", "elem": "7L2EPG3",   "nat": "NAT7/8"},
  "42.75.0.8":     {"cidr": "42.74.0.0/15",    "part": "後半", "elem": "7L2EPG3",   "nat": "NAT7/8"},

  # --- 2L3 機房 ---
  "111.71.212.8":  {"cidr": "111.71.212.0/23", "part": "前半", "elem": "2L3EPG1",   "nat": "NAT7/8"},
  "111.71.213.8":  {"cidr": "111.71.212.0/23", "part": "後半", "elem": "2L3EPG1",   "nat": "NAT7/8"},
  "114.136.0.8":   {"cidr": "114.136.0.0/17",  "part": "前半", "elem": "2L3EPG1",   "nat": "NAT7/8"},
  "114.136.64.8":  {"cidr": "114.136.0.0/17",  "part": "後半", "elem": "2L3EPG1",   "nat": "NAT7/8"},
  "223.136.0.8":   {"cidr": "223.136.0.0/18",  "part": "前半", "elem": "2L3EPG1",   "nat": "NAT7/8"},
  "223.136.32.8":  {"cidr": "223.136.0.0/18",  "part": "後半", "elem": "2L3EPG1",   "nat": "NAT7/8"},
  "223.140.0.8":   {"cidr": "223.140.0.0/17",  "part": "前半", "elem": "2L3EPG1",   "nat": "NAT7/8"},
  "223.140.64.8":  {"cidr": "223.140.0.0/17",  "part": "後半", "elem": "2L3EPG1",   "nat": "NAT7/8"},
  "114.136.128.8": {"cidr": "114.136.128.0/17","part": "前半", "elem": "2L3EPG2",   "nat": "NAT9/10"},
  "114.136.192.8": {"cidr": "114.136.128.0/17","part": "後半", "elem": "2L3EPG2",   "nat": "NAT9/10"},
  "223.136.64.8":  {"cidr": "223.136.64.0/18", "part": "前半", "elem": "2L3EPG2",   "nat": "NAT9/10"},
  "223.136.96.8":  {"cidr": "223.136.64.0/18", "part": "後半", "elem": "2L3EPG2",   "nat": "NAT9/10"},
  "223.140.128.8": {"cidr": "223.140.128.0/17","part": "前半", "elem": "2L3EPG2",   "nat": "NAT9/10"},
  "223.140.192.8": {"cidr": "223.140.128.0/17","part": "後半", "elem": "2L3EPG2",   "nat": "NAT9/10"},
  "223.137.0.8":   {"cidr": "223.137.0.0/17",  "part": "前半", "elem": "2L3EPG3",   "nat": "NAT7/8"},
  "223.137.64.8":  {"cidr": "223.137.0.0/17",  "part": "後半", "elem": "2L3EPG3",   "nat": "NAT7/8"},
  "223.137.128.8": {"cidr": "223.137.128.0/17","part": "前半", "elem": "2L3EPG3",   "nat": "NAT7/8"},
  "223.137.192.8": {"cidr": "223.137.128.0/17","part": "後半", "elem": "2L3EPG3",   "nat": "NAT7/8"},
  "42.79.128.8":   {"cidr": "42.79.128.0/18",  "part": "前半", "elem": "2L3EPG4",   "nat": "NAT9/10"},
  "42.79.160.8":   {"cidr": "42.79.128.0/18",  "part": "後半", "elem": "2L3EPG4",   "nat": "NAT9/10"},
  "111.71.0.8":    {"cidr": "111.71.0.0/17",   "part": "前半", "elem": "2L3EPG4",   "nat": "NAT9/10"},
  "111.71.64.8":   {"cidr": "111.71.0.0/17",   "part": "後半", "elem": "2L3EPG4",   "nat": "NAT9/10"},
  "223.136.128.8": {"cidr": "223.136.128.0/17","part": "前半", "elem": "2L3EPG4",   "nat": "NAT9/10"},
  "223.136.192.8": {"cidr": "223.136.128.0/17","part": "後半", "elem": "2L3EPG4",   "nat": "NAT9/10"},
  "223.143.240.8": {"cidr": "223.143.240.0/22","part": "前半", "elem": "2L3EPG4",   "nat": "NAT9/10"},
  "223.143.242.8": {"cidr": "223.143.240.0/22","part": "後半", "elem": "2L3EPG4",   "nat": "NAT9/10"},
  "223.143.244.8": {"cidr": "223.143.244.0/22","part": "前半", "elem": "2L3EPG4",   "nat": "NAT9/10"},
  "223.143.246.8": {"cidr": "223.143.244.0/22","part": "後半", "elem": "2L3EPG4",   "nat": "NAT9/10"},

  # --- 邊緣雲 - 三重 (2L1) ---
  "42.70.0.8":     {"cidr": "42.70.0.0/18",   "part": "前半", "elem": "2L1EPG11U", "nat": "2L1NAT1/2"},
  "42.70.32.8":    {"cidr": "42.70.0.0/18",   "part": "後半", "elem": "2L1EPG11U", "nat": "2L1NAT1/2"},
  "42.70.64.8":    {"cidr": "42.70.64.0/18",  "part": "前半", "elem": "2L1EPG11U", "nat": "2L1NAT1/2"},
  "42.70.96.8":    {"cidr": "42.70.64.0/18",  "part": "後半", "elem": "2L1EPG11U", "nat": "2L1NAT1/2"},
  "42.70.128.8":   {"cidr": "42.70.128.0/18", "part": "前半", "elem": "2L1EPG12U", "nat": "2L1NAT1/2"},
  "42.70.160.8":   {"cidr": "42.70.128.0/18", "part": "後半", "elem": "2L1EPG12U", "nat": "2L1NAT1/2"},
  "42.70.192.8":   {"cidr": "42.70.192.0/18", "part": "前半", "elem": "2L1EPG12U", "nat": "2L1NAT1/2"},
  "42.70.224.8":   {"cidr": "42.70.192.0/18", "part": "後半", "elem": "2L1EPG12U", "nat": "2L1NAT1/2"},

  # --- 邊緣雲 - 彰化 (CHCH) ---
  "42.71.0.8":     {"cidr": "42.71.0.0/18",   "part": "前半", "elem": "CHCHEPG11U", "nat": "CHCHNAT1/2"},
  "42.71.32.8":    {"cidr": "42.71.0.0/18",   "part": "後半", "elem": "CHCHEPG11U", "nat": "CHCHNAT1/2"},
  "42.71.64.8":    {"cidr": "42.71.64.0/18",  "part": "前半", "elem": "CHCHEPG11U", "nat": "CHCHNAT1/2"},
  "42.71.96.8":    {"cidr": "42.71.64.0/18",  "part": "後半", "elem": "CHCHEPG11U", "nat": "CHCHNAT1/2"},
  "42.71.128.8":   {"cidr": "42.71.128.0/18", "part": "前半", "elem": "CHCHEPG12U", "nat": "CHCHNAT1/2"},
  "42.71.160.8":   {"cidr": "42.71.128.0/18", "part": "後半", "elem": "CHCHEPG12U", "nat": "CHCHNAT1/2"},
  "42.71.192.8":   {"cidr": "42.71.192.0/18", "part": "前半", "elem": "CHCHEPG12U", "nat": "CHCHNAT1/2"},
  "42.71.224.8":   {"cidr": "42.71.192.0/18", "part": "後半", "elem": "CHCHEPG12U", "nat": "CHCHNAT1/2"},

  # --- 邊緣雲 - 台南 (TNE1) ---
  "42.78.0.8":     {"cidr": "42.78.0.0/18",   "part": "前半", "elem": "TNE1EPG11U", "nat": "TNE1NAT1/2"},
  "42.78.32.8":    {"cidr": "42.78.0.0/18",   "part": "後半", "elem": "TNE1EPG11U", "nat": "TNE1NAT1/2"},
  "42.78.64.8":    {"cidr": "42.78.64.0/18",  "part": "前半", "elem": "TNE1EPG11U", "nat": "TNE1NAT1/2"},
  "42.78.96.8":    {"cidr": "42.78.64.0/18",  "part": "後半", "elem": "TNE1EPG11U", "nat": "TNE1NAT1/2"},
  "42.78.128.8":   {"cidr": "42.78.128.0/18", "part": "前半", "elem": "TNE1EPG12U", "nat": "TNE1NAT1/2"},
  "42.78.160.8":   {"cidr": "42.78.128.0/18", "part": "後半", "elem": "TNE1EPG12U", "nat": "TNE1NAT1/2"},
  "42.78.192.8":   {"cidr": "42.78.192.0/18", "part": "前半", "elem": "TNE1EPG12U", "nat": "TNE1NAT1/2"},
  "42.78.224.8":   {"cidr": "42.78.192.0/18", "part": "後半", "elem": "TNE1EPG12U", "nat": "TNE1NAT1/2"}
}

TARGET_IPS = list(TARGET_METADATA.keys())
HISTORY_FILE = "monitor_state.json"

tz = timezone(timedelta(hours=8))
now_dt = datetime.now(tz)
now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')
today_date_str = now_dt.strftime('%Y-%m-%d')

state = {
    "baselines_tw": {},
    "baselines_overseas": {},
    "node_status": {}, # 獨立節點狀態記錄：例如 "TW", "JP", "HK", "SG"
    "event_logs": [],
    "history_snapshots": [],
    "overseas_idx": 0
}

if os.path.exists(HISTORY_FILE):
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            saved_state = json.load(f)
            state.update(saved_state)
            if "node_status" not in state:
                state["node_status"] = {}
    except Exception:
        pass

current_overseas = OVERSEAS_NODES[state.get("overseas_idx", 0) % len(OVERSEAS_NODES)]
state["overseas_idx"] = (state.get("overseas_idx", 0) + 1) % len(OVERSEAS_NODES)


# ==============================================================================
# 第二部分：Teams Webhook 推播函式 (Adaptive Card 格式)
# ==============================================================================

def send_teams_notification(title, is_alert, event_items, timestamp_str):
    webhook_url = os.environ.get("TEAMS_WEBHOOK_URL", "").strip()
    if not webhook_url:
        webhook_url = FALLBACK_WEBHOOK_URL

    header_color = "Attention" if is_alert else "Good"
    icon = "🚨" if is_alert else "🟢"

    facts = []
    for item in event_items:
        if "value" in item:
            facts.append({"title": item["title"], "value": item["value"]})
        else:
            facts.append({
                "title": f"{item['target']}",
                "value": f"{item['elem']} [{item['cidr']} {item['part']}] ➔ {item['desc']}"
            })

    adaptive_payload = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "size": "Large",
                            "weight": "Bolder",
                            "text": f"{icon} {title}",
                            "color": header_color
                        },
                        {
                            "type": "TextBlock",
                            "text": f"回報時間：{timestamp_str} (UTC+8)",
                            "isSubtle": True,
                            "spacing": "None"
                        },
                        {
                            "type": "FactSet",
                            "facts": facts,
                            "separator": True
                        }
                    ],
                    "actions": [
                        {
                            "type": "Action.OpenUrl",
                            "title": "🌐 開啟即時戰情室儀表板",
                            "url": "https://swa1216-wei.github.io/ip-tracer/"
                        }
                    ]
                }
            }
        ]
    }

    try:
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(adaptive_payload).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            print(f"[Teams 推播] 發送成功 (Status: {resp.status}) - {title}")
    except Exception as e:
        print(f"[Teams 推播] 發送失敗: {e}")


# ==============================================================================
# 第三部分：探測與邏輯 (10 並行，進入 HiNet 後才檢驗私網)
# ==============================================================================

def run_single_trace(target, country_code):
    api_url = "https://api.globalping.io/v1/measurements"
    payload = json.dumps({
        "type": "traceroute",
        "target": target,
        "locations": [{"country": country_code}],
        "limit": 1
    }).encode("utf-8")
    
    req = urllib.request.Request(
        api_url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "GitHub-Network-Probe"}
    )
    
    measurement_id = None
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                measurement_id = json.loads(resp.read().decode("utf-8")).get("id")
                break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt == 0:
                time.sleep(3)
                continue
            return target, country_code, f"發起請求失敗: {e}"
        except Exception as e:
            return target, country_code, f"發起請求失敗: {e}"

    if not measurement_id:
        return target, country_code, "無法取得測量ID"

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
                        return target, country_code, results[0].get("result", {}).get("rawOutput", "")
        except Exception:
            pass
    return target, country_code, "探測逾時"

def parse_trace_output(target, raw_output, country_code, city_name):
    last_responded_hop = 0
    last_responded_ip = "無回應"
    last_responded_domain = ""
    entered_hinet = (country_code == "TW") # 台灣端預設直接算在國內
    hinet_private_ips = []

    lines = raw_output.splitlines()
    for line in lines:
        match_hop = re.match(r'^\s*(\d+)\s+', line)
        if match_hop:
            hop_num = int(match_hop.group(1))
            ips_in_line = re.findall(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', line)
            if ips_in_line:
                effective_ips = [ip for ip in ips_in_line if ip != target]
                if effective_ips:
                    last_responded_hop = hop_num
                    last_responded_ip = effective_ips[-1]

                    if "hinet.net" in line.lower():
                        entered_hinet = True
                        domain_match = re.search(r'([a-zA-Z0-9\-\.]+\.hinet\.net)', line)
                        if domain_match:
                            last_responded_domain = domain_match.group(1)

                    # 只要碰到 16 台 BR，也絕對代表進入中華電信
                    if last_responded_ip in BR_LIST:
                        entered_hinet = True

                    # 只有在正式進入 HiNet 骨幹後的跳數，才檢驗私網 IP
                    if entered_hinet and hop_num >= 2:
                        for ip in effective_ips:
                            try:
                                if ipaddress.ip_address(ip) in ipaddress.ip_network("10.0.0.0/8"):
                                    if ip not in hinet_private_ips:
                                        hinet_private_ips.append(ip)
                            except ValueError:
                                pass

    br_name = BR_LIST.get(last_responded_ip, None)
    is_br = (br_name is not None)
    has_private = (len(hinet_private_ips) > 0)

    # 狀態細分：OK、NO_BR（未達BR）、PRIVATE（已達但含私網）
    if not is_br:
        cat = "NO_BR"
        status = "ALERT"
        desc = f"🚨 {city_name}丟包 (未達 BR)"
    elif has_private:
        cat = "PRIVATE"
        status = "ALERT"
        desc = f"⚠️ {br_name} (含私網 {len(hinet_private_ips)}處)"
    else:
        cat = "OK"
        status = "OK"
        desc = f"✔ {br_name}"

    return {
        "last_hop": last_responded_hop,
        "last_ip": last_responded_ip,
        "domain": last_responded_domain,
        "is_br": is_br,
        "br_name": br_name,
        "has_private": has_private,
        "private_ips": hinet_private_ips,
        "cat": cat,
        "status": status,
        "desc": desc,
        "raw": raw_output
    }

# 派發 204 個探測任務（以 10 產線平衡平穩發送）
all_tasks = []
for target in TARGET_IPS:
    all_tasks.append((target, "TW"))
    all_tasks.append((target, current_overseas["country"]))

print(f"[{now_str}] 雙向巡檢 (🇹🇼 台灣 + {current_overseas['flag']} {current_overseas['city']}) 共 {len(all_tasks)} 個任務，以 10 產線啟動...")

raw_results = {}
with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_task = {executor.submit(run_single_trace, t[0], t[1]): t for t in all_tasks}
    for future in as_completed(future_to_task):
        target, c_code, raw_out = future.result()
        raw_results[(target, c_code)] = raw_out

# 解析初測結果
parsed_tw = {}
parsed_overseas = {}
retry_tasks = []

for target in TARGET_IPS:
    res_tw = parse_trace_output(target, raw_results.get((target, "TW"), ""), "TW", "台灣")
    parsed_tw[target] = res_tw
    if res_tw["cat"] == "NO_BR" and "429" not in res_tw["raw"]:
        retry_tasks.append((target, "TW"))

    res_ov = parse_trace_output(target, raw_results.get((target, current_overseas["country"]), ""), current_overseas["country"], current_overseas["city"])
    parsed_overseas[target] = res_ov
    if res_ov["cat"] == "NO_BR" and "429" not in res_ov["raw"]:
        retry_tasks.append((target, current_overseas["country"]))

# 複測未達 BR 任務 (最多 2 次)
if retry_tasks:
    print(f"[{now_str}] 偵測到 {len(retry_tasks)} 個任務未達 BR，啟動複測重試機制 (最多 2 次)...")
    for retry_round in range(1, 3):
        if not retry_tasks:
            break
        time.sleep(2)
        still_failing = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_task = {executor.submit(run_single_trace, t[0], t[1]): t for t in retry_tasks}
            for future in as_completed(future_to_task):
                target, c_code, raw_out = future.result()
                city_label = "台灣" if c_code == "TW" else current_overseas["city"]
                retry_p = parse_trace_output(target, raw_out, c_code, city_label)
                if retry_p["is_br"]:
                    print(f"     ✔ [{city_label}] 目標 {target} 在第 {retry_round} 次複測成功抵達 {retry_p['br_name']}！")
                    if c_code == "TW":
                        parsed_tw[target] = retry_p
                    else:
                        parsed_overseas[target] = retry_p
                else:
                    still_failing.append((target, c_code))
        retry_tasks = still_failing

# 統計四大分類計數
tw_nobr_count = 0
tw_priv_count = 0
ov_nobr_count = 0
ov_priv_count = 0

current_results = []
newly_alerted = []
newly_recovered = []

# 初始化節點專屬狀態庫
for node_key in ["TW", current_overseas["country"]]:
    if node_key not in state["node_status"]:
        state["node_status"][node_key] = {}

for target in TARGET_IPS:
    meta = TARGET_METADATA.get(target, {})
    p_tw = parsed_tw[target]
    p_ov = parsed_overseas[target]

    # 跳數基準
    if target not in state["baselines_tw"] and p_tw["last_hop"] > 0:
        state["baselines_tw"][target] = p_tw["last_hop"]
    base_tw = state["baselines_tw"].get(target, p_tw["last_hop"])
    diff_tw = p_tw["last_hop"] - base_tw
    diff_tw_str = f"+{diff_tw}" if diff_tw > 0 else (str(diff_tw) if diff_tw < 0 else "0")

    ov_base_key = f"{target}_{current_overseas['country']}"
    if ov_base_key not in state["baselines_overseas"] and p_ov["last_hop"] > 0:
        state["baselines_overseas"][ov_base_key] = p_ov["last_hop"]
    base_ov = state["baselines_overseas"].get(ov_base_key, p_ov["last_hop"])
    diff_ov = p_ov["last_hop"] - base_ov
    diff_ov_str = f"+{diff_ov}" if diff_ov > 0 else (str(diff_ov) if diff_ov < 0 else "0")

    # 分類計數
    if p_tw["cat"] == "NO_BR": tw_nobr_count += 1
    elif p_tw["cat"] == "PRIVATE": tw_priv_count += 1

    if p_ov["cat"] == "NO_BR": ov_nobr_count += 1
    elif p_ov["cat"] == "PRIVATE": ov_priv_count += 1

    # 綜合判定 (精準描述，抵達 BR 就老實寫抵達 BR)
    is_alert = (p_tw["status"] == "ALERT" or p_ov["status"] == "ALERT")
    if p_tw["cat"] == "NO_BR" and p_ov["cat"] == "NO_BR":
        combined_status = "ALERT"
        combined_desc = f"🔴 雙向丟包 (台灣未達 / {current_overseas['city']}未達)"
    elif p_tw["cat"] == "NO_BR":
        combined_status = "ALERT"
        combined_desc = "🚨 台灣端丟包 (未達 BR)"
    elif p_ov["cat"] == "NO_BR":
        combined_status = "ALERT"
        combined_desc = f"🚨 {current_overseas['city']}端丟包 (未達 BR)"
    elif p_tw["cat"] == "PRIVATE" or p_ov["cat"] == "PRIVATE":
        combined_status = "ALERT"
        combined_desc = f"⚠️ 雙向抵達 BR (但 HiNet 骨幹含私網)"
    else:
        combined_status = "OK"
        combined_desc = "✔ 雙向正常抵達 BR"

    target_label = f"{target} [{meta.get('cidr')} {meta.get('part')}]"

    # --- 獨立節點狀態比對 (台灣獨立、境外各城市獨立) ---
    # 1. 檢驗台灣端
    prev_tw = state["node_status"]["TW"].get(target, "OK")
    if prev_tw == "OK" and p_tw["status"] == "ALERT":
        state["event_logs"].insert(0, f"🔴 {now_str} 【台灣告警】{target_label} ➔ {p_tw['desc']}")
        newly_alerted.append({"target": target, "elem": meta.get("elem",""), "cidr": meta.get("cidr",""), "part": meta.get("part",""), "desc": f"🇹🇼 台灣端: {p_tw['desc']}"})
    elif prev_tw == "ALERT" and p_tw["status"] == "OK":
        state["event_logs"].insert(0, f"🟢 {now_str} 【台灣恢復】{target_label} ➔ 抵達 {p_tw['br_name']}")
        newly_recovered.append({"target": target, "elem": meta.get("elem",""), "cidr": meta.get("cidr",""), "part": meta.get("part",""), "desc": f"🇹🇼 台灣端已恢復連通 (抵達 {p_tw['br_name']})"})
    state["node_status"]["TW"][target] = p_tw["status"]

    # 2. 檢驗當前境外城市端 (只比對同一個城市的歷史紀錄)
    ov_c = current_overseas["country"]
    prev_ov = state["node_status"][ov_c].get(target, "OK")
    if prev_ov == "OK" and p_ov["status"] == "ALERT":
        state["event_logs"].insert(0, f"🔴 {now_str} 【{current_overseas['city']}告警】{target_label} ➔ {p_ov['desc']}")
        newly_alerted.append({"target": target, "elem": meta.get("elem",""), "cidr": meta.get("cidr",""), "part": meta.get("part",""), "desc": f"{current_overseas['flag']} {current_overseas['city']}端: {p_ov['desc']}"})
    elif prev_ov == "ALERT" and p_ov["status"] == "OK":
        state["event_logs"].insert(0, f"🟢 {now_str} 【{current_overseas['city']}恢復】{target_label} ➔ 抵達 {p_ov['br_name']}")
        newly_recovered.append({"target": target, "elem": meta.get("elem",""), "cidr": meta.get("cidr",""), "part": meta.get("part",""), "desc": f"{current_overseas['flag']} {current_overseas['city']}端已恢復連通 (抵達 {p_ov['br_name']})"})
    state["node_status"][ov_c][target] = p_ov["status"]

    current_results.append({
        "target": target,
        "cidr": meta.get("cidr", ""),
        "part": meta.get("part", ""),
        "elem": meta.get("elem", ""),
        "nat": meta.get("nat", ""),
        "tw_hop": p_tw["last_hop"],
        "tw_base": base_tw,
        "tw_diff": diff_tw_str,
        "tw_node": p_tw["last_ip"],
        "tw_br": p_tw["br_name"],
        "tw_desc": p_tw["desc"],
        "tw_cat": p_tw["cat"],
        "tw_status": p_tw["status"],
        "tw_raw": p_tw["raw"],
        "ov_city": current_overseas["city"],
        "ov_flag": current_overseas["flag"],
        "ov_hop": p_ov["last_hop"],
        "ov_base": base_ov,
        "ov_diff": diff_ov_str,
        "ov_node": p_ov["last_ip"],
        "ov_br": p_ov["br_name"],
        "ov_desc": p_ov["desc"],
        "ov_cat": p_ov["cat"],
        "ov_status": p_ov["status"],
        "ov_raw": p_ov["raw"],
        "status": combined_status,
        "desc": combined_desc
    })

# 排序：異常 (ALERT) 自動置頂
current_results.sort(key=lambda x: (0 if x["status"] == "ALERT" else 1, x["target"]))

total_count = len(current_results)
dual_ok_count = sum(1 for r in current_results if r["status"] == "OK")
total_alert_count = total_count - dual_ok_count

snapshot_entry = {
    "time": now_str,
    "nodes": f"🇹🇼 台灣 ＋ {current_overseas['flag']} {current_overseas['city']}",
    "total": total_count,
    "ok": dual_ok_count,
    "tw_nobr": tw_nobr_count,
    "tw_priv": tw_priv_count,
    "ov_nobr": ov_nobr_count,
    "ov_priv": ov_priv_count,
    "ov_city": current_overseas["city"]
}
state["history_snapshots"].insert(0, snapshot_entry)
state["history_snapshots"] = state["history_snapshots"][:700]
state["event_logs"] = state["event_logs"][:60]

one_day_ago = now_dt - timedelta(hours=24)
snapshots_24h = []
for s in state["history_snapshots"]:
    try:
        s_dt = datetime.strptime(s["time"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=tz)
        if s_dt >= one_day_ago:
            snapshots_24h.append(s)
    except Exception:
        pass

alert_rounds_24h = [s for s in snapshots_24h if (s.get("tw_nobr",0) > 0 or s.get("tw_priv",0) > 0 or s.get("ov_nobr",0) > 0 or s.get("ov_priv",0) > 0)]
alert_rounds_count = len(alert_rounds_24h)


# ==============================================================================
# 第四部分：Teams Webhook 推播觸發
# ==============================================================================

# 1. 每日 08:00 定時發送健康日報
if now_dt.hour >= 8 and state.get("last_heartbeat_date") != today_date_str:
    if alert_rounds_count == 0:
        report_title = "4G/5G 路由巡檢 24小時健康日報"
        is_report_alert = False
        anomaly_summary = "0 次 (全時段國內外正常連通抵達 BR)"
    else:
        report_title = "4G/5G 路由巡檢 24小時健康日報 (曾有異常)"
        is_report_alert = True
        latest_err_time = alert_rounds_24h[0]["time"]
        anomaly_summary = f"⚠️ 曾出現 {alert_rounds_count} 次異常巡檢 (最近一次: {latest_err_time})"

    current_summary = f"102 組全數正常 (抵達 16 台 BR)" if total_alert_count == 0 else f"🚨 目前尚有 {total_alert_count} 組異常待處理"

    daily_facts = [
        {"title": "系統狀態", "value": "🟢 背景巡檢持續運作中 (間隔 15 分鐘)"},
        {"title": "監控規模", "value": "102 組 IP (抵達 16 台 BR 白名單)"},
        {"title": "探測涵蓋", "value": "🇹🇼 台灣本地 ＋ 🌏 境外輪播 (東京/香港/新加坡)"},
        {"title": "統計區間", "value": f"截至今日 08:00 (過去 24 小時)"},
        {"title": "巡檢總次數", "value": f"共執行 {len(snapshots_24h)} 次自動巡檢"},
        {"title": "異常巡檢", "value": anomaly_summary},
        {"title": "當前狀態", "value": current_summary}
    ]

    send_teams_notification(report_title, is_report_alert, daily_facts, now_str)
    state["last_heartbeat_date"] = today_date_str

# 2. 告警通知 (🚨)
if newly_alerted:
    send_teams_notification("4G/5G 路由巡檢異常告警", True, newly_alerted, now_str)

# 3. 恢復通知 (🟢 清楚註明是哪裡恢復)
if newly_recovered:
    send_teams_notification("4G/5G 路由恢復連通通知", False, newly_recovered, now_str)

with open(HISTORY_FILE, "w", encoding="utf-8") as f:
    json.dump(state, f, ensure_ascii=False, indent=2)


# ==============================================================================
# 第五部分：HTML 前端組裝 (六格指標卡、雙音高音量警報、乾淨表格)
# ==============================================================================

if not alert_rounds_24h:
    stat_24h_html = '<span style="color:#34d399;font-weight:bold;">✔ 過去 24 小時無異常事件（全時段國內外平穩正常抵達 BR）</span>'
else:
    latest_err = alert_rounds_24h[0]
    stat_24h_html = f'<span style="color:#fb7185;font-weight:bold;">⚠️ 過去 24 小時曾出現 {len(alert_rounds_24h)} 次異常巡檢（最近一次於 {latest_err["time"]}）</span>'

# 7 天歷史快照
history_rows_html = ""
for item in state["history_snapshots"][:100]:
    has_err = (item.get("tw_nobr",0) > 0 or item.get("tw_priv",0) > 0 or item.get("ov_nobr",0) > 0 or item.get("ov_priv",0) > 0)
    row_bg = "#3f1a1a" if has_err else "transparent"
    
    tw_nobr_b = f'<span style="color:#fb7185;font-weight:bold;">{item.get("tw_nobr",0)} 組 🚨</span>' if item.get("tw_nobr",0) > 0 else '<span style="color:#94a3b8;">0</span>'
    tw_priv_b = f'<span style="color:#facc15;font-weight:bold;">{item.get("tw_priv",0)} 組 ⚠️</span>' if item.get("tw_priv",0) > 0 else '<span style="color:#94a3b8;">0</span>'
    
    ov_c_name = item.get("ov_city", "海外")
    ov_nobr_b = f'<span style="color:#fb7185;font-weight:bold;">{item.get("ov_nobr",0)} 組 🚨</span>' if item.get("ov_nobr",0) > 0 else '<span style="color:#94a3b8;">0</span>'
    ov_priv_b = f'<span style="color:#facc15;font-weight:bold;">{item.get("ov_priv",0)} 組 ⚠️</span>' if item.get("ov_priv",0) > 0 else '<span style="color:#94a3b8;">0</span>'

    history_rows_html += f"""
    <tr style="background-color:{row_bg};border-bottom:1px solid #334155;">
        <td style="padding:10px 12px;font-family:monospace;color:#f8fafc;">{item.get("time")}</td>
        <td style="padding:10px 12px;color:#38bdf8;">{item.get("nodes")}</td>
        <td style="padding:10px 12px;font-weight:bold;color:#f8fafc;">{item.get("total", 102)} 組</td>
        <td style="padding:10px 12px;font-weight:bold;color:#34d399;">{item.get("ok", 102)} 組</td>
        <td style="padding:10px 12px;">{tw_nobr_b}</td>
        <td style="padding:10px 12px;">{tw_priv_b}</td>
        <td style="padding:10px 12px;">{ov_nobr_b} ({ov_c_name})</td>
        <td style="padding:10px 12px;">{ov_priv_b}</td>
    </tr>
    """

# 102 組雙向即時巡檢表格
rows_html = ""
for idx, r in enumerate(current_results):
    bg_color = "#3f1a1a" if r["status"] == "ALERT" else "transparent"
    
    if r["status"] == "ALERT":
        status_badge = f'<span style="color:#f87171;font-weight:bold;font-size:15px;">{r["desc"]}</span>'
    else:
        status_badge = '<span style="color:#4ade80;font-weight:bold;font-size:15px;">✔ 雙向正常</span>'

    # 台灣端顯示 (簡化私網文字)
    if r["tw_cat"] == "OK":
        tw_display = f"""<div style="color:#4ade80;font-weight:bold;font-size:14px;">✔ {r['tw_br']}</div>
                         <div style="font-size:12px;color:#94a3b8;">{r['tw_node']} ({r['tw_hop']}跳 / 差:{r['tw_diff']})</div>"""
    elif r["tw_cat"] == "PRIVATE":
        tw_display = f"""<div style="color:#facc15;font-weight:bold;font-size:14px;">{r['tw_desc']}</div>
                         <div style="font-size:12px;color:#94a3b8;">{r['tw_node']} ({r['tw_hop']}跳)</div>"""
    else:
        tw_display = f"""<div style="color:#f87171;font-weight:bold;font-size:14px;">{r['tw_desc']}</div>
                         <div style="font-size:12px;color:#f87171;">{r['tw_node']} ({r['tw_hop']}跳)</div>"""

    # 境外端顯示 (簡化私網文字)
    if r["ov_cat"] == "OK":
        ov_display = f"""<div style="color:#4ade80;font-weight:bold;font-size:14px;">✔ {r['ov_br']}</div>
                         <div style="font-size:12px;color:#94a3b8;">{r['ov_node']} ({r['ov_hop']}跳 / 差:{r['ov_diff']})</div>"""
    elif r["ov_cat"] == "PRIVATE":
        ov_display = f"""<div style="color:#facc15;font-weight:bold;font-size:14px;">{r['ov_desc']}</div>
                         <div style="font-size:12px;color:#94a3b8;">{r['ov_node']} ({r['ov_hop']}跳)</div>"""
    else:
        ov_display = f"""<div style="color:#f87171;font-weight:bold;font-size:14px;">{r['ov_desc']}</div>
                         <div style="font-size:12px;color:#f87171;">{r['ov_node']} ({r['ov_hop']}跳)</div>"""

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
        <td style="padding:14px 12px;">{tw_display}</td>
        <td style="padding:14px 12px;">{ov_display}</td>
        <td style="padding:14px 12px;">{status_badge}</td>
        <td style="padding:14px 12px;text-align:center;">
            <button onclick="toggleLog('log-{idx}')" style="background:#0284c7;color:#fff;border:none;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:14px;font-weight:bold;">查看</button>
        </td>
    </tr>
    <tr id="log-{idx}" class="log-detail-row" style="display:none;background:#090d16;">
        <td colspan="6" style="padding:16px;">
            <div style="font-weight:bold;color:#34d399;margin-bottom:6px;">🇹🇼 台灣本地節點 Traceroute 日誌：</div>
            <pre style="margin:0 0 16px 0;color:#38bdf8;font-size:13px;line-height:1.5;white-space:pre-wrap;background:#0d1527;padding:12px;border-radius:6px;">{r['tw_raw']}</pre>
            <div style="font-weight:bold;color:#a855f7;margin-bottom:6px;">{r['ov_flag']} 境外節點 ({r['ov_city']}) Traceroute 日誌：</div>
            <pre style="margin:0;color:#c084fc;font-size:13px;line-height:1.5;white-space:pre-wrap;background:#0d1527;padding:12px;border-radius:6px;">{r['ov_raw']}</pre>
        </td>
    </tr>
    """

logs_html = "".join([f"<li style='margin-bottom:8px;'>{log}</li>" for log in state["event_logs"]])
if not logs_html:
    logs_html = "<li style='color:#94a3b8;'>目前尚無狀態變更紀錄（系統正常運行中）</li>"

html_content = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>4G/5G 行網 Internet 路由巡檢儀表板</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; background:#0b1120; color:#e2e8f0; margin:0; padding:28px; font-size:15px; }}
        .container {{ max-width: 1440px; margin: 0 auto; }}
        .header {{ display:flex; justify-content:space-between; align-items:center; margin-bottom:24px; padding-bottom:18px; border-bottom:1px solid #334155; }}
        .cards {{ display:grid; grid-template-columns:repeat(4, 1fr); gap:16px; margin-bottom:20px; }}
        .card {{ background:linear-gradient(145deg, #1e293b, #0f172a); border-radius:10px; padding:16px 18px; border:1px solid #334155; box-shadow:0 6px 16px rgba(0,0,0,0.25); }}
        .history-box {{ background:linear-gradient(145deg, #1e293b, #0f172a); border-left:5px solid #a855f7; padding:22px; border-radius:10px; margin-top:32px; border:1px solid #334155; }}
        .audit-box {{ background:linear-gradient(145deg, #1e293b, #0f172a); border-left:5px solid #38bdf8; padding:22px; border-radius:10px; margin-top:32px; border:1px solid #334155; }}
        table {{ width:100%; border-collapse:collapse; background:#1e293b; border-radius:12px; overflow:hidden; font-size:14px; box-shadow:0 8px 24px rgba(0,0,0,0.3); }}
        th {{ background:#0f172a; padding:16px 12px; text-align:left; color:#94a3b8; font-weight:700; font-size:15px; border-bottom:1px solid #334155; }}
        
        @keyframes pulse-red {{
            0% {{ box-shadow: 0 0 0 0 rgba(239, 68, 68, 0.7); }}
            70% {{ box-shadow: 0 0 0 10px rgba(239, 68, 68, 0); }}
            100% {{ box-shadow: 0 0 0 0 rgba(239, 68, 68, 0); }}
        }}
    </style>
    <script>
        var anyLogOpened = false;
        var totalAlert = {total_alert_count};
        var audioCtx = null;
        var beepInterval = null;
        var currentScanTime = "{now_str}";

        // 高音量、急促高低雙音警報聲
        function playWarningBeep() {{
            try {{
                if (!audioCtx) {{
                    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
                }}
                var now = audioCtx.currentTime;
                
                // 第一聲高音 (1200Hz)
                var osc1 = audioCtx.createOscillator();
                var gain1 = audioCtx.createGain();
                osc1.type = 'sawtooth';
                osc1.frequency.setValueAtTime(1200, now);
                gain1.gain.setValueAtTime(0.85, now);
                gain1.gain.exponentialRampToValueAtTime(0.01, now + 0.18);
                osc1.connect(gain1);
                gain1.connect(audioCtx.destination);
                osc1.start(now);
                osc1.stop(now + 0.18);

                // 第二聲次高音 (800Hz)
                var osc2 = audioCtx.createOscillator();
                var gain2 = audioCtx.createGain();
                osc2.type = 'sawtooth';
                osc2.frequency.setValueAtTime(800, now + 0.22);
                gain2.gain.setValueAtTime(0.85, now + 0.22);
                gain2.gain.exponentialRampToValueAtTime(0.01, now + 0.42);
                osc2.connect(gain2);
                gain2.connect(audioCtx.destination);
                osc2.start(now + 0.22);
                osc2.stop(now + 0.42);
            }} catch(e) {{
                console.log("Audio waiting for user gesture:", e);
            }}
        }}

        function updateAckButtonState() {{
            var ackBtn = document.getElementById("ack-btn");
            var isAcked = sessionStorage.getItem("alarm_acked_" + currentScanTime) === "true";

            if (totalAlert > 0) {{
                if (isAcked) {{
                    if (ackBtn) {{
                        ackBtn.innerText = "✔ 警報已消音 (ACK)";
                        ackBtn.disabled = true;
                        ackBtn.style.background = "#475569";
                        ackBtn.style.color = "#94a3b8";
                        ackBtn.style.cursor = "default";
                        ackBtn.style.boxShadow = "none";
                        ackBtn.style.animation = "none";
                    }}
                    if (beepInterval) {{
                        clearInterval(beepInterval);
                        beepInterval = null;
                    }}
                }} else {{
                    if (ackBtn) {{
                        ackBtn.innerText = "🔕 靜音警報 (點擊消音)";
                        ackBtn.disabled = false;
                        ackBtn.style.background = "#ef4444";
                        ackBtn.style.color = "#ffffff";
                        ackBtn.style.cursor = "pointer";
                        ackBtn.style.boxShadow = "0 0 16px rgba(239,68,68,0.7)";
                        ackBtn.style.animation = "pulse-red 1.5s infinite";
                    }}
                    if (!beepInterval) {{
                        beepInterval = setInterval(playWarningBeep, 1600);
                    }}
                }}
            }} else {{
                if (ackBtn) {{
                    ackBtn.innerText = "🔕 靜音警報 (無告警)";
                    ackBtn.disabled = true;
                    ackBtn.style.background = "#334155";
                    ackBtn.style.color = "#94a3b8";
                    ackBtn.style.cursor = "not-allowed";
                    ackBtn.style.boxShadow = "none";
                    ackBtn.style.animation = "none";
                }}
                if (beepInterval) {{
                    clearInterval(beepInterval);
                    beepInterval = null;
                }}
            }}
        }}

        function ackAlarm() {{
            sessionStorage.setItem("alarm_acked_" + currentScanTime, "true");
            updateAckButtonState();
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

        window.addEventListener('click', function() {{
            if (!audioCtx) {{
                audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            }}
            updateAckButtonState();
        }}, {{ once: true }});

        window.onload = function() {{
            updateAckButtonState();
        }};

        setInterval(function() {{
            if (!anyLogOpened) {{
                window.location.reload();
            }}
        }}, 900000);
    </script>
</head>
<body>
    <div class="container">
        <div class="header">
            <div>
                <h1 style="margin:0 0 10px 0; font-size:26px; color:#f8fafc; font-weight:800; display:flex; align-items:center; gap:10px;">
                    <span style="font-size:28px;">🌐</span> 4G/5G 行網 Internet 路由巡檢儀表板
                </h1>
                <div style="font-size:14px; color:#94a3b8; line-height:1.7;">
                    📍 <strong>探測節點：</strong>🇹🇼 台灣本地 ＋ {current_overseas['flag']} 境外輪播節點 ({current_overseas['city']})<br>
                    ⏱ <strong>最後巡檢時間：</strong><span style="color:#38bdf8; font-weight:bold;">{now_str} (UTC+8)</span>
                </div>
            </div>
            
            <div style="display:flex; gap:12px; align-items:center;">
                <button id="ack-btn" onclick="ackAlarm()" style="background:#334155; color:#94a3b8; border:none; border-radius:8px; padding:11px 18px; cursor:not-allowed; font-size:15px; font-weight:bold;">
                    🔕 靜音警報 (無告警)
                </button>
                <button onclick="window.location.reload()" style="background:#0284c7; color:#fff; border:none; border-radius:8px; padding:11px 20px; cursor:pointer; font-size:15px; font-weight:bold;">
                    🔄 畫面刷新
                </button>
                <a href="https://github.com/swa1216-wei/ip-tracer/actions/workflows/schedule.yml" target="_blank" style="text-decoration:none; background:#10b981; color:#fff; border-radius:8px; padding:11px 20px; font-size:15px; font-weight:bold; display:inline-block;">
                    ⚡ 立即發起探測
                </a>
            </div>
        </div>

<!-- 頂部四大格維運指標卡 (內嵌細項拆分) -->
        <div class="cards">
            <!-- 1. 監控總數 -->
            <div class="card" style="border-top:4px solid #38bdf8;">
                <div style="font-size:20px; font-weight:bold; color:#94a3b8; margin-bottom:6px;">監控目標總數</div>
                <div style="font-size:36px; font-weight:900; color:#ffffff;">{total_count} <span style="font-size:14px; color:#94a3b8; font-weight:normal;">組 IP</span></div>
            </div>

            <!-- 2. 雙向全正常 -->
            <div class="card" style="border-top:4px solid #10b981;">
                <div style="font-size:20px; font-weight:bold; color:#a7f3d0; margin-bottom:6px;">雙向全正常 (抵達 16 台 BR)</div>
                <div style="font-size:36px; font-weight:900; color:#34d399;">{dual_ok_count} <span style="font-size:14px; color:#a7f3d0; font-weight:normal;">組</span></div>
            </div>

            <!-- 3. 台灣端異常 -->
            <div class="card" style="border-top:4px solid {'#ef4444' if (tw_nobr_count + tw_priv_count) > 0 else '#334155'};">
                <div style="font-size:20px; font-weight:bold; color:#fecdd3; margin-bottom:6px;">🇹🇼 台灣端異常</div>
                <div style="font-size:36px; font-weight:900; color:{'#fb7185' if (tw_nobr_count + tw_priv_count) > 0 else '#94a3b8'};">{tw_nobr_count + tw_priv_count} <span style="font-size:14px; font-weight:normal; color:#94a3b8;">組</span></div>
                <div style="display:flex; gap:8px; margin-top:8px; font-size:12px;">
                    <span style="background:rgba(239,68,68,0.2); color:#fca5a5; padding:2px 8px; border-radius:4px;">未達BR: {tw_nobr_count}</span>
                    <span style="background:rgba(245,158,11,0.2); color:#fde047; padding:2px 8px; border-radius:4px;">私網: {tw_priv_count}</span>
                </div>
            </div>

            <!-- 4. 境外端異常 -->
            <div class="card" style="border-top:4px solid {'#ef4444' if (ov_nobr_count + ov_priv_count) > 0 else '#334155'};">
                <div style="font-size:20px; font-weight:bold; color:#fecdd3; margin-bottom:6px;">{current_overseas['flag']} 境外端異常 ({current_overseas['city']})</div>
                <div style="font-size:36px; font-weight:900; color:{'#fb7185' if (ov_nobr_count + ov_priv_count) > 0 else '#94a3b8'};">{ov_nobr_count + ov_priv_count} <span style="font-size:14px; font-weight:normal; color:#94a3b8;">組</span></div>
                <div style="display:flex; gap:8px; margin-top:8px; font-size:12px;">
                    <span style="background:rgba(239,68,68,0.2); color:#fca5a5; padding:2px 8px; border-radius:4px;">未達BR: {ov_nobr_count}</span>
                    <span style="background:rgba(245,158,11,0.2); color:#fde047; padding:2px 8px; border-radius:4px;">私網: {ov_priv_count}</span>
                </div>
            </div>
        </div>

        <div style="background:rgba(15,23,42,0.8); border:1px solid #334155; border-radius:8px; padding:12px 18px; margin-bottom:28px; font-size:14px; display:flex; align-items:center; gap:8px;">
            <span style="font-size:16px;">📊</span> <strong>24小時巡檢健康指標：</strong> {stat_24h_html}
        </div>

        <!-- 表格區塊 -->
        <table>
            <thead>
                <tr>
                    <th style="width:20%;">目標 IP / 所屬網段</th>
                    <th style="width:14%;">元件 / NAT 群組</th>
                    <th style="width:23%;">🇹🇼 台灣端路徑 (本地)</th>
                    <th style="width:23%;">{current_overseas['flag']} 境外端路徑 ({current_overseas['city']})</th>
                    <th style="width:12%;">綜合判定</th>
                    <th style="text-align:center;width:8%;">雙向日誌</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>

        <!-- 7 天快照 -->
        <div class="history-box">
            <h3 style="margin-top:0;margin-bottom:8px;font-size:18px;color:#c084fc;">📈 過去 7 天排程巡檢歷史快照</h3>
            <div style="max-height:280px; overflow-y:auto; border-radius:8px; border:1px solid #334155;">
                <table style="box-shadow:none;">
                    <thead>
                        <tr>
                            <th style="background:#090d16;padding:10px 12px;">巡檢時間點</th>
                            <th style="background:#090d16;padding:10px 12px;">探測節點</th>
                            <th style="background:#090d16;padding:10px 12px;">目標總數</th>
                            <th style="background:#090d16;padding:10px 12px;">雙向全正常</th>
                            <th style="background:#090d16;padding:10px 12px;">🇹🇼 台灣未達BR</th>
                            <th style="background:#090d16;padding:10px 12px;">🇹🇼 台灣含私網</th>
                            <th style="background:#090d16;padding:10px 12px;">🌏 境外未達BR</th>
                            <th style="background:#090d16;padding:10px 12px;">🌏 境外含私網</th>
                        </tr>
                    </thead>
                    <tbody>
                        {history_rows_html}
                    </tbody>
                </table>
            </div>
        </div>

        <!-- 顯示路由變更LOG -->
        <div class="audit-box">
            <h3 style="margin-top:0;margin-bottom:12px;font-size:18px;color:#38bdf8;">📋 顯示路由變更LOG</h3>
            <ul style="margin:0;padding-left:24px;font-size:14px;line-height:1.7;">
                {logs_html}
            </ul>
        </div>
    </div>
</body>
</html>
"""

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 雙向巡檢完成，已產出完整儀表板。")
