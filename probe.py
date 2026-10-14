import json
import time
import urllib.request
from datetime import datetime, timezone, timedelta

# ================= 設定區 =================
TARGET_IP = "42.77.128.1"
# ==========================================

tz = timezone(timedelta(hours=8))
now_str = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

def run_traceroute_via_api(target):
    api_url = "https://api.globalping.io/v1/measurements"
    
    # 建立探測請求 (由全球公共探針執行 traceroute)
    payload = json.dumps({
        "type": "traceroute",
        "target": target,
        "limit": 1
    }).encode("utf-8")
    
    req = urllib.request.Request(
        api_url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "GitHubAction-Traceroute"}
    )
    
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            measurement_id = data.get("id")
    except Exception as e:
        return f"建立探測請求失敗: {e}"

    # 輪詢等待探測完成 (通常 5~10 秒完成)
    poll_url = f"{api_url}/{measurement_id}"
    for _ in range(15):
        time.sleep(2)
        try:
            req_poll = urllib.request.Request(
                poll_url,
                headers={"User-Agent": "GitHubAction-Traceroute"}
            )
            with urllib.request.urlopen(req_poll, timeout=15) as resp:
                result_data = json.loads(resp.read().decode("utf-8"))
                
                if result_data.get("status") == "finished":
                    results = result_data.get("results", [])
                    if results:
                        probe_info = results[0].get("probe", {})
                        location_desc = f"探針來源: {probe_info.get('city', 'Unknown')}, {probe_info.get('country', 'Unknown')} (AS{probe_info.get('asn', '')} {probe_info.get('network', '')})"
                        raw_output = results[0].get("result", {}).get("rawOutput", "無輸出內容")
                        return f"{location_desc}\n\n{raw_output}"
                    return "已完成但未取得結果數據"
        except Exception as e:
            return f"查詢結果時發生錯誤: {e}"
            
    return "探測逾時，未能在時限內取得結果"

# 執行探測
raw_output = run_traceroute_via_api(TARGET_IP)

# 產出網頁
html_content = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Traceroute 路由監控報告</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
            background-color: #0f172a;
            color: #e2e8f0;
            margin: 0;
            padding: 24px;
        }}
        .container {{
            max-width: 950px;
            margin: 0 auto;
            background: #1e293b;
            border-radius: 8px;
            padding: 24px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
        }}
        h1 {{
            margin-top: 0;
            color: #38bdf8;
            font-size: 20px;
        }}
        .meta {{
            font-size: 14px;
            color: #94a3b8;
            margin-bottom: 16px;
        }}
        pre {{
            background-color: #090d16;
            color: #34d399;
            padding: 16px;
            border-radius: 6px;
            overflow-x: auto;
            font-size: 13px;
            line-height: 1.6;
            white-space: pre-wrap;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🌐 定期路由追蹤 (Traceroute) 監控報告</h1>
        <div class="meta">
            <strong>目標 IP:</strong> {TARGET_IP} | 
            <strong>最後更新時間:</strong> {now_str} (UTC+8)
        </div>
        <pre>{raw_output}</pre>
    </div>
</body>
</html>
"""

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"[{now_str}] 成功產出 index.html")
