import subprocess
from datetime import datetime, timezone, timedelta

# ================= 設定區 =================
# 請將 8.8.8.8 改成你要測試的 IP 或域名
TARGET_IP = "42.77.128.1"
# ==========================================

# 取得台灣時間 (UTC+8)
tz = timezone(timedelta(hours=8))
now_str = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

# 執行 mtr 報告模式 (產生單次 10 封封包的統計數據)
try:
    cmd = ["sudo", "mtr", "--report", "--report-cycles", "10", "--no-dns", TARGET_IP]
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
    raw_output = result.stdout if result.stdout else result.stderr
except Exception as e:
    raw_output = f"執行失敗: {e}"

# 產生 index.html 網頁
html_content = f"""<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MTR Traceroute 監控報告</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace;
            background-color: #0f172a;
            color: #e2e8f0;
            margin: 0;
            padding: 24px;
        }}
        .container {{
            max-width: 900px;
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
            line-height: 1.5;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🌐 定期路由追蹤 (MTR) 監控報告</h1>
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
