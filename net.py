from dotenv import load_dotenv
import os
import platform
import gspread
import requests
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# === ✅ 認証設定（Notion） ===
load_dotenv()  # .envファイルを読み込む

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

# === ✅ Googleスプレッドシート認証 ===
def authenticate_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("./credentials.json", scope)
    return gspread.authorize(creds)

# === ✅ Ping処理 ===
def ping_ip(ip):
    system = platform.system().lower()
    if system == "windows":
        command = f"ping -n 1 -w 1000 {ip} > nul"
    elif system == "darwin":
        command = f"ping -c 1 -t 1 {ip} > /dev/null 2>&1"
    else:
        command = f"ping -c 1 -w 1 {ip} > /dev/null 2>&1"
    return 1 if os.system(command) == 0 else 0

# === ✅ Google Sheetsへの書き込み（上書き + ログ列追加） ===
def write_to_google_sheets(data, sheet_name, sheet_log_name):
    client = authenticate_google_sheets()
    spreadsheet = client.open("ネットワーク疎通.glide")

    # 現在値シート
    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{sheet_name}' が見つかりません。作成します。")
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="2")

    sheet.clear()
    values = [["IP Address", "Timestamp"]]
    values.extend(data)
    sheet.update(values, range_name="A1")

    # ログシート（列追加）
    try:
        log_sheet = spreadsheet.worksheet(sheet_log_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"シート '{sheet_log_name}' が見つかりません。作成します。")
        log_sheet = spreadsheet.add_worksheet(title=sheet_log_name, rows="255", cols="2")

    log_values = log_sheet.get_all_values()
    if not log_values:
        log_values = [["IP Address"] + [ip for ip, _ in data]]

    # 挿入する列データ（ヘッダー + タイムスタンプ）
    new_column = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    new_column.extend([ts for _, ts in data])
    log_sheet.insert_cols([new_column], col=2)

# === ✅ NotionのTimestamp更新処理（成功→上書き、失敗→クリア） ===
def update_notion_timestamps(data, notion_token, database_id):
    headers = {
        "Authorization": f"Bearer {notion_token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    query_url = f"https://api.notion.com/v1/databases/{database_id}/query"
    create_url = "https://api.notion.com/v1/pages"

    for ip, timestamp in data:
        # ① IP アドレスのページが存在するか検索
        query_payload = {
            "filter": {
                "property": "IP Address",
                "title": {
                    "equals": ip
                }
            }
        }
        res = requests.post(query_url, headers=headers, json=query_payload)
        results = res.json().get("results", [])

        if results:
            # ② 存在する場合 → Timestamp 更新 or クリア
            page_id = results[0]["id"]
            patch_url = f"https://api.notion.com/v1/pages/{page_id}"
            patch_payload = {
                "properties": {
                    "Timestamp": {
                        "rich_text": [{"text": {"content": timestamp}}] if timestamp else {"rich_text": []}
                    }
                }
            }
            patch_res = requests.patch(patch_url, headers=headers, json=patch_payload)
            if patch_res.status_code == 200:
                print(f"✅ Notion 更新: {ip} → {timestamp if timestamp else '(空白)'}")
            else:
                print(f"⚠️ Notion 更新失敗: {ip} - {patch_res.status_code}")
        else:
            # ③ 存在しない場合 → 新規ページ作成
            create_payload = {
                "parent": {"database_id": database_id},
                "properties": {
                    "IP Address": {
                        "title": [{"text": {"content": ip}}]
                    },
                    "Timestamp": {
                        "rich_text": [{"text": {"content": timestamp}}] if timestamp else {"rich_text": []}
                    }
                }
            }
            create_res = requests.post(create_url, headers=headers, json=create_payload)
            if create_res.status_code == 200:
                print(f"🆕 Notion 新規追加: {ip} → {timestamp if timestamp else '(空白)'}")
            else:
                print(f"❌ Notion 追加失敗: {ip} - {create_res.status_code}: {create_res.text}")

# === ✅ メイン処理 ===
if __name__ == "__main__":
    network_prefixes = ["192.168.10.", "192.168.80.", "192.168.100."]

    for network_prefix in network_prefixes:
        ping_results = []
        for i in range(1, 255):
            ip = f"{network_prefix}{i}"
            result = ping_ip(ip)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if result == 1 else ""
            print(f"Pinging {ip}: {'Success' if result else 'Fail'} at {timestamp or 'No timestamp'}")
            ping_results.append([ip, timestamp])

        sheet_name = network_prefix.replace('.', '_')
        sheet_log_name = f"{sheet_name}log"

        write_to_google_sheets(ping_results, sheet_name, sheet_log_name)
        update_notion_timestamps(ping_results, NOTION_TOKEN, NOTION_DATABASE_ID)

    print("✅ すべての処理が完了しました！")
