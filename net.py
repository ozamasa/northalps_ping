from dotenv import load_dotenv
import os
import platform
import gspread
import requests
import time
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# === ✅ 環境変数の読み込み (.env) ===
load_dotenv()
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
NOTION_LOG_DATABASE_ID = os.getenv("NOTION_LOG_DATABASE_ID")

# === ✅ Google Sheets 認証 ===
def authenticate_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_PATH, scope)
    return gspread.authorize(creds)

# === ✅ Ping 実行 ===
def ping_ip(ip):
    system = platform.system().lower()
    if system == "windows":
        command = f"ping -n 1 -w 1000 {ip} > nul"
    elif system == "darwin":
        command = f"ping -c 1 -t 1 {ip} > /dev/null 2>&1"
    else:
        command = f"ping -c 1 -w 1 {ip} > /dev/null 2>&1"
    return 1 if os.system(command) == 0 else 0

# === ✅ Google Sheets へ書き込み（上書き＋ログ追加）===
def write_to_google_sheets(data, sheet_name, sheet_log_name):
    client = authenticate_google_sheets()
    spreadsheet = client.open(SPREADSHEET_NAME)

    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="2")

    sheet.clear()
    sheet.update([["IP Address", "Timestamp"]] + data, range_name="A1")

    try:
        log_sheet = spreadsheet.worksheet(sheet_log_name)
    except gspread.exceptions.WorksheetNotFound:
        log_sheet = spreadsheet.add_worksheet(title=sheet_log_name, rows="255", cols="2")

    log_values = log_sheet.get_all_values()
    if not log_values:
        log_sheet.insert_row(["IP Address"] + [ip for ip, _ in data], 1)

    column = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
    column += [ts for _, ts in data]
    log_sheet.insert_cols([column], col=2)

# === ✅ Notion に最新の接続状況を更新 ===
def update_notion_timestamps(data, token, db_id):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    for ip, timestamp in data:
        status_name = "接続" if timestamp else "接続不可"

        # クエリでページを検索
        query = {
            "filter": {
                "property": "IP Address",
                "title": {"equals": ip}
            }
        }
        try:
            res = requests.post(
                f"https://api.notion.com/v1/databases/{db_id}/query",
                headers=headers, json=query
            )
            res.raise_for_status()
            results = res.json().get("results", [])

            # 更新 or 新規作成
            if results:
                page_id = results[0]["id"]
                update_payload = {
                    "properties": {
                        "Timestamp": {"rich_text": [{"text": {"content": timestamp or ""}}]},
                        "Status": {"select": {"name": status_name}}
                    }
                }
                patch = requests.patch(
                    f"https://api.notion.com/v1/pages/{page_id}",
                    headers=headers, json=update_payload
                )
                patch.raise_for_status()
                print(f"✅ 更新: {ip} | {status_name} | {timestamp or '―'}")
            else:
                create_payload = {
                    "parent": {"database_id": db_id},
                    "properties": {
                        "IP Address": {"title": [{"text": {"content": ip}}]},
                        "Timestamp": {"rich_text": [{"text": {"content": timestamp or ""}}]},
                        "Status": {"select": {"name": status_name}}
                    }
                }
                create = requests.post("https://api.notion.com/v1/pages", headers=headers, json=create_payload)
                create.raise_for_status()
                print(f"🆕 新規: {ip} | {status_name} | {timestamp or '―'}")

        except requests.exceptions.RequestException as e:
            print(f"❌ 通信エラー: {ip} - {e}")
        time.sleep(0.4)

# === ✅ Notion 履歴データベースにログを追加 ===
def log_connection_to_notion(db_id, ip, timestamp, token):
    status = "接続" if timestamp else "接続不可"
    timestamp_str = timestamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    payload = {
        "parent": {"database_id": db_id},
        "properties": {
            "IP Address": {"title": [{"text": {"content": ip}}]},
            "Timestamp": {"rich_text": [{"text": {"content": timestamp_str}}]},
            "Status": {"select": {"name": status}}
        }
    }

    try:
        res = requests.post("https://api.notion.com/v1/pages", headers=headers, json=payload)
        res.raise_for_status()
        print(f"📝 ログ記録: {ip} | {status} | {timestamp_str}")
    except requests.exceptions.RequestException as e:
        print(f"⚠️ ログ記録エラー: {ip} - {e}")
    time.sleep(0.4)

# === ✅ メイン処理 ===
if __name__ == "__main__":
    network_prefixes = ["192.168.10.", "192.168.80.", "192.168.100."]

    for prefix in network_prefixes:
        ping_results = []
        for i in range(1, 255):
            ip = f"{prefix}{i}"
            result = ping_ip(ip)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if result else ""
            status_msg = "Success" if result else "Fail"
            print(f"📡 Ping: {ip} | {status_msg} | {timestamp or '―'}")
            ping_results.append([ip, timestamp])

        sheet_name = prefix.replace(".", "_")
        write_to_google_sheets(ping_results, sheet_name, f"{sheet_name}log")
        update_notion_timestamps(ping_results, NOTION_TOKEN, NOTION_DATABASE_ID)

        for ip, ts in ping_results:
            log_connection_to_notion(NOTION_LOG_DATABASE_ID, ip, ts, NOTION_TOKEN)

    print("🏁 全処理完了！")
