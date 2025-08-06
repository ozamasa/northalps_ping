from dotenv import load_dotenv
import os
import platform
import gspread
import requests
import time
import random
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# === ✅ 環境変数の読み込み (.env) ===
load_dotenv()
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

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

# === ✅ heading の直後に新しい paragraph を「先頭に」追加
def prepend_log_to_page(ip, timestamp, token, db_id):
    status = "接続" if timestamp else "接続不可"
    timestamp_str = timestamp or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_log_block = {
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": f"{timestamp_str} | {status}"}
                }
            ]
        }
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    # === ページID取得 ===
    try:
        query = {
            "filter": {
                "property": "IP Address",
                "title": {"equals": ip}
            }
        }
        res = requests.post(f"https://api.notion.com/v1/databases/{db_id}/query", headers=headers, json=query)
        res.raise_for_status()
        results = res.json()["results"]
        if not results:
            print(f"⚠️ ページが見つかりません（{ip}）")
            return
        page_id = results[0]["id"]
    except Exception as e:
        print(f"❌ ページ取得失敗: {ip} - {e}")
        return

    # === ページの children（全履歴）取得 ===
    try:
        children_url = f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=100"
        res = requests.get(children_url, headers=headers)
        res.raise_for_status()
        children = res.json()["results"]

        # 既存 paragraph ブロックだけを対象とする
        old_logs = [block for block in children if block["type"] == "paragraph"]

        # 最大100件まで保持（新しいもの含めて）
        remaining_logs = old_logs[:99]  # 新規追加分と合わせて最大100にする

        # 古いログを削除
        for block in old_logs[99:]:
            try:
                requests.delete(f"https://api.notion.com/v1/blocks/{block['id']}", headers=headers)
            except Exception as e:
                print(f"⚠️ ログ削除失敗: {block['id']} - {e}")

        # 全 paragraph を削除（再構成するため）
        for block in old_logs[:99]:
            try:
                requests.delete(f"https://api.notion.com/v1/blocks/{block['id']}", headers=headers)
            except Exception as e:
                print(f"⚠️ 削除失敗: {block['id']} - {e}")

        # 新しい paragraph を先頭に追加し、古いログを続けて再追加
        new_children = [new_log_block] + remaining_logs
        requests.patch(f"https://api.notion.com/v1/blocks/{page_id}/children", headers=headers, json={"children": new_children})
        print(f"📎 履歴追記: {ip} | {timestamp_str} | {status}")

    except Exception as e:
        print(f"❌ ログ更新失敗: {ip} - {e}")

# === ✅ Notion に最新の接続状況を更新 + childrenに履歴追加 ===
def update_notion_timestamps(data, token, db_id):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    ip_to_page_id = {}

    for ip, timestamp in data:
        status_name = "接続" if timestamp else "接続不可"

        if ip not in ip_to_page_id:
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
                if results:
                    page_id = results[0]["id"]
                    ip_to_page_id[ip] = page_id
                else:
                    create_payload = {
                        "parent": {"database_id": db_id},
                        "properties": {
                            "IP Address": {"title": [{"text": {"content": ip}}]},
                            "Timestamp": {"rich_text": [{"text": {"content": timestamp or ""}}]},
                            "Status": {"select": {"name": status_name}}
                        }
                    }
                    res = requests.post("https://api.notion.com/v1/pages", headers=headers, json=create_payload)
                    res.raise_for_status()
                    page_id = res.json()["id"]
                    ip_to_page_id[ip] = page_id
                    print(f"🆕 新規: {ip} | {status_name} | {timestamp or '―'}")
                    prepend_log_to_page(ip, timestamp, token, db_id)
                    time.sleep(random.uniform(0.6, 0.8))
                    continue
            except requests.exceptions.RequestException as e:
                print(f"❌ 通信エラー（ページ検索/作成）: {ip} - {e}")
                time.sleep(random.uniform(0.6, 0.8))
                continue

        page_id = ip_to_page_id[ip]
        try:
            page_url = f"https://api.notion.com/v1/pages/{page_id}"
            res = requests.get(page_url, headers=headers)
            res.raise_for_status()
            properties = res.json()["properties"]
            current_timestamp = properties.get("Timestamp", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "")
            current_status = properties.get("Status", {}).get("select", {}).get("name", "")

            if current_timestamp == (timestamp or "") and current_status == status_name:
                print(f"🔁 履歴のみ追記（プロパティ変更なし）: {ip}")
                prepend_log_to_page(ip, timestamp, token, db_id)
                time.sleep(random.uniform(0.6, 0.8))
                continue

        except requests.exceptions.RequestException as e:
            print(f"⚠️ 現在のプロパティ取得失敗: {ip} - {e}")
            time.sleep(random.uniform(0.6, 0.8))
            continue

        try:
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
            prepend_log_to_page(ip, timestamp, token, db_id)

        except requests.exceptions.RequestException as e:
            print(f"❌ プロパティ更新失敗: {ip} - {e}")

        time.sleep(random.uniform(0.6, 0.8))

# === ✅ メイン処理 ===
if __name__ == "__main__":
    network_prefixes = ["192.168.10.", "192.168.80."]

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

    print("🏁 全処理完了！")
