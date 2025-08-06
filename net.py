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
def prepend_log_under_heading(ip, timestamp, token, db_id):
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
    query = {
        "filter": {
            "property": "IP Address",
            "title": {"equals": ip}
        }
    }
    try:
        res = requests.post(f"https://api.notion.com/v1/databases/{db_id}/query", headers=headers, json=query)
        res.raise_for_status()
        results = res.json()["results"]
        if not results:
            print(f"⚠️ ページが見つかりません（{ip}）")
            return
        page_id = results[0]["id"]
    except Exception as e:
        print(f"❌ 検索失敗: {ip} - {e}")
        return

    # === ページの children を取得して heading_2 を探す ===
    try:
        # ⚠️ 注意: 最大100件までの履歴のみ取得・再構成します。
        # それ以上は削除される仕様です（長期保存不要な方針に基づく）。
        children_url = f"https://api.notion.com/v1/blocks/{page_id}/children?page_size=100"
        res = requests.get(children_url, headers=headers)
        res.raise_for_status()
        blocks = res.json()["results"]

        heading_index = -1
        heading_id = None
        for i, block in enumerate(blocks):
            if block["type"] == "heading_2" and "通信履歴" in block["heading_2"]["rich_text"][0]["text"]["content"]:
                heading_index = i
                heading_id = block["id"]
                break

        if heading_id is None:
            # heading_2 がない場合は作る
            new_heading = {
                "children": [
                    {
                        "object": "block",
                        "type": "heading_2",
                        "heading_2": {
                            "rich_text": [
                                {
                                    "type": "text",
                                    "text": {"content": "通信履歴"}
                                }
                            ]
                        }
                    }
                ]
            }
            res_heading = requests.patch(children_url, headers=headers, json=new_heading)
            res_heading.raise_for_status()
            heading_id = res_heading.json()["results"][0]["id"]
            heading_index = len(blocks)  # 新しく追加されたので最後
            print(f"🆕 通信履歴 heading 作成: {ip}")
            # 子ブロックなしなのでそのまま新しいログだけ追加して return
            requests.patch(f"https://api.notion.com/v1/blocks/{heading_id}/children", headers=headers, json={"children": [new_log_block]})
            print(f"📎 初回ログ追記: {ip} | {timestamp_str} | {status}")
            return

    except Exception as e:
        print(f"❌ heading 検出失敗: {ip} - {e}")
        return

    # === heading の子ブロック（ログ）を取得 ===
    try:
        res = requests.get(f"https://api.notion.com/v1/blocks/{heading_id}/children?page_size=100", headers=headers)
        res.raise_for_status()
        current_children = res.json()["results"]

        # 古いブロックIDを取得
        old_block_ids = [block["id"] for block in current_children]

        # 新しいログを先頭に
        new_children = [new_log_block] + current_children

        # 全削除
        for block_id in old_block_ids:
            try:
                requests.delete(f"https://api.notion.com/v1/blocks/{block_id}", headers=headers)
            except Exception as e:
                print(f"⚠️ 削除失敗: {block_id} - {e}")

        # 新しく並べ替えたものを追加
        res = requests.patch(f"https://api.notion.com/v1/blocks/{heading_id}/children", headers=headers, json={"children": new_children})
        res.raise_for_status()
        print(f"📎 ログ先頭追加: {ip} | {timestamp_str} | {status}")

    except Exception as e:
        print(f"❌ heading 子の更新失敗: {ip} - {e}")

# === ✅ Notion に最新の接続状況を更新 + childrenに履歴追加 ===
def update_notion_timestamps(data, token, db_id):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }

    ip_to_page_id = {}  # IPアドレスとページIDのキャッシュ

    for ip, timestamp in data:
        status_name = "接続" if timestamp else "接続不可"

        # === 1. ページIDをキャッシュから取得 or Notionから取得 ===
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
                    # 新規作成
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
                    # 初回作成時のみログを追加して、次のIPへ
                    prepend_log_under_heading(ip, timestamp, token, db_id)
                    time.sleep(random.uniform(0.6, 0.8))
                    continue
            except requests.exceptions.RequestException as e:
                print(f"❌ 通信エラー（ページ検索/作成）: {ip} - {e}")
                time.sleep(random.uniform(0.6, 0.8))
                continue

        # === 2. 既存プロパティを取得（重複更新を防ぐ） ===
        page_id = ip_to_page_id[ip]
        try:
            page_url = f"https://api.notion.com/v1/pages/{page_id}"
            res = requests.get(page_url, headers=headers)
            res.raise_for_status()
            properties = res.json()["properties"]
            current_timestamp = properties.get("Timestamp", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "")
            current_status = properties.get("Status", {}).get("select", {}).get("name", "")

            if current_timestamp == (timestamp or "") and current_status == status_name:
                print(f"⏭ スキップ: {ip}（変更なし）")
                continue

        except requests.exceptions.RequestException as e:
            print(f"⚠️ 現在のプロパティ取得失敗: {ip} - {e}")
            time.sleep(random.uniform(0.6, 0.8))
            continue

        # === 3. 更新が必要な場合のみ Patch + ログ追記 ===
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
            prepend_log_under_heading(ip, timestamp, token, db_id)

        except requests.exceptions.RequestException as e:
            print(f"❌ プロパティ更新失敗: {ip} - {e}")

        # レート制限回避のために少し待つ
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
