from dotenv import load_dotenv
import os
import platform
import gspread
import requests
import time
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# === ✅ 環境変数 ===
load_dotenv()
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")      # IP一覧DB
NOTION_LOGS_DB_ID = os.getenv("NOTION_LOGS_DB_ID")        # ログ専用DB（新）

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# === ✅ Google Sheets 認証 ===
def authenticate_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_PATH, scope)
    return gspread.authorize(creds)

# === ✅ Ping ===
def ping_ip(ip):
    system = platform.system().lower()
    if system == "windows":
        command = f"ping -n 1 -w 1000 {ip} > nul"
    elif system == "darwin":
        command = f"ping -c 1 -t 1 {ip} > /dev/null 2>&1"
    else:
        command = f"ping -c 1 -w 1 {ip} > /dev/null 2>&1"
    return os.system(command) == 0

def ping_subnet(prefix, workers=100):
    ips = [f"{prefix}{i}" for i in range(1, 255)]
    results = []
    ts_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(ping_ip, ip): ip for ip in ips}
        for fut in as_completed(futs):
            ip = futs[fut]
            ok = fut.result()
            ts = ts_now if ok else ""
            results.append([ip, ts])

    # IPの末尾の数字でソート
    results.sort(key=lambda x: int(x[0].split(".")[-1]))
    return results

# === ✅ Google Sheets 更新（batch + ログ右端append） ===
def write_to_google_sheets(data, sheet_name, sheet_log_name):
    client = authenticate_google_sheets()
    ss = client.open(SPREADSHEET_NAME)

    # メインシート：A列=IP, B列=Timestamp を丸ごと更新（clearしない）
    try:
        sheet = ss.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = ss.add_worksheet(title=sheet_name, rows="300", cols="2")

    values = [["IP Address", "Timestamp"]] + data
    sheet.batch_update([{
        "range": "A1:B{}".format(len(values)),
        "values": values
    }])

    # ログシート：右端に列追加（最新が右へ）
    try:
        log_sheet = ss.worksheet(sheet_log_name)
    except gspread.exceptions.WorksheetNotFound:
        log_sheet = ss.add_worksheet(title=sheet_log_name, rows="300", cols="2")
        # ヘッダ行作成：先頭に "IP Address" 列
        ips = [ip for ip, _ in data]
        log_sheet.update([["IP Address"] + ips], range_name="A1")

    # 右端の次の列を求める
    col_count = log_sheet.col_count
    # 値を右端+1列に書く（1行目はタイムスタンプ、2行目以降は各IPの結果）
    column_values = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] + [ts for _, ts in data]
    rng = gspread.utils.rowcol_to_a1(1, col_count + 1) + ":" + gspread.utils.rowcol_to_a1(len(column_values), col_count + 1)
    # 必要なら列を増やす
    if log_sheet.col_count < col_count + 1:
        log_sheet.add_cols((col_count + 1) - log_sheet.col_count)
    log_sheet.update(rng, [[v] for v in column_values])

# === ✅ Notion：DB全件を一括取得して IP→page_id マップ作成 ===
def fetch_all_pages_map(db_id):
    page_map = {}
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {"page_size": 100}
    while True:
        r = requests.post(url, headers=NOTION_HEADERS, json=payload)
        r.raise_for_status()
        data = r.json()
        for row in data.get("results", []):
            props = row.get("properties", {})
            title = props.get("IP Address", {}).get("title", [])
            ip = title[0]["text"]["content"] if title else None
            if ip:
                page_map[ip] = row["id"]
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data.get("next_cursor")
    return page_map

# === ✅ Notion：ログ専用DBに1行追加（ステータス変化時だけ推奨） ===
def create_log_record(ip, timestamp, status_name, network_prefix=None):
    if NOTION_LOGS_DB_ID is None:
        return
    ts_iso = timestamp.replace(" ", "T") if timestamp else None
    props = {
        "IP": {"title": [{"text": {"content": ip}}]},
        "Status": {"select": {"name": status_name}},
    }
    if ts_iso:
        props["Timestamp"] = {"date": {"start": ts_iso}}
    if network_prefix:
        props["Network"] = {"rich_text": [{"text": {"content": network_prefix}}]}
    payload = {"parent": {"database_id": NOTION_LOGS_DB_ID}, "properties": props}
    try:
        requests.post("https://api.notion.com/v1/pages", headers=NOTION_HEADERS, json=payload, timeout=10)
    except requests.exceptions.RequestException:
        pass  # ログは失敗しても全体停止しない

# === ✅ Notion：差分のあるページだけ更新（子ブロック操作なし） ===
def update_notion_timestamps(data, db_id, network_prefix=None):
    page_map = fetch_all_pages_map(db_id)
    for ip, timestamp in data:
        status_name = "接続" if timestamp else "接続不可"

        # 既存ページある？なければ作成
        page_id = page_map.get(ip)
        if not page_id:
            create_payload = {
                "parent": {"database_id": db_id},
                "properties": {
                    "IP Address": {"title": [{"text": {"content": ip}}]},
                    "Timestamp": {"rich_text": [{"text": {"content": timestamp or ""}}]},
                    "Status": {"select": {"name": status_name}}
                }
            }
            try:
                res = requests.post("https://api.notion.com/v1/pages", headers=NOTION_HEADERS, json=create_payload, timeout=10)
                res.raise_for_status()
                page_id = res.json()["id"]
                # ログ：新規時は1行だけ（任意、毎回でもOK）
                create_log_record(ip, timestamp, status_name, network_prefix)
            except requests.exceptions.RequestException as e:
                print(f"❌ Notion作成失敗: {ip} - {e}")
            time.sleep(0.1)
            continue

        # 既存プロパティ取得 → 差分チェック
        try:
            res = requests.get(f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS, timeout=10)
            res.raise_for_status()
            props = res.json()["properties"]
            current_ts = props.get("Timestamp", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "")
            current_status = props.get("Status", {}).get("select", {}).get("name", "")
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Notionプロパティ取得失敗: {ip} - {e}")
            time.sleep(0.05)
            continue

        if current_ts == (timestamp or "") and current_status == status_name:
            # 完全同一なら何もしない（ログもスキップ推奨）
            continue

        # 差分あり → 更新 & ログ1行
        try:
            update_payload = {
                "properties": {
                    "Timestamp": {"rich_text": [{"text": {"content": timestamp or ""}}]},
                    "Status": {"select": {"name": status_name}}
                }
            }
            requests.patch(f"https://api.notion.com/v1/pages/{page_id}", headers=NOTION_HEADERS, json=update_payload, timeout=10)
            create_log_record(ip, timestamp, status_name, network_prefix)
        except requests.exceptions.RequestException as e:
            print(f"❌ Notion更新失敗: {ip} - {e}")

        time.sleep(0.05)  # レート緩和

# === ✅ メイン ===
if __name__ == "__main__":
    network_prefixes = ["192.168.10.", "192.168.80."]

    for prefix in network_prefixes:
        # 1) 高速Ping（並列）
        ping_results = ping_subnet(prefix, workers=100)
        # コンソールログ（軽量）
        alive = sum(1 for _, ts in ping_results if ts)
        print(f"📡 {prefix} Alive: {alive}/{len(ping_results)}")

        # 2) Google Sheets（batch + 右端append）
        sheet_name = prefix.replace(".", "_")
        write_to_google_sheets(ping_results, sheet_name, f"{sheet_name}_log")

        # 3) Notion（差分のみ更新 + ログDBに1行追加）
        update_notion_timestamps(ping_results, NOTION_DATABASE_ID, network_prefix=prefix)

    print("🏁 全処理完了！")