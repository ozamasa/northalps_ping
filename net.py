#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os, platform, time
import gspread, requests
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========= 設定 =========
PING_WORKERS   = 100
NOTION_TIMEOUT = 10
NOTION_BACKOFF = 0.4
JST            = timezone(timedelta(hours=9))

# ========= ENV =========
load_dotenv()
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
SPREADSHEET_NAME       = os.getenv("SPREADSHEET_NAME")
NOTION_TOKEN           = os.getenv("NOTION_TOKEN")
NOTION_DB_ID           = os.getenv("NOTION_DATABASE_ID")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# ========= HTTP Session =========
def create_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PATCH"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

S = create_session()

# ========= Google Sheets =========
def gs_auth():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_PATH, scope)
    return gspread.authorize(creds)

def insert_log_column(log_ws, timestamp, values):
    # values: 1行目見出しを含まない Timestamp 値（IP順）
    new_col = [timestamp] + values
    log_ws.insert_cols([new_col], col=2)

def write_to_sheets_with_backup(data, sheet_name, log_sheet_name):
    gc = gs_auth()
    ss = gc.open(SPREADSHEET_NAME)

    # メイン
    try:
        sheet = ss.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = ss.add_worksheet(title=sheet_name, rows="300", cols="2")

    values = [["IP Address", "Timestamp"]] + data
    sheet.batch_update([{
        "range": f"A1:B{len(values)}",
        "values": values
    }])

    # ログ
    try:
        log_ws = ss.worksheet(log_sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        log_ws = ss.add_worksheet(title=log_sheet_name, rows="300", cols="2")
        ips = [ip for ip, _ in data]
        log_ws.update([["IP Address"] + ips], range_name="A1")

    # Timestamp列だけを取り出して、2列目に挿入（右にスライド）
    timestamp = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    timestamps = [ts for _, ts in data]
    insert_log_column(log_ws, timestamp, timestamps)

# ========= Ping =========
def ping_ip(ip):
    sys = platform.system().lower()
    if sys == "windows":
        cmd = f"ping -n 1 -w 1000 {ip} > nul"
    elif sys == "darwin":
        cmd = f"ping -c 1 -t 1 {ip} > /dev/null 2>&1"
    else:
        cmd = f"ping -c 1 -w 1 {ip} > /dev/null 2>&1"
    return os.system(cmd) == 0

def ping_subnet(prefix, workers=PING_WORKERS):
    ips = [f"{prefix}{i}" for i in range(1, 255)]
    ts_now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    results = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(ping_ip, ip): ip for ip in ips}
        for fut in as_completed(futs):
            ip = futs[fut]
            ok = fut.result()
            results.append([ip, ts_now if ok else ""])
    results.sort(key=lambda x: int(x[0].split(".")[-1]))
    return results

# ========= Notion: 一覧DBのみ更新 =========
def upsert_notion(data, db_id):
    try:
        r = S.post(f"https://api.notion.com/v1/databases/{db_id}/query",
                   headers=NOTION_HEADERS, json={"page_size": 100}, timeout=NOTION_TIMEOUT)
        r.raise_for_status()
        pages = r.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"❌ Notion DB 読み込み失敗: {e}")
        return

    page_map = {}
    for p in pages:
        props = p["properties"]
        title = props["IP Address"]["title"]
        if title:
            ip = title[0]["text"]["content"]
            page_map[ip] = p["id"]

    for ip, ts in data:
        status = "接続" if ts else "接続不可"
        props = {
            "IP Address": {"title": [{"text": {"content": ip}}]},
            "Timestamp": {"rich_text": [{"text": {"content": ts}}]} if ts else {"rich_text": []},
            "Status": {"select": {"name": status}},
        }

        if ip in page_map:
            try:
                S.patch(f"https://api.notion.com/v1/pages/{page_map[ip]}",
                        headers=NOTION_HEADERS, json={"properties": props}, timeout=NOTION_TIMEOUT)
            except requests.exceptions.RequestException:
                pass
        else:
            try:
                S.post("https://api.notion.com/v1/pages",
                       headers=NOTION_HEADERS, json={"parent": {"database_id": db_id}, "properties": props}, timeout=NOTION_TIMEOUT)
            except requests.exceptions.RequestException:
                pass

        time.sleep(0.05)

# ========= Main =========
if __name__ == "__main__":
    prefixes = ["192.168.10.", "192.168.80."]

    for prefix in prefixes:
        results = ping_subnet(prefix, workers=PING_WORKERS)
        alive = sum(1 for _, ts in results if ts)
        print(f"📡 {prefix} Alive: {alive}/254")

        sheet_name = prefix.replace(".", "_") + "_"
        log_sheet_name = prefix.replace(".", "_") + "_log"

        write_to_sheets_with_backup(results, sheet_name, log_sheet_name)
        upsert_notion(results, NOTION_DB_ID, network_prefix=prefix)

    print("🏁 全処理完了！")