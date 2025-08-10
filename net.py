#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os, platform, time
import gspread, requests
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ========= 設定 =========
PING_WORKERS    = 100   # 並列ping数（Piなら80〜128目安）
NOTION_TIMEOUT  = 10    # Notion API タイムアウト秒
NOTION_BACKOFF  = 0.4   # Notion DBページング時の待ち(429対策)

# ========= ENV =========
load_dotenv()
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH")
SPREADSHEET_NAME       = os.getenv("SPREADSHEET_NAME")
NOTION_TOKEN           = os.getenv("NOTION_TOKEN")
NOTION_DB_ID           = os.getenv("NOTION_DATABASE_ID")      # IP一覧DB
NOTION_LOGS_DB_ID      = os.getenv("NOTION_LOGS_DB_ID")       # ログDB（1つ）

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

# ========= HTTP Session（安定＋再試行） =========
def create_session():
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST", "PATCH"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
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

def write_to_sheets_with_backup(data, sheet_name, log_sheet_name):
    """
    メイン上書き + 旧メインB列をログ右端へ退避 + 今回結果もログ右端へ追加
    （メインは可読のためローカル時刻 "YYYY-MM-DD HH:MM:SS"）
    """
    gc = gs_auth()
    ss = gc.open(SPREADSHEET_NAME)

    # メイン
    try:
        sheet = ss.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = ss.add_worksheet(title=sheet_name, rows="300", cols="2")

    # ログ
    try:
        log_sheet = ss.worksheet(log_sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        log_sheet = ss.add_worksheet(title=log_sheet_name, rows="300", cols="2")
        ips = [ip for ip, _ in data]
        log_sheet.update([["IP Address"] + ips], range_name="A1")

    # 退避：現B列→ログ右端
    try:
        current = sheet.get_all_values()
        if current and len(current) >= 2:
            prev_col = [row[1] if len(row) > 1 else "" for row in current[1:]]
            backup_col = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] + prev_col
            col_count = log_sheet.col_count
            if log_sheet.col_count < col_count + 1:
                log_sheet.add_cols((col_count + 1) - log_sheet.col_count)
            rng = gspread.utils.rowcol_to_a1(1, col_count + 1) + ":" + gspread.utils.rowcol_to_a1(len(backup_col), col_count + 1)
            log_sheet.update([[v] for v in backup_col], range_name=rng)
    except Exception:
        pass  # 退避失敗は無視

    # メイン上書き
    values = [["IP Address", "Timestamp"]] + data
    sheet.batch_update([{
        "range": f"A1:B{len(values)}",
        "values": values
    }])

    # 今回分をログ右端に追加
    col_count = log_sheet.col_count
    run_col = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] + [ts for _, ts in data]
    if log_sheet.col_count < col_count + 1:
        log_sheet.add_cols((col_count + 1) - log_sheet.col_count)
    rng = gspread.utils.rowcol_to_a1(1, col_count + 1) + ":" + gspread.utils.rowcol_to_a1(len(run_col), col_count + 1)
    log_sheet.update([[v] for v in run_col], range_name=rng)

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
    # Notion向けは ISO（date型用）、Sheets向けは可読
    ts_iso = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    ts_human = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    results = []            # [[ip, human_ts or ""]]
    results_iso = []        # [[ip, iso_ts or ""]]
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(ping_ip, ip): ip for ip in ips}
        for fut in as_completed(futs):
            ip = futs[fut]
            ok = fut.result()
            results.append([ip, ts_human if ok else ""])
            results_iso.append([ip, ts_iso if ok else ""])
    results.sort(key=lambda x: int(x[0].split(".")[-1]))
    results_iso.sort(key=lambda x: int(x[0].split(".")[-1]))
    return results, results_iso

# ========= Notion（差分判定は DB query のみで） =========
def fetch_pages_map(db_id):
    """ip -> {'id': page_id, 'ts': 'ISO or ""', 'status': '接続/接続不可'}"""
    page_map = {}
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {"page_size": 100}
    while True:
        r = S.post(url, headers=NOTION_HEADERS, json=payload, timeout=NOTION_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        for row in data.get("results", []):
            props = row.get("properties", {})
            title = props.get("IP Address", {}).get("title", [])
            ip = title[0]["text"]["content"] if title else None
            if not ip:
                continue
            # date型の start を拾う（無ければ ""）
            ts = ""
            try:
                ts = (props.get("Timestamp", {}) or {}).get("date", {}).get("start", "") or ""
            except Exception:
                pass
            status = props.get("Status", {}).get("select", {}).get("name", "")
            page_map[ip] = {"id": row["id"], "ts": ts, "status": status}
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data.get("next_cursor")
        time.sleep(NOTION_BACKOFF)
    return page_map

# ログDBのスキーマ（存在チェック）
def get_db_properties(db_id):
    try:
        r = S.get(f"https://api.notion.com/v1/databases/{db_id}", headers=NOTION_HEADERS, timeout=NOTION_TIMEOUT)
        r.raise_for_status()
        return r.json().get("properties", {})
    except requests.exceptions.RequestException:
        return {}

LOG_DB_PROPS = get_db_properties(NOTION_LOGS_DB_ID) if NOTION_LOGS_DB_ID else {}

def has_prop(props, name, type_):
    p = props.get(name)
    return p and p.get("type") == type_

def create_log_record(ip, ts_iso, status_name, network_prefix=None):
    """ログDBへ1行（Timestamp は date型、未接続はdate None）"""
    if not NOTION_LOGS_DB_ID:
        return

    props = {}
    if has_prop(LOG_DB_PROPS, "IP Address", "title"):
        props["IP Address"] = {"title": [{"text": {"content": ip}}]}
    else:
        # title が無いDBにはページ作れない
        return

    if has_prop(LOG_DB_PROPS, "Status", "select"):
        props["Status"] = {"select": {"name": status_name}}

    if has_prop(LOG_DB_PROPS, "Timestamp", "date"):
        props["Timestamp"] = {"date": {"start": ts_iso}} if ts_iso else {"date": None}

    if network_prefix and has_prop(LOG_DB_PROPS, "Network", "select"):
        props["Network"] = {"select": {"name": network_prefix}}

    payload = {"parent": {"database_id": NOTION_LOGS_DB_ID}, "properties": props}
    try:
        S.post("https://api.notion.com/v1/pages",
               headers=NOTION_HEADERS, json=payload, timeout=NOTION_TIMEOUT)
    except requests.exceptions.RequestException:
        pass  # ログ失敗は無視

def upsert_notion(data_iso, db_id, network_prefix=None):
    """
    data_iso: [[ip, iso_ts or ""]]
    main DB は Timestamp(date型) と Status(select) を差分時のみ更新
    ログDBは毎回1行追加
    """
    try:
        page_map = fetch_pages_map(db_id)
    except requests.exceptions.RequestException as e:
        print(f"❌ Notion DB query 失敗: {e}")
        return

    for ip, ts_iso in data_iso:
        status_name = "接続" if ts_iso else "接続不可"
        pm = page_map.get(ip)

        if not pm:
            # 新規作成
            create_payload = {
                "parent": {"database_id": db_id},
                "properties": {
                    "IP Address": {"title": [{"text": {"content": ip}}]},
                    "Timestamp": {"date": {"start": ts_iso}} if ts_iso else {"date": None},
                    "Status": {"select": {"name": status_name}}
                }
            }
            try:
                S.post("https://api.notion.com/v1/pages",
                       headers=NOTION_HEADERS, json=create_payload, timeout=NOTION_TIMEOUT)
            except requests.exceptions.RequestException as e:
                print(f"❌ Notion作成失敗: {ip} - {e}")
            create_log_record(ip, ts_iso, status_name, network_prefix)
            time.sleep(0.02)
            continue

        # 差分がある時だけ更新
        if pm.get("ts", "") != (ts_iso or "") or pm.get("status", "") != status_name:
            try:
                S.patch(
                    f"https://api.notion.com/v1/pages/{pm['id']}",
                    headers=NOTION_HEADERS,
                    json={"properties": {
                        "Timestamp": {"date": {"start": ts_iso}} if ts_iso else {"date": None},
                        "Status": {"select": {"name": status_name}}
                    }},
                    timeout=NOTION_TIMEOUT
                )
            except requests.exceptions.RequestException as e:
                print(f"❌ Notion更新失敗: {ip} - {e}")

        # ログは毎回1行
        create_log_record(ip, ts_iso, status_name, network_prefix)
        time.sleep(0.02)  # 429対策の軽い間引き

# ========= Main =========
if __name__ == "__main__":
    prefixes = ["192.168.10.", "192.168.80."]

    for prefix in prefixes:
        # 1) 並列 ping（Sheets用とNotion用の時刻を用意）
        results_human, results_iso = ping_subnet(prefix, workers=PING_WORKERS)
        alive = sum(1 for _, ts in results_iso if ts)
        print(f"📡 {prefix} Alive: {alive}/254")

        # 2) Sheets：退避→メイン更新→ログ列追加
        sheet = prefix.replace(".", "_")
        write_to_sheets_with_backup(results_human, sheet, f"{sheet}_log")

        # 3) Notion：差分のみ更新 + ログDBへ毎回1行（Timestampはdate型）
        upsert_notion(results_iso, NOTION_DB_ID, network_prefix=prefix)

    print("🏁 全処理完了！")