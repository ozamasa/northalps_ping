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
PING_WORKERS   = 100   # ping 並列数（Raspberry Piなら 80〜128 目安）
NOTION_TIMEOUT = 10    # Notion API タイムアウト秒
NOTION_BACKOFF = 0.4   # Notion query ページング間の待ち（429対策）
JST            = timezone(timedelta(hours=9))

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

# ========= HTTP Session =========
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
    """メイン上書き + 旧メインB列をログへ退避 + 今回の結果もログ右端に追加"""
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

    # 退避：メインB列をログ右端にコピー
    try:
        current_vals = sheet.get_all_values()  # [[IP, TS], ...]
        if current_vals and len(current_vals) >= 2:
            prev_col = [row[1] if len(row) > 1 else "" for row in current_vals[1:]]
            backup_col = [datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")] + prev_col
            col_count = log_sheet.col_count
            if log_sheet.col_count < col_count + 1:
                log_sheet.add_cols((col_count + 1) - log_sheet.col_count)
            rng = gspread.utils.rowcol_to_a1(1, col_count + 1) + ":" + gspread.utils.rowcol_to_a1(len(backup_col), col_count + 1)
            log_sheet.update([[v] for v in backup_col], range_name=rng)
    except Exception:
        pass  # 退避失敗は無視

    # メイン上書き（ヘッダ含む）
    values = [["IP Address", "Timestamp"]] + data
    sheet.batch_update([{
        "range": f"A1:B{len(values)}",
        "values": values
    }])

    # 今回分もログ右端に追記
    col_count = log_sheet.col_count
    run_col = [datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")] + [ts for _, ts in data]
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
    ts_now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    results = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(ping_ip, ip): ip for ip in ips}
        for fut in as_completed(futs):
            ip = futs[fut]
            ok = fut.result()
            results.append([ip, ts_now if ok else ""])
    results.sort(key=lambda x: int(x[0].split(".")[-1]))  # IP末尾で整列
    return results

# ========= Notion helpers =========
def get_db_properties(db_id):
    try:
        r = S.get(f"https://api.notion.com/v1/databases/{db_id}", headers=NOTION_HEADERS, timeout=NOTION_TIMEOUT)
        r.raise_for_status()
        return r.json().get("properties", {})
    except requests.exceptions.RequestException:
        return {}

MAIN_DB_PROPS = get_db_properties(NOTION_DB_ID) if NOTION_DB_ID else {}
LOG_DB_PROPS  = get_db_properties(NOTION_LOGS_DB_ID) if NOTION_LOGS_DB_ID else {}

def has_prop(props, name, type_):
    p = props.get(name)
    return p and p.get("type") == type_

def jst_iso_from_str(ts_str):
    """'YYYY-MM-DD HH:MM:SS' -> 'YYYY-MM-DDTHH:MM:SS+09:00'"""
    dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=JST).isoformat(timespec="seconds")

def fetch_pages_map(db_id):
    """ip -> {'id': page_id, 'ts': (既存Timestamp文字列), 'status': '接続/接続不可'}"""
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

            # Timestamp の既存値を文字列に吸い出し（date型 or rich_text 両対応）
            ts_existing = ""
            if has_prop(MAIN_DB_PROPS, "Timestamp", "date"):
                ts_existing = (props.get("Timestamp", {}).get("date") or {}).get("start") or ""
            else:
                ts_existing = props.get("Timestamp", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "") or ""

            status = props.get("Status", {}).get("select", {}).get("name", "")
            page_map[ip] = {"id": row["id"], "ts": ts_existing, "status": status}

        if not data.get("has_more"):
            break
        payload["start_cursor"] = data.get("next_cursor")
        time.sleep(NOTION_BACKOFF)
    return page_map

# ========= Notion: ログDB 1行追加 =========
def create_log_record(ip, timestamp, status_name, network_prefix=None):
    if not NOTION_LOGS_DB_ID:
        return

    props = {}

    # タイトル（必須）
    if has_prop(LOG_DB_PROPS, "IP Address", "title"):
        props["IP Address"] = {"title": [{"text": {"content": ip}}]}
    else:
        # タイトルが無いDBにはページを作れない
        return

    # ステータス
    if has_prop(LOG_DB_PROPS, "Status", "select"):
        props["Status"] = {"select": {"name": status_name}}

    # タイムスタンプ（date型を優先）
    if timestamp:
        if has_prop(LOG_DB_PROPS, "Timestamp", "date"):
            props["Timestamp"] = {"date": {"start": jst_iso_from_str(timestamp)}}
        elif has_prop(LOG_DB_PROPS, "Timestamp", "rich_text"):
            props["Timestamp"] = {"rich_text": [{"text": {"content": timestamp}}]}

    # ネットワーク（あれば）
    if network_prefix and has_prop(LOG_DB_PROPS, "Network", "select"):
        props["Network"] = {"select": {"name": network_prefix}}

    payload = {"parent": {"database_id": NOTION_LOGS_DB_ID}, "properties": props}
    try:
        S.post("https://api.notion.com/v1/pages", headers=NOTION_HEADERS, json=payload, timeout=NOTION_TIMEOUT)
    except requests.exceptions.RequestException:
        pass  # ログは落ちても全体停止しない

# ========= Notion: メインDB upsert =========
def upsert_notion(data, db_id, network_prefix=None):
    try:
        page_map = fetch_pages_map(db_id)
    except requests.exceptions.RequestException as e:
        print(f"❌ Notion DB query 失敗: {e}")
        return

    ts_is_date = has_prop(MAIN_DB_PROPS, "Timestamp", "date")
    ts_is_text = has_prop(MAIN_DB_PROPS, "Timestamp", "rich_text")

    for ip, timestamp in data:
        status_name = "接続" if timestamp else "接続不可"
        pm = page_map.get(ip)

        # 新規作成
        if not pm:
            props = {
                "IP Address": {"title": [{"text": {"content": ip}}]},
                "Status": {"select": {"name": status_name}}
            }
            if timestamp:
                if ts_is_date:
                    props["Timestamp"] = {"date": {"start": jst_iso_from_str(timestamp)}}
                elif ts_is_text:
                    props["Timestamp"] = {"rich_text": [{"text": {"content": timestamp}}]}
            else:
                # 空で上書き
                if ts_is_date:
                    props["Timestamp"] = {"date": None}
                elif ts_is_text:
                    props["Timestamp"] = {"rich_text": []}

            try:
                S.post("https://api.notion.com/v1/pages", headers=NOTION_HEADERS, json={"parent": {"database_id": db_id}, "properties": props}, timeout=NOTION_TIMEOUT)
            except requests.exceptions.RequestException:
                pass

            create_log_record(ip, timestamp, status_name, network_prefix)
            time.sleep(0.03)
            continue

        # 既存 → 差分判定（文字列で比較）
        want_ts_str = ""
        if timestamp:
            want_ts_str = jst_iso_from_str(timestamp) if ts_is_date else timestamp

        need_update = False
        if ts_is_date:
            # 既存（日付ISO, 末尾Zの可能性あり）と、JST ISO をそのまま比較
            need_update = (pm.get("ts") or "") != want_ts_str
        else:
            need_update = (pm.get("ts") or "") != (timestamp or "")

        if pm.get("status", "") != status_name:
            need_update = True

        if need_update:
            props = {"Status": {"select": {"name": status_name}}}
            if ts_is_date:
                props["Timestamp"] = {"date": {"start": want_ts_str}} if timestamp else {"date": None}
            elif ts_is_text:
                props["Timestamp"] = {"rich_text": [{"text": {"content": timestamp}}]} if timestamp else {"rich_text": []}

            try:
                S.patch(
                    f"https://api.notion.com/v1/pages/{pm['id']}",
                    headers=NOTION_HEADERS,
                    json={"properties": props},
                    timeout=NOTION_TIMEOUT
                )
            except requests.exceptions.RequestException:
                pass

        # ログは毎回1行
        create_log_record(ip, timestamp, status_name, network_prefix)
        time.sleep(0.03)  # 429対策の軽い間隔

# ========= Main =========
if __name__ == "__main__":
    prefixes = ["192.168.10.", "192.168.80."]

    for prefix in prefixes:
        # 1) 並列 ping
        results = ping_subnet(prefix, workers=PING_WORKERS)
        alive = sum(1 for _, ts in results if ts)
        print(f"📡 {prefix} Alive: {alive}/254")

        # 2) Sheets：退避→メイン更新→ログ列追加
        sheet = prefix.replace(".", "_")
        write_to_sheets_with_backup(results, sheet, f"{sheet}_log")

        # 3) Notion：差分のみ更新 + ログDBへ毎回1行（TimestampはJSTのdate型優先）
        upsert_notion(results, NOTION_DB_ID, network_prefix=prefix)

    print("🏁 全処理完了！")