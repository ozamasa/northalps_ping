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

# ========= 設定（必要なら変更） =========
PING_WORKERS = 100            # ping 並列数（Raspberry Piなら 80〜128 目安）
NOTION_TIMEOUT = 10           # Notion API タイムアウト秒
NOTION_BACKOFF = 0.4          # Notion query ページング間の待ち（429対策）

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

# ========= HTTP Session (高速＆安定) =========
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
    """メイン上書き + 旧メインをログへ退避 + 今回の結果もログ右端に追加（高速）"""
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

    # 退避：メインの現B列をログ右端列へコピー
    try:
        current_vals = sheet.get_all_values()  # [[IP, TS], ...]
        if current_vals and len(current_vals) >= 2:
            prev_col = [row[1] if len(row) > 1 else "" for row in current_vals[1:]]
            backup_col = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] + prev_col
            col_count = log_sheet.col_count
            # 必要なら列増設
            if log_sheet.col_count < col_count + 1:
                log_sheet.add_cols((col_count + 1) - log_sheet.col_count)
            rng = gspread.utils.rowcol_to_a1(1, col_count + 1) + ":" + gspread.utils.rowcol_to_a1(len(backup_col), col_count + 1)
            log_sheet.update([[v] for v in backup_col], range_name=rng)
    except Exception:
        # 退避失敗は無視（続行）
        pass

    # メイン上書き（ヘッダ含む）
    values = [["IP Address", "Timestamp"]] + data
    sheet.batch_update([{
        "range": f"A1:B{len(values)}",
        "values": values
    }])

    # 今回分もログ右端に追記
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
    ts_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    results = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(ping_ip, ip): ip for ip in ips}
        for fut in as_completed(futs):
            ip = futs[fut]
            ok = fut.result()
            results.append([ip, ts_now if ok else ""])
    results.sort(key=lambda x: int(x[0].split(".")[-1]))  # IP末尾で整列
    return results

# ========= Notion（高速化：DB query だけで差分判定） =========
def fetch_pages_map(db_id):
    """ip -> {'id': page_id, 'ts': 'YYYY-..', 'status': '接続/接続不可'}"""
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
            ts = ""
            try:
                ts = props.get("Timestamp", {}).get("rich_text", [{}])[0].get("text", {}).get("content", "")
            except Exception:
                pass
            status = props.get("Status", {}).get("select", {}).get("name", "")
            page_map[ip] = {"id": row["id"], "ts": ts, "status": status}
        if not data.get("has_more"):
            break
        payload["start_cursor"] = data.get("next_cursor")
        time.sleep(NOTION_BACKOFF)
    return page_map

# 追加: DBスキーマを一度だけ取得して、存在/型チェック
def get_db_properties(db_id):
    try:
        r = S.get(f"https://api.notion.com/v1/databases/{db_id}", headers=NOTION_HEADERS, timeout=NOTION_TIMEOUT)
        r.raise_for_status()
        return r.json().get("properties", {})
    except requests.exceptions.RequestException as e:
        print(f"❌ Notion DB schema 取得失敗: {e}")
        return {}

LOG_DB_PROPS = get_db_properties(NOTION_LOGS_DB_ID) if NOTION_LOGS_DB_ID else {}

def has_prop(name, type_):
    p = LOG_DB_PROPS.get(name)
    return p and p.get("type") == type_

# 修正: 失敗時に内容を表示 / 存在しないプロパティは送らない
def create_log_record(ip, timestamp, status_name, network_prefix=None):
    if not NOTION_LOGS_DB_ID:
        print("⚠️ NOTION_LOGS_DB_ID が未設定です。ログ作成をスキップ。")
        return

    ts_iso = timestamp.replace(" ", "T") if timestamp else None

    props = {}
    if has_prop("IP Address", "title"):
        props["IP Address"] = {"title": [{"text": {"content": ip}}]}
    else:
        print("⚠️ ログDBに title プロパティ『IP Address』がありません。")
        return  # titleが無いDBにはページを作れない

    if has_prop("Status", "select"):
        props["Status"] = {"select": {"name": status_name}}

    if ts_iso and has_prop("Timestamp", "date"):
        props["Timestamp"] = {"date": {"start": ts_iso}}

    if network_prefix and has_prop("Network", "select"):
        props["Network"] = {"select": {"name": network_prefix}}

    payload = {"parent": {"database_id": NOTION_LOGS_DB_ID}, "properties": props}
    try:
        res = S.post("https://api.notion.com/v1/pages",
                     headers=NOTION_HEADERS, json=payload, timeout=NOTION_TIMEOUT)
        if not (200 <= res.status_code < 300):
            print(f"❌ ログ作成失敗 {res.status_code}: {res.text[:300]}")
        else:
            # 成功時の確認（任意）
            # print(f"📝 Log OK: {ip} {status_name} {timestamp or '—'}")
            pass
    except requests.exceptions.RequestException as e:
        print(f"❌ ログ作成通信失敗: {e}")

def upsert_notion(data, db_id, network_prefix=None):
    try:
        page_map = fetch_pages_map(db_id)
    except requests.exceptions.RequestException as e:
        print(f"❌ Notion DB query 失敗: {e}")
        return

    for ip, timestamp in data:
        status_name = "接続" if timestamp else "接続不可"
        pm = page_map.get(ip)

        if not pm:
            # 新規作成（最小プロパティだけ）
            create_payload = {
                "parent": {"database_id": db_id},
                "properties": {
                    "IP Address": {"title": [{"text": {"content": ip}}]},
                    "Timestamp": {"rich_text": [{"text": {"content": timestamp or ""}}]},
                    "Status": {"select": {"name": status_name}}
                }
            }
            try:
                S.post("https://api.notion.com/v1/pages", headers=NOTION_HEADERS, json=create_payload, timeout=NOTION_TIMEOUT)
            except requests.exceptions.RequestException as e:
                print(f"❌ Notion作成失敗: {ip} - {e}")
            # ログは毎回1行
            create_log_record(ip, timestamp, status_name, network_prefix)
            time.sleep(0.03)
            continue

        # 既存 → 差分がある時だけ更新
        if pm.get("ts", "") != (timestamp or "") or pm.get("status", "") != status_name:
            try:
                S.patch(
                    f"https://api.notion.com/v1/pages/{pm['id']}",
                    headers=NOTION_HEADERS,
                    json={"properties": {
                        "Timestamp": {"rich_text": [{"text": {"content": timestamp or ""}}]},
                        "Status": {"select": {"name": status_name}}
                    }},
                    timeout=NOTION_TIMEOUT
                )
            except requests.exceptions.RequestException as e:
                print(f"❌ Notion更新失敗: {ip} - {e}")

        # ログは毎回1行（要件どおり）
        create_log_record(ip, timestamp, status_name, network_prefix)
        time.sleep(0.03)  # 軽く間引き（429対策）

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

        # 3) Notion：差分のみ更新 + ログDBへ毎回1行
        upsert_notion(results, NOTION_DB_ID, network_prefix=prefix)

    print("🏁 全処理完了！")