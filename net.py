import os
import platform
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# Googleスプレッドシートの認証
def authenticate_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)  # 認証ファイルを指定
    client = gspread.authorize(creds)
    return client

# Pingを実行し、成功したら1、失敗したら0を返す
def ping_ip(ip):
    system = platform.system().lower()

    if system == "windows":
        command = f"ping -n 1 -w 1000 {ip} > nul"
    elif system == "darwin":  # macOS
        command = f"ping -c 1 -t 1 {ip} > /dev/null 2>&1"
    else:  # Linux
        command = f"ping -c 1 -w 1 {ip} > /dev/null 2>&1"

    return 1 if os.system(command) == 0 else 0

# Googleスプレッドシートにデータを書き込む
def write_to_google_sheets(data, sheet_name, sheet_log_name):
    client = authenticate_google_sheets()
    sheet = None

    try:
        # 指定したシート名を開く
        sheet = client.open("ネットワーク疎通.glide").worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        # シートが見つからなければ新しいシートを作成
        print(f"シート '{sheet_name}' が見つかりません。新しく作成します。")
        sheet = client.open("ネットワーク疎通.glide").add_worksheet(title=sheet_name, rows="100", cols="2")

    # 既存のデータをクリア
    sheet.clear()

    # ヘッダー行を追加（Ping ResultをTimestampに変更）
    values = [["IP Address", "Timestamp"]]

    # データを一括追加
    values.extend(data)

    # まとめて書き込む（API制限回避）
    sheet.update(values, range_name="A1")


    try:
        # 指定したシート名を開く
        sheet = client.open("ネットワーク疎通.glide").worksheet(sheet_log_name)
    except gspread.exceptions.WorksheetNotFound:
        # シートが見つからなければ新しいシートを作成
        print(f"シート '{sheet_log_name}' が見つかりません。新しく作成します。")
        sheet = client.open("ネットワーク疎通.glide").add_worksheet(title=sheet_log_name, rows="255", cols="2")

    # 新しい列を挿入
    sheet.insert_cols([[""] * len(sheet.get_all_values())], 2)  # 2列目に空の列を挿入

    # ヘッダー行を追加（Ping ResultをTimestampに変更）
    values = [["IP Address", "Timestamp"]]

    # データを一括追加
    values.extend(data)

    # まとめて書き込む（API制限回避）
    sheet.update(values, range_name="A1")

# メイン処理
if __name__ == "__main__":
    # 対象となるネットワークの接頭辞
    network_prefixes = ["192.168.10.", "192.168.80.", "192.168.100."]

    for network_prefix in network_prefixes:
        ping_results = []

        for i in range(1, 255):  # 1〜254 までPingを実行
            ip = f"{network_prefix}{i}"
            result = ping_ip(ip)

            # Pingが成功した場合、日時を記録。失敗の場合は空白を記録
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if result == 1 else ""
            print(f"Pinging {ip}: {'Success' if result else 'Fail'} at {timestamp if result else 'No timestamp'}")
            ping_results.append([ip, timestamp])  # IPアドレスと時間を記録（失敗の場合は空白）

        # 対象ネットワークに対応したシートに書き込み
        sheet_name = network_prefix.replace('.', '_')  # ネットワークに対応するシート名（.を_に変換）
        sheet_log_name = f"{sheet_name}log"  # logシート名を生成
        write_to_google_sheets(ping_results, sheet_name, sheet_log_name)

    print("Googleスプレッドシートに書き込み完了！")