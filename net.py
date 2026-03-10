#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dotenv import load_dotenv
import os
import platform
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PING_WORKERS = 100
API_TIMEOUT = 15
JST = timezone(timedelta(hours=9))

load_dotenv()

PING_API_URL = os.getenv("PING_API_URL", "https://ping.shiolab.com/api/ping_results")
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "change-me")
PREFIXES = [
    p.strip()
    for p in os.getenv("PING_PREFIXES", "192.168.10.,192.168.80.").split(",")
    if p.strip()
]

def create_session():
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

S = create_session()

def ping_ip(ip: str) -> bool:
    sysname = platform.system().lower()
    if sysname == "windows":
        cmd = f"ping -n 1 -w 1000 {ip} > nul"
    elif sysname == "darwin":
        cmd = f"ping -c 1 -t 1 {ip} > /dev/null 2>&1"
    else:
        cmd = f"ping -c 1 -w 1 {ip} > /dev/null 2>&1"
    return os.system(cmd) == 0

def ping_subnet(prefix: str, workers: int = PING_WORKERS):
    ips = [f"{prefix}{i}" for i in range(1, 255)]
    ts_now = datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S")
    results = []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(ping_ip, ip): ip for ip in ips}
        for fut in as_completed(futures):
            ip = futures[fut]
            ok = fut.result()
            results.append({
                "ip": ip,
                "timestamp": ts_now if ok else ""
            })

    results.sort(key=lambda x: int(x["ip"].split(".")[-1]))
    return results

def send_to_api(results, prefix: str) -> bool:
    payload = {
        "subnet": prefix,
        "results": results,
    }

    headers = {
        "Content-Type": "application/json",
        "X-INGEST-TOKEN": INGEST_TOKEN,
    }

    try:
        r = S.post(
            PING_API_URL,
            json=payload,
            headers=headers,
            timeout=API_TIMEOUT,
        )
        print(f"API送信 {prefix}: {r.status_code}")
        print(r.text)
        r.raise_for_status()
        return True
    except requests.exceptions.RequestException as e:
        print(f"API送信失敗 {prefix}: {e}")
        return False

if __name__ == "__main__":
    print("Ping監視開始")

    for prefix in PREFIXES:
        results = ping_subnet(prefix, workers=PING_WORKERS)
        alive = sum(1 for row in results if row["timestamp"])
        print(f"{prefix} Alive: {alive}/254")

        ok = send_to_api(results, prefix)
        if not ok:
            print(f"{prefix} の送信に失敗しました")

    print("全処理完了")