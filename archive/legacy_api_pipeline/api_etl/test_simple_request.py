"""
Minimal Xilnex API smoke-test script.

Usage:
    python api_etl\\test_simple_request.py --limit 10
"""

import argparse
import json
import os
import sys
import time

import requests
from dotenv import load_dotenv


CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Load local env for parity with extract_fast_sample.py
ENV_PATH = os.path.join(PROJECT_ROOT, ".env.local")
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)

from api_etl.config_api import API_HOST, APP_ID, TOKEN, AUTH_LEVEL  # noqa: E402


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "appid": APP_ID,
            "token": TOKEN,
            "auth": AUTH_LEVEL,
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip",
        }
    )
    return session


def run_request(limit: int, starttimestamp: str | None) -> None:
    base_url = f"https://{API_HOST}/apps/v2/sync/sales?limit={limit}"
    if starttimestamp:
        base_url += f"&starttimestamp={starttimestamp}"

    session = build_session()
    print(f"[INFO] GET {base_url}")

    start = time.perf_counter()
    try:
        resp = session.get(base_url, timeout=90)
    except requests.RequestException as exc:
        print(f"[ERROR] Request failed: {exc}")
        return

    latency = time.perf_counter() - start
    print(f"[INFO] Status: {resp.status_code} ({latency:.2f}s)")

    for name in ("Date", "Retry-After", "Content-Length", "Content-Encoding"):
        if name in resp.headers:
            print(f"  {name}: {resp.headers[name]}")

    if resp.status_code != 200:
        print(f"[BODY] {resp.text[:500]}")
        return

    try:
        data = resp.json()
    except json.JSONDecodeError:
        print("[ERROR] Response is not valid JSON, first 500 bytes shown below:")
        print(resp.text[:500])
        return

    total = len(data) if isinstance(data, list) else data.get("count")
    print(f"[INFO] Parsed payload type={type(data).__name__} total={total}")

    if isinstance(data, list) and data:
        print("[SAMPLE RECORD]")
        print(json.dumps(data[0], indent=2)[:1000])


def parse_args():
    parser = argparse.ArgumentParser(description="Simple Xilnex API request tester.")
    parser.add_argument("--limit", type=int, default=1, help="Records per API call.")
    parser.add_argument(
        "--starttimestamp",
        type=str,
        default=None,
        help="Optional hex starttimestamp for pagination.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_request(args.limit, args.starttimestamp)

