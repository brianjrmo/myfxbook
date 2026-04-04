#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import time
import requests
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

API_BASE = "https://www.myfxbook.com/api"
class MyfxbookError(RuntimeError):
    pass
@dataclass(frozen=True)
class Credentials:
    email: str
    password: str
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def next_utc_aligned_time(now: datetime, interval_s: int) -> datetime:
    """
    Returns the next UTC datetime whose Unix timestamp is a multiple of interval_s.
    This avoids drift from variable execution time.
    """
    if interval_s <= 0:
        raise ValueError("interval_s must be > 0")
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    t = now.timestamp()
    k = int(t // interval_s)
    target_t = (k + 1) * interval_s
    # If we're already exactly on a boundary, run immediately.
    if abs(target_t - t) < 1e-6:
        target_t = t
    return datetime.fromtimestamp(target_t, tz=timezone.utc)


def sleep_until_utc_aligned(interval_s: int) -> None:
    now = datetime.now(timezone.utc)
    target = next_utc_aligned_time(now, interval_s=interval_s)
    sleep_s = (target - now).total_seconds()
    if sleep_s > 0:
        time.sleep(sleep_s)
def http_get_json(url: str, timeout_s: float = 30.0) -> Dict[str, Any]:
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = json.loads(response.text)
    return data

def build_url(method: str, params: Dict[str, Any]) -> str:
    return f"{API_BASE}/{method}.json?{urlencode(params)}"
def login(creds: Credentials, timeout_s: float = 30.0) -> str:
    url = build_url("login", {"email": creds.email, "password": creds.password})
    data = http_get_json(url, timeout_s=timeout_s)
    session = data.get("session")
    if not session or not isinstance(session, str):
        raise MyfxbookError("Login succeeded but no session returned")
    return session

def logout(session: str, timeout_s: float = 30.0) -> None:
    url = build_url("logout", {"session": session})
    http_get_json(url, timeout_s=timeout_s)

def next_utc_sunday_midnight(now: datetime) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now = now.astimezone(timezone.utc)
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    # weekday(): Monday=0 ... Sunday=6
    days_until_sunday = (6 - now.weekday()) % 7
    candidate = today_midnight + timedelta(days=days_until_sunday)
    if now <= candidate:
        return candidate
    return candidate + timedelta(days=7)
def get_outlook_by_country(session: str, symbol: str, timeout_s: float = 30.0) -> List[Dict[str, Any]]:
    url = build_url("get-community-outlook-by-country", {"session": session, "symbol": symbol})
    data = http_get_json(url, timeout_s=timeout_s)
    countries = data.get("countries")
    if not isinstance(countries, list):
        raise MyfxbookError("Unexpected response: missing 'countries' list")
    return countries
def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None
def safe_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except (TypeError, ValueError):
        return None
def pct(n: float, d: float) -> Optional[float]:
    if d == 0:
        return None
    return (n / d) * 100.0
CSV_FIELDS = [
    "timestamp_utc",
    "symbol",
    "country_name",
    "country_code",
    "long_volume",
    "short_volume",
    "long_positions",
    "short_positions",
    "long_pct",
    "short_pct",
]
def write_rows(csv_path: str, rows: List[Dict[str, Any]]) -> None:
    needs_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    os.makedirs(os.path.dirname(os.path.abspath(csv_path)) or ".", exist_ok=True)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if needs_header:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in CSV_FIELDS})
def build_csv_rows(timestamp_utc: str, symbol: str, countries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    total_long_vol = 0.0
    total_short_vol = 0.0
    total_long_pos = 0
    total_short_pos = 0
    for c in countries:
        name = c.get("name")
        code = c.get("code")
        long_vol = safe_float(c.get("longVolume")) or 0.0
        short_vol = safe_float(c.get("shortVolume")) or 0.0
        long_pos = safe_int(c.get("longPositions")) or 0
        short_pos = safe_int(c.get("shortPositions")) or 0
        total_long_vol += long_vol
        total_short_vol += short_vol
        total_long_pos += long_pos
        total_short_pos += short_pos
        denom_vol = long_vol + short_vol
        rows.append(
            {
                "timestamp_utc": timestamp_utc,
                "symbol": symbol.upper(),
                "country_name": name,
                "country_code": code,
                "long_volume": long_vol,
                "short_volume": short_vol,
                "long_positions": long_pos,
                "short_positions": short_pos,
                "long_pct": pct(long_vol, denom_vol),
                "short_pct": pct(short_vol, denom_vol),
            }
        )
    denom_total = total_long_vol + total_short_vol
    rows.insert(
        0,
        {
            "timestamp_utc": timestamp_utc,
            "symbol": symbol.upper(),
            "country_name": "ALL",
            "country_code": "ALL",
            "long_volume": total_long_vol,
            "short_volume": total_short_vol,
            "long_positions": total_long_pos,
            "short_positions": total_short_pos,
            "long_pct": pct(total_long_vol, denom_total),
            "short_pct": pct(total_short_vol, denom_total),
        },
    )
    return rows
def get_creds(args: argparse.Namespace) -> Credentials:
    email = args.email or os.environ.get("MYFXBOOK_EMAIL") or ""
    password = args.password or os.environ.get("MYFXBOOK_PASSWORD") or ""
    if not email or not password:
        raise MyfxbookError(
            "Missing credentials. Set MYFXBOOK_EMAIL and MYFXBOOK_PASSWORD env vars (recommended), "
            "or pass --email/--password."
        )
    return Credentials(email=email, password=password)
def run_loop(args: argparse.Namespace) -> int:
    creds = get_creds(args)
    symbol = (args.symbol or "EURUSD").strip()
    interval_s = int(args.interval_seconds)
    csv_path = os.path.expanduser(args.csv_path)
    session = None  # type: Optional[str]
    next_forced_relogin = next_utc_sunday_midnight(datetime.now(timezone.utc))

    def fetch_write_once(timestamp_utc: str) -> None:
        nonlocal session
        if session is None:
            session = login(creds, timeout_s=args.timeout_seconds)
        countries = get_outlook_by_country(session, symbol=symbol, timeout_s=args.timeout_seconds)
        rows = build_csv_rows(timestamp_utc, symbol, countries)
        write_rows(csv_path, rows)
        print(f"[{timestamp_utc}] wrote {len(rows)} rows to {csv_path}")

    while True:
        # Drift-free scheduling: align to wall-clock boundaries derived from interval_s.
        # For example:
        # - 900  -> :00/:15/:30/:45
        # - 1800 -> :00/:30
        # - 3600 -> :00
        if not args.once and args.align_interval:
            sleep_until_utc_aligned(interval_s=interval_s)

        # Force a session refresh at (or after) Sunday 00:00 UTC, once per week.
        # If your interval doesn't land exactly on 00:00, the first run after 00:00 will still refresh.
        now_utc = datetime.now(timezone.utc)
        if now_utc >= next_forced_relogin:
            if session is not None:
                try:
                    logout(session, timeout_s=args.timeout_seconds)
                except MyfxbookError as e:
                    print(f"[{utc_now_iso()}] logout warning: {e}", file=sys.stderr)
            session = None
            try:
                session = login(creds, timeout_s=args.timeout_seconds)
                print(f"[{utc_now_iso()}] forced weekly re-login (Sunday 00:00 UTC)", file=sys.stderr)
            except MyfxbookError as e:
                print(f"[{utc_now_iso()}] forced weekly re-login failed: {e}", file=sys.stderr)
            next_forced_relogin = next_forced_relogin + timedelta(days=7)

        ts = utc_now_iso()
        try:
            fetch_write_once(ts)
        except MyfxbookError as e:
            msg = str(e)
            should_retry_login = session is not None
            session = None

            if should_retry_login:
                try:
                    fetch_write_once(ts)
                except MyfxbookError as e2:
                    print(f"[{ts}] error (after re-login retry): {e2}", file=sys.stderr)
            else:
                print(f"[{ts}] error: {msg}", file=sys.stderr)
        if args.once:
            return 0
        if not args.align_interval:
            time.sleep(max(1, interval_s))
def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch Myfxbook community outlook by country every N seconds and append to CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--symbol", default="EURUSD", help="Symbol to fetch (e.g. EURUSD).")
    p.add_argument("--interval-seconds", type=int, default=900, help="Polling interval in seconds (15m = 900).")
    p.add_argument(
        "--align-interval",
        action="store_true",
        help="Align polling to UTC wall-clock boundaries (reduces drift).",
    )
    p.add_argument(
        "--csv-path",
        default="~/workspace/myfxbook/eurusd_sentiment.csv",
        help="CSV output path (appends).",
    )
    p.add_argument("--timeout-seconds", type=float, default=30.0, help="HTTP request timeout.")
    p.add_argument("--once", action="store_true", help="Run one fetch and exit.")
    p.add_argument("--email", default=None, help="Myfxbook email (or set MYFXBOOK_EMAIL).")
    p.add_argument("--password", default=None, help="Myfxbook password (or set MYFXBOOK_PASSWORD).")
    return p.parse_args(argv)
def main() -> int:
    args = parse_args(sys.argv[1:])
    return run_loop(args)
if __name__ == "__main__":
    raise SystemExit(main())

