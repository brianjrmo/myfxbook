#!/usr/bin/env python3
import argparse
import os
import sys
import pandas as pd
from typing import Any, Dict, List
from refreshSession import login, get_creds, build_url, http_get_json, MyfxbookError, utc_now_iso

def get_community_outlook(session: str, timeout_s: float = 30.0) -> List[Dict[str, Any]]:
    url = build_url("get-community-outlook", {"session": session})
    data = http_get_json(url, timeout_s=timeout_s)
    community_outlook_symbols_list = data.get("symbols")
    community_outlook_general_list = data.get("general")
    if not isinstance(community_outlook_symbols_list, list):
        raise MyfxbookError("Unexpected response: missing 'symbols' list")
    return community_outlook_symbols_list, community_outlook_general_list
    
def write_sentiment_data(data_list, data_name: str, timestamp_utc: str, args: argparse.Namespace) -> None:
    df = pd.DataFrame(data_list)
    df["DateTime"] = timestamp_utc
    df = df[["DateTime"] + [c for c in df.columns if c != "DateTime"]]
    output_path = os.path.expanduser(args.sentiment_file + data_name + ".csv")
    exists = os.path.exists(output_path) and os.path.getsize(output_path) > 0
    df.to_csv(
        output_path,
        mode="a" if exists else "w",
        header=not exists,
        index=False,
    )

def get_data(timestamp_utc: str, args: argparse.Namespace) -> int:
    session_path = os.path.expanduser(args.session_file)
    creds = get_creds(args.email, args.password)
    if os.path.exists(session_path):
        with open(session_path, "r") as f:
            session = f.read()
    else:  
        session = login(creds, timeout_s=args.timeout_seconds)
        with open(session_path, "w") as f:
            f.write(session)

    try:
        community_outlook_symbols_list, community_outlook_general_list = get_community_outlook(session, timeout_s=args.timeout_seconds)
    except Exception as e:
        # any error, login again and try again
        print(f"[{ utc_now_iso()}] Error getting community outlook: {e}, logging in again")
        session = login(creds, timeout_s=args.timeout_seconds)
        with open(session_path, "w") as f:
            f.write(session)
        community_outlook_symbols_list, community_outlook_general_list = get_community_outlook(session, timeout_s=args.timeout_seconds)

    write_sentiment_data(community_outlook_symbols_list, "symbols", timestamp_utc, args)
    write_sentiment_data([community_outlook_general_list], "general", timestamp_utc, args)

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch Myfxbook community outlook and append to CSV.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--session_file",
        default="~/workspace/myfxbook/session.txt",
        help="Session file path.",
    )
    p.add_argument(
        "--sentiment_file",
        default="~/workspace/myfxbook/sentiment_",
        help="Sentiment CSV file path.",
    )
    p.add_argument("--timeout-seconds", type=float, default=30.0, help="HTTP request timeout.")
    p.add_argument("--once", action="store_true", help="Run one fetch and exit.")
    p.add_argument("--email", default=None, help="Myfxbook email (or set MYFXBOOK_EMAIL).")
    p.add_argument("--password", default=None, help="Myfxbook password (or set MYFXBOOK_PASSWORD).")
    return p.parse_args(argv)

def main() -> int:
    timestamp_utc = utc_now_iso(adjust_second_0=True)
    args = parse_args(sys.argv[1:])
    return get_data(timestamp_utc, args)

if __name__ == "__main__":
    raise SystemExit(main())
