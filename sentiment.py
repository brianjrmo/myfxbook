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
    community_outlook_list = data.get("symbols")
    if not isinstance(community_outlook_list, list):
        raise MyfxbookError("Unexpected response: missing 'symbols' list")
    return community_outlook_list
    
def get_data(timestamp_utc: str, args: argparse.Namespace) -> int:
    session_path = os.path.expanduser(args.session_file)
    if os.path.exists(session_path):
        with open(session_path, "r") as f:
            session = f.read()
    else:  
        creds = get_creds(args.email, args.password)
        session = login(creds, timeout_s=args.timeout_seconds)
        with open(session_path, "w") as f:
            f.write(session)
    
    sentiment_path = os.path.expanduser(args.sentiment_file)
    community_outlook_list = get_community_outlook(session, timeout_s=args.timeout_seconds)
    community_outlook_df = pd.DataFrame(community_outlook_list)
    community_outlook_df["DateTime"] = timestamp_utc
    community_outlook_df = community_outlook_df[["DateTime"] + [c for c in community_outlook_df.columns if c != "DateTime"]]
    exists = os.path.exists(sentiment_path) and os.path.getsize(sentiment_path) > 0
    community_outlook_df.to_csv(
        sentiment_path,
        mode="a" if exists else "w",
        header=not exists,
        index=False,
    )

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
        default="~/workspace/myfxbook/sentiment.csv",
        help="Sentiment CSV file path.",
    )
    p.add_argument("--timeout-seconds", type=float, default=30.0, help="HTTP request timeout.")
    p.add_argument("--once", action="store_true", help="Run one fetch and exit.")
    p.add_argument("--email", default=None, help="Myfxbook email (or set MYFXBOOK_EMAIL).")
    p.add_argument("--password", default=None, help="Myfxbook password (or set MYFXBOOK_PASSWORD).")
    return p.parse_args(argv)

def main() -> int:
    timestamp_utc = utc_now_iso()
    args = parse_args(sys.argv[1:])
    return get_data(timestamp_utc, args)

if __name__ == "__main__":
    raise SystemExit(main())
