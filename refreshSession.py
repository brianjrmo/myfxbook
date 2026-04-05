#!/usr/bin/env python3
import argparse
import json
import os
import sys
import requests
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import urlencode

API_BASE = "https://www.myfxbook.com/api"
class MyfxbookError(RuntimeError):
    pass

class Credentials:
    def __init__(self, email: str, password: str) -> None:
        self.email = email
        self.password = password

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def http_get_json(url: str, timeout_s: float = 30.0) -> Dict[str, Any]:
    payload = {}
    headers = {}
    try:
        response = requests.request("GET", url, headers=headers, data=payload, timeout=timeout_s)
        data = json.loads(response.text)
        return data
    except requests.exceptions.Timeout:
        raise MyfxbookError("Request timed out")
    except requests.exceptions.RequestException as e:
        raise MyfxbookError(f"Request failed: {e}")

def build_url(method: str, params: Dict[str, Any]) -> str:
    return f"{API_BASE}/{method}.json?{urlencode(params, safe='%')}"

def login(creds: Credentials, timeout_s: float = 30.0) -> str:
    url = build_url("login", {"email": creds.email, "password": creds.password})
    data = http_get_json(url, timeout_s=timeout_s)
    session = data.get("session")
    if not session or not isinstance(session, str):
        raise MyfxbookError("Login succeeded but no session returned")
    return session

def logout(session: str, timeout_s: float = 30.0) -> None:
    url = build_url("logout", {"session": session})
    logout_data = http_get_json(url, timeout_s=timeout_s)
    print(f"[{utc_now_iso()}] logout successfully")
    print(f"[{utc_now_iso()}] logout session: {session}")

def get_creds(email: str, password: str) -> Credentials:
    email = email
    password = password
    return Credentials(email, password)

def refresh_session(args: argparse.Namespace) -> int:
    # logout if session file exists
    session_path = os.path.expanduser(args.session_file)
    if os.path.exists(session_path):
        with open(session_path, "r") as f:
            session = f.read()
        logout(session, timeout_s=args.timeout_seconds)
        os.remove(session_path)

    creds = get_creds(args.email, args.password)

    session = login(creds, timeout_s=args.timeout_seconds)
    print(f"[{utc_now_iso()}] logged in successfully")
    print(f"[{utc_now_iso()}] logged in session: {session}")
    with open(session_path, "w") as f:
        f.write(session)

def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Login Myfxbook and save session to file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--session_file",
        default="~/workspace/myfxbook/session.txt",
        help="Session file path.",
    )
    p.add_argument(
        "--timeout_seconds",
        default=30,
        help="HTTP request timeout.",
    )
    p.add_argument("--email", required=True, help="Myfxbook email.")
    p.add_argument("--password", required=True, help="Myfxbook password.")
    return p.parse_args(argv)
def main() -> int:
    args = parse_args(sys.argv[1:])
    return refresh_session(args)
if __name__ == "__main__":
    raise SystemExit(main())
