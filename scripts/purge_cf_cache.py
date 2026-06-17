#!/usr/bin/env python3
"""
Purge Cloudflare cache for all zones defined in .env.

Usage:
    python scripts/purge_cf_cache.py
"""

import os
import sys
import json
import urllib.request
import urllib.error
from pathlib import Path


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ[key.strip()] = value.strip()


def main():
    load_env(Path(__file__).parent.parent / ".env")

    token   = os.environ.get("CF_API_TOKEN", "")
    zone_id = os.environ.get("CF_ZONE_ID", "")

    if not token or not zone_id:
        sys.exit("Missing CF_API_TOKEN or CF_ZONE_ID — set them in .env or environment.")

    url  = f"https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache"
    body = json.dumps({"purge_everything": True}).encode()
    req  = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )

    print(f"Purging Cloudflare cache for zone {zone_id}...")
    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read())
        if result.get("success"):
            print("✓ Cache purged successfully.")
        else:
            print(f"✗ API returned errors: {result.get('errors')}")
            sys.exit(1)
    except urllib.error.HTTPError as e:
        print(f"✗ HTTP {e.code}: {e.read().decode()}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
