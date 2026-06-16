#!/usr/bin/env python3
"""
Add fraudhunter public hostnames to the existing Cloudflare Tunnel.

Run this ONCE on any machine (including your laptop) to wire up:
  fraudhunter.blackdiamondconsulting.ai     -> http://10.0.0.45:8083  (web UI)
  fraudhunterapi.blackdiamondconsulting.ai  -> http://10.0.0.45:8084  (API)

Prerequisites:
  pip install requests
  export CF_API_TOKEN="<Cloudflare API token with Tunnel:Edit + DNS:Edit permissions>"
  export CF_ACCOUNT_ID="<your Cloudflare account ID>"
  export CF_ZONE_ID="<zone ID for blackdiamondconsulting.ai>"

Where to find these values:
  - API token: dash.cloudflare.com -> My Profile -> API Tokens -> Create Token
    (use "Edit Cloudflare Tunnel" template, add DNS:Edit permission)
  - Account ID + Zone ID: dash.cloudflare.com -> select your domain -> right sidebar
"""

import json
import os
import sys
import urllib.error
import urllib.request

# ── Config ──────────────────────────────────────────────────────────────────

TUNNEL_ID  = "df5e6ba5-50bd-429d-8f95-81869ee60468"
DOMAIN     = "blackdiamondconsulting.ai"

SERVER_IP = "10.0.0.45"

NEW_HOSTNAMES = [
    {"hostname": f"fraudhunter.{DOMAIN}",    "service": f"http://{SERVER_IP}:8083"},
    {"hostname": f"fraudhunterapi.{DOMAIN}", "service": f"http://{SERVER_IP}:8084"},
]

# ── Helpers ──────────────────────────────────────────────────────────────────

def cf(method: str, path: str, token: str, account_id: str, body=None):
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  HTTP {e.code}: {body}", file=sys.stderr)
        raise

def dns(method: str, path: str, token: str, zone_id: str, body=None):
    url = f"https://api.cloudflare.com/client/v4/zones/{zone_id}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  HTTP {e.code}: {body}", file=sys.stderr)
        raise


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    token      = os.environ.get("CF_API_TOKEN", "")
    account_id = os.environ.get("CF_ACCOUNT_ID", "")
    zone_id    = os.environ.get("CF_ZONE_ID", "")

    missing = [k for k, v in [
        ("CF_API_TOKEN", token), ("CF_ACCOUNT_ID", account_id), ("CF_ZONE_ID", zone_id)
    ] if not v]
    if missing:
        sys.exit(f"Missing env vars: {', '.join(missing)}\nSee script header for instructions.")

    # Step 1: fetch existing tunnel ingress config so we don't overwrite analytics route
    print(f"Fetching tunnel config for {TUNNEL_ID}...")
    resp = cf("GET", f"/cfd_tunnel/{TUNNEL_ID}/configurations", token, account_id)
    existing_ingress = resp.get("result", {}).get("config", {}).get("ingress", [])

    # Remove the catch-all "http_status:404" fallback so we can append before it
    catch_all = [r for r in existing_ingress if "hostname" not in r or not r.get("hostname")]
    named     = [r for r in existing_ingress if r.get("hostname")]

    # Strip any existing entries for the hostnames we're about to add so re-runs are idempotent
    new_hostname_set = {entry["hostname"] for entry in NEW_HOSTNAMES}
    keep = [r for r in named if r.get("hostname") not in new_hostname_set]

    new_ingress = keep + NEW_HOSTNAMES + (catch_all or [{"service": "http_status:404"}])

    print("New ingress rules:")
    for r in new_ingress:
        print(f"  {r.get('hostname', '(catch-all)')} -> {r['service']}")

    # Step 2: PUT updated config
    print("\nUpdating tunnel ingress config...")
    cf("PUT", f"/cfd_tunnel/{TUNNEL_ID}/configurations", token, account_id,
       body={"config": {"ingress": new_ingress}})
    print("  Done.")

    # Step 3: Ensure DNS CNAME records exist for the two new hostnames
    tunnel_cname = f"{TUNNEL_ID}.cfargotunnel.com"
    for entry in NEW_HOSTNAMES:
        subdomain = entry["hostname"].replace(f".{DOMAIN}", "")
        print(f"\nEnsuring DNS CNAME: {entry['hostname']} -> {tunnel_cname}")

        # Check if record already exists
        search = dns("GET", f"/dns_records?name={entry['hostname']}&type=CNAME", token, zone_id)
        existing_dns = search.get("result", [])

        if existing_dns:
            print(f"  Record already exists (id={existing_dns[0]['id']}), skipping.")
        else:
            dns("POST", "/dns_records", token, zone_id, body={
                "type":    "CNAME",
                "name":    subdomain,
                "content": tunnel_cname,
                "proxied": True,
                "comment": "Added by cloudflare_setup.py",
            })
            print("  Created.")

    print("\n✓ Setup complete.")
    print(f"  UI:  https://fraudhunter.{DOMAIN}")
    print(f"  API: https://fraudhunterapi.{DOMAIN}")
    print(f"\nRemember to register fraudhunter.{DOMAIN} as a new site in Plausible CE at")
    print(f"  https://analytics.{DOMAIN}")


if __name__ == "__main__":
    main()
