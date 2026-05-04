#!/usr/bin/env python3
"""
seed_xld_login.py

1. Fetches all users from an XLDeploy instance via /deployit/security/user/list
2. Resolves each via CDP /sam/{username} to get userEmail + userGgi
3. Adds the emails to the XLD_LOGIN role
4. Exports a CSV of active users in IAM provisioning format

Service accounts (userAccountType="generic") and unresolvable users are skipped.

Usage:
    python seed_xld_login.py --instance URL                    # dry-run
    python seed_xld_login.py --instance URL --execute          # apply
    python seed_xld_login.py --instance URL --role MY_LOGIN    # custom target role
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urlparse

import aiohttp
import requests

# ── Config ────────────────────────────────────────────────────────────────────
CDP_BATCH = 10
XLD_CONCURRENCY = 20
CDP_RETRIES = 5
XLD_RETRIES = 3
BACKUP_DIR = "login_seed_backups"
DEFAULT_ROLE = "XLD_LOGIN"

CDP_SAM_URL = "https://cdp-users-api.fr.world.socgen/sam"

# IAM CSV defaults
CSV_HEADER = [
    "IGG", "SGCONNECT_LOGIN", "RESOURCE_NAME", "PROFILE_TECHNICAL_NAME",
    "NEEDPROVISIONING", "NEEDNOTIFY", "XLD_INSTANCES",
]
CSV_RESOURCE_NAME = "xld"
CSV_PROFILE = "XLDEPLOY_USER"

USERNAME = os.getenv("XLD_ADMIN_USERNAME")
PASSWORD = os.getenv("XLD_ADMIN_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("seed_xld_login")


# ── XLD helpers ───────────────────────────────────────────────────────────────
def fetch_users(base_url):
    """
    GET /deployit/security/user/list. Returns list of dicts:
      [{"username": "m12345", "email": "alice@x.com" or None, "fullName": "..."}]
    """
    url = f"{base_url.rstrip('/')}/deployit/security/user/list"
    resp = requests.get(url, auth=(USERNAME, PASSWORD))
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    users = []
    for user in root.iter():
        if user.tag.lower() not in ("user", "users") or user is root:
            continue
        username = user.findtext("username") or user.get("username")
        if not username:
            continue
        email = user.findtext("email") or user.findtext("userProfile/email")
        full_name = user.findtext("fullName") or user.findtext("userProfile/fullName")
        users.append({"username": username, "email": email, "fullName": full_name})
    return users


def fetch_role_principals(base_url, role_name):
    """GET current principals for a single role."""
    url = f"{base_url.rstrip('/')}/deployit/security/role/principals"
    resp = requests.get(url, auth=(USERNAME, PASSWORD))
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    for rp in root.findall(".//rolePrincipals"):
        role = rp.find("role")
        name = role.get("name") if role is not None else None
        if name == role_name:
            return [p.text for p in rp.findall("principals") if p.text]
    return []


async def xld_add_principal(session, sem, base_url, role_name, principal):
    """PUT a principal to a role with semaphore-bounded concurrency + retry."""
    url = f"{base_url.rstrip('/')}/deployit/security/role/{role_name}/{principal}"
    async with sem:
        for attempt in range(1, XLD_RETRIES + 1):
            try:
                async with session.put(url) as resp:
                    resp.raise_for_status()
                    return True
            except Exception as e:
                log.warning("ADD failed (attempt %d/%d) principal=%s: %s",
                            attempt, XLD_RETRIES, principal, e)
                if attempt < XLD_RETRIES:
                    await asyncio.sleep(2 ** attempt)
        return False


# ── CDP resolution ────────────────────────────────────────────────────────────
async def _fetch_sam(session, username):
    """GET /sam/{username} with retries. Returns parsed JSON or None."""
    url = f"{CDP_SAM_URL}/{username}"
    for attempt in range(CDP_RETRIES):
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    return None
                resp.raise_for_status()
                return json.loads(await resp.text())
        except Exception as e:
            log.warning("CDP /sam/%s (attempt %d/%d): %s",
                        username, attempt + 1, CDP_RETRIES, e)
            if attempt < CDP_RETRIES - 1:
                await asyncio.sleep(5)
    return None


async def resolve_users(usernames):
    """Resolve usernames -> {username: cdp_data_dict_or_None}. Batched async."""
    resolved = {}
    usernames = list(usernames)
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(usernames), CDP_BATCH):
            batch = usernames[i : i + CDP_BATCH]
            results = await asyncio.gather(*[_fetch_sam(session, u) for u in batch])
            for u, data in zip(batch, results):
                resolved[u] = data
    return resolved


# ── Output helpers ────────────────────────────────────────────────────────────
def save_json(data, prefix):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H_%M")
    path = os.path.join(BACKUP_DIR, f"{prefix}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    log.info("Saved %s", path)
    return path


def save_csv(rows, prefix, instance_label):
    """Write IAM-format CSV with one row per active user."""
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H_%M")
    path = os.path.join(BACKUP_DIR, f"{prefix}_{instance_label}_{ts}.csv")
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADER)
        for row in rows:
            writer.writerow([
                row["igg"],
                row["email"],
                CSV_RESOURCE_NAME,
                CSV_PROFILE,
                "TRUE",
                "TRUE",
                instance_label,
            ])
    log.info("Saved CSV %s (%d rows)", path, len(rows))
    return path


def short_instance_name(url):
    """Derive a short label from the URL hostname (e.g. xldeploy-prd-16)."""
    host = urlparse(url).hostname or url
    return host.split(".")[0]


# ── Main ──────────────────────────────────────────────────────────────────────
async def run(instance_url, target_role, dry_run):
    if not USERNAME or not PASSWORD:
        log.error("Set XLD_ADMIN_USERNAME and XLD_ADMIN_PASSWORD env vars")
        sys.exit(1)

    instance_label = short_instance_name(instance_url)

    # 1. Fetch all users from XLD
    log.info("Fetching users from %s ...", instance_url)
    xld_users = fetch_users(instance_url)
    log.info("  Found %d users", len(xld_users))

    # 2. Resolve every user via CDP (even those with email, to get userGgi)
    usernames = [u["username"] for u in xld_users]
    log.info("Resolving %d usernames via CDP /sam/ ...", len(usernames))
    cdp_data = await resolve_users(usernames)

    # 3. Build active user list
    active = []          # CSV-eligible: has email + IGG, not service account
    service_accts = []
    unresolved = []

    for u in xld_users:
        username = u["username"]
        data = cdp_data.get(username)

        if data is None:
            # CDP didn't know the user; fall back to XLD email if any
            if u["email"]:
                active.append({"username": username, "email": u["email"].lower(), "igg": ""})
            else:
                unresolved.append(username)
            continue

        if data.get("userAccountType") == "generic":
            service_accts.append(username)
            continue

        email = (data.get("userEmail") or u["email"] or "").lower()
        igg = data.get("userGgi") or ""

        if not email:
            unresolved.append(username)
            continue

        active.append({"username": username, "email": email, "igg": igg})

    # Dedup by email
    seen = set()
    deduped = []
    for row in active:
        if row["email"] in seen:
            continue
        seen.add(row["email"])
        deduped.append(row)
    active = deduped

    log.info("  Active users with email: %d (CSV-eligible)", len(active))
    log.info("  Service accounts skipped: %d", len(service_accts))
    log.info("  Unresolved users skipped: %d", len(unresolved))

    emails = {u["email"] for u in active}

    # 4. Compute what's missing from target role
    log.info("Fetching current principals of role '%s' ...", target_role)
    current = set(fetch_role_principals(instance_url, target_role))
    to_add = sorted(emails - current)

    log.info("Target role '%s' currently has %d principals", target_role, len(current))
    log.info("Will add %d new emails (skipping %d already present)",
             len(to_add), len(emails) - len(to_add))

    # 5. Save backup + CSV
    backup = {
        "instance_url": instance_url,
        "target_role": target_role,
        "timestamp": datetime.now().isoformat(),
        "current_principals": sorted(current),
        "emails_to_add": to_add,
        "all_active_users": active,
        "skipped_service_accounts": sorted(service_accts),
        "unresolved_usernames": sorted(unresolved),
    }
    backup_path = save_json(backup, f"seed_{target_role}")
    csv_path = save_csv(active, "iam_users", instance_label)

    if not to_add:
        log.info("Nothing to add to XLD. Done.")
        return

    # 6. Apply
    mode = "DRY-RUN" if dry_run else "EXECUTE"
    log.info("Adding emails to '%s' (%s, concurrency=%d) ...",
             target_role, mode, XLD_CONCURRENCY)

    if dry_run:
        for email in to_add:
            log.info("[DRY-RUN] ADD '%s' -> role '%s'", email, target_role)
        log.info("Done (dry-run). Review backup at %s and CSV at %s, then re-run with --execute.",
                 backup_path, csv_path)
        return

    sem = asyncio.Semaphore(XLD_CONCURRENCY)
    auth = aiohttp.BasicAuth(USERNAME, PASSWORD)
    connector = aiohttp.TCPConnector(limit=XLD_CONCURRENCY)

    added = failed = 0
    async with aiohttp.ClientSession(auth=auth, connector=connector) as session:
        tasks = [
            xld_add_principal(session, sem, instance_url, target_role, email)
            for email in to_add
        ]
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            ok = await coro
            if ok:
                added += 1
            else:
                failed += 1
            if i % 100 == 0:
                log.info("Progress: %d/%d (%d ok, %d failed)", i, len(tasks), added, failed)

    log.info("Done: %d added, %d failed (target role: %s)", added, failed, target_role)


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed XLD role + export IAM CSV from XLD user list")
    parser.add_argument("--instance", metavar="URL", required=True, help="XLD instance URL")
    parser.add_argument("--role", default=DEFAULT_ROLE, help=f"Target role (default: {DEFAULT_ROLE})")
    parser.add_argument("--execute", action="store_true", help="Apply changes (default is dry-run)")
    args = parser.parse_args()

    asyncio.run(run(args.instance, args.role, dry_run=not args.execute))