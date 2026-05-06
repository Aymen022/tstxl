#!/usr/bin/env python3
"""
seed_xld_login.py

Fetches all users from an XLDeploy instance via /deployit/security/user/list,
collects their emails (resolving sAMAccountNames via CDP /api/sam/ when needed),
and adds them to the XLD_LOGIN role. Service accounts (userAccountType="generic")
are also added — using their sAMAccountName as the principal, since they have
no email. Only fully-unresolved users are skipped.

Usage:
    python seed_xld_login.py --instance URL                    # dry-run
    python seed_xld_login.py --instance URL --execute          # apply
    python seed_xld_login.py --instance URL --role MY_LOGIN    # custom target role
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

import aiohttp
import requests

# ── Config ────────────────────────────────────────────────────────────────────
CDP_BATCH = 10
XLD_CONCURRENCY = 20
CDP_RETRIES = 5
XLD_RETRIES = 3
BACKUP_DIR = "login_seed_backups"
DEFAULT_ROLE = "XLD_LOGIN"

CDP_SAM_URL = "https://cdp-users-api.fr.world.socgen/api/sam"

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
    # Try a few common XML shapes; the User object may appear under different tags
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


async def classify_user(session, username):
    """
    Returns one of:
      ("email",   "alice@x.com")  -- has email
      ("service", username)        -- service account, skip
      ("unknown", None)            -- couldn't resolve
    """
    data = await _fetch_sam(session, username)
    if data and isinstance(data, dict):
        if data.get("userAccountType") == "generic":
            return ("service", username)
        email = data.get("userEmail")
        if email:
            return ("email", email.lower())
    return ("unknown", None)


async def resolve_usernames(usernames):
    """Resolve usernames -> {username: (kind, value)}. Batched async."""
    resolved = {}
    usernames = list(usernames)
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(usernames), CDP_BATCH):
            batch = usernames[i : i + CDP_BATCH]
            results = await asyncio.gather(*[classify_user(session, u) for u in batch])
            for u, (kind, value) in zip(batch, results):
                resolved[u] = (kind, value)
    return resolved


# ── Backup ────────────────────────────────────────────────────────────────────
def save_json(data, prefix):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H_%M")
    path = os.path.join(BACKUP_DIR, f"{prefix}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    log.info("Saved %s", path)
    return path


# ── Main ──────────────────────────────────────────────────────────────────────
async def run(instance_url, target_role, dry_run):
    if not USERNAME or not PASSWORD:
        log.error("Set XLD_ADMIN_USERNAME and XLD_ADMIN_PASSWORD env vars")
        sys.exit(1)

    # 1. Fetch all users from XLD
    log.info("Fetching users from %s ...", instance_url)
    users = fetch_users(instance_url)
    log.info("  Found %d users", len(users))

    # 2. Split: users with email vs. users needing CDP lookup
    principals = set()  # emails for users + sAMAccountNames for service accounts
    to_resolve = []
    for u in users:
        if u["email"]:
            principals.add(u["email"].lower())
        else:
            to_resolve.append(u["username"])

    log.info("  %d users have email directly, %d need CDP /api/sam/ lookup",
             len(principals), len(to_resolve))

    # 3. Resolve sAMAccountNames via CDP
    resolved = await resolve_usernames(to_resolve)
    counts = {"email": 0, "service": 0, "unknown": 0}
    for username, (kind, value) in resolved.items():
        counts[kind] += 1
        if kind == "email":
            principals.add(value)
        elif kind == "service":
            principals.add(username)  # add the sAMAccountName as-is

    log.info("  CDP results: users=%d, service accounts=%d, unresolved=%d",
             counts["email"], counts["service"], counts["unknown"])
    log.info("Total unique principals collected: %d (emails + service accounts)",
             len(principals))

    # 4. Compute what's missing from target role
    log.info("Fetching current principals of role '%s' ...", target_role)
    current = set(fetch_role_principals(instance_url, target_role))
    to_add = sorted(principals - current)

    log.info("Target role '%s' currently has %d principals", target_role, len(current))
    log.info("Will add %d new principals (skipping %d already present)",
             len(to_add), len(principals) - len(to_add))

    # 5. Save backup
    backup = {
        "instance_url": instance_url,
        "target_role": target_role,
        "timestamp": datetime.now().isoformat(),
        "current_principals": sorted(current),
        "principals_to_add": to_add,
        "all_collected_principals": sorted(principals),
        "service_accounts_added": sorted(
            u for u, (k, _) in resolved.items() if k == "service"
        ),
        "unresolved_usernames": sorted(
            u for u, (k, _) in resolved.items() if k == "unknown"
        ),
    }
    backup_path = save_json(backup, f"seed_{target_role}")

    if not to_add:
        log.info("Nothing to add. Done.")
        return

    # 6. Apply
    mode = "DRY-RUN" if dry_run else "EXECUTE"
    log.info("Adding principals to '%s' (%s, concurrency=%d) ...",
             target_role, mode, XLD_CONCURRENCY)

    if dry_run:
        for principal in to_add:
            log.info("[DRY-RUN] ADD '%s' -> role '%s'", principal, target_role)
        log.info("Done (dry-run). Review backup at %s, then re-run with --execute.", backup_path)
        return

    sem = asyncio.Semaphore(XLD_CONCURRENCY)
    auth = aiohttp.BasicAuth(USERNAME, PASSWORD)
    connector = aiohttp.TCPConnector(limit=XLD_CONCURRENCY)

    added = failed = 0
    async with aiohttp.ClientSession(auth=auth, connector=connector) as session:
        tasks = [
            xld_add_principal(session, sem, instance_url, target_role, principal)
            for principal in to_add
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
    parser = argparse.ArgumentParser(description="Seed an XLD role with all user emails (from XLD user list)")
    parser.add_argument("--instance", metavar="URL", required=True, help="XLD instance URL")
    parser.add_argument("--role", default=DEFAULT_ROLE, help=f"Target role (default: {DEFAULT_ROLE})")
    parser.add_argument("--execute", action="store_true", help="Apply changes (default is dry-run)")
    args = parser.parse_args()

    asyncio.run(run(args.instance, args.role, dry_run=not args.execute))
