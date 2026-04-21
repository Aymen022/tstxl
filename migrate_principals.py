#!/usr/bin/env python3
"""
migrate_principals.py

Migrates XLDeploy role principals from AD group names to individual
user email addresses by resolving groups via the CDP Users API.

Usage:
    python migrate_principals.py --instance URL                     # dry-run
    python migrate_principals.py --instance URL --execute           # live
    python migrate_principals.py --instance URL --rollback backup   # restore
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime

import aiohttp
import requests

# ── Config ────────────────────────────────────────────────────────────────────
BATCH_SIZE = 10
CDP_RETRIES = 5
XLD_RETRIES = 3
BACKUP_DIR = "migration_backups"
CDP_API_URL = "https://cdp-users-api.fr.world.socgen/api/group"

XLD_INSTANCES = [
    "https://xldeploy-prd-16.fr.world.socgen:4516",
    "https://xldeploy-prd-18.fr.world.socgen:4516",
    "https://xldeploy-prd-29.fr.world.socgen:4516",
    "https://xldeploy-prd-30.fr.world.socgen:4516",
    "https://xldeploy-prd-01.fr.world.socgen:4516",
    "https://deploy-prd-01.fr.world.socgen:4516",
    "https://xldeploy-prd-04.fr.world.socgen:4516",
    "https://xldeploy-prd-05.fr.world.socgen:4516",
    "https://xldeploy-prd-07.fr.world.socgen:4516",
    "https://xldeploy-prd-08.fr.world.socgen:4516",
    "https://xldeploy-prd-86.fr.world.socgen:4516",
    "https://xldeploy-prd-03.fr.world.socgen:4516",
    "https://deploy-prd-03.fr.world.socgen:4516",
    "https://deploy-prd-02.fr.world.socgen:4516",
    "https://xldeploy-prd-14.fr.world.socgen:4516",
    "https://xldeploy-prd-15.fr.world.socgen:4516",
]

USERNAME = os.getenv("XLD_ADMIN_USERNAME")
PASSWORD = os.getenv("XLD_ADMIN_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("migrate_principals")


# ── XLD helpers ───────────────────────────────────────────────────────────────
def xld_url(base_url, path):
    return f"{base_url.rstrip('/')}/deployit/{path}"


def fetch_role_principals(base_url):
    """GET role principals from XLD, returns dict: role_name -> [principals]."""
    resp = requests.get(
        xld_url(base_url, "security/role/principals"),
        auth=(USERNAME, PASSWORD),
    )
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    result = {}
    for rp in root.findall(".//rolePrincipals"):
        role = rp.find("role")
        name = role.get("name") if role is not None else None
        if name:
            result[name] = [p.text for p in rp.findall("principals") if p.text]
    return result


def xld_request(method, base_url, role_name, principal):
    """PUT or DELETE a principal on a role with retry + backoff."""
    url = xld_url(base_url, f"security/role/{role_name}/{principal}")
    for attempt in range(1, XLD_RETRIES + 1):
        try:
            resp = requests.request(method, url, auth=(USERNAME, PASSWORD))
            resp.raise_for_status()
            return True
        except Exception as e:
            log.warning(
                "%s failed (attempt %d/%d) role=%s principal=%s: %s",
                method, attempt, XLD_RETRIES, role_name, principal, e,
            )
            if attempt < XLD_RETRIES:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"{method} failed after {XLD_RETRIES} attempts: role={role_name} principal={principal}")


# ── CDP resolution ────────────────────────────────────────────────────────────
async def _fetch_group(session, group_name):
    url = f"{CDP_API_URL}/{group_name}"
    for attempt in range(CDP_RETRIES):
        try:
            async with session.get(url) as resp:
                return json.loads(await resp.text())
        except Exception as e:
            log.warning("CDP fetch '%s' (attempt %d/%d): %s", group_name, attempt + 1, CDP_RETRIES, e)
            if attempt < CDP_RETRIES - 1:
                await asyncio.sleep(5)
    return None


async def resolve_ad_groups(group_names):
    """Resolve AD group names -> {group: [emails] or None}. Batched async."""
    resolved = {}
    groups = list(group_names)

    async with aiohttp.ClientSession() as session:
        for i in range(0, len(groups), BATCH_SIZE):
            batch = groups[i : i + BATCH_SIZE]
            results = await asyncio.gather(*[_fetch_group(session, g) for g in batch])

            for name, data in zip(batch, results):
                if data is None or "name" not in data:
                    log.warning("CDP resolution failed for: %s", name)
                    resolved[name] = None
                else:
                    resolved[name] = [
                        m["userEmail"].lower()
                        for m in data.get("members", [])
                        if m.get("userEmail")
                    ]
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


# ── Migration ─────────────────────────────────────────────────────────────────
def apply_changes(base_url, roles, dry_run, direction="migrate"):
    """
    Apply migration or rollback on one instance.
    direction="migrate": add emails, remove AD groups
    direction="rollback": add AD groups, remove emails
    """
    added = removed = errors = 0

    for role in roles:
        role_name = role["role_name"]

        if direction == "migrate":
            to_add = role["new_principals"]
            to_remove = [g for g in role["ad_groups"] if g not in role["skipped_groups"]]
        else:
            to_add = role["ad_groups"]
            to_remove = role["new_principals"]

        # Phase 1: ADD
        add_ok = True
        for principal in to_add:
            if dry_run:
                log.info("[DRY-RUN] ADD '%s' -> role '%s'", principal, role_name)
                added += 1
            else:
                try:
                    xld_request("PUT", base_url, role_name, principal)
                    log.info("ADDED '%s' -> role '%s'", principal, role_name)
                    added += 1
                except Exception as e:
                    add_ok = False
                    errors += 1
                    log.error("ADD failed: role=%s principal=%s: %s", role_name, principal, e)

        # Phase 2: REMOVE (only if all adds succeeded)
        if not add_ok:
            log.warning("Skipping REMOVE for role '%s' due to ADD failures", role_name)
            continue

        for principal in to_remove:
            if dry_run:
                log.info("[DRY-RUN] REMOVE '%s' from role '%s'", principal, role_name)
                removed += 1
            else:
                try:
                    xld_request("DELETE", base_url, role_name, principal)
                    log.info("REMOVED '%s' from role '%s'", principal, role_name)
                    removed += 1
                except Exception as e:
                    errors += 1
                    log.error("REMOVE failed: role=%s principal=%s: %s", role_name, principal, e)

    return {"added": added, "removed": removed, "errors": errors, "roles": len(roles)}


# ── Main ──────────────────────────────────────────────────────────────────────
async def run_migration(instance_url, dry_run=True, keep_empty_groups=False):
    if not USERNAME or not PASSWORD:
        log.error("Set XLD_ADMIN_USERNAME and XLD_ADMIN_PASSWORD env vars")
        sys.exit(1)

    # 1. Fetch role principals
    log.info("Fetching role principals from %s ...", instance_url)
    role_principals = fetch_role_principals(instance_url)
    log.info("  %d roles, %d principals", len(role_principals), sum(len(v) for v in role_principals.values()))

    # 2. Resolve AD groups
    ad_groups = {p for ps in role_principals.values() for p in ps if "@" not in p}
    log.info("Resolving %d AD groups via CDP ...", len(ad_groups))
    resolved = await resolve_ad_groups(ad_groups)
    ok = sum(1 for v in resolved.values() if v is not None)
    log.info("  Resolved: %d, Failed: %d", ok, len(resolved) - ok)

    # 3. Build snapshot and backup
    roles = []
    for role_name, principals in role_principals.items():
        groups = [p for p in principals if "@" not in p]
        skipped = []
        emails = set()

        for g in groups:
            member_emails = resolved.get(g)
            if member_emails is None:
                skipped.append(g)
            elif member_emails:
                emails.update(member_emails)
            elif keep_empty_groups:
                skipped.append(g)

        roles.append({
            "role_name": role_name,
            "original_principals": principals,
            "ad_groups": groups,
            "new_principals": sorted(emails),
            "skipped_groups": skipped,
        })

    snapshot = {"instance_url": instance_url, "timestamp": datetime.now().isoformat(), "roles": roles}
    backup_path = save_json([snapshot], "backup")

    # 4. Apply
    mode = "DRY-RUN" if dry_run else "EXECUTE"
    log.info("Running migration (%s) ...", mode)
    result = apply_changes(instance_url, roles, dry_run, direction="migrate")

    save_json(result, f"report_{'dryrun' if dry_run else 'migration'}")
    log.info("Done: %d roles, %d added, %d removed, %d errors", result["roles"], result["added"], result["removed"], result["errors"])

    if dry_run:
        log.info("Dry run. Review backup at %s, then re-run with --execute.", backup_path)


async def run_rollback(backup_path, instance_url):
    if not USERNAME or not PASSWORD:
        log.error("Set XLD_ADMIN_USERNAME and XLD_ADMIN_PASSWORD env vars")
        sys.exit(1)

    with open(backup_path, "r") as f:
        snapshots = json.load(f)

    matching = [s for s in snapshots if s["instance_url"] == instance_url]
    if not matching:
        log.error("Instance %s not in backup. Found: %s", instance_url, [s["instance_url"] for s in snapshots])
        sys.exit(1)

    log.info("Rolling back %s ...", instance_url)
    result = apply_changes(instance_url, matching[0]["roles"], dry_run=False, direction="rollback")

    save_json(result, "report_rollback")
    log.info("Rollback done: %d roles, %d added, %d removed, %d errors", result["roles"], result["added"], result["removed"], result["errors"])


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate XLD principals: AD groups -> emails (one instance at a time)")
    parser.add_argument("--instance", metavar="URL", help="XLD instance URL (required for migrate/rollback)")
    parser.add_argument("--execute", action="store_true", help="Apply changes (default is dry-run)")
    parser.add_argument("--rollback", metavar="BACKUP_FILE", help="Rollback from backup file")
    parser.add_argument("--keep-empty-groups", action="store_true", help="Keep AD groups with no members")
    args = parser.parse_args()

    if not args.instance:
        log.error("--instance URL is required.")
        sys.exit(1)

    if args.rollback:
        asyncio.run(run_rollback(args.rollback, args.instance))
    else:
        asyncio.run(run_migration(args.instance, dry_run=not args.execute, keep_empty_groups=args.keep_empty_groups))