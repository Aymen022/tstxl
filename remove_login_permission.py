#!/usr/bin/env python3
"""
remove_login_permission.py

Removes the 'login' permission from every role on an XLDeploy instance,
except for protected roles (XLD_LOGIN and DPY_ADMIN by default).

Backs up the full role->permission map before any change so that a rollback
re-adds the permission to the same set of roles.

Usage:
    python remove_login_permission.py --instance URL                           # dry-run
    python remove_login_permission.py --instance URL --execute                 # apply
    python remove_login_permission.py --instance URL --rollback BACKUP_FILE    # restore
    python remove_login_permission.py --instance URL --permission xyz          # different perm
    python remove_login_permission.py --instance URL --keep ROLE1 --keep ROLE2 # extra protected roles
"""

import argparse
import asyncio
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
XLD_CONCURRENCY = 20
XLD_RETRIES = 3
BACKUP_DIR = "permission_backups"
DEFAULT_PROTECTED = {"XLD_LOGIN", "DPY_ADMIN"}
DEFAULT_PERMISSION = "login"

# Direct permissions only (not inherited) — we only want to revoke what's
# explicitly assigned to each role.
PERMISSIONS_ENDPOINT = (
    "/xldeploy/internal/security/roles/v2/permissions/global"
    "?includeInherited=false&order=role:asc&page=1&resultsPerPage=100000&rolePattern="
)

USERNAME = os.getenv("XLD_ADMIN_USERNAME")
PASSWORD = os.getenv("XLD_ADMIN_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("remove_login_permission")


# ── XLD helpers ───────────────────────────────────────────────────────────────
def fetch_role_permissions(base_url):
    """
    GET role->permissions map (direct, not inherited).
    Returns dict: role_name -> set(permission_names).
    """
    url = f"{base_url.rstrip('/')}{PERMISSIONS_ENDPOINT}"
    resp = requests.get(url, auth=(USERNAME, PASSWORD))
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    roles = {}
    for rp in root.findall(".//rolePermissions"):
        role_elem = rp.find("role")
        name = role_elem.get("name") if role_elem is not None else None
        if not name:
            continue
        perms = {p.text for p in rp.findall("permissions") if p.text}
        roles.setdefault(name, set()).update(perms)
    return roles


async def xld_permission_request(session, sem, base_url, method, permission, role):
    """PUT or DELETE /deployit/security/permission/{perm}/{role} with retry + backoff."""
    url = f"{base_url.rstrip('/')}/deployit/security/permission/{permission}/{role}"
    async with sem:
        for attempt in range(1, XLD_RETRIES + 1):
            try:
                async with session.request(method, url) as resp:
                    resp.raise_for_status()
                    return True
            except Exception as e:
                log.warning("%s failed (attempt %d/%d) perm=%s role=%s: %s",
                            method, attempt, XLD_RETRIES, permission, role, e)
                if attempt < XLD_RETRIES:
                    await asyncio.sleep(2 ** attempt)
        return False


# ── Output helpers ────────────────────────────────────────────────────────────
def save_json(data, prefix, instance_label):
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H_%M")
    path = os.path.join(BACKUP_DIR, f"{prefix}_{instance_label}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    log.info("Saved %s", path)
    return path


def short_instance_name(url):
    host = urlparse(url).hostname or url
    return host.split(".")[0]


# ── Main ──────────────────────────────────────────────────────────────────────
async def apply_permission_change(instance_url, roles, permission, method, dry_run):
    """Apply PUT or DELETE concurrently for the permission across the given roles."""
    if dry_run:
        for role in sorted(roles):
            verb = "Would REVOKE" if method == "DELETE" else "Would GRANT"
            log.info("[DRY-RUN] %s '%s' from role '%s'", verb, permission, role)
        return len(roles), 0

    sem = asyncio.Semaphore(XLD_CONCURRENCY)
    auth = aiohttp.BasicAuth(USERNAME, PASSWORD)
    connector = aiohttp.TCPConnector(limit=XLD_CONCURRENCY)

    ok = failed = 0
    async with aiohttp.ClientSession(auth=auth, connector=connector) as session:
        tasks = [
            xld_permission_request(session, sem, instance_url, method, permission, role)
            for role in roles
        ]
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            success = await coro
            if success:
                ok += 1
            else:
                failed += 1
            if i % 50 == 0:
                log.info("Progress: %d/%d (%d ok, %d failed)", i, len(tasks), ok, failed)
    return ok, failed


async def run(instance_url, permission, protected_roles, dry_run):
    if not USERNAME or not PASSWORD:
        log.error("Set XLD_ADMIN_USERNAME and XLD_ADMIN_PASSWORD env vars")
        sys.exit(1)

    instance_label = short_instance_name(instance_url)

    # 1. Fetch role permissions
    log.info("Fetching role permissions from %s ...", instance_url)
    role_perms = fetch_role_permissions(instance_url)
    log.info("  %d roles with direct permissions", len(role_perms))

    # 2. Identify roles to update
    targets = sorted(
        role for role, perms in role_perms.items()
        if role not in protected_roles and permission in perms
    )

    log.info("Protected roles (not touched): %s", sorted(protected_roles))
    log.info("Roles with '%s' permission to revoke: %d", permission, len(targets))

    # 3. Save backup (always)
    backup = {
        "instance_url": instance_url,
        "permission": permission,
        "protected_roles": sorted(protected_roles),
        "timestamp": datetime.now().isoformat(),
        "all_role_permissions": {r: sorted(p) for r, p in role_perms.items()},
        "roles_to_revoke": targets,
    }
    backup_path = save_json(backup, f"revoke_{permission}", instance_label)

    if not targets:
        log.info("No roles to update. Done.")
        return

    # 4. Apply
    mode = "DRY-RUN" if dry_run else "EXECUTE"
    log.info("Revoking '%s' from %d roles (%s, concurrency=%d) ...",
             permission, len(targets), mode, XLD_CONCURRENCY)
    ok, failed = await apply_permission_change(
        instance_url, targets, permission, "DELETE", dry_run,
    )
    log.info("Done: %d revoked, %d failed", ok, failed)

    if dry_run:
        log.info("Dry run. Review backup at %s, then re-run with --execute.", backup_path)


async def run_rollback(backup_path, instance_url):
    if not USERNAME or not PASSWORD:
        log.error("Set XLD_ADMIN_USERNAME and XLD_ADMIN_PASSWORD env vars")
        sys.exit(1)

    with open(backup_path, "r", encoding="utf-8") as f:
        backup = json.load(f)

    if backup.get("instance_url") != instance_url:
        log.error("Backup is for instance %s, not %s",
                  backup.get("instance_url"), instance_url)
        sys.exit(1)

    permission = backup["permission"]
    targets = backup["roles_to_revoke"]
    log.info("Rolling back: re-granting '%s' to %d roles on %s ...",
             permission, len(targets), instance_url)

    ok, failed = await apply_permission_change(
        instance_url, targets, permission, "PUT", dry_run=False,
    )
    log.info("Rollback done: %d granted, %d failed", ok, failed)


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Revoke a permission (default: 'login') from all roles except protected ones",
    )
    parser.add_argument("--instance", metavar="URL", required=True, help="XLD instance URL")
    parser.add_argument("--permission", default=DEFAULT_PERMISSION,
                        help=f"Permission to revoke (default: {DEFAULT_PERMISSION})")
    parser.add_argument("--keep", action="append", default=[],
                        help="Extra role to protect (in addition to defaults). Can be passed multiple times.")
    parser.add_argument("--execute", action="store_true", help="Apply changes (default is dry-run)")
    parser.add_argument("--rollback", metavar="BACKUP_FILE",
                        help="Re-grant the permission to the roles listed in the backup file")
    args = parser.parse_args()

    if args.rollback:
        asyncio.run(run_rollback(args.rollback, args.instance))
    else:
        protected = DEFAULT_PROTECTED | set(args.keep)
        asyncio.run(run(args.instance, args.permission, protected, dry_run=not args.execute))