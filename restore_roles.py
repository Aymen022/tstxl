#!/usr/bin/env python3
"""
restore_roles.py

Restores XLDeploy role principals from a backup file produced by backup_roles.py.
For each role: adds principals from the backup that are missing, then removes
principals currently present that aren't in the backup (full sync to backup state).

Usage:
    python restore_roles.py BACKUP_FILE                     # dry-run all instances
    python restore_roles.py BACKUP_FILE --execute           # apply to all instances
    python restore_roles.py BACKUP_FILE --instance URL      # one instance only
"""

import argparse
import json
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET

import requests

XLD_RETRIES = 3

USERNAME = os.getenv("XLD_ADMIN_USERNAME")
PASSWORD = os.getenv("XLD_ADMIN_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("restore_roles")


def fetch_role_principals(base_url):
    """GET current role principals from XLD, returns dict: role_name -> [principals]."""
    url = f"{base_url.rstrip('/')}/deployit/security/role/principals"
    resp = requests.get(url, auth=(USERNAME, PASSWORD))
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
    url = f"{base_url.rstrip('/')}/deployit/security/role/{role_name}/{principal}"
    for attempt in range(1, XLD_RETRIES + 1):
        try:
            resp = requests.request(method, url, auth=(USERNAME, PASSWORD))
            resp.raise_for_status()
            return True
        except Exception as e:
            log.warning("%s failed (attempt %d/%d) role=%s principal=%s: %s",
                        method, attempt, XLD_RETRIES, role_name, principal, e)
            if attempt < XLD_RETRIES:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"{method} failed: role={role_name} principal={principal}")


def restore_instance(base_url, backed_up_roles, dry_run):
    """Sync current state to match the backup: add missing, remove extra."""
    log.info("Fetching current state from %s ...", base_url)
    current_roles = fetch_role_principals(base_url)

    added = removed = errors = 0

    for role_name, backup_principals in backed_up_roles.items():
        backup_set = set(backup_principals)
        current_set = set(current_roles.get(role_name, []))

        to_add = backup_set - current_set       # In backup but missing now
        to_remove = current_set - backup_set    # Present now but not in backup

        # Phase 1: ADD missing principals
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

        # Phase 2: REMOVE extras (only if all adds succeeded)
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

    return {"added": added, "removed": removed, "errors": errors, "roles": len(backed_up_roles)}


def main():
    parser = argparse.ArgumentParser(description="Restore XLD role principals from a backup file")
    parser.add_argument("backup_file", help="Path to backup JSON file from backup_roles.py")
    parser.add_argument("--execute", action="store_true", help="Apply changes (default is dry-run)")
    parser.add_argument("--instance", metavar="URL", help="Restore only this instance (default: all in backup)")
    args = parser.parse_args()

    if not USERNAME or not PASSWORD:
        log.error("Set XLD_ADMIN_USERNAME and XLD_ADMIN_PASSWORD env vars")
        sys.exit(1)

    with open(args.backup_file, "r", encoding="utf-8") as f:
        backup = json.load(f)

    if args.instance:
        backup = [s for s in backup if s["instance_url"] == args.instance]
        if not backup:
            log.error("Instance %s not found in backup", args.instance)
            sys.exit(1)

    dry_run = not args.execute
    mode = "DRY-RUN" if dry_run else "EXECUTE"
    log.info("Restoring from %s (%s) ...", args.backup_file, mode)

    for snapshot in backup:
        url = snapshot["instance_url"]
        if "error" in snapshot or "roles" not in snapshot:
            log.warning("Skipping %s (backup had error or no roles)", url)
            continue

        try:
            result = restore_instance(url, snapshot["roles"], dry_run)
            log.info("%s: %d roles, %d added, %d removed, %d errors",
                     url, result["roles"], result["added"], result["removed"], result["errors"])
        except Exception as e:
            log.error("Failed to restore %s: %s", url, e)


if __name__ == "__main__":
    main()
