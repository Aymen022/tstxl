#!/usr/bin/env python3
"""
backup_roles.py

Fetches all roles and their principals from one or more XLDeploy instances
and saves them to a timestamped JSON file.

Usage:
    python backup_roles.py                    # backup all known instances
    python backup_roles.py --instance URL     # backup one instance
"""

import argparse
import json
import logging
import os
import sys
import xml.etree.ElementTree as ET
from datetime import datetime

import requests

BACKUP_DIR = "role_backups"

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
log = logging.getLogger("backup_roles")


def fetch_role_principals(base_url):
    """GET role principals from XLD, returns dict: role_name -> [principals]."""
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


def main():
    parser = argparse.ArgumentParser(description="Backup XLD roles and principals to JSON")
    parser.add_argument("--instance", metavar="URL", help="XLD instance URL (default: all known)")
    args = parser.parse_args()

    if not USERNAME or not PASSWORD:
        log.error("Set XLD_ADMIN_USERNAME and XLD_ADMIN_PASSWORD env vars")
        sys.exit(1)

    instances = [args.instance] if args.instance else XLD_INSTANCES

    backup = []
    for url in instances:
        log.info("Fetching from %s ...", url)
        try:
            roles = fetch_role_principals(url)
            log.info("  %d roles, %d principals", len(roles), sum(len(v) for v in roles.values()))
            backup.append({
                "instance_url": url,
                "timestamp": datetime.now().isoformat(),
                "roles": roles,
            })
        except Exception as e:
            log.error("  Failed: %s", e)
            backup.append({"instance_url": url, "error": str(e)})

    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H_%M")
    path = os.path.join(BACKUP_DIR, f"roles_backup_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(backup, f, indent=4)

    log.info("Saved backup to %s (%d instances)", path, len(backup))


if __name__ == "__main__":
    main()
