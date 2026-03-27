from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from app.utils.logging import get_logger, log_with_context

logger = get_logger(__name__)


class XLDeployClient:
    """REST client for Digital.ai Deploy (XLDeploy) role principal management."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 30,
        verify_ssl: bool = True,
    ) -> None:
        self.base_url: str = base_url.rstrip("/")
        self.auth: HTTPBasicAuth = HTTPBasicAuth(username, password)
        self.timeout: int = timeout
        self.verify: bool = verify_ssl
        self.session: requests.Session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({"Accept": "application/json"})

    def _url(self, path: str) -> str:
        return f"{self.base_url}/deployit/{path}"

    def get_role_principals(self, role_name: str) -> list[str]:
        """Get the list of principals assigned to a role."""
        url = self._url(f"security/role/{role_name}/principals")
        resp = self.session.get(url, timeout=self.timeout, verify=self.verify)
        resp.raise_for_status()
        return resp.json()

    def add_principal(self, role_name: str, principal: str) -> bool:
        """Add a principal (email) to a role. Returns True if successful."""
        url = self._url(f"security/role/{role_name}/principals/{principal}")
        resp = self.session.put(url, timeout=self.timeout, verify=self.verify)
        resp.raise_for_status()
        log_with_context(
            logger, "INFO", "Principal added to role",
            role=role_name, principal=principal, status_code=resp.status_code,
        )
        return True

    def remove_principal(self, role_name: str, principal: str) -> bool:
        """Remove a principal (email) from a role. Returns True if successful."""
        url = self._url(f"security/role/{role_name}/principals/{principal}")
        resp = self.session.delete(url, timeout=self.timeout, verify=self.verify)
        resp.raise_for_status()
        log_with_context(
            logger, "INFO", "Principal removed from role",
            role=role_name, principal=principal, status_code=resp.status_code,
        )
        return True

    def health_check(self) -> dict[str, Any]:
        """Check if the XLD instance is reachable."""
        url = self._url("server/state")
        resp = self.session.get(url, timeout=self.timeout, verify=self.verify)
        resp.raise_for_status()
        return resp.json()
