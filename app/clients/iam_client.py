import time
from typing import Any

import requests

from app.utils.logging import get_logger, log_with_context

logger = get_logger(__name__)


def send_callback(
    callback_url: str,
    payload: dict[str, Any],
    timeout: int = 30,
    retries: int = 3,
) -> bool:
    """Send provisioning result callback to IAM with retry logic.

    Args:
        callback_url: IAM callback endpoint URL.
        payload: Dict with provisioningId, taskId, status, details.
        timeout: Request timeout in seconds.
        retries: Number of retry attempts.

    Returns:
        True if callback was sent successfully, False otherwise.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(
                callback_url,
                json=payload,
                timeout=timeout,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            log_with_context(
                logger, "INFO", "IAM callback sent successfully",
                provisioningId=payload.get("provisioningId"),
                status=payload.get("status"),
                attempt=attempt,
            )
            return True
        except requests.RequestException as e:
            log_with_context(
                logger, "WARNING", "IAM callback failed",
                provisioningId=payload.get("provisioningId"),
                attempt=attempt,
                max_retries=retries,
                error=str(e),
            )
            if attempt < retries:
                # Exponential backoff: 2s, 4s, 8s...
                time.sleep(2 ** attempt)

    log_with_context(
        logger, "ERROR", "IAM callback failed after all retries",
        provisioningId=payload.get("provisioningId"),
        max_retries=retries,
    )
    return False
