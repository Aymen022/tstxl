from datetime import datetime, timezone
from typing import Any

from app.clients.xldeploy_client import XLDeployClient
from app.clients.iam_client import send_callback
from app.utils.logging import get_logger, log_with_context

logger = get_logger(__name__)


def process_provisioning(
    data: dict[str, Any],
    instance_names: list[str],
    config: dict[str, Any],
) -> None:
    """Background thread: process GRANT/REVOKE across XLD instances.

    Args:
        data: Validated IAM request payload.
        instance_names: List of target XLD instance names.
        config: Dict with xld_instances, xld_login_role, callback settings.
    """
    provisioning_id: str = data["provisioningId"]
    task_id: str = data["taskId"]
    action: str = data["action"]
    mail: str = data["mail"]
    role_name: str = config["xld_login_role"]

    instances_processed: list[str] = []
    instances_failed: list[str] = []
    errors: dict[str, str] = {}

    for instance_name in instance_names:
        instance_cfg: dict[str, Any] = config["xld_instances"][instance_name]
        try:
            client = XLDeployClient(
                base_url=instance_cfg["url"],
                username=instance_cfg["username"],
                password=instance_cfg["password"],
                timeout=config["xld_api_timeout"],
                verify_ssl=instance_cfg.get("verify_ssl", True),
            )

            if action == "GRANT":
                client.add_principal(role_name, mail)
            else:
                client.remove_principal(role_name, mail)

            instances_processed.append(instance_name)
            log_with_context(
                logger, "INFO", f"{action} succeeded on instance",
                provisioningId=provisioning_id,
                instance=instance_name,
                mail=mail,
            )

        except Exception as e:
            instances_failed.append(instance_name)
            errors[instance_name] = str(e)
            log_with_context(
                logger, "ERROR", f"{action} failed on instance",
                provisioningId=provisioning_id,
                instance=instance_name,
                mail=mail,
                error=str(e),
            )

    # Determine overall status
    if not instances_failed:
        status = "SUCCESS"
    elif not instances_processed:
        status = "FAILURE"
    else:
        status = "PARTIAL_SUCCESS"

    # Send callback to IAM
    callback_payload: dict[str, Any] = {
        "provisioningId": provisioning_id,
        "taskId": task_id,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "details": {
            "action": action,
            "mail": mail,
            "applicationId": data["applicationId"],
            "instancesProcessed": instances_processed,
            "instancesFailed": instances_failed,
            "errors": errors if errors else None,
        },
    }

    send_callback(
        callback_url=config["iam_callback_url"],
        payload=callback_payload,
        timeout=config["iam_callback_timeout"],
        retries=config["iam_callback_retries"],
    )

    log_with_context(
        logger, "INFO", "Provisioning request completed",
        provisioningId=provisioning_id,
        status=status,
        instancesProcessed=instances_processed,
        instancesFailed=instances_failed,
    )
