import threading
from datetime import datetime, timezone
from typing import Any

from flask import current_app
from connexion import problem

from app import processed_ids, processed_ids_lock
from app.utils.logging import get_logger, log_with_context

logger = get_logger(__name__)


def provision(
    body: dict[str, Any],
) -> tuple[dict, int]:
    """Submit a provisioning request (GRANT or REVOKE).

    Connexion validates the request body against the
    OpenAPI spec before calling this function.
    """
    provisioning_id: str = body["provisioningId"]
    task_id: str = body["taskId"]

    # Idempotency check
    with processed_ids_lock:
        if provisioning_id in processed_ids:
            log_with_context(
                logger, "WARNING",
                "Duplicate provisioningId",
                provisioningId=provisioning_id,
            )
            now = datetime.now(timezone.utc)
            return {
                "provisioningId": provisioning_id,
                "taskId": task_id,
                "status": "ALREADY_PROCESSED",
                "timestamp": now.isoformat(),
            }, 200
        processed_ids.add(provisioning_id)

    # Extract target instances from constraints
    instance_names: list[str] = []
    for constraint in body["constraints"]:
        tech = constraint["technicalName"]
        if tech == "XLD_INSTANCE_NAME":
            instance_names = constraint["values"]
            break

    if not instance_names:
        with processed_ids_lock:
            processed_ids.discard(provisioning_id)
        return problem(
            status=400,
            title="Bad Request",
            detail="Missing XLD_INSTANCE_NAME constraint",
        )

    # Validate instances exist in config
    xld_instances = current_app.config[
        "XLD_INSTANCES"
    ]
    unknown = [
        n for n in instance_names
        if n not in xld_instances
    ]
    if unknown:
        with processed_ids_lock:
            processed_ids.discard(provisioning_id)
        return problem(
            status=400,
            title="Bad Request",
            detail=(
                "Unknown XLD instances: "
                f"{unknown}"
            ),
        )

    # Validate app is allowed on these instances
    app_instances = current_app.config[
        "ALLOWED_INSTANCES_BY_APP"
    ]
    app_id: str = body["applicationId"]
    allowed = app_instances.get(app_id, [])
    unauthorized = [
        n for n in instance_names
        if n not in allowed
    ]
    if unauthorized:
        with processed_ids_lock:
            processed_ids.discard(provisioning_id)
        return problem(
            status=403,
            title="Forbidden",
            detail=(
                f"Application {app_id} "
                f"not authorized on: "
                f"{unauthorized}"
            ),
        )

    log_with_context(
        logger, "INFO",
        "Provisioning request accepted",
        provisioningId=provisioning_id,
        taskId=task_id,
        action=body["action"],
        mail=body["mail"],
        instances=instance_names,
    )

    # Spawn background thread
    cfg = current_app.config
    thread_config: dict[str, Any] = {
        "xld_instances": {
            n: xld_instances[n]
            for n in instance_names
        },
        "xld_login_role": cfg["XLD_LOGIN_ROLE"],
        "xld_api_timeout": cfg["XLD_API_TIMEOUT"],
        "iam_callback_url": cfg["IAM_CALLBACK_URL"],
        "iam_callback_timeout": cfg[
            "IAM_CALLBACK_TIMEOUT"
        ],
        "iam_callback_retries": cfg[
            "IAM_CALLBACK_RETRIES"
        ],
        "xld_api_retries": cfg["XLD_API_RETRIES"],
    }

    from app.services.provisioning_service import (
        process_provisioning,
    )

    thread = threading.Thread(
        target=process_provisioning,
        args=(body, instance_names, thread_config),
        daemon=True,
    )
    thread.start()

    # Return immediate ACK
    now = datetime.now(timezone.utc)
    return {
        "provisioningId": provisioning_id,
        "taskId": task_id,
        "status": "ACKNOWLEDGED",
        "timestamp": now.isoformat(),
    }, 202