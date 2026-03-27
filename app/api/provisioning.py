import threading
from datetime import datetime, timezone
from typing import Any

from flask import current_app
from flask_smorest import Blueprint, abort

from app import processed_ids, processed_ids_lock
from app.api.schemas import (
    ProvisioningRequestSchema,
    AckResponseSchema,
    ErrorResponseSchema,
)
from app.utils.decorators import require_api_key
from app.utils.logging import get_logger, log_with_context

provisioning_blp = Blueprint(
    "provisioning", __name__,
    description="IAM provisioning operations (GRANT/REVOKE login access)",
)
logger = get_logger(__name__)


@provisioning_blp.route("/provision", methods=["POST"])
@provisioning_blp.doc(security=[{"ApiKeyAuth": []}])
@require_api_key
@provisioning_blp.arguments(ProvisioningRequestSchema)
@provisioning_blp.response(202, AckResponseSchema, description="Request accepted for async processing")
@provisioning_blp.alt_response(200, schema=AckResponseSchema, description="Duplicate request, already processed")
@provisioning_blp.alt_response(400, schema=ErrorResponseSchema, description="Validation or config error")
@provisioning_blp.alt_response(403, schema=ErrorResponseSchema, description="Application not authorized on instance")
def provision(data: dict[str, Any]) -> dict[str, str] | tuple[dict[str, str], int]:
    """Submit a provisioning request (GRANT or REVOKE).

    Receives a provisioning request from IAM, validates it, and acknowledges
    immediately with HTTP 202. Processing happens asynchronously in a background
    thread. A callback is sent to IAM when processing completes.
    """
    provisioning_id: str = data["provisioningId"]
    task_id: str = data["taskId"]

    # Idempotency check
    with processed_ids_lock:
        if provisioning_id in processed_ids:
            log_with_context(
                logger, "WARNING", "Duplicate provisioningId, skipping",
                provisioningId=provisioning_id,
            )
            return {
                "provisioningId": provisioning_id,
                "taskId": task_id,
                "status": "ALREADY_PROCESSED",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }, 200
        processed_ids.add(provisioning_id)

    # Extract target instances from constraints
    instance_names: list[str] = []
    for constraint in data["constraints"]:
        if constraint["technicalName"] == "XLD_INSTANCE_NAME":
            instance_names = constraint["values"]
            break

    # Validate instances exist in config
    xld_instances: dict[str, Any] = current_app.config["XLD_INSTANCES"]
    unknown: list[str] = [name for name in instance_names if name not in xld_instances]
    if unknown:
        with processed_ids_lock:
            processed_ids.discard(provisioning_id)
        abort(400, message=f"Unknown XLD instances: {unknown}")

    # Validate application is allowed on these instances
    app_instances: dict[str, list[str]] = current_app.config["APPLICATION_INSTANCES"]
    application_id: str = data["applicationId"]
    allowed: list[str] = app_instances.get(application_id, [])
    unauthorized: list[str] = [name for name in instance_names if name not in allowed]
    if unauthorized:
        with processed_ids_lock:
            processed_ids.discard(provisioning_id)
        abort(403, message=f"Application {application_id} not authorized on: {unauthorized}")

    log_with_context(
        logger, "INFO", "Provisioning request accepted",
        provisioningId=provisioning_id,
        taskId=task_id,
        action=data["action"],
        mail=data["mail"],
        instances=instance_names,
    )

    # Spawn background thread
    thread_config: dict[str, Any] = {
        "xld_instances": {name: xld_instances[name] for name in instance_names},
        "xld_login_role": current_app.config["XLD_LOGIN_ROLE"],
        "xld_api_timeout": current_app.config["XLD_API_TIMEOUT"],
        "iam_callback_url": current_app.config["IAM_CALLBACK_URL"],
        "iam_callback_timeout": current_app.config["IAM_CALLBACK_TIMEOUT"],
        "iam_callback_retries": current_app.config["IAM_CALLBACK_RETRIES"],
    }

    from app.services.provisioning_service import process_provisioning

    thread = threading.Thread(
        target=process_provisioning,
        args=(data, instance_names, thread_config),
        daemon=True,
    )
    thread.start()

    # Return immediate ACK
    return {
        "provisioningId": provisioning_id,
        "taskId": task_id,
        "status": "ACKNOWLEDGED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
