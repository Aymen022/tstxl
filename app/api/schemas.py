from marshmallow import Schema, fields, validate, validates, ValidationError


class ConstraintSchema(Schema):
    technicalName = fields.String(required=True)
    values = fields.List(fields.String(), required=True)


class ProvisioningRequestSchema(Schema):
    igg = fields.String(required=True)
    sesameId = fields.String(required=True)
    mail = fields.Email(required=True)
    sgconnect = fields.String(required=True)
    namespaceId = fields.String(required=True)
    applicationId = fields.String(required=True)
    profileName = fields.String(
        required=True,
        validate=validate.Equal("XLD_LOGIN"),
    )
    action = fields.String(
        required=True,
        validate=validate.OneOf(["GRANT", "REVOKE"]),
    )
    taskId = fields.String(required=True)
    provisioningId = fields.String(required=True)
    constraints = fields.List(fields.Nested(ConstraintSchema), required=True)

    @validates("constraints")
    def validate_constraints(self, value):
        """Ensure at least one XLD_INSTANCE_NAME constraint with non-empty values."""
        instance_constraints = [
            c for c in value if c["technicalName"] == "XLD_INSTANCE_NAME"
        ]
        if not instance_constraints:
            raise ValidationError(
                "Missing constraint with technicalName 'XLD_INSTANCE_NAME'"
            )
        if not instance_constraints[0]["values"]:
            raise ValidationError(
                "XLD_INSTANCE_NAME constraint must have at least one value"
            )


# --- Response schemas for OpenAPI documentation ---

class AckResponseSchema(Schema):
    """202 ACK / 200 already-processed response."""
    provisioningId = fields.String(required=True)
    taskId = fields.String(required=True)
    status = fields.String(required=True)
    timestamp = fields.String(required=True)


class ErrorResponseSchema(Schema):
    """Error response (400, 401, 403)."""
    error = fields.String(required=True)
    details = fields.Raw(allow_none=True)


class HealthResponseSchema(Schema):
    """Health check response."""
    status = fields.String(required=True)
