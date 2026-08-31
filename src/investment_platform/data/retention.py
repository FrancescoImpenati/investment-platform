"""Exact provider-by-dataset retention policy and fail-closed enforcement."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from importlib.resources import files
from pathlib import Path
from typing import Annotated, Final, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from investment_platform.runtime import RuntimeEnvironment

_DEFAULT_CATALOG_RESOURCE: Final = "retention_catalog.v1.json"
_SHA256_HEX = r"^[0-9a-f]{64}$"
_PAGE_RELATION = r"^(?:root|after:[0-9]+)$"
_MEDIA_TYPE = r"^[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*$"
_CONTENT_ENCODING = r"^[a-z0-9][a-z0-9._+-]{0,63}$"
NonEmptyStr = Annotated[str, Field(min_length=1)]


def _canonical_page_relation(page_ordinal: int) -> str:
    if page_ordinal < 0:
        raise ValueError("page ordinal must be non-negative")
    return "root" if page_ordinal == 0 else f"after:{page_ordinal - 1}"


class RetentionPolicyError(RuntimeError):
    """Base error for invalid catalogs or denied retention behavior."""


class DatasetPolicyDenied(RetentionPolicyError):
    """Raised before an unclassified or unauthorized dataset can be used."""


class RetentionMode(StrEnum):
    PROHIBITED = "PROHIBITED"
    EPHEMERAL = "EPHEMERAL"
    TTL = "TTL"
    SUBSCRIPTION_BOUND = "SUBSCRIPTION_BOUND"
    DURABLE_AUTHORIZED = "DURABLE_AUTHORIZED"
    SYNTHETIC_UNRESTRICTED = "SYNTHETIC_UNRESTRICTED"


class DatasetPolicyStatus(StrEnum):
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"
    EXPIRED = "EXPIRED"
    PENDING = "PENDING"


class RetentionLayer(StrEnum):
    RAW = "raw"
    NORMALIZED = "normalized"
    DERIVED = "derived"


class RetentionOperation(StrEnum):
    REQUEST = "request"
    PROCESS = "process"
    PERSIST = "persist"
    QUERY = "query"
    WATERMARK = "watermark"
    QUARANTINE = "quarantine"
    EXPORT = "export"
    PURGE = "purge"


class _FrozenPolicyModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        validate_default=True,
    )


class LayerRetentionPolicy(_FrozenPolicyModel):
    """Retention behavior for one materialization layer."""

    mode: RetentionMode
    ttl_seconds: Annotated[int, Field(gt=0)] | None = None
    quarantine_allowed: bool = False

    @model_validator(mode="after")
    def validate_ttl(self) -> Self:
        if self.mode is RetentionMode.TTL and self.ttl_seconds is None:
            raise ValueError("TTL layer retention requires ttl_seconds")
        if self.mode is not RetentionMode.TTL and self.ttl_seconds is not None:
            raise ValueError("ttl_seconds is valid only for TTL layer retention")
        if self.mode is RetentionMode.PROHIBITED and self.quarantine_allowed:
            raise ValueError("a prohibited layer cannot be quarantined")
        return self


class DatasetRetentionPolicy(_FrozenPolicyModel):
    """One exact, versioned provider/dataset policy entry."""

    policy_id: NonEmptyStr
    revision: Annotated[int, Field(gt=0)]
    provider: NonEmptyStr
    dataset: NonEmptyStr
    mode: RetentionMode
    status: DatasetPolicyStatus
    permitted_environments: Annotated[tuple[RuntimeEnvironment, ...], Field(min_length=1)]
    use_scope: NonEmptyStr
    processing_allowed: bool
    raw: LayerRetentionPolicy
    normalized: LayerRetentionPolicy
    derived: LayerRetentionPolicy
    minimum_observation_age_seconds: Annotated[int, Field(ge=0)] = 0
    finalization_buffer_seconds: Annotated[int, Field(ge=0)] = 0
    delete_on_termination: bool = False
    public_display_allowed: bool = False
    redistribution_allowed: bool = False
    external_export_allowed: bool = False
    evidence_reference: NonEmptyStr
    verified_on: date
    review_on: date | None = None
    notes: NonEmptyStr

    @field_validator("provider", "dataset", mode="after")
    @classmethod
    def require_canonical_key(cls, value: str) -> str:
        normalized = value.casefold()
        if value != normalized or any(character.isspace() for character in value):
            raise ValueError(
                "provider and dataset keys must be lowercase and contain no whitespace"
            )
        return value

    @model_validator(mode="after")
    def validate_mode_consistency(self) -> Self:
        layers = (self.raw.mode, self.normalized.mode, self.derived.mode)
        if self.mode is RetentionMode.PROHIBITED:
            if self.processing_allowed or any(
                mode is not RetentionMode.PROHIBITED for mode in layers
            ):
                raise ValueError("PROHIBITED policy must deny processing and every layer")
        elif not self.processing_allowed:
            raise ValueError("non-PROHIBITED policy must explicitly allow processing")
        if self.mode is RetentionMode.EPHEMERAL and any(
            mode not in {RetentionMode.EPHEMERAL, RetentionMode.PROHIBITED} for mode in layers
        ):
            raise ValueError("EPHEMERAL policy cannot grant durable layer retention")
        compatible_layer_modes = {
            RetentionMode.PROHIBITED,
            self.mode,
        }
        if any(mode not in compatible_layer_modes for mode in layers):
            raise ValueError("layer retention cannot expand or change the dataset retention mode")
        if self.mode is RetentionMode.TTL and RetentionMode.TTL not in layers:
            raise ValueError("TTL dataset policy requires at least one TTL-retained layer")
        if self.redistribution_allowed and not self.external_export_allowed:
            raise ValueError("redistribution requires explicit external export permission")
        return self

    def layer(self, layer: RetentionLayer) -> LayerRetentionPolicy:
        return {
            RetentionLayer.RAW: self.raw,
            RetentionLayer.NORMALIZED: self.normalized,
            RetentionLayer.DERIVED: self.derived,
        }[layer]

    @property
    def content_hash(self) -> str:
        return _hash_json(self.model_dump(mode="json"))


class PendingDatasetReview(_FrozenPolicyModel):
    """Documentary catalog note that deliberately is not an active software policy."""

    provider: NonEmptyStr
    dataset: NonEmptyStr
    status: str
    notes: NonEmptyStr


class RetentionCatalogDocument(_FrozenPolicyModel):
    schema_version: Annotated[int, Field(gt=0)]
    catalog_id: NonEmptyStr
    revision: Annotated[int, Field(gt=0)]
    policies: tuple[DatasetRetentionPolicy, ...]
    pending_reviews: tuple[PendingDatasetReview, ...] = ()

    @model_validator(mode="after")
    def require_unique_exact_keys(self) -> Self:
        keys = [(policy.provider, policy.dataset) for policy in self.policies]
        if len(keys) != len(set(keys)):
            raise ValueError("retention catalog contains duplicate provider/dataset keys")
        pending_keys = [(review.provider, review.dataset) for review in self.pending_reviews]
        if len(pending_keys) != len(set(pending_keys)):
            raise ValueError("retention catalog contains duplicate pending-review keys")
        if set(keys) & set(pending_keys):
            raise ValueError("an exact dataset cannot be both a policy and a pending review")
        return self

    @property
    def content_hash(self) -> str:
        return _hash_json(self.model_dump(mode="json"))


class DatasetPolicySnapshot(_FrozenPolicyModel):
    """Immutable non-secret authorization provenance attached to a run."""

    catalog_id: NonEmptyStr
    catalog_revision: Annotated[int, Field(gt=0)]
    catalog_hash: NonEmptyStr
    policy_id: NonEmptyStr
    policy_revision: Annotated[int, Field(gt=0)]
    policy_hash: NonEmptyStr
    provider: NonEmptyStr
    dataset: NonEmptyStr
    mode: RetentionMode
    status: DatasetPolicyStatus
    verified_on: date
    captured_at: datetime

    @field_validator("captured_at", mode="after")
    @classmethod
    def normalize_captured_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("captured_at must be timezone-aware")
        return value.astimezone(UTC)


class DatasetRuntimeStatus(_FrozenPolicyModel):
    """A restrictive runtime overlay; it can never expand the committed catalog."""

    provider: NonEmptyStr
    dataset: NonEmptyStr
    policy_id: NonEmptyStr
    policy_revision: Annotated[int, Field(gt=0)]
    policy_hash: NonEmptyStr
    enabled: bool = True
    entitlement_active: bool | None = None
    retention_started_at: datetime | None = None
    expires_at: datetime | None = None

    @field_validator("retention_started_at", "expires_at", mode="after")
    @classmethod
    def normalize_expiry(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("expires_at must be timezone-aware")
        return value.astimezone(UTC)

    @classmethod
    def for_policy(
        cls,
        policy: DatasetRetentionPolicy,
        *,
        enabled: bool = True,
        entitlement_active: bool | None = None,
        retention_started_at: datetime | None = None,
        expires_at: datetime | None = None,
    ) -> DatasetRuntimeStatus:
        return cls(
            provider=policy.provider,
            dataset=policy.dataset,
            policy_id=policy.policy_id,
            policy_revision=policy.revision,
            policy_hash=policy.content_hash,
            enabled=enabled,
            entitlement_active=entitlement_active,
            retention_started_at=retention_started_at,
            expires_at=expires_at,
        )


class PlanningPolicyAuthorization(_FrozenPolicyModel):
    """One frozen policy and age frontier shared by every request in a plan."""

    policy_snapshot: DatasetPolicySnapshot
    environment: RuntimeEnvironment
    eligible_before: datetime
    authorized_at: datetime

    @field_validator("eligible_before", "authorized_at", mode="after")
    @classmethod
    def normalize_times(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("planning authorization timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_snapshot_time(self) -> Self:
        if self.policy_snapshot.captured_at != self.authorized_at:
            raise ValueError("policy snapshot capture must equal planning authorization time")
        return self


class RequestPolicyAuthorization(_FrozenPolicyModel):
    """Immutable authorization for one exact bounded request."""

    policy_snapshot: DatasetPolicySnapshot
    request_spec_hash: Annotated[str, Field(pattern=_SHA256_HEX)]
    environment: RuntimeEnvironment
    request_start: datetime
    request_end: datetime
    eligible_before: datetime
    authorized_at: datetime

    @field_validator(
        "request_start",
        "request_end",
        "eligible_before",
        "authorized_at",
        mode="after",
    )
    @classmethod
    def normalize_times(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("request authorization timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.request_end <= self.request_start:
            raise ValueError("authorized request end must be later than start")
        if self.request_end >= self.eligible_before:
            raise ValueError("authorized request must remain strictly before the eligibility bound")
        return self

    @property
    def policy_id(self) -> str:
        return self.policy_snapshot.policy_id


class ResponsePageAuthorization(_FrozenPolicyModel):
    """Authorization bound to one inspected transient provider response page."""

    request: RequestPolicyAuthorization
    page_ordinal: Annotated[int, Field(ge=0)]
    page_relation: Annotated[str, Field(pattern=_PAGE_RELATION)]
    payload_sha256: Annotated[str, Field(pattern=_SHA256_HEX)]
    payload_size_bytes: Annotated[int, Field(ge=0)]
    canonical_media_type: Annotated[str, Field(pattern=_MEDIA_TYPE)]
    content_encoding: Annotated[str, Field(pattern=_CONTENT_ENCODING)]
    observed_start: datetime | None = None
    observed_end: datetime | None = None
    authorized_at: datetime

    @field_validator("observed_start", "observed_end", "authorized_at", mode="after")
    @classmethod
    def normalize_optional_times(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("response authorization timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("canonical_media_type", "content_encoding", mode="before")
    @classmethod
    def canonicalize_content_metadata(cls, value: object) -> object:
        return value.strip().lower() if isinstance(value, str) else value

    @model_validator(mode="after")
    def validate_observed_bounds(self) -> Self:
        if self.page_relation != _canonical_page_relation(self.page_ordinal):
            raise ValueError("response page relation does not match its deterministic ordinal")
        if (self.observed_start is None) != (self.observed_end is None):
            raise ValueError("observed response bounds must both be present or both be absent")
        if self.observed_start is not None and self.observed_end is not None:
            if self.observed_end <= self.observed_start:
                raise ValueError("observed response end must be later than start")
            if (
                self.observed_start < self.request.request_start
                or self.observed_end > self.request.request_end
                or self.observed_end >= self.request.eligible_before
            ):
                raise ValueError("observed response lies outside authorized bounds")
        return self

    @property
    def artifact_descriptor(self) -> AuthorizedRawArtifactDescriptor:
        return AuthorizedRawArtifactDescriptor(
            request_spec_hash=self.request.request_spec_hash,
            page_ordinal=self.page_ordinal,
            page_relation=self.page_relation,
            content_sha256=self.payload_sha256,
            byte_count=self.payload_size_bytes,
            media_type=self.canonical_media_type,
            content_encoding=self.content_encoding,
        )


class AuthorizedRawArtifactDescriptor(_FrozenPolicyModel):
    """Exact immutable raw-page identity fields authorized for downstream use."""

    request_spec_hash: Annotated[str, Field(pattern=_SHA256_HEX)]
    page_ordinal: Annotated[int, Field(ge=0)]
    page_relation: Annotated[str, Field(pattern=_PAGE_RELATION)]
    content_sha256: Annotated[str, Field(pattern=_SHA256_HEX)]
    byte_count: Annotated[int, Field(ge=0)]
    media_type: Annotated[str, Field(pattern=_MEDIA_TYPE)]
    content_encoding: Annotated[str, Field(pattern=_CONTENT_ENCODING)]

    @field_validator("media_type", "content_encoding", mode="before")
    @classmethod
    def canonicalize_content_metadata(cls, value: object) -> object:
        return value.strip().lower() if isinstance(value, str) else value

    @model_validator(mode="after")
    def validate_page_chain(self) -> Self:
        if self.page_relation != _canonical_page_relation(self.page_ordinal):
            raise ValueError("raw artifact page relation does not match its ordinal")
        return self


class AcquisitionPolicyAuthorization(_FrozenPolicyModel):
    """Proof that every authorized page reached verified pagination termination."""

    request: RequestPolicyAuthorization
    ordered_artifacts: Annotated[tuple[AuthorizedRawArtifactDescriptor, ...], Field(min_length=1)]
    pagination_complete: bool
    terminal_page_verified: bool
    authorized_at: datetime

    @field_validator("authorized_at", mode="after")
    @classmethod
    def normalize_authorized_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("acquisition authorization time must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def require_complete_pagination(self) -> Self:
        if not self.pagination_complete or not self.terminal_page_verified:
            raise ValueError("acquisition authorization requires verified pagination completion")
        if tuple(value.page_ordinal for value in self.ordered_artifacts) != tuple(
            range(len(self.ordered_artifacts))
        ):
            raise ValueError("acquisition artifacts must be a complete 0-based page sequence")
        if any(
            value.request_spec_hash != self.request.request_spec_hash
            for value in self.ordered_artifacts
        ):
            raise ValueError("acquisition artifacts belong to a different request specification")
        return self

    @property
    def ordered_page_sha256(self) -> tuple[str, ...]:
        """Compatibility view; full descriptors remain the authorization boundary."""

        return tuple(value.content_sha256 for value in self.ordered_artifacts)


def _hash_json(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


class RetentionPolicyCatalog:
    """Validated exact-key lookup; unknown datasets are always denied."""

    def __init__(self, document: RetentionCatalogDocument) -> None:
        self._document = document
        self._policies = {(policy.provider, policy.dataset): policy for policy in document.policies}

    @classmethod
    def load(cls, path: Path) -> RetentionPolicyCatalog:
        try:
            value = json.loads(Path(path).read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RetentionPolicyError("retention catalog is missing or invalid JSON") from error
        try:
            document = RetentionCatalogDocument.model_validate(value)
        except ValueError as error:
            raise RetentionPolicyError("retention catalog failed schema validation") from error
        return cls(document)

    @classmethod
    def load_default(cls) -> RetentionPolicyCatalog:
        resource = files("investment_platform.data").joinpath(_DEFAULT_CATALOG_RESOURCE)
        try:
            value = json.loads(resource.read_bytes())
            document = RetentionCatalogDocument.model_validate(value)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise RetentionPolicyError("bundled retention catalog is invalid") from error
        return cls(document)

    @property
    def document(self) -> RetentionCatalogDocument:
        return self._document

    @property
    def content_hash(self) -> str:
        return self._document.content_hash

    def lookup(self, provider: str, dataset: str) -> DatasetRetentionPolicy:
        key = (provider.strip().casefold(), dataset.strip().casefold())
        policy = self._policies.get(key)
        if policy is None:
            raise DatasetPolicyDenied(
                f"no active retention policy exists for exact dataset {key[0]}/{key[1]}"
            )
        return policy

    def snapshot(
        self,
        provider: str,
        dataset: str,
        *,
        captured_at: datetime,
    ) -> DatasetPolicySnapshot:
        policy = self.lookup(provider, dataset)
        return DatasetPolicySnapshot(
            catalog_id=self._document.catalog_id,
            catalog_revision=self._document.revision,
            catalog_hash=self.content_hash,
            policy_id=policy.policy_id,
            policy_revision=policy.revision,
            policy_hash=policy.content_hash,
            provider=policy.provider,
            dataset=policy.dataset,
            mode=policy.mode,
            status=policy.status,
            verified_on=policy.verified_on,
            captured_at=captured_at,
        )


class RetentionPolicyEnforcer:
    """Central policy gate used before every sensitive ingestion boundary."""

    def __init__(
        self,
        catalog: RetentionPolicyCatalog,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._catalog = catalog
        self._clock = clock or (lambda: datetime.now(UTC))

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise RetentionPolicyError("retention clock must return an aware datetime")
        return value.astimezone(UTC)

    def _active_policy(
        self,
        provider: str,
        dataset: str,
        *,
        environment: RuntimeEnvironment,
        runtime_status: DatasetRuntimeStatus | None = None,
    ) -> DatasetRetentionPolicy:
        policy = self._catalog.lookup(provider, dataset)
        if policy.status is not DatasetPolicyStatus.ACTIVE:
            raise DatasetPolicyDenied(
                f"dataset policy {policy.policy_id!r} is {policy.status.value.lower()}"
            )
        if policy.mode is RetentionMode.PROHIBITED or not policy.processing_allowed:
            raise DatasetPolicyDenied(f"dataset {provider}/{dataset} is prohibited")
        if environment not in policy.permitted_environments:
            raise DatasetPolicyDenied(
                f"dataset {provider}/{dataset} is not permitted in {environment.value}"
            )
        self._validate_runtime_status(policy, runtime_status)
        return policy

    def _validate_runtime_status(
        self,
        policy: DatasetRetentionPolicy,
        status: DatasetRuntimeStatus | None,
    ) -> None:
        if status is None:
            if policy.mode is RetentionMode.TTL:
                raise DatasetPolicyDenied("TTL dataset requires an exact unexpired runtime status")
            if policy.mode is RetentionMode.SUBSCRIPTION_BOUND:
                raise DatasetPolicyDenied("exact dataset subscription/entitlement is not active")
            return
        expected_identity = (
            policy.provider,
            policy.dataset,
            policy.policy_id,
            policy.revision,
            policy.content_hash,
        )
        actual_identity = (
            status.provider,
            status.dataset,
            status.policy_id,
            status.policy_revision,
            status.policy_hash,
        )
        if actual_identity != expected_identity:
            raise DatasetPolicyDenied("runtime status does not match the exact dataset policy")
        if not status.enabled:
            raise DatasetPolicyDenied("runtime policy status is disabled")
        now = self._now()
        if status.expires_at is not None and now >= status.expires_at:
            raise DatasetPolicyDenied("runtime policy status has expired")
        if policy.mode is RetentionMode.TTL:
            if status.retention_started_at is None or status.expires_at is None:
                raise DatasetPolicyDenied(
                    "TTL dataset requires exact retention start and expiry timestamps"
                )
            if status.retention_started_at > now:
                raise DatasetPolicyDenied("TTL retention start cannot be in the future")
            ttl_values = [
                rule.ttl_seconds
                for rule in (policy.raw, policy.normalized, policy.derived)
                if rule.mode is RetentionMode.TTL and rule.ttl_seconds is not None
            ]
            maximum_expiry = status.retention_started_at + timedelta(seconds=min(ttl_values))
            if status.expires_at > maximum_expiry:
                raise DatasetPolicyDenied("runtime TTL status cannot expand the committed TTL")
        if (
            policy.mode is RetentionMode.SUBSCRIPTION_BOUND
            and status.entitlement_active is not True
        ):
            raise DatasetPolicyDenied("exact dataset subscription/entitlement is not active")

    def _assert_request_authorization(
        self,
        authorization: RequestPolicyAuthorization,
        policy: DatasetRetentionPolicy,
        environment: RuntimeEnvironment,
    ) -> None:
        snapshot = authorization.policy_snapshot
        if (
            authorization.environment is not environment
            or snapshot.catalog_id != self._catalog.document.catalog_id
            or snapshot.catalog_revision != self._catalog.document.revision
            or snapshot.catalog_hash != self._catalog.content_hash
            or snapshot.provider != policy.provider
            or snapshot.dataset != policy.dataset
            or snapshot.policy_id != policy.policy_id
            or snapshot.policy_revision != policy.revision
            or snapshot.policy_hash != policy.content_hash
            or snapshot.status is not DatasetPolicyStatus.ACTIVE
        ):
            raise DatasetPolicyDenied(
                "request authorization does not match the current exact dataset policy"
            )

    def _assert_page_authorization(
        self,
        authorization: ResponsePageAuthorization,
        policy: DatasetRetentionPolicy,
        environment: RuntimeEnvironment,
    ) -> None:
        self._assert_request_authorization(authorization.request, policy, environment)

    def _assert_acquisition_authorization(
        self,
        authorization: AcquisitionPolicyAuthorization,
        policy: DatasetRetentionPolicy,
        environment: RuntimeEnvironment,
    ) -> None:
        self._assert_request_authorization(authorization.request, policy, environment)
        if not authorization.pagination_complete or not authorization.terminal_page_verified:
            raise DatasetPolicyDenied("provider pagination is not completely verified")

    def _assert_materialization_authorization(
        self,
        *,
        policy: DatasetRetentionPolicy,
        environment: RuntimeEnvironment,
        layer: RetentionLayer,
        response_authorization: ResponsePageAuthorization | None,
        acquisition_authorization: AcquisitionPolicyAuthorization | None,
        payload_sha256: str | None,
        payload_size_bytes: int | None,
        canonical_media_type: str | None,
        content_encoding: str | None,
        request_spec_hash: str | None,
        page_ordinal: int | None,
        page_relation: str | None,
        input_artifacts: tuple[AuthorizedRawArtifactDescriptor, ...] | None,
        input_page_sha256: tuple[str, ...] | None,
    ) -> None:
        if policy.mode is RetentionMode.SYNTHETIC_UNRESTRICTED:
            return
        if layer is RetentionLayer.RAW:
            if response_authorization is None:
                raise DatasetPolicyDenied(
                    "raw materialization requires an inspected response authorization"
                )
            self._assert_page_authorization(response_authorization, policy, environment)
            candidate_identity = (
                payload_sha256.casefold() if payload_sha256 is not None else None,
                payload_size_bytes,
                canonical_media_type,
                content_encoding,
                request_spec_hash,
                page_ordinal,
                page_relation,
            )
            authorized_identity = (
                response_authorization.payload_sha256,
                response_authorization.payload_size_bytes,
                response_authorization.canonical_media_type,
                response_authorization.content_encoding,
                response_authorization.request.request_spec_hash,
                response_authorization.page_ordinal,
                response_authorization.page_relation,
            )
            if candidate_identity != authorized_identity:
                raise DatasetPolicyDenied(
                    "raw materialization does not match the inspected response bytes"
                )
            return
        if acquisition_authorization is None:
            raise DatasetPolicyDenied(
                f"{layer.value} materialization requires complete acquisition authorization"
            )
        self._assert_acquisition_authorization(
            acquisition_authorization,
            policy,
            environment,
        )
        if input_artifacts != acquisition_authorization.ordered_artifacts:
            raise DatasetPolicyDenied(
                "materialization inputs do not match the authorized raw artifact sequence"
            )
        if input_page_sha256 is not None and (
            input_page_sha256 != acquisition_authorization.ordered_page_sha256
        ):
            raise DatasetPolicyDenied("materialization page hash view is inconsistent")

    def authorize_request(
        self,
        provider: str,
        dataset: str,
        *,
        environment: RuntimeEnvironment,
        start: datetime,
        end: datetime,
        request_spec_hash: str,
        runtime_status: DatasetRuntimeStatus | None = None,
        planning_authorization: PlanningPolicyAuthorization | None = None,
    ) -> RequestPolicyAuthorization:
        policy = self._active_policy(
            provider,
            dataset,
            environment=environment,
            runtime_status=runtime_status,
        )
        if start.tzinfo is None or start.utcoffset() is None:
            raise ValueError("request start must be timezone-aware")
        if end.tzinfo is None or end.utcoffset() is None:
            raise ValueError("request end must be timezone-aware")
        start = start.astimezone(UTC)
        end = end.astimezone(UTC)
        if end <= start:
            raise ValueError("request end must be later than start")
        if planning_authorization is None:
            authorized_at = self._now()
            safe_end = self.request_safe_end(policy, evaluation_time=authorized_at)
            policy_snapshot = self._catalog.snapshot(
                provider,
                dataset,
                captured_at=authorized_at,
            )
        else:
            self._assert_planning_authorization(
                planning_authorization,
                policy,
                environment,
            )
            authorized_at = planning_authorization.authorized_at
            safe_end = planning_authorization.eligible_before
            policy_snapshot = planning_authorization.policy_snapshot
        # Alpaca's grant is strictly older than the age boundary; equality is denied.
        if end >= safe_end:
            raise DatasetPolicyDenied(
                "request includes observations that have not passed the dataset "
                "age/finalization gate"
            )
        return RequestPolicyAuthorization(
            policy_snapshot=policy_snapshot,
            request_spec_hash=request_spec_hash.casefold(),
            environment=environment,
            request_start=start,
            request_end=end,
            eligible_before=safe_end,
            authorized_at=authorized_at,
        )

    def authorize_planning(
        self,
        provider: str,
        dataset: str,
        *,
        environment: RuntimeEnvironment,
        runtime_status: DatasetRuntimeStatus | None = None,
    ) -> PlanningPolicyAuthorization:
        """Freeze one exact policy snapshot and strict safe end for a whole plan."""

        policy = self._active_policy(
            provider,
            dataset,
            environment=environment,
            runtime_status=runtime_status,
        )
        authorized_at = self._now()
        return PlanningPolicyAuthorization(
            policy_snapshot=self._catalog.snapshot(
                provider,
                dataset,
                captured_at=authorized_at,
            ),
            environment=environment,
            eligible_before=self.request_safe_end(
                policy,
                evaluation_time=authorized_at,
            ),
            authorized_at=authorized_at,
        )

    def _assert_planning_authorization(
        self,
        authorization: PlanningPolicyAuthorization,
        policy: DatasetRetentionPolicy,
        environment: RuntimeEnvironment,
    ) -> None:
        snapshot = authorization.policy_snapshot
        expected = (
            self._catalog.document.catalog_id,
            self._catalog.document.revision,
            self._catalog.content_hash,
            policy.policy_id,
            policy.revision,
            policy.content_hash,
            policy.provider,
            policy.dataset,
            policy.mode,
            policy.status,
            policy.verified_on,
        )
        actual = (
            snapshot.catalog_id,
            snapshot.catalog_revision,
            snapshot.catalog_hash,
            snapshot.policy_id,
            snapshot.policy_revision,
            snapshot.policy_hash,
            snapshot.provider,
            snapshot.dataset,
            snapshot.mode,
            snapshot.status,
            snapshot.verified_on,
        )
        if actual != expected or authorization.environment is not environment:
            raise DatasetPolicyDenied("planning authorization is stale or for a different policy")

    def request_safe_end(
        self,
        policy: DatasetRetentionPolicy,
        *,
        evaluation_time: datetime | None = None,
    ) -> datetime:
        delay = timedelta(
            seconds=(policy.minimum_observation_age_seconds + policy.finalization_buffer_seconds)
        )
        evaluated = self._now() if evaluation_time is None else evaluation_time
        if evaluated.tzinfo is None or evaluated.utcoffset() is None:
            raise ValueError("evaluation_time must be timezone-aware")
        return evaluated.astimezone(UTC) - delay

    def authorize_response_page(
        self,
        request_authorization: RequestPolicyAuthorization,
        *,
        page_ordinal: int,
        page_relation: str,
        payload_sha256: str,
        payload_size_bytes: int,
        canonical_media_type: str,
        content_encoding: str,
        observed_start: datetime | None,
        observed_end: datetime | None,
        runtime_status: DatasetRuntimeStatus | None = None,
    ) -> ResponsePageAuthorization:
        """Authorize inspected transient bytes before they may enter raw or quarantine."""

        snapshot = request_authorization.policy_snapshot
        policy = self._active_policy(
            snapshot.provider,
            snapshot.dataset,
            environment=request_authorization.environment,
            runtime_status=runtime_status,
        )
        self._assert_request_authorization(
            request_authorization,
            policy,
            request_authorization.environment,
        )
        if (observed_start is None) != (observed_end is None):
            raise DatasetPolicyDenied("response bounds must both be verified or both be empty")
        if observed_start is not None and observed_end is not None:
            if observed_start.tzinfo is None or observed_start.utcoffset() is None:
                raise DatasetPolicyDenied("observed response start must be timezone-aware")
            if observed_end.tzinfo is None or observed_end.utcoffset() is None:
                raise DatasetPolicyDenied("observed response end must be timezone-aware")
            start = observed_start.astimezone(UTC)
            end = observed_end.astimezone(UTC)
            if (
                end <= start
                or start < request_authorization.request_start
                or end > request_authorization.request_end
                or end >= request_authorization.eligible_before
            ):
                raise DatasetPolicyDenied(
                    "provider response contains observations outside authorized bounds"
                )
        return ResponsePageAuthorization(
            request=request_authorization,
            page_ordinal=page_ordinal,
            page_relation=page_relation,
            payload_sha256=payload_sha256.casefold(),
            payload_size_bytes=payload_size_bytes,
            canonical_media_type=canonical_media_type,
            content_encoding=content_encoding,
            observed_start=observed_start,
            observed_end=observed_end,
            authorized_at=self._now(),
        )

    def authorize_completed_acquisition(
        self,
        request_authorization: RequestPolicyAuthorization,
        pages: tuple[ResponsePageAuthorization, ...],
        *,
        pagination_complete: bool,
        terminal_page_verified: bool,
        runtime_status: DatasetRuntimeStatus | None = None,
    ) -> AcquisitionPolicyAuthorization:
        snapshot = request_authorization.policy_snapshot
        policy = self._active_policy(
            snapshot.provider,
            snapshot.dataset,
            environment=request_authorization.environment,
            runtime_status=runtime_status,
        )
        self._assert_request_authorization(
            request_authorization,
            policy,
            request_authorization.environment,
        )
        if not pages:
            raise DatasetPolicyDenied("a completed acquisition requires at least one response page")
        if not pagination_complete or not terminal_page_verified:
            raise DatasetPolicyDenied("provider pagination is not completely verified")
        for expected_ordinal, page in enumerate(pages):
            self._assert_page_authorization(
                page,
                policy,
                request_authorization.environment,
            )
            if page.request != request_authorization or page.page_ordinal != expected_ordinal:
                raise DatasetPolicyDenied(
                    "response pages do not form the authorized deterministic page sequence"
                )
        return AcquisitionPolicyAuthorization(
            request=request_authorization,
            ordered_artifacts=tuple(page.artifact_descriptor for page in pages),
            pagination_complete=True,
            terminal_page_verified=True,
            authorized_at=self._now(),
        )

    def authorize_processing(
        self,
        provider: str,
        dataset: str,
        *,
        environment: RuntimeEnvironment,
        runtime_status: DatasetRuntimeStatus | None = None,
    ) -> DatasetRetentionPolicy:
        return self._active_policy(
            provider,
            dataset,
            environment=environment,
            runtime_status=runtime_status,
        )

    def authorize_persistence(
        self,
        provider: str,
        dataset: str,
        *,
        environment: RuntimeEnvironment,
        layer: RetentionLayer,
        runtime_status: DatasetRuntimeStatus | None = None,
        response_authorization: ResponsePageAuthorization | None = None,
        acquisition_authorization: AcquisitionPolicyAuthorization | None = None,
        payload_sha256: str | None = None,
        payload_size_bytes: int | None = None,
        canonical_media_type: str | None = None,
        content_encoding: str | None = None,
        request_spec_hash: str | None = None,
        page_ordinal: int | None = None,
        page_relation: str | None = None,
        input_artifacts: tuple[AuthorizedRawArtifactDescriptor, ...] | None = None,
        input_page_sha256: tuple[str, ...] | None = None,
    ) -> DatasetRetentionPolicy:
        policy = self._active_policy(
            provider,
            dataset,
            environment=environment,
            runtime_status=runtime_status,
        )
        rule = policy.layer(layer)
        if rule.mode in {RetentionMode.PROHIBITED, RetentionMode.EPHEMERAL}:
            raise DatasetPolicyDenied(
                f"dataset policy does not permit durable {layer.value} persistence"
            )
        if (
            policy.mode is not RetentionMode.SYNTHETIC_UNRESTRICTED
            and environment is not RuntimeEnvironment.PRIVATE_RESEARCH
        ):
            raise DatasetPolicyDenied("real provider persistence requires private_research")
        self._assert_materialization_authorization(
            policy=policy,
            environment=environment,
            layer=layer,
            response_authorization=response_authorization,
            acquisition_authorization=acquisition_authorization,
            payload_sha256=payload_sha256,
            payload_size_bytes=payload_size_bytes,
            canonical_media_type=canonical_media_type,
            content_encoding=content_encoding,
            request_spec_hash=request_spec_hash,
            page_ordinal=page_ordinal,
            page_relation=page_relation,
            input_artifacts=input_artifacts,
            input_page_sha256=input_page_sha256,
        )
        return policy

    def authorize_query(
        self,
        provider: str,
        dataset: str,
        *,
        environment: RuntimeEnvironment,
        layer: RetentionLayer = RetentionLayer.NORMALIZED,
        runtime_status: DatasetRuntimeStatus | None = None,
    ) -> DatasetRetentionPolicy:
        policy = self._active_policy(
            provider,
            dataset,
            environment=environment,
            runtime_status=runtime_status,
        )
        if policy.layer(layer).mode in {RetentionMode.PROHIBITED, RetentionMode.EPHEMERAL}:
            raise DatasetPolicyDenied(f"query is prohibited for {layer.value} data")
        return policy

    def authorize_watermark(
        self,
        provider: str,
        dataset: str,
        *,
        environment: RuntimeEnvironment,
        runtime_status: DatasetRuntimeStatus | None = None,
        acquisition_authorization: AcquisitionPolicyAuthorization | None = None,
    ) -> DatasetRetentionPolicy:
        policy = self._active_policy(
            provider,
            dataset,
            environment=environment,
            runtime_status=runtime_status,
        )
        if policy.mode in {RetentionMode.PROHIBITED, RetentionMode.EPHEMERAL}:
            raise DatasetPolicyDenied("dataset policy cannot create a durable historical watermark")
        if policy.normalized.mode in {RetentionMode.PROHIBITED, RetentionMode.EPHEMERAL}:
            raise DatasetPolicyDenied("watermark lacks durable normalized support")
        if policy.mode is not RetentionMode.SYNTHETIC_UNRESTRICTED:
            if acquisition_authorization is None:
                raise DatasetPolicyDenied("watermark requires complete acquisition authorization")
            self._assert_acquisition_authorization(
                acquisition_authorization,
                policy,
                environment,
            )
        return policy

    def authorize_quarantine(
        self,
        provider: str,
        dataset: str,
        *,
        environment: RuntimeEnvironment,
        layer: RetentionLayer,
        runtime_status: DatasetRuntimeStatus | None = None,
        response_authorization: ResponsePageAuthorization | None = None,
        acquisition_authorization: AcquisitionPolicyAuthorization | None = None,
        payload_sha256: str | None = None,
        payload_size_bytes: int | None = None,
        canonical_media_type: str | None = None,
        content_encoding: str | None = None,
        request_spec_hash: str | None = None,
        page_ordinal: int | None = None,
        page_relation: str | None = None,
        input_artifacts: tuple[AuthorizedRawArtifactDescriptor, ...] | None = None,
        input_page_sha256: tuple[str, ...] | None = None,
    ) -> DatasetRetentionPolicy:
        policy = self._active_policy(
            provider,
            dataset,
            environment=environment,
            runtime_status=runtime_status,
        )
        if not policy.layer(layer).quarantine_allowed:
            raise DatasetPolicyDenied(f"quarantine is not permitted for {layer.value} data")
        self._assert_materialization_authorization(
            policy=policy,
            environment=environment,
            layer=layer,
            response_authorization=response_authorization,
            acquisition_authorization=acquisition_authorization,
            payload_sha256=payload_sha256,
            payload_size_bytes=payload_size_bytes,
            canonical_media_type=canonical_media_type,
            content_encoding=content_encoding,
            request_spec_hash=request_spec_hash,
            page_ordinal=page_ordinal,
            page_relation=page_relation,
            input_artifacts=input_artifacts,
            input_page_sha256=input_page_sha256,
        )
        return policy

    def authorize_export(
        self,
        provider: str,
        dataset: str,
        *,
        environment: RuntimeEnvironment,
        runtime_status: DatasetRuntimeStatus | None = None,
    ) -> DatasetRetentionPolicy:
        policy = self._active_policy(
            provider,
            dataset,
            environment=environment,
            runtime_status=runtime_status,
        )
        if not policy.external_export_allowed:
            raise DatasetPolicyDenied("external export is not authorized")
        return policy

    def authorize_purge(self, provider: str, dataset: str) -> DatasetRetentionPolicy:
        """Permit safe removal of exact cataloged targets even after policy deactivation."""

        return self._catalog.lookup(provider, dataset)


__all__ = [
    "AcquisitionPolicyAuthorization",
    "AuthorizedRawArtifactDescriptor",
    "DatasetPolicyDenied",
    "DatasetPolicySnapshot",
    "DatasetPolicyStatus",
    "DatasetRetentionPolicy",
    "DatasetRuntimeStatus",
    "LayerRetentionPolicy",
    "PendingDatasetReview",
    "PlanningPolicyAuthorization",
    "RequestPolicyAuthorization",
    "ResponsePageAuthorization",
    "RetentionCatalogDocument",
    "RetentionLayer",
    "RetentionMode",
    "RetentionOperation",
    "RetentionPolicyCatalog",
    "RetentionPolicyEnforcer",
    "RetentionPolicyError",
]
