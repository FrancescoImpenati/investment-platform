"""Unit tests for exact-key retention policy and fail-closed enforcement."""

import hashlib
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from investment_platform.data.provenance import DataSource, LicenseClassification
from investment_platform.data.retention import (
    AcquisitionPolicyAuthorization,
    DatasetPolicyDenied,
    DatasetPolicyStatus,
    DatasetRetentionPolicy,
    DatasetRuntimeStatus,
    LayerRetentionPolicy,
    RequestPolicyAuthorization,
    ResponsePageAuthorization,
    RetentionCatalogDocument,
    RetentionLayer,
    RetentionMode,
    RetentionPolicyCatalog,
    RetentionPolicyEnforcer,
)
from investment_platform.runtime import RuntimeEnvironment

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 31, 12, 0, tzinfo=UTC)
_ALPACA_KEY = ("alpaca", "price_bars_sip")


def _clock() -> datetime:
    return _NOW


def _catalog_with(*policies: DatasetRetentionPolicy) -> RetentionPolicyCatalog:
    return RetentionPolicyCatalog(
        RetentionCatalogDocument(
            schema_version=1,
            catalog_id="test-retention-catalog",
            revision=1,
            policies=policies,
        )
    )


def _policy_copy(
    policy: DatasetRetentionPolicy,
    **updates: object,
) -> DatasetRetentionPolicy:
    payload = policy.model_dump(mode="json")
    payload.update(updates)
    return DatasetRetentionPolicy.model_validate(payload)


def _with_status(
    policy: DatasetRetentionPolicy,
    status: DatasetPolicyStatus,
) -> DatasetRetentionPolicy:
    return _policy_copy(policy, status=status.value)


def _ttl_policy() -> DatasetRetentionPolicy:
    return DatasetRetentionPolicy(
        policy_id="test-ttl-price-bars",
        revision=1,
        provider="test_ttl",
        dataset="price_bars",
        mode=RetentionMode.TTL,
        status=DatasetPolicyStatus.ACTIVE,
        permitted_environments=(RuntimeEnvironment.PRIVATE_RESEARCH,),
        use_scope="Private test of bounded retention.",
        processing_allowed=True,
        raw=LayerRetentionPolicy(
            mode=RetentionMode.TTL,
            ttl_seconds=3600,
            quarantine_allowed=True,
        ),
        normalized=LayerRetentionPolicy(
            mode=RetentionMode.TTL,
            ttl_seconds=3600,
            quarantine_allowed=True,
        ),
        derived=LayerRetentionPolicy(mode=RetentionMode.PROHIBITED),
        delete_on_termination=True,
        evidence_reference="tests/unit/test_retention.py",
        verified_on=date(2026, 8, 31),
        notes="Synthetic policy object used only to exercise TTL enforcement.",
    )


def _complete_authorization(
    enforcer: RetentionPolicyEnforcer,
    policy: DatasetRetentionPolicy,
    *,
    runtime_status: DatasetRuntimeStatus | None = None,
) -> tuple[
    RequestPolicyAuthorization,
    ResponsePageAuthorization,
    AcquisitionPolicyAuthorization,
]:
    """Create exact request, inspected-page, and complete-acquisition proof tokens."""

    request = enforcer.authorize_request(
        policy.provider,
        policy.dataset,
        environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        start=_NOW - timedelta(hours=2),
        end=_NOW - timedelta(hours=1),
        request_spec_hash="a" * 64,
        runtime_status=runtime_status,
    )
    payload = b'{"bars":[]}'
    page = enforcer.authorize_response_page(
        request,
        page_ordinal=0,
        page_relation="root",
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        payload_size_bytes=len(payload),
        canonical_media_type="application/json",
        content_encoding="identity",
        observed_start=request.request_start,
        observed_end=request.request_end,
        runtime_status=runtime_status,
    )
    acquisition = enforcer.authorize_completed_acquisition(
        request,
        (page,),
        pagination_complete=True,
        terminal_page_verified=True,
        runtime_status=runtime_status,
    )
    return request, page, acquisition


def test_default_catalog_load_hash_and_snapshot_are_deterministic(tmp_path: Path) -> None:
    catalog = RetentionPolicyCatalog.load_default()
    catalog_path = tmp_path / "retention.json"
    catalog_path.write_text(catalog.document.model_dump_json(), encoding="utf-8")
    reloaded = RetentionPolicyCatalog.load(catalog_path)

    assert reloaded.document == catalog.document
    assert reloaded.content_hash == catalog.content_hash
    assert len(catalog.content_hash) == 64
    assert int(catalog.content_hash, 16) >= 0

    captured_at = datetime(2026, 8, 31, 14, 30, tzinfo=timezone(timedelta(hours=2)))
    policy = catalog.lookup(*_ALPACA_KEY)
    snapshot = catalog.snapshot(*_ALPACA_KEY, captured_at=captured_at)

    assert snapshot.catalog_id == catalog.document.catalog_id
    assert snapshot.catalog_revision == catalog.document.revision
    assert snapshot.catalog_hash == catalog.content_hash
    assert snapshot.policy_id == policy.policy_id
    assert snapshot.policy_revision == policy.revision
    assert snapshot.policy_hash == policy.content_hash
    assert snapshot.provider == policy.provider
    assert snapshot.dataset == policy.dataset
    assert snapshot.mode is RetentionMode.DURABLE_AUTHORIZED
    assert snapshot.captured_at == datetime(2026, 8, 31, 12, 30, tzinfo=UTC)


def test_default_catalog_contains_only_the_approved_exact_keys() -> None:
    document = RetentionPolicyCatalog.load_default().document

    assert {(policy.provider, policy.dataset) for policy in document.policies} == {
        ("alpaca", "price_bars_sip"),
        ("twelve_data", "price_bars_us_daily"),
        ("twelve_data", "price_bars_standard_us_intraday"),
        ("databento", "opra.pillar"),
        ("massive", "price_bars"),
        ("massive", "corporate_actions"),
        ("synthetic", "price_bars"),
        ("sample", "price_bars"),
    }
    assert {
        (review.provider, review.dataset, review.status) for review in document.pending_reviews
    } == {
        ("alpaca", "historical_options", "UNVERIFIED_PENDING"),
        ("alpaca", "crypto", "UNVERIFIED_PENDING"),
    }


@pytest.mark.parametrize("dataset", ["historical_options", "crypto"])
def test_alpaca_pending_reviews_are_not_active_policies(dataset: str) -> None:
    catalog = RetentionPolicyCatalog.load_default()

    with pytest.raises(DatasetPolicyDenied, match="no active retention policy"):
        catalog.lookup("alpaca", dataset)


@pytest.mark.parametrize(
    ("provider", "dataset"),
    [
        ("alpaca", "real_time"),
        ("alpaca", "news"),
        ("alpaca", "historical_sip_options"),
        ("unknown", "price_bars_sip"),
        ("alpaca", "unknown"),
    ],
)
def test_unlisted_or_different_dataset_keys_fail_closed(provider: str, dataset: str) -> None:
    with pytest.raises(DatasetPolicyDenied, match="no active retention policy"):
        RetentionPolicyCatalog.load_default().lookup(provider, dataset)


@pytest.mark.parametrize(
    ("provider", "dataset"),
    [
        ("Alpaca", "price_bars_sip"),
        ("alpaca", "PRICE_BARS_SIP"),
        (" alpaca", "price_bars_sip"),
        ("alpaca ", "price_bars_sip"),
        ("alpaca", " price_bars_sip"),
        ("alpaca", "price_bars_sip "),
    ],
)
def test_catalog_lookup_rejects_noncanonical_near_match_keys(
    provider: str,
    dataset: str,
) -> None:
    catalog = RetentionPolicyCatalog.load_default()

    with pytest.raises(DatasetPolicyDenied, match="exact provider/dataset key"):
        catalog.lookup(provider, dataset)


def test_alpaca_request_gate_is_strictly_older_than_age_plus_buffer() -> None:
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=_clock)
    policy = enforcer.authorize_processing(
        *_ALPACA_KEY,
        environment=RuntimeEnvironment.PRIVATE_RESEARCH,
    )
    safe_end = _NOW - timedelta(minutes=16)

    assert policy.minimum_observation_age_seconds == 900
    assert policy.finalization_buffer_seconds == 60
    assert enforcer.request_safe_end(policy) == safe_end
    with pytest.raises(DatasetPolicyDenied, match="age/finalization gate"):
        enforcer.authorize_request(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            start=safe_end - timedelta(hours=1),
            end=safe_end,
            request_spec_hash="b" * 64,
        )

    authorized = enforcer.authorize_request(
        *_ALPACA_KEY,
        environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        start=safe_end - timedelta(hours=1),
        end=safe_end - timedelta(microseconds=1),
        request_spec_hash="b" * 64,
    )
    assert authorized.policy_id == "alpaca-historical-sip-us-stock-bars"


@pytest.mark.parametrize(
    "environment",
    [
        RuntimeEnvironment.TEST,
        RuntimeEnvironment.CI,
        RuntimeEnvironment.DEVELOPMENT,
        RuntimeEnvironment.DEMO,
    ],
)
def test_alpaca_policy_is_confined_to_private_research(
    environment: RuntimeEnvironment,
) -> None:
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=_clock)

    with pytest.raises(DatasetPolicyDenied, match="not permitted"):
        enforcer.authorize_processing(*_ALPACA_KEY, environment=environment)


def test_alpaca_layer_permissions_allow_raw_and_normalized_but_not_derived() -> None:
    catalog = RetentionPolicyCatalog.load_default()
    enforcer = RetentionPolicyEnforcer(catalog, clock=_clock)
    policy = catalog.lookup(*_ALPACA_KEY)
    _, page_authorization, acquisition_authorization = _complete_authorization(enforcer, policy)

    persisted_raw = enforcer.authorize_persistence(
        *_ALPACA_KEY,
        environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        layer=RetentionLayer.RAW,
        response_authorization=page_authorization,
        payload_sha256=page_authorization.payload_sha256,
        payload_size_bytes=page_authorization.payload_size_bytes,
        canonical_media_type=page_authorization.canonical_media_type,
        content_encoding=page_authorization.content_encoding,
        request_spec_hash=page_authorization.request.request_spec_hash,
        page_ordinal=page_authorization.page_ordinal,
        page_relation=page_authorization.page_relation,
    )
    persisted_normalized = enforcer.authorize_persistence(
        *_ALPACA_KEY,
        environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        layer=RetentionLayer.NORMALIZED,
        acquisition_authorization=acquisition_authorization,
        input_artifacts=acquisition_authorization.ordered_artifacts,
        input_page_sha256=acquisition_authorization.ordered_page_sha256,
    )
    assert persisted_raw.raw.mode is RetentionMode.DURABLE_AUTHORIZED
    assert persisted_normalized.normalized.mode is RetentionMode.DURABLE_AUTHORIZED

    for layer in (RetentionLayer.RAW, RetentionLayer.NORMALIZED):
        queried = enforcer.authorize_query(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=layer,
        )
        assert queried == policy

    assert (
        enforcer.authorize_watermark(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            acquisition_authorization=acquisition_authorization,
        )
        == policy
    )

    with pytest.raises(DatasetPolicyDenied, match="durable derived persistence"):
        enforcer.authorize_persistence(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.DERIVED,
        )
    with pytest.raises(DatasetPolicyDenied, match="query is prohibited"):
        enforcer.authorize_query(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.DERIVED,
        )


def test_provider_overfetch_cannot_obtain_page_persistence_or_quarantine_authority() -> None:
    catalog = RetentionPolicyCatalog.load_default()
    enforcer = RetentionPolicyEnforcer(catalog, clock=_clock)
    policy = catalog.lookup(*_ALPACA_KEY)
    request, _, _ = _complete_authorization(enforcer, policy)
    payload_hash = hashlib.sha256(b"overfetched page").hexdigest()
    overfetched_bounds = (
        # Older than the provider gate but outside the requested lower bound.
        (request.request_start - timedelta(minutes=5), request.request_start),
        # Outside the requested upper bound while still older than the provider gate.
        (request.request_end, request.request_end + timedelta(minutes=1)),
        # Includes observations exactly at the younger eligibility boundary.
        (request.request_start, request.eligible_before),
    )

    for observed_start, observed_end in overfetched_bounds:
        with pytest.raises(DatasetPolicyDenied, match="outside authorized bounds"):
            enforcer.authorize_response_page(
                request,
                page_ordinal=0,
                page_relation="root",
                payload_sha256=payload_hash,
                payload_size_bytes=16,
                canonical_media_type="application/json",
                content_encoding="identity",
                observed_start=observed_start,
                observed_end=observed_end,
            )

    with pytest.raises(DatasetPolicyDenied, match="raw materialization requires"):
        enforcer.authorize_persistence(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.RAW,
        )
    with pytest.raises(DatasetPolicyDenied, match="inspected response authorization"):
        enforcer.authorize_quarantine(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.RAW,
        )


def test_incomplete_pagination_cannot_authorize_normalized_effects() -> None:
    catalog = RetentionPolicyCatalog.load_default()
    enforcer = RetentionPolicyEnforcer(catalog, clock=_clock)
    policy = catalog.lookup(*_ALPACA_KEY)
    request, page, _ = _complete_authorization(enforcer, policy)

    for pagination_complete, terminal_page_verified in ((False, True), (True, False)):
        with pytest.raises(DatasetPolicyDenied, match="pagination is not completely verified"):
            enforcer.authorize_completed_acquisition(
                request,
                (page,),
                pagination_complete=pagination_complete,
                terminal_page_verified=terminal_page_verified,
            )

    with pytest.raises(DatasetPolicyDenied, match="complete acquisition authorization"):
        enforcer.authorize_persistence(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.NORMALIZED,
        )
    with pytest.raises(DatasetPolicyDenied, match="complete acquisition authorization"):
        enforcer.authorize_watermark(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        )
    with pytest.raises(DatasetPolicyDenied, match="complete acquisition authorization"):
        enforcer.authorize_quarantine(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.NORMALIZED,
        )


def test_materialization_identity_must_match_the_authorized_bytes_and_pages() -> None:
    catalog = RetentionPolicyCatalog.load_default()
    enforcer = RetentionPolicyEnforcer(catalog, clock=_clock)
    policy = catalog.lookup(*_ALPACA_KEY)
    _, page, acquisition = _complete_authorization(enforcer, policy)

    with pytest.raises(DatasetPolicyDenied, match="inspected response bytes"):
        enforcer.authorize_persistence(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.RAW,
            response_authorization=page,
            payload_sha256="0" * 64,
            payload_size_bytes=page.payload_size_bytes,
            canonical_media_type=page.canonical_media_type,
            content_encoding=page.content_encoding,
            request_spec_hash=page.request.request_spec_hash,
            page_ordinal=page.page_ordinal,
            page_relation=page.page_relation,
        )
    wrong_artifact = acquisition.ordered_artifacts[0].model_copy(
        update={"content_sha256": "0" * 64}
    )
    with pytest.raises(DatasetPolicyDenied, match="authorized raw artifact sequence"):
        enforcer.authorize_persistence(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.NORMALIZED,
            acquisition_authorization=acquisition,
            input_artifacts=(wrong_artifact,),
        )


def test_authorization_is_bound_to_request_spec_and_zero_based_page_relation() -> None:
    catalog = RetentionPolicyCatalog.load_default()
    enforcer = RetentionPolicyEnforcer(catalog, clock=_clock)
    policy = catalog.lookup(*_ALPACA_KEY)
    request, page, _ = _complete_authorization(enforcer, policy)

    with pytest.raises(DatasetPolicyDenied, match="inspected response bytes"):
        enforcer.authorize_persistence(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.RAW,
            response_authorization=page,
            payload_sha256=page.payload_sha256,
            payload_size_bytes=page.payload_size_bytes,
            canonical_media_type=page.canonical_media_type,
            content_encoding=page.content_encoding,
            request_spec_hash="f" * 64,
            page_ordinal=page.page_ordinal,
            page_relation=page.page_relation,
        )
    with pytest.raises(ValueError, match="relation"):
        enforcer.authorize_response_page(
            request,
            page_ordinal=1,
            page_relation="root",
            payload_sha256=page.payload_sha256,
            payload_size_bytes=page.payload_size_bytes,
            canonical_media_type=page.canonical_media_type,
            content_encoding=page.content_encoding,
            observed_start=request.request_start,
            observed_end=request.request_end,
        )


def test_authorization_tokens_cannot_cross_provider_or_dataset_policy_identity() -> None:
    alpaca_policy = RetentionPolicyCatalog.load_default().lookup(*_ALPACA_KEY)
    other_policy = _policy_copy(
        alpaca_policy,
        provider="other_provider",
        dataset="other_price_bars",
        policy_id="other-provider-price-bars",
    )
    enforcer = RetentionPolicyEnforcer(
        _catalog_with(alpaca_policy, other_policy),
        clock=_clock,
    )
    _, page, acquisition = _complete_authorization(enforcer, alpaca_policy)

    with pytest.raises(DatasetPolicyDenied, match="current exact dataset policy"):
        enforcer.authorize_persistence(
            other_policy.provider,
            other_policy.dataset,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.RAW,
            response_authorization=page,
            payload_sha256=page.payload_sha256,
            payload_size_bytes=page.payload_size_bytes,
            canonical_media_type=page.canonical_media_type,
            content_encoding=page.content_encoding,
            request_spec_hash=page.request.request_spec_hash,
            page_ordinal=page.page_ordinal,
            page_relation=page.page_relation,
        )
    with pytest.raises(DatasetPolicyDenied, match="current exact dataset policy"):
        enforcer.authorize_persistence(
            other_policy.provider,
            other_policy.dataset,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.NORMALIZED,
            acquisition_authorization=acquisition,
            input_artifacts=acquisition.ordered_artifacts,
            input_page_sha256=acquisition.ordered_page_sha256,
        )

    alpaca_status = DatasetRuntimeStatus.for_policy(alpaca_policy)
    with pytest.raises(DatasetPolicyDenied, match="exact dataset policy"):
        enforcer.authorize_processing(
            other_policy.provider,
            other_policy.dataset,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            runtime_status=alpaca_status,
        )


def test_authorization_tokens_are_invalidated_by_a_policy_revision() -> None:
    original = RetentionPolicyCatalog.load_default().lookup(*_ALPACA_KEY)
    original_enforcer = RetentionPolicyEnforcer(_catalog_with(original), clock=_clock)
    _, page, acquisition = _complete_authorization(original_enforcer, original)
    revised = _policy_copy(
        original,
        revision=original.revision + 1,
        notes=f"{original.notes} Revised for token invalidation test.",
    )
    revised_enforcer = RetentionPolicyEnforcer(_catalog_with(revised), clock=_clock)

    with pytest.raises(DatasetPolicyDenied, match="current exact dataset policy"):
        revised_enforcer.authorize_persistence(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.RAW,
            response_authorization=page,
            payload_sha256=page.payload_sha256,
            payload_size_bytes=page.payload_size_bytes,
            canonical_media_type=page.canonical_media_type,
            content_encoding=page.content_encoding,
            request_spec_hash=page.request.request_spec_hash,
            page_ordinal=page.page_ordinal,
            page_relation=page.page_relation,
        )
    with pytest.raises(DatasetPolicyDenied, match="current exact dataset policy"):
        revised_enforcer.authorize_watermark(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            acquisition_authorization=acquisition,
        )


def test_active_ephemeral_policy_allows_processing_but_not_persistence_or_watermark() -> None:
    default = RetentionPolicyCatalog.load_default().lookup("databento", "opra.pillar")
    policy = _with_status(default, DatasetPolicyStatus.ACTIVE)
    enforcer = RetentionPolicyEnforcer(_catalog_with(policy), clock=_clock)

    assert (
        enforcer.authorize_processing(
            "databento",
            "opra.pillar",
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        ).mode
        is RetentionMode.EPHEMERAL
    )
    for layer in (RetentionLayer.RAW, RetentionLayer.NORMALIZED):
        with pytest.raises(DatasetPolicyDenied, match=f"durable {layer.value} persistence"):
            enforcer.authorize_persistence(
                "databento",
                "opra.pillar",
                environment=RuntimeEnvironment.PRIVATE_RESEARCH,
                layer=layer,
            )
    with pytest.raises(DatasetPolicyDenied, match="durable historical watermark"):
        enforcer.authorize_watermark(
            "databento",
            "opra.pillar",
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        )


@pytest.mark.parametrize("dataset", ["price_bars", "corporate_actions"])
def test_prohibited_massive_datasets_are_denied_before_processing(dataset: str) -> None:
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=_clock)

    with pytest.raises(DatasetPolicyDenied, match="prohibited"):
        enforcer.authorize_processing(
            "massive",
            dataset,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        )


def test_subscription_policy_requires_both_active_catalog_and_runtime_entitlement() -> None:
    default = RetentionPolicyCatalog.load_default().lookup("twelve_data", "price_bars_us_daily")
    default_enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=_clock)
    pending_entitled = DatasetRuntimeStatus.for_policy(default, entitlement_active=True)

    with pytest.raises(DatasetPolicyDenied, match="pending"):
        default_enforcer.authorize_processing(
            "twelve_data",
            "price_bars_us_daily",
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            runtime_status=pending_entitled,
        )

    active = _with_status(default, DatasetPolicyStatus.ACTIVE)
    enforcer = RetentionPolicyEnforcer(_catalog_with(active), clock=_clock)
    inactive = DatasetRuntimeStatus.for_policy(active, entitlement_active=False)
    for status in (None, inactive):
        with pytest.raises(DatasetPolicyDenied, match="entitlement is not active"):
            enforcer.authorize_processing(
                "twelve_data",
                "price_bars_us_daily",
                environment=RuntimeEnvironment.PRIVATE_RESEARCH,
                runtime_status=status,
            )

    entitled = DatasetRuntimeStatus.for_policy(active, entitlement_active=True)
    assert (
        enforcer.authorize_processing(
            "twelve_data",
            "price_bars_us_daily",
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            runtime_status=entitled,
        ).mode
        is RetentionMode.SUBSCRIPTION_BOUND
    )


def test_ttl_without_exact_expiry_or_status_denies_query_persistence_and_watermark() -> None:
    policy = _ttl_policy()
    enforcer = RetentionPolicyEnforcer(_catalog_with(policy), clock=_clock)
    valid_status = DatasetRuntimeStatus.for_policy(
        policy,
        retention_started_at=_NOW,
        expires_at=_NOW + timedelta(hours=1),
    )
    _, _, acquisition_authorization = _complete_authorization(
        enforcer,
        policy,
        runtime_status=valid_status,
    )
    missing_expiry = DatasetRuntimeStatus.for_policy(policy)

    for runtime_status in (None, missing_expiry):
        with pytest.raises(DatasetPolicyDenied, match="TTL dataset requires"):
            enforcer.authorize_query(
                policy.provider,
                policy.dataset,
                environment=RuntimeEnvironment.PRIVATE_RESEARCH,
                runtime_status=runtime_status,
            )
        with pytest.raises(DatasetPolicyDenied, match="TTL dataset requires"):
            enforcer.authorize_persistence(
                policy.provider,
                policy.dataset,
                environment=RuntimeEnvironment.PRIVATE_RESEARCH,
                layer=RetentionLayer.NORMALIZED,
                runtime_status=runtime_status,
                acquisition_authorization=acquisition_authorization,
                input_artifacts=acquisition_authorization.ordered_artifacts,
                input_page_sha256=acquisition_authorization.ordered_page_sha256,
            )
        with pytest.raises(DatasetPolicyDenied, match="TTL dataset requires"):
            enforcer.authorize_watermark(
                policy.provider,
                policy.dataset,
                environment=RuntimeEnvironment.PRIVATE_RESEARCH,
                runtime_status=runtime_status,
                acquisition_authorization=acquisition_authorization,
            )


def test_ttl_policy_requires_ttl_metadata_and_expires_at_the_runtime_boundary() -> None:
    policy = _ttl_policy()
    enforcer = RetentionPolicyEnforcer(_catalog_with(policy), clock=_clock)

    assert policy.raw.ttl_seconds == 3600
    assert policy.normalized.ttl_seconds == 3600
    future_status = DatasetRuntimeStatus.for_policy(
        policy,
        retention_started_at=_NOW - timedelta(seconds=3599),
        expires_at=_NOW + timedelta(seconds=1),
    )
    _, _, acquisition_authorization = _complete_authorization(
        enforcer,
        policy,
        runtime_status=future_status,
    )
    assert (
        enforcer.authorize_persistence(
            "test_ttl",
            "price_bars",
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.NORMALIZED,
            runtime_status=future_status,
            acquisition_authorization=acquisition_authorization,
            input_artifacts=acquisition_authorization.ordered_artifacts,
            input_page_sha256=acquisition_authorization.ordered_page_sha256,
        )
        == policy
    )

    for expires_at in (_NOW, _NOW - timedelta(microseconds=1)):
        with pytest.raises(DatasetPolicyDenied, match="expired"):
            enforcer.authorize_persistence(
                "test_ttl",
                "price_bars",
                environment=RuntimeEnvironment.PRIVATE_RESEARCH,
                layer=RetentionLayer.NORMALIZED,
                runtime_status=DatasetRuntimeStatus.for_policy(
                    policy,
                    retention_started_at=expires_at - timedelta(seconds=3600),
                    expires_at=expires_at,
                ),
                acquisition_authorization=acquisition_authorization,
                input_artifacts=acquisition_authorization.ordered_artifacts,
                input_page_sha256=acquisition_authorization.ordered_page_sha256,
            )

    with pytest.raises(ValueError, match="requires ttl_seconds"):
        LayerRetentionPolicy(mode=RetentionMode.TTL)


def test_layer_retention_mode_cannot_change_the_dataset_mode() -> None:
    policy = RetentionPolicyCatalog.load_default().lookup(*_ALPACA_KEY)
    payload = policy.model_dump(mode="json")
    payload["raw"] = {
        "mode": RetentionMode.TTL.value,
        "ttl_seconds": 3600,
        "quarantine_allowed": True,
    }

    with pytest.raises(ValueError, match="cannot expand or change"):
        DatasetRetentionPolicy.model_validate(payload)


def test_quarantine_export_and_purge_are_exact_policy_gates() -> None:
    catalog = RetentionPolicyCatalog.load_default()
    enforcer = RetentionPolicyEnforcer(catalog, clock=_clock)
    policy = catalog.lookup(*_ALPACA_KEY)
    _, page_authorization, acquisition_authorization = _complete_authorization(enforcer, policy)

    assert (
        enforcer.authorize_quarantine(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.RAW,
            response_authorization=page_authorization,
            payload_sha256=page_authorization.payload_sha256,
            payload_size_bytes=page_authorization.payload_size_bytes,
            canonical_media_type=page_authorization.canonical_media_type,
            content_encoding=page_authorization.content_encoding,
            request_spec_hash=page_authorization.request.request_spec_hash,
            page_ordinal=page_authorization.page_ordinal,
            page_relation=page_authorization.page_relation,
        )
        == policy
    )
    assert (
        enforcer.authorize_quarantine(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.NORMALIZED,
            acquisition_authorization=acquisition_authorization,
            input_artifacts=acquisition_authorization.ordered_artifacts,
            input_page_sha256=acquisition_authorization.ordered_page_sha256,
        )
        == policy
    )
    with pytest.raises(DatasetPolicyDenied, match="quarantine is not permitted"):
        enforcer.authorize_quarantine(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            layer=RetentionLayer.DERIVED,
        )
    with pytest.raises(DatasetPolicyDenied, match="external export"):
        enforcer.authorize_export(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
        )

    assert enforcer.authorize_purge("massive", "price_bars").mode is RetentionMode.PROHIBITED
    assert enforcer.authorize_purge("databento", "opra.pillar").mode is RetentionMode.EPHEMERAL
    with pytest.raises(DatasetPolicyDenied, match="no active retention policy"):
        enforcer.authorize_purge("alpaca", "crypto")


def test_runtime_status_can_only_restrict_committed_policy() -> None:
    catalog = RetentionPolicyCatalog.load_default()
    enforcer = RetentionPolicyEnforcer(catalog, clock=_clock)
    alpaca_policy = catalog.lookup(*_ALPACA_KEY)
    twelve_policy = catalog.lookup("twelve_data", "price_bars_us_daily")

    with pytest.raises(DatasetPolicyDenied, match="disabled"):
        enforcer.authorize_processing(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            runtime_status=DatasetRuntimeStatus.for_policy(
                alpaca_policy,
                enabled=False,
                entitlement_active=True,
            ),
        )
    with pytest.raises(DatasetPolicyDenied, match="expired"):
        enforcer.authorize_processing(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            runtime_status=DatasetRuntimeStatus.for_policy(
                alpaca_policy,
                entitlement_active=True,
                expires_at=_NOW,
            ),
        )
    with pytest.raises(DatasetPolicyDenied, match="not permitted"):
        enforcer.authorize_processing(
            *_ALPACA_KEY,
            environment=RuntimeEnvironment.DEVELOPMENT,
            runtime_status=DatasetRuntimeStatus.for_policy(
                alpaca_policy,
                entitlement_active=True,
            ),
        )
    with pytest.raises(DatasetPolicyDenied, match="pending"):
        enforcer.authorize_processing(
            "twelve_data",
            "price_bars_us_daily",
            environment=RuntimeEnvironment.PRIVATE_RESEARCH,
            runtime_status=DatasetRuntimeStatus.for_policy(
                twelve_policy,
                entitlement_active=True,
            ),
        )


@pytest.mark.parametrize("environment", list(RuntimeEnvironment))
def test_synthetic_policy_supports_all_layers_and_offline_profiles(
    environment: RuntimeEnvironment,
) -> None:
    enforcer = RetentionPolicyEnforcer(RetentionPolicyCatalog.load_default(), clock=_clock)

    policy = enforcer.authorize_processing(
        "synthetic",
        "price_bars",
        environment=environment,
    )
    assert policy.mode is RetentionMode.SYNTHETIC_UNRESTRICTED
    assert (
        enforcer.authorize_watermark("synthetic", "price_bars", environment=environment) == policy
    )
    assert enforcer.authorize_export("synthetic", "price_bars", environment=environment) == policy
    for layer in RetentionLayer:
        assert (
            enforcer.authorize_persistence(
                "synthetic",
                "price_bars",
                environment=environment,
                layer=layer,
            )
            == policy
        )
        assert (
            enforcer.authorize_query(
                "synthetic",
                "price_bars",
                environment=environment,
                layer=layer,
            )
            == policy
        )
        assert (
            enforcer.authorize_quarantine(
                "synthetic",
                "price_bars",
                environment=environment,
                layer=layer,
            )
            == policy
        )


def test_license_classification_does_not_replace_dataset_retention_policy() -> None:
    source = DataSource(
        source_id=UUID("10000000-0000-4000-8000-000000000001"),
        provider="alpaca",
        dataset="price_bars_sip",
        logical_endpoint="v2/stocks/bars",
        license_classification=LicenseClassification.PRIVATE,
    )
    policy = RetentionPolicyCatalog.load_default().lookup(source.provider, source.dataset)

    assert source.license_classification is LicenseClassification.PRIVATE
    assert policy.mode is RetentionMode.DURABLE_AUTHORIZED
    assert policy.redistribution_allowed is False
    assert policy.external_export_allowed is False
    assert type(source.license_classification) is LicenseClassification
    assert type(policy.mode) is RetentionMode
