"""Forward-only SQLite schema owned by Phase 2 ingestion.

The operational database contains workflow metadata only.  Raw response bytes and canonical
market-data values remain in the private filesystem and Parquet respectively.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True, slots=True)
class Migration:
    """One atomic, numbered, forward-only schema migration."""

    version: int
    name: str
    statements: tuple[str, ...]

    @property
    def checksum_sha256(self) -> str:
        encoded = "\n-- statement boundary --\n".join(self.statements).encode()
        return hashlib.sha256(encoded).hexdigest()


_RELATIVE_PATH_CHECK = """
length({column}) > 0
AND substr({column}, 1, 1) <> '/'
AND instr({column}, '\\') = 0
AND instr({column}, ':') = 0
AND {column} <> '.'
AND {column} <> '..'
AND {column} NOT LIKE '../%'
AND {column} NOT LIKE '%/../%'
AND {column} NOT LIKE '%/..'
AND {column} NOT LIKE './%'
AND {column} NOT LIKE '%/./%'
AND {column} NOT LIKE '%/.'
""".strip()


def _relative_path_check(column: str) -> str:
    return _RELATIVE_PATH_CHECK.format(column=column)


_MIGRATION_1_STATEMENTS: Final[tuple[str, ...]] = (
    """
    CREATE TABLE store_metadata (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        root_id TEXT NOT NULL UNIQUE,
        created_at TEXT NOT NULL,
        schema_version INTEGER NOT NULL CHECK (schema_version > 0)
    )
    """,
    """
    CREATE TABLE policy_snapshots (
        policy_snapshot_id TEXT PRIMARY KEY,
        policy_id TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK (revision > 0),
        policy_hash TEXT NOT NULL CHECK (length(policy_hash) = 64),
        provider TEXT NOT NULL,
        dataset TEXT NOT NULL,
        retention_mode TEXT NOT NULL CHECK (retention_mode IN (
            'PROHIBITED', 'EPHEMERAL', 'TTL', 'SUBSCRIPTION_BOUND',
            'DURABLE_AUTHORIZED', 'SYNTHETIC_UNRESTRICTED'
        )),
        verified_at TEXT NOT NULL,
        captured_at TEXT NOT NULL,
        expires_at TEXT,
        entitlement_active INTEGER CHECK (entitlement_active IN (0, 1)),
        UNIQUE (policy_id, revision, policy_hash),
        UNIQUE (policy_snapshot_id, provider, dataset)
    )
    """,
    """
    CREATE TABLE dataset_policy_status (
        provider TEXT NOT NULL,
        dataset TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN (
            'ACTIVE', 'PENDING', 'SUSPENDED', 'EXPIRED', 'TERMINATED', 'PROHIBITED'
        )),
        retention_mode TEXT CHECK (retention_mode IN (
            'PROHIBITED', 'EPHEMERAL', 'TTL', 'SUBSCRIPTION_BOUND',
            'DURABLE_AUTHORIZED', 'SYNTHETIC_UNRESTRICTED'
        )),
        policy_snapshot_id TEXT,
        effective_at TEXT NOT NULL,
        expires_at TEXT,
        unavailable_at TEXT,
        last_checked_at TEXT NOT NULL,
        PRIMARY KEY (provider, dataset),
        FOREIGN KEY (policy_snapshot_id, provider, dataset)
            REFERENCES policy_snapshots (policy_snapshot_id, provider, dataset)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE ingestion_runs (
        run_id TEXT PRIMARY KEY,
        mode TEXT NOT NULL CHECK (mode IN ('BACKFILL', 'UPDATE', 'REPAIR', 'VERIFY', 'PURGE')),
        environment TEXT NOT NULL CHECK (environment IN (
            'test', 'ci', 'development', 'private_research', 'demo'
        )),
        provider TEXT NOT NULL,
        dataset TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN (
            'PLANNED', 'RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED', 'CANCELLED'
        )),
        policy_snapshot_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        started_at TEXT,
        completed_at TEXT,
        planned_request_count INTEGER NOT NULL DEFAULT 0 CHECK (planned_request_count >= 0),
        succeeded_request_count INTEGER NOT NULL DEFAULT 0 CHECK (succeeded_request_count >= 0),
        failed_request_count INTEGER NOT NULL DEFAULT 0 CHECK (failed_request_count >= 0),
        FOREIGN KEY (policy_snapshot_id, provider, dataset)
            REFERENCES policy_snapshots (policy_snapshot_id, provider, dataset),
        CHECK (completed_at IS NULL OR status IN ('SUCCESS', 'PARTIAL', 'FAILED', 'CANCELLED'))
    )
    """,
    """
    CREATE TABLE stream_keys (
        stream_id TEXT PRIMARY KEY,
        stream_hash TEXT NOT NULL UNIQUE CHECK (length(stream_hash) = 64),
        provider TEXT NOT NULL,
        dataset TEXT NOT NULL,
        instrument_id TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        session TEXT NOT NULL,
        adjustment TEXT NOT NULL,
        dimensions_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        UNIQUE (provider, dataset, instrument_id, timeframe, session, adjustment, dimensions_json)
    )
    """,
    """
    CREATE TABLE request_specs (
        request_spec_id TEXT PRIMARY KEY,
        request_spec_hash TEXT NOT NULL UNIQUE CHECK (length(request_spec_hash) = 64),
        provider TEXT NOT NULL,
        dataset TEXT NOT NULL,
        interval_start TEXT NOT NULL,
        interval_end TEXT NOT NULL,
        mapping_version TEXT NOT NULL,
        specification_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        CHECK (interval_start < interval_end)
    )
    """,
    """
    CREATE TABLE request_spec_streams (
        request_spec_id TEXT NOT NULL REFERENCES request_specs(request_spec_id) ON DELETE RESTRICT,
        stream_id TEXT NOT NULL REFERENCES stream_keys(stream_id) ON DELETE RESTRICT,
        provider_identifier TEXT NOT NULL,
        ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
        PRIMARY KEY (request_spec_id, stream_id),
        UNIQUE (request_spec_id, ordinal)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE request_instances (
        request_instance_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id) ON DELETE RESTRICT,
        request_spec_id TEXT NOT NULL REFERENCES request_specs(request_spec_id) ON DELETE RESTRICT,
        intent TEXT NOT NULL CHECK (intent IN ('BACKFILL', 'UPDATE', 'REPAIR')),
        reason TEXT NOT NULL,
        plan_ordinal INTEGER NOT NULL CHECK (plan_ordinal >= 0),
        status TEXT NOT NULL CHECK (status IN (
            'PLANNED', 'DISPATCHING', 'ACQUIRING', 'RAW_COMPLETE', 'PROCESSING',
            'RETRY_WAIT', 'SUCCESS', 'PARTIAL', 'FAILED', 'BLOCKED', 'CANCELLED'
        )),
        created_at TEXT NOT NULL,
        completed_at TEXT,
        UNIQUE (run_id, plan_ordinal)
    )
    """,
    """
    CREATE TABLE request_attempts (
        attempt_id TEXT PRIMARY KEY,
        request_instance_id TEXT NOT NULL
            REFERENCES request_instances(request_instance_id) ON DELETE RESTRICT,
        attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
        status TEXT NOT NULL CHECK (status IN (
            'PLANNED', 'RUNNING', 'RAW_COMPLETE', 'SUCCESS',
            'RETRYABLE_FAILED', 'FATAL_FAILED', 'ABORTED'
        )),
        started_at TEXT,
        completed_at TEXT,
        next_eligible_at TEXT,
        safe_provider_request_id TEXT,
        page_count INTEGER NOT NULL DEFAULT 0 CHECK (page_count >= 0),
        pagination_complete INTEGER NOT NULL DEFAULT 0 CHECK (pagination_complete IN (0, 1)),
        terminal_page_verified INTEGER NOT NULL DEFAULT 0
            CHECK (terminal_page_verified IN (0, 1)),
        UNIQUE (request_instance_id, attempt_number),
        CHECK (pagination_complete = 0 OR page_count > 0),
        CHECK (terminal_page_verified = 0 OR pagination_complete = 1),
        CHECK (
            status NOT IN ('RAW_COMPLETE', 'SUCCESS')
            OR (
                page_count > 0
                AND pagination_complete = 1
                AND terminal_page_verified = 1
            )
        )
    )
    """,
    """
    CREATE TABLE errors (
        error_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id) ON DELETE RESTRICT,
        request_instance_id TEXT
            REFERENCES request_instances(request_instance_id) ON DELETE RESTRICT,
        attempt_id TEXT REFERENCES request_attempts(attempt_id) ON DELETE RESTRICT,
        category TEXT NOT NULL,
        code TEXT NOT NULL,
        sanitized_message TEXT NOT NULL,
        retryable INTEGER NOT NULL CHECK (retryable IN (0, 1)),
        occurred_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE retry_state (
        request_instance_id TEXT PRIMARY KEY
            REFERENCES request_instances(request_instance_id) ON DELETE RESTRICT,
        retry_count INTEGER NOT NULL CHECK (retry_count >= 0),
        max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
        next_eligible_at TEXT,
        last_error_id TEXT REFERENCES errors(error_id) ON DELETE RESTRICT,
        updated_at TEXT NOT NULL,
        CHECK (retry_count <= max_attempts)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE raw_artifacts (
        artifact_id TEXT PRIMARY KEY,
        request_spec_id TEXT NOT NULL REFERENCES request_specs(request_spec_id) ON DELETE RESTRICT,
        page_ordinal INTEGER NOT NULL CHECK (page_ordinal >= 0),
        page_relation_hash TEXT NOT NULL CHECK (length(page_relation_hash) = 64),
        content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
        byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
        media_type TEXT NOT NULL,
        content_encoding TEXT NOT NULL,
        relative_path TEXT NOT NULL UNIQUE CHECK (
            """
    + _relative_path_check("relative_path")
    + """
        ),
        manifest_relative_path TEXT NOT NULL UNIQUE CHECK (
            """
    + _relative_path_check("manifest_relative_path")
    + """
        ),
        first_persisted_at TEXT NOT NULL,
        verified_at TEXT,
        state TEXT NOT NULL CHECK (state IN ('PRESENT', 'VERIFIED', 'INVALID', 'PURGED')),
        UNIQUE (
            request_spec_id, page_ordinal, page_relation_hash,
            content_sha256, byte_count, media_type, content_encoding
        )
    )
    """,
    """
    CREATE TABLE attempt_artifact_observations (
        attempt_id TEXT NOT NULL REFERENCES request_attempts(attempt_id) ON DELETE RESTRICT,
        artifact_id TEXT NOT NULL REFERENCES raw_artifacts(artifact_id) ON DELETE RESTRICT,
        retrieved_at TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        safe_provider_request_id TEXT,
        PRIMARY KEY (attempt_id, artifact_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE calendar_snapshots (
        calendar_snapshot_id TEXT PRIMARY KEY,
        calendar_name TEXT NOT NULL,
        timezone_name TEXT NOT NULL,
        package_name TEXT NOT NULL,
        package_version TEXT NOT NULL,
        tzdata_version TEXT NOT NULL,
        session_start_date TEXT NOT NULL,
        session_end_date TEXT NOT NULL,
        schedule_checksum TEXT NOT NULL CHECK (
            length(schedule_checksum) = 71
            AND substr(schedule_checksum, 1, 7) = 'sha256:'
            AND substr(schedule_checksum, 8) NOT GLOB '*[^0-9a-f]*'
        ),
        generated_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        state TEXT NOT NULL CHECK (state IN ('CURRENT', 'STALE')),
        CHECK (session_start_date < session_end_date),
        UNIQUE (
            calendar_name, timezone_name, package_name, package_version, tzdata_version,
            session_start_date, session_end_date, schedule_checksum
        )
    )
    """,
    """
    CREATE TABLE calendar_sessions (
        calendar_snapshot_id TEXT NOT NULL
            REFERENCES calendar_snapshots(calendar_snapshot_id) ON DELETE RESTRICT,
        session_date TEXT NOT NULL,
        open_at TEXT NOT NULL,
        close_at TEXT NOT NULL,
        is_early_close INTEGER NOT NULL CHECK (is_early_close IN (0, 1)),
        expected_1d_count INTEGER NOT NULL CHECK (expected_1d_count IN (0, 1)),
        expected_5m_count INTEGER NOT NULL CHECK (expected_5m_count >= 0),
        PRIMARY KEY (calendar_snapshot_id, session_date),
        CHECK (open_at < close_at)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE batch_contexts (
        batch_context_id TEXT PRIMARY KEY,
        canonical_batch_id TEXT NOT NULL UNIQUE,
        request_spec_id TEXT NOT NULL REFERENCES request_specs(request_spec_id) ON DELETE RESTRICT,
        ordered_artifacts_hash TEXT NOT NULL CHECK (length(ordered_artifacts_hash) = 64),
        canonical_schema_version TEXT NOT NULL,
        normalizer_version TEXT NOT NULL,
        validator_version TEXT NOT NULL,
        calendar_snapshot_id TEXT
            REFERENCES calendar_snapshots(calendar_snapshot_id) ON DELETE RESTRICT,
        fixed_ingested_at TEXT NOT NULL,
        manifest_created_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE (
            request_spec_id, ordered_artifacts_hash, canonical_schema_version,
            normalizer_version, validator_version, calendar_snapshot_id
        )
    )
    """,
    """
    CREATE TABLE batch_context_artifacts (
        batch_context_id TEXT NOT NULL
            REFERENCES batch_contexts(batch_context_id) ON DELETE RESTRICT,
        artifact_id TEXT NOT NULL REFERENCES raw_artifacts(artifact_id) ON DELETE RESTRICT,
        ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
        PRIMARY KEY (batch_context_id, ordinal),
        UNIQUE (batch_context_id, artifact_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE batch_context_requests (
        batch_context_id TEXT NOT NULL
            REFERENCES batch_contexts(batch_context_id) ON DELETE RESTRICT,
        request_instance_id TEXT NOT NULL
            REFERENCES request_instances(request_instance_id) ON DELETE RESTRICT,
        linked_at TEXT NOT NULL,
        PRIMARY KEY (batch_context_id, request_instance_id)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE canonical_batches (
        canonical_batch_id TEXT PRIMARY KEY,
        batch_context_id TEXT NOT NULL UNIQUE
            REFERENCES batch_contexts(batch_context_id) ON DELETE RESTRICT,
        policy_snapshot_id TEXT NOT NULL
            REFERENCES policy_snapshots(policy_snapshot_id) ON DELETE RESTRICT,
        relative_path TEXT NOT NULL UNIQUE CHECK (
            """
    + _relative_path_check("relative_path")
    + """
        ),
        manifest_relative_path TEXT NOT NULL UNIQUE CHECK (
            """
    + _relative_path_check("manifest_relative_path")
    + """
        ),
        state TEXT NOT NULL CHECK (state IN ('PUBLISHED', 'VERIFIED', 'INVALID', 'PURGED')),
        row_count INTEGER NOT NULL CHECK (row_count >= 0),
        published_at TEXT NOT NULL,
        verified_at TEXT,
        invalidated_at TEXT,
        UNIQUE (canonical_batch_id, policy_snapshot_id)
    )
    """,
    """
    CREATE TABLE canonical_files (
        canonical_batch_id TEXT NOT NULL
            REFERENCES canonical_batches(canonical_batch_id) ON DELETE RESTRICT,
        file_ordinal INTEGER NOT NULL CHECK (file_ordinal >= 0),
        relative_path TEXT NOT NULL UNIQUE CHECK (
            """
    + _relative_path_check("relative_path")
    + """
        ),
        content_sha256 TEXT NOT NULL CHECK (length(content_sha256) = 64),
        byte_count INTEGER NOT NULL CHECK (byte_count >= 0),
        row_count INTEGER NOT NULL CHECK (row_count >= 0),
        interval_start TEXT,
        interval_end TEXT,
        schema_fingerprint TEXT NOT NULL,
        PRIMARY KEY (canonical_batch_id, file_ordinal),
        CHECK (interval_start IS NULL OR interval_end IS NULL OR interval_start < interval_end)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE canonical_batch_streams (
        canonical_batch_id TEXT NOT NULL
            REFERENCES canonical_batches(canonical_batch_id) ON DELETE RESTRICT,
        stream_id TEXT NOT NULL REFERENCES stream_keys(stream_id) ON DELETE RESTRICT,
        outcome TEXT NOT NULL CHECK (outcome IN ('PUBLISHABLE', 'BLOCKED')),
        row_count INTEGER NOT NULL CHECK (row_count >= 0),
        interval_start TEXT NOT NULL,
        interval_end TEXT NOT NULL,
        validation_summary_json TEXT NOT NULL,
        semantic_duplicate_count INTEGER NOT NULL DEFAULT 0 CHECK (semantic_duplicate_count >= 0),
        revision_count INTEGER NOT NULL DEFAULT 0 CHECK (revision_count >= 0),
        PRIMARY KEY (canonical_batch_id, stream_id),
        CHECK (interval_start < interval_end)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE canonical_batch_requests (
        canonical_batch_id TEXT NOT NULL
            REFERENCES canonical_batches(canonical_batch_id) ON DELETE RESTRICT,
        request_instance_id TEXT NOT NULL
            REFERENCES request_instances(request_instance_id) ON DELETE RESTRICT,
        policy_snapshot_id TEXT NOT NULL
            REFERENCES policy_snapshots(policy_snapshot_id) ON DELETE RESTRICT,
        linked_at TEXT NOT NULL,
        PRIMARY KEY (canonical_batch_id, request_instance_id),
        FOREIGN KEY (canonical_batch_id, policy_snapshot_id)
            REFERENCES canonical_batches(canonical_batch_id, policy_snapshot_id)
            ON DELETE RESTRICT
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE coverage_segments (
        coverage_id TEXT PRIMARY KEY,
        stream_id TEXT NOT NULL REFERENCES stream_keys(stream_id) ON DELETE RESTRICT,
        canonical_batch_id TEXT NOT NULL
            REFERENCES canonical_batches(canonical_batch_id) ON DELETE RESTRICT,
        calendar_snapshot_id TEXT NOT NULL
            REFERENCES calendar_snapshots(calendar_snapshot_id) ON DELETE RESTRICT,
        policy_snapshot_id TEXT NOT NULL
            REFERENCES policy_snapshots(policy_snapshot_id) ON DELETE RESTRICT,
        coverage_start TEXT NOT NULL,
        interval_start TEXT NOT NULL,
        interval_end TEXT NOT NULL,
        classification TEXT NOT NULL CHECK (classification IN ('OBSERVED', 'VERIFIED_EMPTY')),
        verification_state TEXT NOT NULL
            CHECK (verification_state IN ('VERIFIED', 'STALE', 'INVALID')),
        retained INTEGER NOT NULL CHECK (retained IN (0, 1)),
        row_count INTEGER NOT NULL CHECK (row_count >= 0),
        artifact_count INTEGER NOT NULL CHECK (artifact_count > 0),
        request_completed INTEGER NOT NULL CHECK (request_completed IN (0, 1)),
        pagination_verified INTEGER NOT NULL CHECK (pagination_verified IN (0, 1)),
        provider_semantics_version TEXT,
        generation INTEGER NOT NULL CHECK (generation > 0),
        verified_at TEXT NOT NULL,
        invalidated_at TEXT,
        CHECK (coverage_start <= interval_start),
        CHECK (interval_start < interval_end),
        CHECK (
            (classification = 'OBSERVED' AND row_count > 0)
            OR (classification = 'VERIFIED_EMPTY' AND row_count = 0)
        ),
        CHECK (
            classification <> 'VERIFIED_EMPTY'
            OR (
                request_completed = 1
                AND pagination_verified = 1
                AND provider_semantics_version IS NOT NULL
                AND length(provider_semantics_version) > 0
            )
        ),
        UNIQUE (stream_id, interval_start, interval_end, canonical_batch_id),
        FOREIGN KEY (canonical_batch_id, stream_id)
            REFERENCES canonical_batch_streams(canonical_batch_id, stream_id) ON DELETE RESTRICT,
        FOREIGN KEY (canonical_batch_id, policy_snapshot_id)
            REFERENCES canonical_batches(canonical_batch_id, policy_snapshot_id)
            ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE gaps (
        gap_id TEXT PRIMARY KEY,
        stream_id TEXT NOT NULL REFERENCES stream_keys(stream_id) ON DELETE RESTRICT,
        interval_start TEXT NOT NULL,
        interval_end TEXT NOT NULL,
        gap_type TEXT NOT NULL CHECK (gap_type IN (
            'ACQUISITION', 'INTEGRITY', 'EXPECTED_OBSERVATION',
            'CORRECTION', 'CALENDAR_STALE'
        )),
        status TEXT NOT NULL CHECK (status IN ('OPEN', 'REPAIRING', 'RESOLVED', 'INVALIDATED')),
        blocking INTEGER NOT NULL CHECK (blocking IN (0, 1)),
        detected_at TEXT NOT NULL,
        resolved_at TEXT,
        request_instance_id TEXT
            REFERENCES request_instances(request_instance_id) ON DELETE RESTRICT,
        canonical_batch_id TEXT REFERENCES canonical_batches(canonical_batch_id) ON DELETE RESTRICT,
        CHECK (interval_start < interval_end),
        UNIQUE (stream_id, interval_start, interval_end, gap_type)
    )
    """,
    """
    CREATE TABLE watermarks (
        stream_id TEXT PRIMARY KEY REFERENCES stream_keys(stream_id) ON DELETE RESTRICT,
        coverage_start TEXT NOT NULL,
        exclusive_frontier TEXT NOT NULL,
        verification_state TEXT NOT NULL
            CHECK (verification_state IN ('VERIFIED', 'STALE', 'INVALID')),
        generation INTEGER NOT NULL CHECK (generation > 0),
        calendar_snapshot_id TEXT NOT NULL
            REFERENCES calendar_snapshots(calendar_snapshot_id) ON DELETE RESTRICT,
        policy_snapshot_id TEXT NOT NULL
            REFERENCES policy_snapshots(policy_snapshot_id) ON DELETE RESTRICT,
        last_run_id TEXT NOT NULL REFERENCES ingestion_runs(run_id) ON DELETE RESTRICT,
        last_batch_id TEXT NOT NULL
            REFERENCES canonical_batches(canonical_batch_id) ON DELETE RESTRICT,
        last_verified_session TEXT NOT NULL,
        blocking_gap_count INTEGER NOT NULL CHECK (blocking_gap_count >= 0),
        computed_at TEXT NOT NULL,
        invalidated_at TEXT,
        CHECK (coverage_start <= exclusive_frontier)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE provider_budget_state (
        provider TEXT NOT NULL,
        dataset TEXT NOT NULL,
        budget_key TEXT NOT NULL,
        window_start TEXT NOT NULL,
        window_end TEXT NOT NULL,
        limit_count INTEGER NOT NULL CHECK (limit_count >= 0),
        used_count INTEGER NOT NULL CHECK (used_count >= 0),
        reserved_count INTEGER NOT NULL CHECK (reserved_count >= 0),
        observed_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (provider, dataset, budget_key, window_start),
        CHECK (window_start < window_end),
        CHECK (used_count + reserved_count <= limit_count)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE purge_runs (
        purge_run_id TEXT PRIMARY KEY,
        provider TEXT NOT NULL,
        dataset TEXT NOT NULL,
        policy_snapshot_id TEXT REFERENCES policy_snapshots(policy_snapshot_id) ON DELETE RESTRICT,
        reason TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN (
            'PLANNED', 'UNAVAILABLE', 'DELETING', 'SUCCESS', 'FAILED'
        )),
        created_at TEXT NOT NULL,
        completed_at TEXT
    )
    """,
    """
    CREATE TABLE purge_targets (
        purge_run_id TEXT NOT NULL REFERENCES purge_runs(purge_run_id) ON DELETE RESTRICT,
        target_type TEXT NOT NULL CHECK (target_type IN (
            'RAW_ARTIFACT', 'CANONICAL_BATCH', 'CANONICAL_FILE', 'QUARANTINE'
        )),
        target_id TEXT NOT NULL,
        relative_path TEXT NOT NULL CHECK (
            """
    + _relative_path_check("relative_path")
    + """
        ),
        deletion_status TEXT NOT NULL CHECK (deletion_status IN (
            'PLANNED', 'DELETED', 'ABSENT', 'FAILED'
        )),
        deleted_at TEXT,
        PRIMARY KEY (purge_run_id, target_type, target_id),
        UNIQUE (purge_run_id, relative_path)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE writer_leases (
        lease_name TEXT PRIMARY KEY CHECK (lease_name = 'ingestion-writer'),
        owner_id TEXT NOT NULL,
        generation INTEGER NOT NULL CHECK (generation > 0),
        state TEXT NOT NULL CHECK (state IN ('ACTIVE', 'RELEASED')),
        acquired_at TEXT NOT NULL,
        heartbeat_at TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        released_at TEXT,
        previous_owner_id TEXT,
        CHECK (heartbeat_at <= expires_at)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE writer_lease_events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        lease_name TEXT NOT NULL REFERENCES writer_leases(lease_name) ON DELETE RESTRICT,
        event_type TEXT NOT NULL CHECK (event_type IN (
            'ACQUIRED', 'RENEWED', 'RELEASED', 'STALE_TAKEOVER'
        )),
        owner_id TEXT NOT NULL,
        previous_owner_id TEXT,
        generation INTEGER NOT NULL CHECK (generation > 0),
        occurred_at TEXT NOT NULL
    )
    """,
    """
    CREATE INDEX ingestion_runs_status_idx ON ingestion_runs(status, created_at)
    """,
    """
    CREATE INDEX request_instances_status_idx ON request_instances(status, created_at)
    """,
    """
    CREATE INDEX request_attempts_request_idx
        ON request_attempts(request_instance_id, attempt_number)
    """,
    """
    CREATE INDEX errors_run_time_idx ON errors(run_id, occurred_at)
    """,
    """
    CREATE INDEX raw_artifacts_request_idx ON raw_artifacts(request_spec_id, page_ordinal)
    """,
    """
    CREATE INDEX canonical_batches_state_idx ON canonical_batches(state, published_at)
    """,
    """
    CREATE INDEX coverage_stream_interval_idx
        ON coverage_segments(stream_id, interval_start, interval_end, verification_state, retained)
    """,
    """
    CREATE INDEX gaps_stream_status_idx ON gaps(stream_id, status, interval_start)
    """,
    """
    CREATE TRIGGER ingestion_runs_status_transition
    BEFORE UPDATE OF status ON ingestion_runs
    FOR EACH ROW WHEN NEW.status <> OLD.status
    BEGIN
        SELECT CASE WHEN NOT (
            (OLD.status = 'PLANNED' AND NEW.status IN ('RUNNING', 'FAILED', 'CANCELLED'))
            OR (OLD.status = 'RUNNING' AND NEW.status IN (
                'SUCCESS', 'PARTIAL', 'FAILED', 'CANCELLED'
            ))
        ) THEN RAISE(ABORT, 'invalid ingestion run status transition') END;
    END
    """,
    """
    CREATE TRIGGER request_instances_status_transition
    BEFORE UPDATE OF status ON request_instances
    FOR EACH ROW WHEN NEW.status <> OLD.status
    BEGIN
        SELECT CASE WHEN NOT (
            (OLD.status = 'PLANNED' AND NEW.status IN (
                'DISPATCHING', 'RETRY_WAIT', 'FAILED', 'BLOCKED', 'CANCELLED'
            ))
            OR (OLD.status = 'DISPATCHING' AND NEW.status IN (
                'ACQUIRING', 'RETRY_WAIT', 'FAILED', 'CANCELLED'
            ))
            OR (OLD.status = 'ACQUIRING' AND NEW.status IN (
                'RAW_COMPLETE', 'RETRY_WAIT', 'FAILED', 'CANCELLED'
            ))
            OR (OLD.status = 'RAW_COMPLETE' AND NEW.status IN (
                'PROCESSING', 'RETRY_WAIT', 'FAILED'
            ))
            OR (OLD.status = 'PROCESSING' AND NEW.status IN (
                'SUCCESS', 'PARTIAL', 'RETRY_WAIT', 'FAILED', 'BLOCKED'
            ))
            OR (OLD.status = 'RETRY_WAIT' AND NEW.status IN (
                'DISPATCHING', 'FAILED', 'CANCELLED'
            ))
        ) THEN RAISE(ABORT, 'invalid request status transition') END;
    END
    """,
    """
    CREATE TRIGGER request_attempts_status_transition
    BEFORE UPDATE OF status ON request_attempts
    FOR EACH ROW WHEN NEW.status <> OLD.status
    BEGIN
        SELECT CASE WHEN NOT (
            (OLD.status = 'PLANNED' AND NEW.status IN ('RUNNING', 'ABORTED'))
            OR (OLD.status = 'RUNNING' AND NEW.status IN (
                'RAW_COMPLETE', 'RETRYABLE_FAILED', 'FATAL_FAILED', 'ABORTED'
            ))
            OR (OLD.status = 'RAW_COMPLETE' AND NEW.status IN ('SUCCESS', 'FATAL_FAILED'))
        ) THEN RAISE(ABORT, 'invalid attempt status transition') END;
    END
    """,
    """
    CREATE TRIGGER canonical_batches_state_transition
    BEFORE UPDATE OF state ON canonical_batches
    FOR EACH ROW WHEN NEW.state <> OLD.state
    BEGIN
        SELECT CASE WHEN NOT (
            (OLD.state = 'PUBLISHED' AND NEW.state IN ('VERIFIED', 'INVALID', 'PURGED'))
            OR (OLD.state = 'VERIFIED' AND NEW.state IN ('INVALID', 'PURGED'))
            OR (OLD.state = 'INVALID' AND NEW.state = 'PURGED')
        ) THEN RAISE(ABORT, 'invalid canonical batch state transition') END;
    END
    """,
)


_MIGRATION_2_STATEMENTS: Final[tuple[str, ...]] = (
    """
    CREATE TABLE policy_snapshot_provenance (
        policy_snapshot_id TEXT PRIMARY KEY
            REFERENCES policy_snapshots(policy_snapshot_id) ON DELETE RESTRICT,
        policy_status TEXT NOT NULL CHECK (policy_status = 'ACTIVE'),
        verified_on TEXT NOT NULL,
        recorded_at TEXT NOT NULL
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE policy_catalog_snapshots (
        catalog_snapshot_id TEXT PRIMARY KEY,
        catalog_id TEXT NOT NULL,
        catalog_revision INTEGER NOT NULL CHECK (catalog_revision > 0),
        catalog_hash TEXT NOT NULL CHECK (
            length(catalog_hash) = 64
            AND catalog_hash NOT GLOB '*[^0-9a-f]*'
        ),
        captured_at TEXT NOT NULL,
        UNIQUE (catalog_id, catalog_revision, catalog_hash)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE ingestion_plan_records (
        run_id TEXT PRIMARY KEY REFERENCES ingestion_runs(run_id) ON DELETE RESTRICT,
        plan_hash TEXT NOT NULL CHECK (
            length(plan_hash) = 64
            AND plan_hash NOT GLOB '*[^0-9a-f]*'
        ),
        planner_contract_version INTEGER NOT NULL CHECK (planner_contract_version = 1),
        calendar_snapshot_id TEXT NOT NULL
            REFERENCES calendar_snapshots(calendar_snapshot_id) ON DELETE RESTRICT,
        calendar_snapshot_checksum TEXT NOT NULL CHECK (
            length(calendar_snapshot_checksum) = 71
            AND substr(calendar_snapshot_checksum, 1, 7) = 'sha256:'
            AND substr(calendar_snapshot_checksum, 8) NOT GLOB '*[^0-9a-f]*'
        ),
        policy_snapshot_id TEXT NOT NULL
            REFERENCES policy_snapshots(policy_snapshot_id) ON DELETE RESTRICT,
        catalog_snapshot_id TEXT NOT NULL
            REFERENCES policy_catalog_snapshots(catalog_snapshot_id) ON DELETE RESTRICT,
        acquisition_strategy TEXT NOT NULL CHECK (acquisition_strategy = 'NETWORK'),
        repair_strategy TEXT CHECK (repair_strategy IN ('MISSING_ONLY', 'PROVIDER_REFRESH')),
        repair_reason TEXT,
        reason TEXT NOT NULL CHECK (length(reason) BETWEEN 1 AND 256),
        max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
        desired_start TEXT NOT NULL,
        desired_end TEXT NOT NULL,
        safe_end TEXT NOT NULL,
        authorized_at TEXT NOT NULL,
        eligible_slot_count INTEGER NOT NULL CHECK (eligible_slot_count >= 0),
        eligible_observation_count INTEGER NOT NULL CHECK (eligible_observation_count >= 0),
        missing_observation_count INTEGER NOT NULL CHECK (missing_observation_count >= 0),
        pending_observation_count INTEGER NOT NULL CHECK (pending_observation_count >= 0),
        estimated_pages INTEGER NOT NULL CHECK (estimated_pages >= 0),
        estimated_calls INTEGER NOT NULL CHECK (estimated_calls >= 0),
        estimated_bytes INTEGER NOT NULL CHECK (estimated_bytes >= 0),
        estimated_cost TEXT NOT NULL CHECK (length(estimated_cost) BETWEEN 1 AND 128),
        lease_owner_id TEXT NOT NULL,
        lease_generation INTEGER NOT NULL CHECK (lease_generation > 0),
        recorded_at TEXT NOT NULL,
        CHECK (desired_start < desired_end),
        CHECK (missing_observation_count = pending_observation_count),
        CHECK (missing_observation_count <= eligible_observation_count),
        CHECK (
            (repair_strategy IS NULL AND repair_reason IS NULL)
            OR (repair_strategy IS NOT NULL AND length(repair_reason) BETWEEN 1 AND 256)
        ),
        UNIQUE (run_id, plan_hash)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE ingestion_plan_streams (
        run_id TEXT NOT NULL REFERENCES ingestion_plan_records(run_id) ON DELETE RESTRICT,
        stream_id TEXT NOT NULL REFERENCES stream_keys(stream_id) ON DELETE RESTRICT,
        ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
        PRIMARY KEY (run_id, stream_id),
        UNIQUE (run_id, ordinal)
    ) WITHOUT ROWID
    """,
    """
    CREATE TABLE request_plan_estimates (
        request_instance_id TEXT PRIMARY KEY
            REFERENCES request_instances(request_instance_id) ON DELETE RESTRICT,
        policy_snapshot_id TEXT NOT NULL
            REFERENCES policy_snapshots(policy_snapshot_id) ON DELETE RESTRICT,
        calendar_snapshot_id TEXT NOT NULL
            REFERENCES calendar_snapshots(calendar_snapshot_id) ON DELETE RESTRICT,
        expected_slot_count INTEGER NOT NULL CHECK (expected_slot_count > 0),
        expected_observation_count INTEGER NOT NULL CHECK (expected_observation_count > 0),
        estimated_pages INTEGER NOT NULL CHECK (estimated_pages > 0),
        estimated_calls INTEGER NOT NULL CHECK (estimated_calls > 0),
        estimated_bytes INTEGER NOT NULL CHECK (estimated_bytes > 0),
        estimated_cost TEXT NOT NULL CHECK (length(estimated_cost) BETWEEN 1 AND 128),
        first_slot_start TEXT NOT NULL,
        last_slot_end TEXT NOT NULL,
        authorization_eligible_before TEXT NOT NULL,
        authorized_at TEXT NOT NULL,
        CHECK (first_slot_start < last_slot_end),
        CHECK (last_slot_end < authorization_eligible_before)
    ) WITHOUT ROWID
    """,
    """
    CREATE INDEX ingestion_plan_calendar_policy_idx
        ON ingestion_plan_records(
            calendar_snapshot_id, policy_snapshot_id, catalog_snapshot_id, recorded_at
        )
    """,
    """
    CREATE UNIQUE INDEX request_spec_provider_identifier_unique_idx
        ON request_spec_streams(request_spec_id, provider_identifier)
    """,
)


MIGRATIONS: Final[tuple[Migration, ...]] = (
    Migration(version=1, name="initial_operational_state", statements=_MIGRATION_1_STATEMENTS),
    Migration(
        version=2,
        name="durable_ingestion_plan_provenance",
        statements=_MIGRATION_2_STATEMENTS,
    ),
)
LATEST_SCHEMA_VERSION: Final = MIGRATIONS[-1].version


__all__ = ["LATEST_SCHEMA_VERSION", "MIGRATIONS", "Migration"]
