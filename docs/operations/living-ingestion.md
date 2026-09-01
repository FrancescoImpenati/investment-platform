# Living ingestion operator guide

- **Status:** Offline control plane implemented; live operation and scheduling not activated
- **Last review:** 2026-09-01

This guide operates the approved Phase 2 control plane. It does not authorize a dataset, replace
the retention catalog, or mark live acceptance complete. Run `investment-platform --help` and the
relevant subcommand `--help` against the checked-out revision before constructing a command.

## 1. Runtime configuration

Private ingestion reads configuration only from the process environment:

~~~text
INVESTMENT_PLATFORM_ENV=private_research
INVESTMENT_PLATFORM_DATA_ROOT=<ABSOLUTE_DEDICATED_EXTERNAL_PATH>
APCA_API_KEY_ID=<injected secret>
APCA_API_SECRET_KEY=<injected secret>
~~~

Do not put credentials in command arguments, Git-tracked files, scheduler descriptions, logs, or
documentation. The application does not require or load a `.env` file. `test`, `ci`, and `demo`
are credential-free and cannot use a private root; `development` is offline by default and cannot
implicitly persist real provider data.

The root path must be absolute, local, dedicated, and physically separate from the repository. It
must not be a drive/filesystem root, the user home, a generic temporary location, a UNC path, the
repository, or an ancestor/descendant of the repository. Its parent must already exist. The target
itself must be nonexistent or empty on first initialization.

## 2. Intentional root initialization

After setting the two non-secret runtime variables, initialize the selected root exactly once:

~~~text
uv run --locked investment-platform data-root init
~~~

The command is idempotent only for the same valid sentinel-owned root. It refuses to adopt a
nonempty unowned directory. Successful initialization creates the sentinel, the approved managed
namespaces, and the empty locator:

~~~text
governance/evidence/alpaca/ticket-342496/
~~~

Every later mutation revalidates the root and sentinel. `.gitignore` is only a secondary accident
barrier and is not a substitute for this physical separation.

## 3. Private Alpaca evidence

The repository contains only the
[redacted rights record](../governance/data-rights/alpaca-historical-sip.md). The root initializer
does not copy, generate, or claim possession of the support response.

Place the actual private files, when available, only below:

~~~text
<PRIVATE_DATA_ROOT>/governance/evidence/alpaca/ticket-342496/
~~~

Recommended private contents are the original `.eml`, a PDF copy when one actually exists, and a
small local manifest recording filenames, acquisition provenance, and SHA-256 hashes. Compute each
hash from the saved file with an operating-system or trusted hashing tool and verify it after the
copy. Never invent an email, PDF, hash, timestamp, header, attachment, or personal datum. Do not
move any of those files or the private manifest into Git.

By default, an empty locator does not block offline software tests but keeps the controlled live
acceptance gate closed. The explicitly approved AAPL-only M6 acceptance is the sole scoped
exception: it may record `pending_manual_archive`, but does not authorize the 16-security rollout
or any broader live use. Do not report the evidence as archived until the files are genuinely
present and their hashes have been checked.

## 4. Manual control plane

The CLI is non-interactive and supports sanitized `--json` output. Its main command families are:

~~~text
investment-platform data-root init
investment-platform backfill
investment-platform update
investment-platform repair
investment-platform resume
investment-platform status
investment-platform verify
investment-platform retention enforce
~~~

For the approved live dataset, the exact catalog keys are `alpaca` and `price_bars_sip`. The only
initial live timeframes are `1d` and `5m`, the only session is `regular`, and adjustments are
limited to `unadjusted` and `split_adjusted`. Each ingestion command requires explicit provider
budgets. Bounds are aware ISO-8601 instants and represent half-open intervals `[start, end)`.

A bounded command shape is:

~~~text
uv run --locked investment-platform backfill \
  --provider alpaca --dataset price_bars_sip \
  --instrument AAPL --timeframe 5m --session regular --adjustment unadjusted \
  --start <AWARE_START> --end <AWARE_EXCLUSIVE_END> \
  --max-calls <LIMIT> --max-pages <LIMIT> \
  --max-expected-observations <LIMIT> --max-estimated-bytes <LIMIT> \
  --max-estimated-cost <LIMIT>
~~~

`update` omits `--start` and derives its start from a valid contiguous watermark; its optional
`--end` is still capped by policy and the Alpaca age/finalization gate. `repair` requires explicit
bounds, a non-sensitive reason, and one of `MISSING_ONLY`, `PROVIDER_REFRESH`, or `RAW_REPLAY`.
`resume --run-id <UUID>` resumes one durable run identity. Use `--json` for scheduler parsing, not
for exposing private values.

Successful exact replays or an update with no eligible missing interval return a meaningful no-op
without a provider call. The process exit codes are stable:

| Exit code | Meaning |
| --- | --- |
| 0 | Success or meaningful no-op |
| 2 | Invalid bounded command/usage |
| 3 | Missing or unsafe runtime configuration/root |
| 4 | Durable work remains incomplete and may be resumed |
| 5 | Operation failed |
| 6 | Integrity verification failed |

## 5. Status and verification

Run these before and after every controlled operation:

~~~text
uv run --locked investment-platform status
uv run --locked investment-platform verify
~~~

`status` reports sanitized aggregate operational state: environment/root validation, schema health,
dataset policy, stream dimensions, coverage, watermark, gaps, latest run/error categories, and
raw/canonical/Parquet counts. It does not print credentials or OHLCV values.

`verify` checks sentinel ownership, SQLite integrity, catalog/file consistency, raw and canonical
checksums, Parquet reopening, staging/orphan state, reconstructed coverage/watermark consistency,
and retention-policy consistency. A nonzero result must be investigated; do not weaken checks or
advance a watermark manually.

## 6. External scheduler templates — not activated

These are handoff templates only. Do not register or enable a schedule until manual AAPL and
bounded-sample live acceptance are complete.

### Windows Task Scheduler

Configure the task with:

- **Program:** the absolute path to `uv.exe`;
- **Start in:** the repository root;
- **Arguments:** `run --locked investment-platform update <NON_SECRET_BOUNDED_ARGUMENTS> --json`;
- **Account environment:** inject the profile, root, and Alpaca credentials for the dedicated task
  account through an approved local secret/environment mechanism.

Do not place secret values in the Arguments field, task name, description, exported task XML, or a
Git-tracked wrapper. Configure the task to treat exit codes 3–6 as failures requiring attention and
to prevent overlapping instances.

### cron

The crontab command shape is:

~~~text
<schedule> cd <REPOSITORY_ROOT> && <ABSOLUTE_UV_PATH> run --locked investment-platform update <NON_SECRET_BOUNDED_ARGUMENTS> --json
~~~

Do not write credentials into the crontab. Arrange for the scheduler account or an approved local
secret launcher to provide them in the child process environment. Prevent concurrent writers and
retain only sanitized stdout/stderr.

### Other local schedulers

Supply four explicit fields: repository working directory, absolute `uv` executable, non-secret
argument array, and securely injected process environment. Do not invoke a second internal daemon
or queue. Alert on nonzero exit codes, never retry unboundedly, and use the durable `resume` command
when status shows an incomplete run.

## 7. Live gate and stop conditions

The first authorized live scope is only Alpaca historical SIP US stock bars at 1d/5m, US RTH,
strictly older than 15 minutes plus the policy's finalization buffer. Options, crypto, real-time,
news, and all unlisted Alpaca datasets fail closed.

Before any live request, verify without printing values that the two Alpaca variables exist, the
explicit profile is `private_research`, the root is initialized and external, and the actual
ticket evidence is present in its private locator. If any item is absent, stop before provider
construction. The current offline checkpoint deliberately stops at this gate.
