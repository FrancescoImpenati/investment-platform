# Alpaca historical market-data rights record

## Record status

This document is a redacted engineering governance record. It is not legal advice, a copy of the
provider correspondence, or a substitute for the applicable Alpaca agreements and market-data
terms.

| Field | Recorded value |
| --- | --- |
| Provider | Alpaca |
| Evidence type | Written response from Alpaca Support |
| Support ticket | `342496` |
| Response date | `2026-08-29` |
| Account scope | Individual, non-professional user |
| Use scope | Private, educational, non-commercial, local research |
| Primary dataset | Historical SIP US stock bars older than 15 minutes |
| Governance classification | `DURABLE_AUTHORIZED` |
| Evidence reference | `alpaca-support-ticket-342496` |
| Last verified | `2026-08-29` |

The full evidence is deliberately excluded from Git. Its intended location under the future
external private data root is:

```text
governance/evidence/alpaca/ticket-342496/
```

That private location may contain the original correspondence and any sensitive metadata needed
to establish provenance. Screenshots, full email text, personal addresses, message headers,
attachments, and other ticket data must not be copied into this repository.

## Rights recorded from the response

Within the account and use scope above, the written response expressly permits:

- permanent retention of historical API responses;
- permanent retention of normalized copies, including OHLCV records and Parquet datasets;
- local use for private research;
- backtesting;
- reproducible research; and
- continued retention after cancellation or termination of the account or market-data
  subscription.

The same historical-data retention decision is recorded for Alpaca historical options data and
Alpaca historical crypto data within the identical individual, private, educational,
non-commercial scope. This extension does not expand the Phase 2 implementation scope: the first
persistent live ingestion remains limited to historical SIP US stock bars at `1d` and `5m`.

## Restrictions

The response does not permit:

- redistribution;
- resale; or
- public display of raw market data to third parties.

Repository fixtures, documentation, examples, reports, and other public artifacts therefore must
not contain licensed Alpaca market data unless a separate, documented redistribution right
applies. Aggregated or derived outputs require their own policy decision before publication.

## Dataset decisions

| Provider dataset | Decision | Engineering consequence |
| --- | --- | --- |
| Historical SIP US stock bars older than 15 minutes | `DURABLE_AUTHORIZED` | Raw and normalized data may be retained permanently only in the external private data root and within the recorded use scope. |
| Historical options data | `DURABLE_AUTHORIZED` | The right is recorded, but software policy remains inactive until an exact dataset key and eligibility rule are approved. |
| Historical crypto data | `DURABLE_AUTHORIZED` | The right is recorded, but software policy remains inactive until an exact dataset key and eligibility rule are approved. |
| Real-time or streaming data | Not authorized for durable retention | Fail closed: do not persist or create durable coverage state without dataset-specific evidence and a policy update. |
| Any other Alpaca dataset | Not authorized for durable retention | Fail closed until its rights are classified from dataset-specific evidence. |

`DURABLE_AUTHORIZED` is an input to the future retention-aware dataset policy. This record alone
does not implement enforcement, storage, ingestion, watermarking, or purge behavior.

The future policy entry must encode the 15-minute restriction as a machine-enforced minimum
observation age for backfill, incremental update, and repair. Real-time or unexpectedly younger
data must not be retained merely because it arrives through a historical endpoint.

## Caveats and review triggers

This record is based on the user-provided redacted summary of the support response. Its conclusions
are limited to the stated account category, purposes, locality, and non-commercial use. Reverify
the evidence and update the dataset policy before proceeding if any of the following changes
materially:

- the account becomes professional, organizational, shared, or commercial;
- data is exposed outside private local research;
- publication, display, redistribution, or resale is contemplated;
- the provider, entitlement, subscription, dataset, or delivery mode changes; or
- Alpaca's applicable agreements or market-data terms change.

The private evidence locator should be validated when the external data root is provisioned. Until
the Phase 2 policy enforcement is implemented and tested, this file remains an approved design and
governance input rather than proof that the running pipeline enforces these restrictions.
