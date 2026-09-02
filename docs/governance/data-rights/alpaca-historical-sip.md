# Alpaca historical SIP US stock-bar rights record

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
| Primary dataset classification | `DURABLE_AUTHORIZED` |
| Evidence reference | `alpaca-support-ticket-342496` |
| Last verified | `2026-08-29` |
| Private archive state | `pending_manual_archive` |
| Archive state checked | `2026-09-01` |

The full evidence is deliberately excluded from Git. The Phase 2 root initializer creates this
private locator under the configured external data root:

```text
governance/evidence/alpaca/ticket-342496/
```

The initializer creates only an empty directory; it neither fabricates nor proves archival of the
evidence. The operator must place the actual original correspondence and any provenance metadata
there, following the [private operator procedure](../../operations/living-ingestion.md). Screenshots,
full email text, personal addresses, message headers, attachments, and other ticket data must not
be copied into this repository.

## Rights recorded from the response

For historical SIP US stock bars older than 15 minutes, within the account and use scope above, the
written response expressly permits:

- permanent retention of historical API responses;
- permanent retention of normalized copies, including OHLCV records and Parquet datasets;
- local use for private research;
- backtesting;
- reproducible research; and
- continued retention after cancellation or termination of the account or market-data
  subscription.

The evidence does not establish rights for historical options, crypto, real-time data, news, or any
other Alpaca dataset. No permission for those datasets may be inferred from the provider name, the
word “historical,” or the SIP stock-bar authorization.

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
| Historical options data | `UNVERIFIED / PENDING` | The evidence does not cover this dataset; no active policy entry. |
| Crypto data | `UNVERIFIED / PENDING` | The evidence does not cover historical or real-time crypto; no active policy entry. |
| Real-time or streaming data | No active entry | Fail closed before request or persistence until dataset-specific evidence is approved. |
| News | No active entry | Fail closed before request or persistence until dataset-specific evidence is approved. |
| Any other Alpaca dataset | No active entry | Fail closed until rights are classified from dataset-specific evidence. |

`DURABLE_AUTHORIZED` applies only to the historical SIP US stock-bar row above and is encoded by
the exact active Phase 2 retention policy. `UNVERIFIED / PENDING` is an evidence/policy status, not
a retention grant. This record alone does not grant runtime capabilities: code enforcement,
`private_research`, a validated external root, and the strict age/finalization gate must all agree.

The implemented policy encodes the 15-minute restriction plus a conservative finalization buffer
as a machine-enforced minimum observation age for backfill, incremental update, and repair.
Real-time or unexpectedly younger data is rejected before durable raw or quarantine persistence,
even when it arrives through a historical endpoint.

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

The private evidence locator is validated with the external root. At the M7 review it contained no
manually saved correspondence and no checksum manifest, only the non-evidentiary operational status
marker. The archive therefore remains `pending_manual_archive`; no evidence was invented. M6 AAPL
and the capped M7 mini rollout occurred under explicit scoped approval despite that pending archive.
This file remains a redacted governance input, not the evidence itself and not proof of live
provider behavior.
