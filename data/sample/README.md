# Sample data policy

This directory is the only repository location intended for committed data examples.

Allowed content:

- small, deterministic synthetic datasets generated for documentation or examples;
- small public datasets whose license explicitly permits redistribution;
- metadata that states origin, generation method, date, license, and any attribution requirement.

Not allowed:

- provider payloads copied from a private, trial, paid, or otherwise restricted account;
- real data whose redistribution rights have not been verified;
- API keys, authorization headers, account identifiers, signed URLs, or other secrets;
- large generated outputs, caches, full-universe downloads, or test-run artifacts.

Prefer tests that generate fixtures in a temporary directory. Add a committed sample only when it
improves a public example and cannot be represented clearly by a small inline fixture. Use stable
instrument UUIDs and UTC timestamps, and include provenance/licensing notes next to every sample.

No sample dataset is included in Phase 0.
