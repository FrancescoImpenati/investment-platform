# ADR 0006: External private data root for licensed runtime state

- **Status:** Accepted
- **Date:** 2026-08-31
- **Phase:** 2 — Living Data Ingestion
- **Implementation status:** Planned for Phase 2

## Context

The repository is intended to remain public and reproducible, but live ingestion produces licensed
market data, operational state, logs, quarantine material, and private governance evidence. A Git
ignore rule reduces accidental staging; it does not create a security or lifecycle boundary.

Phase 2 also needs safe recovery and atomic canonical-batch publication. Writes and cleanup must not
be able to escape into the repository, a user profile, a system directory, or another broad path
because of missing configuration, path traversal, links, or an unsafe default.

## Decision

`private_research` requires an explicitly configured, dedicated private data root. The future
configuration boundary will accept an absolute path, such as `INVESTMENT_PLATFORM_DATA_ROOT`, and
will fail closed when it is absent or invalid. Credentials remain environment variables and are
never fields in the root manifest.

Before initialization, the platform will resolve both the repository and proposed root to canonical
paths. It will reject a root that:

- is relative, the filesystem root, the user's home/profile, a default temporary directory, or a
  system directory;
- equals the repository or is an ancestor or descendant of it;
- is a UNC path, mapped/network filesystem, or other storage without the required local atomic
  rename semantics;
- contains a symlink, junction, mount, or other reparse-point escape that makes the resolved target
  differ from the validated local tree; or
- is otherwise too broad to identify as a dedicated platform-owned directory.

Initialization will place a sentinel, `.investment-platform-root.json`, directly under the root.
It will contain at least a format/schema version, a platform marker, an immutable root UUID, the
canonical path recorded at initialization, and a creation timestamp. It contains no credentials.
An existing non-empty directory without a valid matching sentinel is not adopted implicitly.

Every mutation and deletion will revalidate the root, sentinel, canonical path, and target. The
resolved target must remain beneath an allowed platform-managed namespace in the validated root.
Missing, malformed, moved, or mismatched sentinels; link escapes; and path traversal stop the
operation before mutation. Destructive operations may target only explicitly managed artifacts,
never the root through an unresolved variable or broad recursive path.

Directories are created lazily rather than as empty repository packages. The initial namespaces
are:

```text
<PRIVATE_DATA_ROOT>/
├── .investment-platform-root.json
├── raw/
├── normalized/
├── curated/
├── features/
├── operational/
├── logs/
├── quarantine/
├── staging/
└── governance/
    └── evidence/
```

`staging/` is explicit because canonical output must be completed and verified before publication.
It is kept on the same local filesystem as its destination so publication can use an atomic rename.
`features/` reserves private, reproducible future outputs; it does not authorize feature execution
in Phase 2. Private evidence, including full provider correspondence, belongs under
`governance/evidence/`, outside Git.

Test and CI storage uses framework-created temporary directories and synthetic fixtures, without
adopting the private root. Demo mode never reads the private root. Development can opt into only a
bounded ephemeral live preflight; durable real-data persistence requires `private_research`.
`.gitignore` remains a secondary defense, not the isolation mechanism.

## Consequences

- Licensed data and durable operational state are physically separated from source and fixtures.
- Unsafe or ambiguous configuration prevents live persistence instead of choosing a convenient
  default.
- Atomic directory publication and crash recovery can rely on one local filesystem boundary.
- Moving a root requires an explicit, validated administrative procedure because the sentinel's
  canonical path must not silently change.
- Filesystem access controls, backups, capacity monitoring, and eventual secure purge remain local
  operator responsibilities; the sentinel is an identity and safety control, not encryption.

## Alternatives considered

- **Ignored directories inside the repository:** rejected because Git configuration is not a
  physical isolation or deletion-safety boundary.
- **An arbitrary user-configured path without a sentinel:** rejected because a typo or stale
  environment variable could redirect destructive operations to unrelated data.
- **A default under the home or temporary directory:** rejected because it is broad, easy to clean
  accidentally, and incompatible with explicit private-research setup.
- **Network storage or object storage:** deferred because Phase 2 is single-machine and its commit
  protocol relies on local filesystem behavior.
