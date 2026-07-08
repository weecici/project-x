# ADR-006: `src/utils/` Package for Shared Cross-Cutting Utilities

## Status

Accepted

## Context

As the platform grows across phases (ingestion → batch → streaming → ml → orchestration),
infrastructure helpers like structured logging configuration and retry decorators are
needed in every package. Initially, these lived inside `src/ingestion/utils/`, which
created a clear problem: other phases would either duplicate the code or import from
`ingestion`, creating an implicit and misleading dependency between unrelated layers.

## Decision

All cross-cutting infrastructure utilities live in `src/utils/` — a dedicated top-level
package inside `src/`, co-equal with each phase package:

```
src/
├── utils/          ← cross-cutting shared utilities
│   ├── logging.py  ← loguru configure_logging()
│   ├── retry.py    ← async_retry() decorator (tenacity)
│   └── storage.py  ← make_s3_client() factory (boto3)
├── ingestion/
├── batch/
└── ...
```

## Why `utils/` and not `core/`

In Python projects following Domain-Driven Design (DDD) patterns, `core/` is reserved
for domain models, business logic, and interfaces (ports in hexagonal architecture).
Logging, retry, and storage factories are **infrastructure concerns** — they do not
encode any business rules. Using `core/` for them would be semantically incorrect and
confusing for contributors familiar with DDD conventions.

`utils/` is the standard, universally understood Python convention for helper/utility
code, consistent with the stdlib's `pathlib`, `collections`, `functools` approach to
providing reusable tools.

## Modules

| Module | Purpose |
|---|---|
| `utils.logging` | `configure_logging()` — loguru JSON structured logging, called once at startup |
| `utils.retry` | `async_retry()` — tenacity exponential-jitter backoff decorator for async I/O |
| `utils.storage` | `make_s3_client()` — boto3 S3 client factory (SigV4, MinIO-compatible) |

## Consequences

- Every phase package imports from `utils.*` — no circular dependencies, no duplication.
- `src/ingestion/utils/` is deleted; all consumers updated.
- Adding new shared utilities (e.g. a metrics client, a schema registry helper) goes
  into `src/utils/` — a single, obvious location.
- `utils` is a namespace package (no explicit `__init__.py` content) — importing
  individual modules is preferred over `from utils import *`.
