# ADR-008: `httpx` as the HTTP Client for Binance REST Backfill

## Status

Accepted

## Context

The Phase 2 backfill fetches years of historical kline data from `GET /api/v3/klines`.
This is a long-running, paginated async operation that must handle:

- Rate-limit headers (`X-MBX-USED-WEIGHT-1M`)
- HTTP 429 / 5xx transient errors (via the shared `async_retry` decorator)
- Connection timeouts (Binance has variable latency)
- Clean session reuse across thousands of sequential requests

Three candidate HTTP clients were evaluated: `requests`, `aiohttp`, and `httpx`.

## Decision

Use **`httpx`** with `httpx.AsyncClient`.

## Comparison

| Criterion | `requests` | `aiohttp` | `httpx` ✅ |
|---|---|---|---|
| Native async support | ❌ blocking | ✅ | ✅ |
| Sync API also available | ✅ | ❌ | ✅ |
| Test mocking (no real network) | `responses` | `aioresponses` | `pytest-httpx` (first-class) |
| Header access | `response.headers` | `response.headers` | `response.headers` |
| Connection pooling | ❌ (per-request) | ✅ (session) | ✅ (client) |
| HTTP/2 support | ❌ | ❌ | ✅ (optional) |
| Type annotations | partial | partial | full |
| `raise_for_status()` | ✅ | ✅ | ✅ |

## Why Not `requests`

`requests` is synchronous. The backfill loop is `async def` to integrate with the
existing async architecture (`asyncio.run`, `async_retry`). Using `requests` would
require `asyncio.to_thread` wrappers — unnecessary complexity.

## Why Not `aiohttp`

`aiohttp` is mature but its testing story is weaker: `aioresponses` requires manual
fixture registration and does not integrate as cleanly with pytest as `pytest-httpx`.
`httpx`'s `pytest-httpx` plugin provides a first-class fixture (`httpx_mock`) that
intercepts all requests transparently — making unit tests cleaner and more reliable.

## Consequences

- `httpx` added to `[project.dependencies]` in `pyproject.toml`.
- `pytest-httpx` added to `[dependency-groups.dev]`.
- All unit tests for `binance_rest.py` use `pytest-httpx` mocks — zero real network calls.
- Session is created once per `backfill_symbol_interval` call and shared across all
  paginated chunk requests — minimises TCP connection overhead.
