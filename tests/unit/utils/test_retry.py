"""Unit tests for the shared async_retry decorator."""

from __future__ import annotations

import pytest

from utils.retry import async_retry


@pytest.mark.asyncio
async def test_retry_succeeds_on_first_attempt() -> None:
    """Function that succeeds immediately should be called exactly once."""
    call_count = 0

    @async_retry(max_attempts=3, min_wait=0.01, max_wait=0.1)
    async def succeeds() -> str:
        nonlocal call_count
        call_count += 1
        return "ok"

    result = await succeeds()

    assert result == "ok"
    assert call_count == 1


@pytest.mark.asyncio
async def test_retry_retries_on_transient_failure() -> None:
    """Function that fails twice then succeeds should be called three times."""
    call_count = 0

    @async_retry(max_attempts=5, min_wait=0.01, max_wait=0.1)
    async def flaky() -> str:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("transient")
        return "recovered"

    result = await flaky()

    assert result == "recovered"
    assert call_count == 3


@pytest.mark.asyncio
async def test_retry_reraises_after_max_attempts() -> None:
    """Function that always fails should raise after exhausting max_attempts."""
    call_count = 0

    @async_retry(max_attempts=3, min_wait=0.01, max_wait=0.1)
    async def always_fails() -> None:
        nonlocal call_count
        call_count += 1
        raise ValueError("permanent failure")

    with pytest.raises(ValueError, match="permanent failure"):
        await always_fails()

    assert call_count == 3
