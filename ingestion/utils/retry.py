"""Retry and backoff decorators for I/O-bound async operations.

Uses tenacity's async support for exponential backoff with jitter.
Import ``async_retry`` and apply it to any ``async def`` that may
encounter transient I/O failures (network, Kafka, S3).
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import Any, TypeVar

from loguru import logger
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter

F = TypeVar("F", bound=Callable[..., Any])

_DEFAULT_MAX_ATTEMPTS = 5
_DEFAULT_MIN_WAIT = 1.0
_DEFAULT_MAX_WAIT = 60.0


def async_retry(
    *,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    min_wait: float = _DEFAULT_MIN_WAIT,
    max_wait: float = _DEFAULT_MAX_WAIT,
) -> Callable[[F], F]:
    """Decorate an async function with exponential-backoff retry logic.

    On each failed attempt the decorator logs a warning with the attempt
    number and exception. After ``max_attempts`` the original exception
    is re-raised.

    Args:
        max_attempts: Maximum number of total attempts (including the
            first call). Must be >= 1.
        min_wait: Minimum wait time in seconds before the first retry.
        max_wait: Maximum wait time in seconds between retries (cap).

    Returns:
        A decorator that wraps the target async function.

    Example::

        @async_retry(max_attempts=10, min_wait=2, max_wait=60)
        async def connect_to_upstream() -> None:
            ...
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:  # noqa: ANN401
            attempt_number = 0
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential_jitter(initial=min_wait, max=max_wait),
                reraise=True,
            ):
                with attempt:
                    attempt_number += 1
                    try:
                        return await func(*args, **kwargs)
                    except Exception as exc:
                        logger.warning(
                            "Attempt {n}/{max} of {func} failed | error={err}",
                            n=attempt_number,
                            max=max_attempts,
                            func=func.__qualname__,
                            err=str(exc),
                        )
                        raise

        return wrapper  # type: ignore[return-value]

    return decorator
