"""Clock helpers that keep duration and wall-clock time separate."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Protocol

UTC = timezone.utc


class Clock(Protocol):
    """Injectable clock used by state machines and monitoring payloads."""

    def monotonic(self) -> float:
        """Return a monotonic value suitable for durations."""

    def utc_now(self) -> datetime:
        """Return the current timezone-aware UTC wall-clock time."""


class SystemClock:
    """Production clock implementation."""

    def monotonic(self) -> float:
        return time.monotonic()

    def utc_now(self) -> datetime:
        return datetime.now(UTC)


SYSTEM_CLOCK = SystemClock()


def as_utc(value: datetime) -> datetime:
    """Normalize a datetime to UTC and reject ambiguous naive values."""

    if value.tzinfo is None:
        raise ValueError("UTC timestamps must be timezone-aware")
    return value.astimezone(UTC)


def format_utc(value: datetime | None) -> str | None:
    """Render a timestamp as stable ISO-8601 with a trailing ``Z``."""

    if value is None:
        return None
    normalized = as_utc(value)
    return normalized.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def non_negative_elapsed(now: float, then: float | None) -> float | None:
    """Calculate an elapsed duration without exposing negative clock artefacts."""

    if then is None:
        return None
    return max(0.0, now - then)
