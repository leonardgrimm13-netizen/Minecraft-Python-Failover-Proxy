"""Minecraft Python Failover Proxy."""

from __future__ import annotations

import re
from importlib import metadata
from pathlib import Path

_DISTRIBUTION_NAME = "minecraft-failover-proxy"
_UNKNOWN_VERSION = "0+unknown"
_VERSION_LINE = re.compile(
    r"""version\s*=\s*(?P<quote>["'])(?P<version>[^"']+)(?P=quote)(?:\s+#.*)?"""
)


def _version_from_pyproject(path: Path) -> str:
    """Read the static project version when running an uninstalled source checkout."""

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return _UNKNOWN_VERSION
    in_project_table = False
    for line in lines:
        stripped = line.strip()
        if stripped == "[project]":
            in_project_table = True
            continue
        if in_project_table and stripped.startswith("["):
            break
        if in_project_table and (match := _VERSION_LINE.fullmatch(stripped)) is not None:
            return match.group("version")
    return _UNKNOWN_VERSION


def _resolve_version(*, pyproject: Path | None = None) -> str:
    try:
        return metadata.version(_DISTRIBUTION_NAME)
    except metadata.PackageNotFoundError:
        source_pyproject = pyproject or Path(__file__).resolve().parents[2] / "pyproject.toml"
        return _version_from_pyproject(source_pyproject)


__version__ = _resolve_version()

__all__ = ["__version__"]
