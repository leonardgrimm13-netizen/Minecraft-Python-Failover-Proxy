from __future__ import annotations

from importlib import metadata
from pathlib import Path

import pytest

import mc_failover


def test_installed_and_source_metadata_versions_match() -> None:
    project_root = Path(__file__).resolve().parents[2]

    assert mc_failover.__version__ == metadata.version("minecraft-failover-proxy")
    assert (
        mc_failover._version_from_pyproject(project_root / "pyproject.toml")
        == mc_failover.__version__
    )


def test_metadata_version_takes_precedence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    requested: list[str] = []

    def fake_version(distribution_name: str) -> str:
        requested.append(distribution_name)
        return "9.8.7"

    monkeypatch.setattr(metadata, "version", fake_version)

    assert mc_failover._resolve_version(pyproject=tmp_path / "missing.toml") == "9.8.7"
    assert requested == ["minecraft-failover-proxy"]


def test_uninstalled_checkout_reads_only_project_version(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        """
[build-system]
version = "ignored"

[project]
name = "minecraft-failover-proxy"
version = "2.3.4" # canonical

[project.optional-dependencies]
version = ["also-ignored"]
""".strip(),
        encoding="utf-8",
    )

    def missing_distribution(_distribution_name: str) -> str:
        raise metadata.PackageNotFoundError

    monkeypatch.setattr(metadata, "version", missing_distribution)

    assert mc_failover._resolve_version(pyproject=pyproject) == "2.3.4"


@pytest.mark.parametrize("contents", [None, "[project]\nname = 'missing-version'\n"])
def test_source_version_fallback_is_explicit_for_missing_or_invalid_metadata(
    tmp_path: Path,
    contents: str | None,
) -> None:
    pyproject = tmp_path / "pyproject.toml"
    if contents is not None:
        pyproject.write_text(contents, encoding="utf-8")

    assert mc_failover._version_from_pyproject(pyproject) == "0+unknown"
