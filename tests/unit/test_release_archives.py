from __future__ import annotations

import pytest
from tools.check_release_archives import forbidden_member


@pytest.mark.parametrize(
    "member",
    [
        "project/.git/config",
        "project/.venv/bin/python",
        "project/venv/bin/python",
        "project/env/bin/python",
        "project/src/mc_failover/__pycache__/cli.cpython-312.pyc",
        "project/.pytest_cache/README.md",
        "project/.mypy_cache/3.12/cache.json",
        "project/.ruff_cache/content",
        "project/.coverage",
        "project/.coverage.worker",
        "project/coverage.xml",
        "project/htmlcov/index.html",
        "project/build/lib/module.py",
        "project/dist/package.whl",
        "project/package.egg-info/PKG-INFO",
        "project/server.log",
    ],
)
def test_forbidden_release_archive_members(member: str) -> None:
    assert forbidden_member(member)


@pytest.mark.parametrize(
    "member",
    [
        "minecraft_failover_proxy-1.0.0/README.md",
        "minecraft_failover_proxy-1.0.0/src/mc_failover/cli.py",
        "minecraft_failover_proxy-1.0.0/tests/unit/test_cli.py",
        "minecraft_failover_proxy-1.0.0/config.example.toml",
    ],
)
def test_expected_release_archive_members_are_allowed(member: str) -> None:
    assert not forbidden_member(member)
