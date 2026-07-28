#!/usr/bin/env python3
"""Fail when built release archives contain local or generated workspace files."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

FORBIDDEN_DIRECTORIES = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "env",
    "htmlcov",
    "venv",
}


def forbidden_member(name: str) -> bool:
    parts = PurePosixPath(name).parts
    return any(
        part in FORBIDDEN_DIRECTORIES or part.endswith(".egg-info") for part in parts
    ) or any(
        part == ".coverage"
        or part.startswith(".coverage.")
        or part == "coverage.xml"
        or part.endswith((".log", ".pyc", ".pyo"))
        for part in parts
    )


def archive_members(path: Path) -> Iterable[str]:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            yield from archive.namelist()
        return
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, mode="r:gz") as archive:
            yield from archive.getnames()
        return
    raise ValueError(f"Unsupported release archive: {path}")


def validate_release_archives(dist: Path) -> tuple[Path, Path]:
    wheels = sorted(dist.glob("*.whl"))
    source_distributions = sorted(dist.glob("*.tar.gz"))
    if len(wheels) != 1 or len(source_distributions) != 1:
        raise RuntimeError(
            "Expected exactly one wheel and one source distribution, "
            f"found {len(wheels)} wheel(s) and {len(source_distributions)} sdist(s)"
        )
    archives = (wheels[0], source_distributions[0])
    violations = [
        f"{archive.name}:{member}"
        for archive in archives
        for member in archive_members(archive)
        if forbidden_member(member)
    ]
    if violations:
        raise RuntimeError("Forbidden release archive members:\n" + "\n".join(violations))
    return archives


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dist", nargs="?", type=Path, default=Path("dist"))
    args = parser.parse_args()
    archives = validate_release_archives(args.dist)
    print("Release archive hygiene OK:", ", ".join(path.name for path in archives))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
