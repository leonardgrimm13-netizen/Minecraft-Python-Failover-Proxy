#!/usr/bin/env python3
"""Backward-compatible script entrypoint."""

import sys
from pathlib import Path

source_root = Path(__file__).resolve().parent / "src"
if source_root.is_dir():
    sys.path.insert(0, str(source_root))

from mc_failover.cli import main  # noqa: E402 -- source-checkout compatibility

if __name__ == "__main__":
    main()
