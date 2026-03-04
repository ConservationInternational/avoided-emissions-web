"""Shared bootstrap helpers for r-analysis Python entrypoints."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_scripts_dir_on_path(current_file: str) -> None:
    """Add r-analysis/scripts to ``sys.path`` for shared script helpers."""
    current_dir = Path(current_file).resolve().parent
    candidate_dirs = (current_dir / "scripts", current_dir.parent / "scripts")
    for scripts_dir in candidate_dirs:
        scripts_dir_str = str(scripts_dir)
        if scripts_dir.is_dir() and scripts_dir_str not in sys.path:
            sys.path.insert(0, scripts_dir_str)
            break
