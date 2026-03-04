# utils/paths.py
from __future__ import annotations

from pathlib import Path


def get_project_root() -> Path:
    """
    Liefert den Projekt-Root,
    unabhängig davon, von wo aus das Modul gestartet wird.
    """
    # Diese Datei liegt in: <project>/utils/paths.py
    # -> parent von utils = Projektroot
    return Path(__file__).resolve().parents[1]


# Export: PROJECT_ROOT (für Imports, falls du es so nutzen willst)
PROJECT_ROOT: Path = get_project_root()

# Standard-Ordner im Projekt
CONFIG_DIR: Path = PROJECT_ROOT / "config"
DATA_DIR: Path = PROJECT_ROOT / "data"
LOGS_DIR: Path = PROJECT_ROOT / "logs"
OUTPUT_DIR: Path = PROJECT_ROOT / "output"
STATE_DIR: Path = PROJECT_ROOT / "state"


def ensure_dirs() -> None:
    """Legt alle Standardordner an, falls sie fehlen."""
    for p in (CONFIG_DIR, DATA_DIR, LOGS_DIR, OUTPUT_DIR, STATE_DIR):
        p.mkdir(parents=True, exist_ok=True)
