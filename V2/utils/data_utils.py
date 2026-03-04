# utils/data_utils.py
from __future__ import annotations

import pandas as pd

from utils.paths import DATA_DIR


def load_extended_symbols(filename: str = "extended_symbols.csv") -> list[str]:
    """
    Lädt die erweiterte Symbolliste (~1781 Symbole NYSE/NASDAQ).
    Erwartet eine Spalte 'symbol' (case-insensitive).
    """
    path = DATA_DIR / filename
    df = pd.read_csv(path)

    # Spaltennamen normalisieren
    df.columns = [c.strip().lower() for c in df.columns]

    if "symbol" not in df.columns:
        raise ValueError(
            f"{filename} muss eine Spalte 'symbol' enthalten. Gefunden: {list(df.columns)}"
        )

    symbols = (
        df["symbol"]
        .astype(str)
        .str.strip()
        .str.upper()
        .dropna()
        .tolist()
    )

    # Duplikate entfernen, Reihenfolge beibehalten
    seen = set()
    unique = []
    for s in symbols:
        if s and s not in seen:
            seen.add(s)
            unique.append(s)

    return unique
