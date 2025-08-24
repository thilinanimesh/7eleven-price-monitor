# app/processor.py
from __future__ import annotations
from typing import Iterable, List, Dict, Any


def filter_by_state(rows: Iterable[dict], state: str) -> List[dict]:
    state = state.upper()
    return [r for r in rows if (r.get("state") or "").upper() == state]


def filter_by_fuel(rows: Iterable[dict], fuel: str) -> List[dict]:
    fuel = fuel.upper()
    return [r for r in rows if (r.get("fuel_type") or "").upper() == fuel]


def top_n_cheapest(rows: Iterable[dict], n: int = 20) -> List[dict]:
    return sorted(rows, key=lambda r: float(r.get("price") or 0.0))[:n]
