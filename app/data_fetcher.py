# app/data_fetcher.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
import requests
import os, json

PZT_URL = "https://projectzerothree.info/api.php?format=json"

def save_snapshot(payload: any, folder: str = "./snapshots") -> str:
    os.makedirs(folder, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(folder, f"projectzerothree_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path

def fetch_prices(timeout: int = 15) -> Any:
    """
    Fetch raw JSON from Project Zero Three API.
    Depending on the API, this may be a list or dict – we handle both.
    """
    resp = requests.get(PZT_URL, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    # Try a few common formats; fall back to None on failure
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def normalize_pzt(payload: Any) -> Iterable[Dict[str, Any]]:
    """
    Yield normalized dicts for DB insert from ProjectZeroThree payload.
    Because the API shape can evolve, we read fields defensively.
    Update the key mapping below once you confirm exact keys.
    """
    # If payload is a dict with an items array
    if isinstance(payload, dict):
        # guess common container keys
        for key in ("items", "data", "results", "prices"):
            if key in payload and isinstance(payload[key], list):
                payload = payload[key]
                break

    if not isinstance(payload, list):
        # If it’s still not a list, just stop; caller can inspect raw in the UI
        return []

    for item in payload:
        # Try a few probable field names; adjust once you inspect a sample row in your UI.
        state = item.get("state") or item.get("State") or item.get("region")
        brand = item.get("brand") or item.get("Brand")
        station_name = item.get("name") or item.get("station") or item.get("SiteName")
        address = item.get("address") or item.get("Address")
        lat = item.get("lat") or item.get("latitude")
        lng = item.get("lng") or item.get("longitude")
        fuel = item.get("fuel") or item.get("FuelType") or item.get("type")
        price = item.get("price") or item.get("Price")
        currency = item.get("currency") or "AUD"
        station_id = (
            item.get("station_id") or item.get("id") or item.get("SiteId") or item.get("StationCode")
        )
        updated = item.get("last_updated") or item.get("Updated") or item.get("timestamp")

        yield {
            "source": "projectzerothree",
            "ext_station_id": str(station_id) if station_id is not None else None,
            "state": str(state).upper() if state else None,
            "brand": brand,
            "station_name": station_name,
            "address": address,
            "lat": float(lat) if lat not in (None, "") else None,
            "lng": float(lng) if lng not in (None, "") else None,
            "fuel_type": str(fuel).upper() if fuel else "UNKNOWN",
            "price": float(price) if price not in (None, "") else 0.0,
            "currency": currency,
            "source_updated": _parse_dt(updated),
            # fetched_at is auto-set in the DB model
        }