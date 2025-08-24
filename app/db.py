# app/db.py
from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional

from sqlalchemy import (
    create_engine, String, Integer, Float, DateTime, Numeric, UniqueConstraint, select, text
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
import os

DB_PATH = os.getenv("FUEL_DB_URL", "sqlite:///./data.db")


class Base(DeclarativeBase):
    pass


class Price(Base):
    """
    A normalized price row from any source.
    We deduplicate using (source, ext_station_id, fuel_type, source_updated),
    so repeated fetches don’t balloon the DB.
    """
    __tablename__ = "prices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(50))                  # e.g., 'projectzerothree', 'refinery'
    ext_station_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    brand: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    station_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    address: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    lat: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    lng: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fuel_type: Mapped[str] = mapped_column(String(16))
    price: Mapped[float] = mapped_column(Numeric(10, 3))             # store as decimal for money-ish values
    currency: Mapped[str] = mapped_column(String(8), default="AUD")
    source_updated: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        UniqueConstraint(
            "source", "ext_station_id", "fuel_type", "source_updated",
            name="uq_source_station_fuel_time"
        ),
    )


def get_engine():
    # Ensure directory exists for SQLite file
    if DB_PATH.startswith("sqlite") and "///" in DB_PATH:
        folder = os.path.dirname(DB_PATH.split("///", 1)[1])
        if folder and not os.path.exists(folder):
            os.makedirs(folder, exist_ok=True)
    engine = create_engine(DB_PATH, echo=False, future=True)
    return engine


def init_db():
    engine = get_engine()
    Base.metadata.create_all(engine)
    return engine


def upsert_prices(rows: Iterable[dict]) -> int:
    """
    Upsert a batch of normalized rows (dicts matching Price columns).
    Returns number of rows inserted/updated.
    """
    engine = get_engine()
    count = 0
    with Session(engine) as session:
        for row in rows:
            stmt = sqlite_insert(Price).values(**row)
            # ON CONFLICT DO NOTHING (or update selected fields if you prefer)
            stmt = stmt.on_conflict_do_update(
                index_elements=["source", "ext_station_id", "fuel_type", "source_updated"],
                set_={
                    "price": stmt.excluded.price,
                    "brand": stmt.excluded.brand,
                    "station_name": stmt.excluded.station_name,
                    "address": stmt.excluded.address,
                    "lat": stmt.excluded.lat,
                    "lng": stmt.excluded.lng,
                    "fetched_at": datetime.utcnow(),
                },
            )
            session.execute(stmt)
            count += 1
        session.commit()
    return count


def query_prices(
    state: Optional[str] = None,
    fuel_type: Optional[str] = None,
    start: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: int = 5000,
) -> list[Price]:
    engine = get_engine()
    with Session(engine) as session:
        q = select(Price).order_by(Price.fetched_at.desc())
        if state:
            q = q.filter(Price.state == state)
        if fuel_type:
            q = q.filter(Price.fuel_type == fuel_type)
        if start:
            q = q.filter(Price.fetched_at >= start)
        if end:
            q = q.filter(Price.fetched_at <= end)
        q = q.limit(limit)
        return list(session.scalars(q))
