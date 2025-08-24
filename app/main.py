# app/main.py
from __future__ import annotations

import streamlit as st
import pandas as pd
import os, json

from datetime import datetime, timedelta
from datetime import datetime
from app.data_fetcher import fetch_prices, normalize_pzt, save_snapshot
from app.db import init_db, upsert_prices, query_prices
from app.processor import filter_by_state, filter_by_fuel, top_n_cheapest

st.set_page_config(page_title="Fuel Price Monitor", layout="wide")
st.title("⛽️ Fuel Price Monitor (Educational)")

# Ensure DB exists
init_db()

# --- Sidebar controls ---
st.sidebar.header("Controls")
state = st.sidebar.selectbox(
    "State",
    ["ANY", "NSW", "VIC", "QLD", "SA", "WA", "TAS", "ACT", "NT"],
    index=0,
)
fuel = st.sidebar.selectbox("Fuel Type", ["ANY", "U91", "U95", "U98", "E10", "Diesel"], index=0)
limit = st.sidebar.slider("Show top N cheapest", min_value=10, max_value=200, value=50, step=10)

col1, col2, col3 = st.columns([1, 1, 2])

with col1:
    if st.button("Fetch latest (ProjectZeroThree)"):
        try:
            raw = fetch_prices()
            st.session_state["raw_payload"] = raw

            # Save snapshot to disk
            path = save_snapshot(raw)
            st.caption(f"Saved snapshot → {path}")

            # Provide an in-browser download of the same JSON
            with open(path, "r", encoding="utf-8") as f:
                #json_text = f.read()  # already pretty-printed
            
                json_text = json.dumps(raw, ensure_ascii=False, indent=2)
                st.download_button(
                    "⬇️ Download this snapshot (JSON)",
                    data=json_text,
                    file_name=f"projectzerothree_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                )

            st.success("Fetched latest data.")
        except Exception as e:
            st.error(f"Fetch error: {e}")

with col2:
    if st.button("Normalize & Save to DB"):
        raw = st.session_state.get("raw_payload")
        if not raw:
            st.warning("Fetch data first.")
        else:
            rows = list(normalize_pzt(raw))
            # Apply sidebar pre-filters during save if desired
            if state != "ANY":
                rows = filter_by_state(rows, state)
            if fuel != "ANY":
                rows = filter_by_fuel(rows, fuel)
            
            saved = upsert_prices(rows)
            st.success(f"Saved/updated {saved} rows.")

with col3:
    st.write("")


# --- Show fetched (raw) preview ---
st.subheader("Latest Fetch (Raw Preview)")
raw = st.session_state.get("raw_payload")
if raw is not None:
    # If list, show small sample; if dict, show dict
    if isinstance(raw, list):
        st.json(raw[: min(len(raw), 25)])  # show up to 25 items
    else:
        st.json(raw)


# --- History / Query section ---
st.subheader("History (from SQLite)")
with st.expander("Query stored history", expanded=True):
    left, right = st.columns(2)
    with left:
        q_state = st.selectbox(
            "State filter",
            ["ANY", "NSW", "VIC", "QLD", "SA", "WA", "TAS", "ACT", "NT"],
            index=0,
            key="q_state",
        )
        q_fuel = st.selectbox(
            "Fuel filter",
            ["ANY", "U91", "U95", "U98", "E10", "Diesel"],
            index=0,
            key="q_fuel",
        )
    with right:
        default_start = datetime.utcnow() - timedelta(days=7)
        start_dt = st.date_input("Start date (UTC)", default_start).strftime("%Y-%m-%d")
        end_dt = st.date_input("End date (UTC)", datetime.utcnow()).strftime("%Y-%m-%d")

    if st.button("Load history"):
        qstate = None if q_state == "ANY" else q_state
        qfuel = None if q_fuel == "ANY" else q_fuel
        start = datetime.strptime(start_dt, "%Y-%m-%d")
        end = datetime.strptime(end_dt, "%Y-%m-%d") + timedelta(days=1)

        rows = query_prices(state=qstate, fuel_type=qfuel, start=start, end=end, limit=10000)
        # Convert ORM objects to dicts
        records = [
            {
                "when": r.fetched_at,
                "state": r.state,
                "brand": r.brand,
                "station_name": r.station_name,
                "address": r.address,
                "fuel": r.fuel_type,
                "price": float(r.price),
                "updated": r.source_updated,
                "lat": r.lat,
                "lng": r.lng,
                "source": r.source,
                "ext_station_id": r.ext_station_id,
            }
            for r in rows
        ]
        if not records:
            st.info("No records found for that query.")
        else:
            df = pd.DataFrame.from_records(records)
            st.dataframe(df, use_container_width=True)

            # Chart: cheapest over time (group by day)
            if "price" in df and "when" in df:
                df["day"] = pd.to_datetime(df["when"]).dt.date
                agg = df.groupby(["day", "fuel"])["price"].min().reset_index()
                st.subheader("Cheapest price per day")
                st.line_chart(agg.pivot(index="day", columns="fuel", values="price"))