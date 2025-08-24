import streamlit as st
from app.data_fetcher import fetch_prices

st.title("7-Eleven Fuel Price Monitor (Educational)")

if st.button("Fetch Prices"):
    data = fetch_prices()
    if data:
        st.json(data)
    else:
        st.error("Failed to fetch prices")