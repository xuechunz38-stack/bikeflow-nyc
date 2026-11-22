"""C5: BikeFlow NYC Streamlit dashboard (skeleton: parts 1-3 of spec section 7).

Reads only the v_station_risk view (C4). Run with:
    streamlit run dashboard/app.py
"""

import polars as pl
import pydeck as pdk
import streamlit as st

from bikeflow.db import get_conn

# Map colors per risk level (spec C5 part 3). no_data stations are not drawn.
RISK_COLORS = {
    "empty_risk": [220, 38, 38],
    "full_risk": [37, 99, 235],
    "ok": [160, 160, 160],
}
# Columns shown in the hover box (and later in the part 4 table).
HOVER_COLUMNS = [
    "name",
    "snapshot_at",
    "window_end",
    "bikes_now",
    "docks_now",
    "projected_bikes",
    "projected_docks",
]
NYC_CENTER = {"latitude": 40.73, "longitude": -73.97}


@st.cache_data(ttl=60)
def load_risk() -> pl.DataFrame:
    """C5: read every row of v_station_risk.

    Input: none (connects through bikeflow.db.get_conn).
    Output: polars DataFrame with the 14 view columns; cached for 60 seconds.
    """
    with get_conn() as conn:
        cur = conn.execute("SELECT * FROM v_station_risk")
        names = [col.name for col in cur.description]
        rows = cur.fetchall()
    return pl.DataFrame(rows, schema=names, orient="row", infer_schema_length=None)


def map_records(df: pl.DataFrame) -> list[dict]:
    """C5: turn view rows into pydeck records.

    Input: v_station_risk rows.
    Output: one dict per drawable station (no_data dropped) with a color,
    timestamps as text and projections rounded to 1 decimal for the tooltip.
    """
    drawable = df.filter(pl.col("risk_level").is_in(list(RISK_COLORS)))
    drawable = drawable.with_columns(
        pl.col("snapshot_at").dt.strftime("%Y-%m-%d %H:%M"),
        pl.col("window_end").dt.strftime("%Y-%m-%d %H:%M"),
        pl.col("projected_bikes").round(1),
        pl.col("projected_docks").round(1),
    )
    records = drawable.select(["lat", "lon", "risk_level", *HOVER_COLUMNS]).to_dicts()
    for rec in records:
        rec["color"] = RISK_COLORS[rec["risk_level"]]
    return records


def main() -> None:
    """C5: render the dashboard page."""
    st.set_page_config(page_title="BikeFlow NYC", layout="wide")

    # Part 1: title
    st.title("BikeFlow NYC")

    df = load_risk()
    if df.is_empty():
        st.warning("v_station_risk has no rows yet. Load dim_station first.")
        return

    # Part 2: four metrics. The update time is the newest snapshot of any station;
    # each station's own forecast window is only shown in its hover box.
    counts = df["risk_level"].value_counts()
    count_of = dict(zip(counts["risk_level"].to_list(), counts["count"].to_list()))
    latest = df["snapshot_at"].max()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Likely out of bikes", count_of.get("empty_risk", 0))
    c2.metric("Likely out of docks", count_of.get("full_risk", 0))
    c3.metric("Normal", count_of.get("ok", 0))
    c4.metric(
        "Data updated at",
        latest.strftime("%Y-%m-%d %H:%M") if latest is not None else "no data",
    )

    # Part 3: station map
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_records(df),
        get_position=["lon", "lat"],
        get_fill_color="color",
        # Radius is in meters (deck.gl default). Do not pass radius_units as a
        # string: pydeck turns plain strings into expressions ("@@=meters").
        get_radius=40,
        pickable=True,
    )
    tooltip = {
        "html": (
            "<b>{name}</b><br/>"
            "Snapshot: {snapshot_at}<br/>"
            "Window end: {window_end}<br/>"
            "Bikes now: {bikes_now} | Docks now: {docks_now}<br/>"
            "Projected bikes: {projected_bikes} | "
            "Projected docks: {projected_docks}"
        )
    }
    view = pdk.ViewState(**NYC_CENTER, zoom=11)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view, tooltip=tooltip))
    st.caption(
        "Red: likely out of bikes · Blue: likely out of docks · Grey: normal. "
        "Stations without a valid snapshot or forecast are not drawn."
    )


main()
