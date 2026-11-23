"""C5: BikeFlow NYC Streamlit dashboard (all 6 parts of spec section 7, C5).

Parts 1-4 read the v_station_risk view (C4), part 5 reads fact_station_hour,
part 6 reads artifacts/metrics.json (C2). Run with:
    streamlit run dashboard/app.py
"""

import json
from pathlib import Path

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
# Columns shown in the hover box and in the part 4 table.
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
METRICS_PATH = Path("artifacts/metrics.json")

# Part 5: average pickups/dropoffs per hour of day over July-September.
HOURLY_PROFILE_SQL = """
    SELECT EXTRACT(HOUR FROM hour_start)::int AS hour,
           avg(pickups)::double precision AS avg_pickups,
           avg(dropoffs)::double precision AS avg_dropoffs
    FROM fact_station_hour
    WHERE station_code = %s
        AND hour_start >= '2026-07-01' AND hour_start < '2026-10-01'
    GROUP BY 1
    ORDER BY 1
"""


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


@st.cache_data(ttl=60)
def load_hourly_profile(station_code: str) -> pl.DataFrame:
    """C5 part 5: one station's average pickups/dropoffs for each hour of day.

    Input: station_code.
    Output: up to 24 rows with hour, avg_pickups, avg_dropoffs (cached 60 s).
    """
    with get_conn() as conn:
        rows = conn.execute(HOURLY_PROFILE_SQL, [station_code]).fetchall()
    return pl.DataFrame(
        rows,
        schema={
            "hour": pl.Int32,
            "avg_pickups": pl.Float64,
            "avg_dropoffs": pl.Float64,
        },
        orient="row",
    )


@st.cache_data(ttl=60)
def load_metrics() -> dict | None:
    """C5 part 6: read artifacts/metrics.json (written by C2); None if missing."""
    if not METRICS_PATH.exists():
        return None
    return json.loads(METRICS_PATH.read_text())


def map_records(df: pl.DataFrame) -> list[dict]:
    """C5: turn view rows into pydeck records.

    Input: v_station_risk rows.
    Output: one dict per drawable station (no_data dropped) with a color,
    timestamps as text and projections rounded to 2 decimals for the tooltip.
    """
    drawable = df.filter(pl.col("risk_level").is_in(list(RISK_COLORS)))
    drawable = drawable.with_columns(
        pl.col("snapshot_at").dt.strftime("%Y-%m-%d %H:%M"),
        pl.col("window_end").dt.strftime("%Y-%m-%d %H:%M"),
        pl.col("projected_bikes").round(2),
        pl.col("projected_docks").round(2),
    )
    records = drawable.select(["lat", "lon", "risk_level", *HOVER_COLUMNS]).to_dicts()
    for rec in records:
        rec["color"] = RISK_COLORS[rec["risk_level"]]
    return records


def risk_table(df: pl.DataFrame) -> pl.DataFrame:
    """C5 part 4: all empty_risk and full_risk stations, hover columns only.

    Output: sorted by projected_bikes ascending (most likely empty first).
    """
    return (
        df.filter(pl.col("risk_level").is_in(["empty_risk", "full_risk"]))
        .sort("projected_bikes")
        .select(HOVER_COLUMNS)
        .with_columns(
            pl.col("projected_bikes").round(2),
            pl.col("projected_docks").round(2),
        )
    )


def metrics_table(metrics: dict) -> pl.DataFrame:
    """C5 part 6: model vs baseline MAE, one row per target."""
    return pl.DataFrame(
        {
            "target": ["pickups", "dropoffs"],
            "model_mae": [metrics[t]["model_mae"] for t in ("pickups", "dropoffs")],
            "baseline_mae": [
                metrics[t]["baseline_mae"] for t in ("pickups", "dropoffs")
            ],
        }
    ).with_columns(pl.col("model_mae").round(3), pl.col("baseline_mae").round(3))


def main() -> None:
    """C5: render the dashboard page (parts 1-6, top to bottom)."""
    st.set_page_config(page_title="BikeFlow NYC", layout="wide")

    # Part 1: title
    st.title("BikeFlow NYC")

    df = load_risk()
    if df.is_empty():
        st.warning("v_station_risk has no rows yet. Load dim_station first.")
        return

    # Part 2: four metrics. The update time is the newest snapshot of any station;
    # each station's own forecast window is only shown in its hover box and table.
    counts = df["risk_level"].value_counts()
    count_of = dict(zip(counts["risk_level"].to_list(), counts["count"].to_list()))
    latest = df["snapshot_at"].max()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Likely out of bikes", count_of.get("empty_risk", 0))
    c2.metric("Likely out of docks", count_of.get("full_risk", 0))
    c3.metric("Normal", count_of.get("ok", 0))
    # Show only the clock time so it fits the column; the full timestamp is in
    # the hover help. Valid snapshots are always less than 60 minutes old.
    c4.metric(
        "Data updated at",
        latest.strftime("%H:%M") if latest is not None else "no data",
        help=(
            f"Newest snapshot: {latest:%Y-%m-%d %H:%M}" if latest is not None else None
        ),
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

    # Part 4: table of at-risk stations
    st.subheader("Stations at risk")
    table = risk_table(df)
    if table.is_empty():
        st.info("No station is currently at risk.")
    else:
        st.dataframe(table, hide_index=True, width="stretch")

    # Part 5: station detail (July-September hourly profile, 24 points)
    st.subheader("Station detail")
    stations = df.select("station_code", "name").sort("name")
    labels = {
        code: f"{name} ({code})"
        for code, name in zip(stations["station_code"], stations["name"])
    }
    code = st.selectbox(
        "Station", options=list(labels), format_func=lambda c: labels[c]
    )
    profile = load_hourly_profile(code)
    if profile.is_empty():
        st.info("No July-September history for this station.")
    else:
        st.caption("Average pickups and dropoffs per hour of day, July-September 2026")
        # Long format (hour, series, trips) for one line per series. A fixed
        # Vega-Lite spec without zoom/pan, so scrolling the page over the chart
        # cannot rescale the axes (st.line_chart charts are zoomable).
        chart = profile.unpivot(
            index="hour",
            on=["avg_pickups", "avg_dropoffs"],
            variable_name="series",
            value_name="trips",
        ).with_columns(
            pl.col("series").replace(
                {"avg_pickups": "Avg pickups", "avg_dropoffs": "Avg dropoffs"}
            )
        )
        st.vega_lite_chart(
            chart,
            {
                "mark": {"type": "line", "point": True},
                "encoding": {
                    "x": {
                        "field": "hour",
                        "type": "quantitative",
                        "title": "Hour of day",
                        "scale": {"domain": [0, 23]},
                        "axis": {"tickCount": 24, "format": "d"},
                    },
                    "y": {
                        "field": "trips",
                        "type": "quantitative",
                        "title": "Trips per hour",
                        "scale": {"zero": True},
                    },
                    "color": {"field": "series", "type": "nominal", "title": None},
                    "tooltip": [
                        {"field": "hour", "title": "Hour"},
                        {"field": "series", "title": "Series"},
                        {"field": "trips", "title": "Trips", "format": ".2f"},
                    ],
                },
            },
            width="stretch",
        )

    # Part 6: model performance from C2
    st.subheader("Model performance (September, MAE)")
    metrics = load_metrics()
    if metrics is None:
        st.info("artifacts/metrics.json not found. Run `make train` first.")
    else:
        st.dataframe(metrics_table(metrics), hide_index=True)
        st.caption(
            f"{metrics['eval_rows']:,} evaluation rows across "
            f"{metrics['eval_stations']:,} stations; "
            f"{metrics['excluded_new_stations']} new stations excluded."
        )


main()
