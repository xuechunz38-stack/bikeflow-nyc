"""C2: train and evaluate the two Poisson regressions (spec section 7, C2).

Called by `make train` (not part of Airflow).
"""

import io
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import joblib
import polars as pl
from sklearn.linear_model import PoissonRegressor
from sklearn.metrics import mean_absolute_error

from bikeflow.db import get_conn
from bikeflow.model.features import FEATURE_COLUMNS, build_features

MODEL_VERSION = "poisson_v1"
NY = ZoneInfo("America/New_York")
ARTIFACTS = Path("artifacts")
TEST_START = datetime.fromisoformat("2026-09-01T00:00")
SAMPLE_ROWS = 1_000_000
SEED = 42
TARGETS = ["pickups", "dropoffs"]

# Step a: station-hour counts joined with historical weather for July-September.
# The inner join plus the NOT NULL filters drop hours with missing weather.
HISTORY_SQL = """
    SELECT f.station_code, f.hour_start, f.pickups, f.dropoffs,
           w.temperature_c, w.precipitation_mm
    FROM fact_station_hour AS f
    JOIN weather_hour AS w
        ON w.hour_start = f.hour_start AND w.is_forecast = false
    WHERE f.hour_start >= '2026-07-01' AND f.hour_start < '2026-10-01'
        AND w.temperature_c IS NOT NULL AND w.precipitation_mm IS NOT NULL
"""


def read_history() -> pl.DataFrame:
    """C2 step a: read the training/evaluation history from Postgres.

    Uses COPY ... TO STDOUT (CSV) because it is much faster than fetching
    ~5 million rows as Python tuples.
    Output: station_code, hour_start, pickups, dropoffs, temperature_c,
    precipitation_mm.
    """
    buf = io.BytesIO()
    copy_sql = f"COPY ({HISTORY_SQL}) TO STDOUT (FORMAT csv, HEADER)"
    with get_conn() as conn, conn.cursor() as cur, cur.copy(copy_sql) as cp:
        for chunk in cp:
            buf.write(chunk)
    buf.seek(0)
    return pl.read_csv(
        buf,
        schema={
            "station_code": pl.Utf8,
            "hour_start": pl.Datetime("us"),
            "pickups": pl.Int32,
            "dropoffs": pl.Int32,
            "temperature_c": pl.Float64,
            "precipitation_mm": pl.Float64,
        },
    )


def station_means_from(train: pl.DataFrame) -> pl.DataFrame:
    """C2 step c: average pickups/dropoffs per station over the training set."""
    return train.group_by("station_code").agg(
        pl.col("pickups").mean().alias("mean_pickups"),
        pl.col("dropoffs").mean().alias("mean_dropoffs"),
    )


def _baseline_keys(df: pl.DataFrame) -> pl.DataFrame:
    """Add the baseline grouping keys: hour of day and weekend flag."""
    return df.with_columns(
        pl.col("hour_start").dt.hour().alias("hod"),
        (pl.col("hour_start").dt.weekday() >= 6).alias("is_weekend"),
    )


def baseline_predictions(train: pl.DataFrame, evals: pl.DataFrame) -> pl.DataFrame:
    """C2 step f: baseline = training mean per (station, hour, weekend flag).

    Input: training rows and evaluation rows (both with station_code, hour_start).
    Output: evals with base_pickups and base_dropoffs added, same row order.
    The station-hour grid is complete, so every eval group exists in training;
    the station mean is only a safety net for an empty group.
    """
    keys = ["station_code", "hod", "is_weekend"]
    table = (
        _baseline_keys(train)
        .group_by(keys)
        .agg(
            pl.col("pickups").mean().alias("base_pickups"),
            pl.col("dropoffs").mean().alias("base_dropoffs"),
        )
    )
    out = (
        _baseline_keys(evals)
        .with_row_index("_row")
        .join(table, on=keys, how="left")
        .join(station_means_from(train), on="station_code", how="left")
        .with_columns(
            pl.col("base_pickups").fill_null(pl.col("mean_pickups")),
            pl.col("base_dropoffs").fill_null(pl.col("mean_dropoffs")),
        )
        .sort("_row")
    )
    return out.drop("_row", "hod", "is_weekend", "mean_pickups", "mean_dropoffs")


def write_forecasts(preds: pl.DataFrame) -> int:
    """Upsert prediction rows into forecast_hour (shared by C2 and C3).

    Input: DataFrame with station_code, hour_start, pred_pickups, pred_dropoffs.
    Output: number of rows written. Uses COPY into a temp table, then
    INSERT ... ON CONFLICT DO UPDATE, so re-runs never create duplicates.
    """
    rows = preds.select(
        "station_code",
        "hour_start",
        pl.lit(MODEL_VERSION).alias("model_version"),
        "pred_pickups",
        "pred_dropoffs",
        # created_at is New York local time without tz (spec section 2).
        pl.lit(datetime.now(NY).replace(tzinfo=None, microsecond=0)).alias(
            "created_at"
        ),
    )
    cols = ", ".join(rows.columns)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE tmp_fc (LIKE forecast_hour) ON COMMIT DROP")
        with cur.copy(f"COPY tmp_fc ({cols}) FROM STDIN (FORMAT csv)") as cp:
            for batch in rows.iter_slices(200_000):
                buf = io.BytesIO()
                batch.write_csv(buf, include_header=False)
                cp.write(buf.getvalue())
        cur.execute(
            f"INSERT INTO forecast_hour ({cols}) SELECT {cols} FROM tmp_fc "
            "ON CONFLICT (station_code, hour_start, model_version) DO UPDATE SET "
            "pred_pickups = EXCLUDED.pred_pickups, "
            "pred_dropoffs = EXCLUDED.pred_dropoffs, "
            "created_at = EXCLUDED.created_at"
        )
        return cur.rowcount


def train() -> dict:
    """C2: train both Poisson models, compare with the baseline, save artifacts.

    Input: none (reads fact_station_hour + weather_hour from Postgres).
    Output: the metrics dict, also saved as artifacts/metrics.json. Also writes
    artifacts/station_means.parquet, poisson_pickups.joblib,
    poisson_dropoffs.joblib, and the September evaluation predictions into
    forecast_hour (model_version poisson_v1, used by A7 query 5).
    """
    ARTIFACTS.mkdir(exist_ok=True)

    # Steps a-b: read history, split by time (July-August train, September test).
    history = read_history()
    train_df = history.filter(pl.col("hour_start") < TEST_START)
    test_df = history.filter(pl.col("hour_start") >= TEST_START)

    # Step c: per-station training means.
    means = station_means_from(train_df)
    means.write_parquet(ARTIFACTS / "station_means.parquet")

    # Step d: evaluation rows = September rows of stations seen in training.
    known = means["station_code"]
    evals = test_df.filter(pl.col("station_code").is_in(known.implode()))
    excluded = test_df.filter(~pl.col("station_code").is_in(known.implode()))

    # Step e: fit both models on a 1,000,000-row random sample of the training set.
    sample = train_df.sample(n=min(SAMPLE_ROWS, train_df.height), seed=SEED)
    x_train = build_features(sample, means)
    y_train = sample.join(
        x_train.select("station_code", "hour_start"),
        on=["station_code", "hour_start"],
        how="semi",
    ).sort("station_code", "hour_start")
    x_eval = build_features(evals, means)
    y_eval = evals.sort("station_code", "hour_start")
    # Features and targets must line up row by row.
    assert x_train.height == y_train.height and x_eval.height == y_eval.height

    models = {}
    for target in TARGETS:
        model = PoissonRegressor(alpha=1e-4, max_iter=300)
        model.fit(x_train.select(FEATURE_COLUMNS).to_numpy(), y_train[target])
        models[target] = model
        joblib.dump(model, ARTIFACTS / f"poisson_{target}.joblib")

    x_eval_np = x_eval.select(FEATURE_COLUMNS).to_numpy()
    preds = x_eval.select("station_code", "hour_start").with_columns(
        pl.Series("pred_pickups", models["pickups"].predict(x_eval_np)),
        pl.Series("pred_dropoffs", models["dropoffs"].predict(x_eval_np)),
    )

    # Steps f-g: baseline and MAE on the same evaluation rows.
    base = baseline_predictions(train_df, y_eval)
    metrics = {
        target: {
            "model_mae": float(
                mean_absolute_error(y_eval[target], preds[f"pred_{target}"])
            ),
            "baseline_mae": float(
                mean_absolute_error(y_eval[target], base[f"base_{target}"])
            ),
        }
        for target in TARGETS
    }
    metrics.update(
        {
            "train_rows": x_train.height,
            "eval_rows": x_eval.height,
            "eval_stations": x_eval["station_code"].n_unique(),
            "excluded_new_stations": excluded["station_code"].n_unique(),
        }
    )

    # Step h: metrics file. Step i: September predictions into forecast_hour.
    (ARTIFACTS / "metrics.json").write_text(json.dumps(metrics, indent=2))
    write_forecasts(preds)
    return metrics


if __name__ == "__main__":
    print(json.dumps(train(), indent=2))
