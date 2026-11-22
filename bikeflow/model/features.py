"""C1: model features for the Poisson regressions (spec section 7, C1)."""

import holidays
import polars as pl

# Fixed 37 feature columns, in this exact order (spec C1).
HOUR_COLUMNS = [f"hour_{h}" for h in range(24)]
DOW_COLUMNS = [f"dow_{d}" for d in range(7)]  # Monday = 0
FEATURE_COLUMNS = [
    *HOUR_COLUMNS,
    *DOW_COLUMNS,
    "is_holiday",
    "temperature_c",
    "precipitation_mm",
    "is_rain",
    "log_mean_pickups",
    "log_mean_dropoffs",
]

WEATHER_COLUMNS = ["temperature_c", "precipitation_mm"]


def _holiday_dates(df: pl.DataFrame) -> list:
    """Return US holiday dates (holidays.US()) for every year present in df."""
    years = df["hour_start"].dt.year().unique().to_list()
    return list(holidays.US(years=years).keys())


def build_features(df: pl.DataFrame, station_means: pl.DataFrame) -> pl.DataFrame:
    """C1: build the model feature table.

    Input:
        df: columns station_code, hour_start, temperature_c, precipitation_mm
            (one row per station x hour to predict or train on).
        station_means: columns station_code, mean_pickups, mean_dropoffs
            (training-period averages per station, from C2).
    Output:
        DataFrame with columns station_code, hour_start + FEATURE_COLUMNS,
        all features Float64, no nulls. Stations missing from station_means are
        dropped (not predicted). Rows with missing weather are dropped too, so
        the output never has nulls.
    """
    base = (
        df.select("station_code", "hour_start", *WEATHER_COLUMNS)
        .drop_nulls(WEATHER_COLUMNS)
        # Inner join: stations without training means are dropped (spec C1).
        .join(
            station_means.select("station_code", "mean_pickups", "mean_dropoffs"),
            on="station_code",
            how="inner",
        )
    )

    hour = pl.col("hour_start").dt.hour()
    dow = pl.col("hour_start").dt.weekday() - 1  # polars: Monday = 1 -> 0
    holiday_dates = _holiday_dates(base) if base.height else []

    features = base.with_columns(
        # Hour of day and day of week as one-hot columns.
        *[(hour == h).cast(pl.Float64).alias(f"hour_{h}") for h in range(24)],
        *[(dow == d).cast(pl.Float64).alias(f"dow_{d}") for d in range(7)],
        pl.col("hour_start")
        .dt.date()
        .is_in(holiday_dates)
        .cast(pl.Float64)
        .alias("is_holiday"),
        pl.col("temperature_c").cast(pl.Float64),
        pl.col("precipitation_mm").cast(pl.Float64),
        (pl.col("precipitation_mm") > 0).cast(pl.Float64).alias("is_rain"),
        # +0.1 keeps the log finite for stations with a zero average.
        (pl.col("mean_pickups") + 0.1).log().alias("log_mean_pickups"),
        (pl.col("mean_dropoffs") + 0.1).log().alias("log_mean_dropoffs"),
    )

    return features.select("station_code", "hour_start", *FEATURE_COLUMNS).sort(
        "station_code", "hour_start"
    )
