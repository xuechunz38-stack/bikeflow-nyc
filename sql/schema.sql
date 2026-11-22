-- B2: tables from spec section 4.2
CREATE TABLE IF NOT EXISTS dim_station (
    station_code    TEXT PRIMARY KEY,
    gbfs_station_id TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    lat             DOUBLE PRECISION NOT NULL,
    lon             DOUBLE PRECISION NOT NULL,
    capacity        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_station_hour (
    station_code TEXT REFERENCES dim_station,
    hour_start   TIMESTAMP,
    pickups      INTEGER NOT NULL,
    dropoffs     INTEGER NOT NULL,
    PRIMARY KEY (station_code, hour_start)
);

CREATE TABLE IF NOT EXISTS weather_hour (
    hour_start       TIMESTAMP PRIMARY KEY,
    temperature_c    DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    wind_speed_kmh   DOUBLE PRECISION,
    is_forecast      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS station_status (
    gbfs_station_id      TEXT,
    last_reported        TIMESTAMP,
    fetched_at           TIMESTAMP NOT NULL,
    num_bikes_available  INTEGER NOT NULL,
    num_ebikes_available INTEGER,
    num_docks_available  INTEGER NOT NULL,
    is_renting           BOOLEAN NOT NULL,
    is_returning         BOOLEAN NOT NULL,
    PRIMARY KEY (gbfs_station_id, last_reported)
);

CREATE TABLE IF NOT EXISTS forecast_hour (
    station_code  TEXT REFERENCES dim_station,
    hour_start    TIMESTAMP,
    model_version TEXT,
    pred_pickups  DOUBLE PRECISION NOT NULL,
    pred_dropoffs DOUBLE PRECISION NOT NULL,
    created_at    TIMESTAMP NOT NULL,
    PRIMARY KEY (station_code, hour_start, model_version)
);
