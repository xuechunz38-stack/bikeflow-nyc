-- C4: station risk view (spec section 2 "risk_level" rules, section 4.2 columns).
-- One row per dim_station station. The dashboard (C5) reads only this view.
-- "Now" is LOCALTIMESTAMP; the database timezone is America/New_York (compose).

CREATE OR REPLACE VIEW v_station_risk AS
WITH latest AS (
    -- Step 1: newest row per station by fetched_at.
    -- Validity is checked AFTER picking the newest row, so a station whose
    -- newest row is stale becomes no_data instead of falling back to an older row.
    SELECT DISTINCT ON (gbfs_station_id) *
    FROM station_status
    ORDER BY gbfs_station_id, fetched_at DESC, last_reported DESC
),

valid AS (
    -- Step 2: keep only valid snapshots and map them to station_code.
    SELECT
        d.station_code,
        l.last_reported AS snapshot_at,
        l.num_bikes_available AS bikes_now,
        l.num_docks_available AS docks_now
    FROM latest AS l
    INNER JOIN dim_station AS d ON d.gbfs_station_id = l.gbfs_station_id
    WHERE l.fetched_at >= LOCALTIMESTAMP - INTERVAL '30 minutes'
        AND l.last_reported >= LOCALTIMESTAMP - INTERVAL '60 minutes'
        AND l.is_renting
        AND l.is_returning
),

win AS (
    -- Step 3: forecast window. H0 = snapshot hour, H1 = H0 + 1h,
    -- window_end = H1 + 1h, r = share of H0 still left after the snapshot.
    SELECT
        v.*,
        date_trunc('hour', v.snapshot_at) AS h0,
        date_trunc('hour', v.snapshot_at) + INTERVAL '1 hour' AS h1,
        date_trunc('hour', v.snapshot_at) + INTERVAL '2 hours' AS window_end,
        (60 - EXTRACT(MINUTE FROM v.snapshot_at))::double precision / 60 AS r
    FROM valid AS v
),

calc AS (
    -- Step 4: join H0 and H1 forecasts. INNER JOIN drops stations missing
    -- either hour, so they end up as no_data in the final SELECT.
    SELECT
        w.station_code,
        w.snapshot_at,
        w.bikes_now,
        w.docks_now,
        w.window_end,
        w.r * f0.pred_pickups + f1.pred_pickups AS exp_pickups,
        w.r * f0.pred_dropoffs + f1.pred_dropoffs AS exp_dropoffs
    FROM win AS w
    INNER JOIN forecast_hour AS f0
        ON f0.station_code = w.station_code
        AND f0.hour_start = w.h0
        AND f0.model_version = 'poisson_v1'
    INNER JOIN forecast_hour AS f1
        ON f1.station_code = w.station_code
        AND f1.hour_start = w.h1
        AND f1.model_version = 'poisson_v1'
)

-- Step 5: every station gets a row. Stations not in calc get NULLs
-- (except the first 5 columns) and risk_level = no_data.
SELECT
    d.station_code,
    d.name,
    d.lat,
    d.lon,
    d.capacity,
    c.snapshot_at,
    c.bikes_now,
    c.docks_now,
    c.window_end,
    c.exp_pickups,
    c.exp_dropoffs,
    c.bikes_now - c.exp_pickups + c.exp_dropoffs AS projected_bikes,
    c.docks_now + c.exp_pickups - c.exp_dropoffs AS projected_docks,
    CASE
        WHEN c.station_code IS NULL THEN 'no_data'
        WHEN c.bikes_now - c.exp_pickups + c.exp_dropoffs <= 2 THEN 'empty_risk'
        WHEN c.docks_now + c.exp_pickups - c.exp_dropoffs <= 2 THEN 'full_risk'
        ELSE 'ok'
    END AS risk_level
FROM dim_station AS d
LEFT JOIN calc AS c ON c.station_code = d.station_code;
