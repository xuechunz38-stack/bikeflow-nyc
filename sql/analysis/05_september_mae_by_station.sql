-- A7 query 5
-- Question: In September 2026 (the test month), which 10 stations does the
-- pickup model predict worst? Rank stations by pickup MAE
-- (mean absolute error between pred_pickups and actual pickups).
-- Data: forecast_hour (model_version = 'poisson_v1', written by C2 for the
-- September evaluation rows) joined to fact_station_hour.
-- Only hours that have both a prediction and an actual value are counted.

WITH errors AS (
    SELECT
        f.station_code,
        ABS(p.pred_pickups - f.pickups) AS abs_error,
        f.pickups
    FROM forecast_hour AS p
    JOIN fact_station_hour AS f
        ON f.station_code = p.station_code
       AND f.hour_start = p.hour_start
    WHERE p.model_version = 'poisson_v1'
      AND p.hour_start >= TIMESTAMP '2026-09-01'
      AND p.hour_start <  TIMESTAMP '2026-10-01'
)
SELECT
    e.station_code,
    s.name,
    COUNT(*) AS n_hours,
    ROUND(AVG(e.abs_error)::numeric, 3) AS pickup_mae,
    ROUND(AVG(e.pickups)::numeric, 2) AS avg_actual_pickups
FROM errors AS e
JOIN dim_station AS s USING (station_code)
GROUP BY e.station_code, s.name
ORDER BY pickup_mae DESC, e.station_code
LIMIT 10;
