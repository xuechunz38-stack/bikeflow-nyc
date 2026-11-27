-- A7 query 1
-- Question: Which 10 stations have the highest average hourly pickups in the
-- morning peak (07:00-09:00) and in the evening peak (17:00-19:00)?
-- Data: fact_station_hour, Jul-Sep 2026, all days.
-- "Average" = mean pickups per station-hour over every peak hour in the data.
-- Peak hours are hour_start 7 and 8 (morning) and 17 and 18 (evening).
-- Ties share a rank (RANK()), so a period can list more than 10 rows.

WITH peak_hours AS (
    SELECT
        f.station_code,
        CASE
            WHEN EXTRACT(HOUR FROM f.hour_start) IN (7, 8) THEN 'morning'
            ELSE 'evening'
        END AS peak,
        f.pickups
    FROM fact_station_hour AS f
    WHERE EXTRACT(HOUR FROM f.hour_start) IN (7, 8, 17, 18)
),
station_avg AS (
    SELECT
        peak,
        station_code,
        AVG(pickups) AS avg_pickups
    FROM peak_hours
    GROUP BY peak, station_code
),
ranked AS (
    SELECT
        peak,
        station_code,
        avg_pickups,
        RANK() OVER (PARTITION BY peak ORDER BY avg_pickups DESC) AS pickup_rank
    FROM station_avg
)
SELECT
    r.peak,
    r.pickup_rank,
    r.station_code,
    s.name,
    ROUND(r.avg_pickups, 2) AS avg_pickups
FROM ranked AS r
JOIN dim_station AS s USING (station_code)
WHERE r.pickup_rank <= 10
ORDER BY r.peak DESC, r.pickup_rank, r.station_code;
