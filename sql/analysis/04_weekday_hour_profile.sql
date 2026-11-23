-- A7 query 4
-- Question: How does system-wide demand change by day of week and hour of day?
-- Average number of pickups per hour across the whole system, for each
-- day of week x hour of day (7 x 24 = 168 rows).
-- Data: fact_station_hour, Jul-Sep 2026. ISO day of week: 1 = Monday ... 7 = Sunday.

WITH system_hour AS (
    SELECT
        hour_start,
        SUM(pickups) AS system_pickups
    FROM fact_station_hour
    GROUP BY hour_start
)
SELECT
    EXTRACT(ISODOW FROM hour_start)::int AS iso_dow,
    TO_CHAR(hour_start, 'Dy') AS day_name,
    EXTRACT(HOUR FROM hour_start)::int AS hour_of_day,
    ROUND(AVG(system_pickups), 1) AS avg_system_pickups
FROM system_hour
GROUP BY 1, 2, 3
ORDER BY 1, 3;
