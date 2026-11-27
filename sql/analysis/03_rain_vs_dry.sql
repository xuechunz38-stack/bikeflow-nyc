-- A7 query 3
-- Question: Is system-wide hourly demand lower when it rains? Compare the
-- average number of pickups per hour across the whole system in rainy hours
-- (precipitation_mm > 0) and dry hours.
-- Data: fact_station_hour joined to historical weather (is_forecast = false),
-- Jul-Sep 2026. Hours without weather data are left out.

WITH system_hour AS (
    SELECT
        hour_start,
        SUM(pickups) AS system_pickups
    FROM fact_station_hour
    GROUP BY hour_start
)
SELECT
    CASE WHEN w.precipitation_mm > 0 THEN 'rain' ELSE 'dry' END AS weather,
    COUNT(*) AS n_hours,
    ROUND(AVG(h.system_pickups), 1) AS avg_system_pickups_per_hour
FROM system_hour AS h
JOIN weather_hour AS w
    ON w.hour_start = h.hour_start
   AND NOT w.is_forecast
WHERE w.precipitation_mm IS NOT NULL
GROUP BY 1
ORDER BY 1;
