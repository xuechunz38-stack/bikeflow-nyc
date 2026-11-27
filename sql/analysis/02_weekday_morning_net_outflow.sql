-- A7 query 2
-- Question: On weekday mornings (07:00-09:00, Mon-Fri), which 10 stations have
-- the lowest average net inflow (dropoffs - pickups) per hour, i.e. lose the
-- most bikes on net?
-- Note: this is net outflow, not "most likely to run out of bikes". Whether a
-- station empties also depends on how many bikes it starts with.
-- Data: fact_station_hour, Jul-Sep 2026, Monday-Friday only.

WITH weekday_morning AS (
    SELECT
        station_code,
        dropoffs - pickups AS net_inflow
    FROM fact_station_hour
    WHERE EXTRACT(ISODOW FROM hour_start) BETWEEN 1 AND 5
      AND EXTRACT(HOUR FROM hour_start) IN (7, 8)
),
station_net AS (
    SELECT
        station_code,
        AVG(net_inflow) AS avg_net_inflow
    FROM weekday_morning
    GROUP BY station_code
)
SELECT
    n.station_code,
    s.name,
    ROUND(n.avg_net_inflow, 2) AS avg_net_inflow_per_hour
FROM station_net AS n
JOIN dim_station AS s USING (station_code)
ORDER BY n.avg_net_inflow, n.station_code
LIMIT 10;
