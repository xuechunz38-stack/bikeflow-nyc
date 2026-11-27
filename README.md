# BikeFlow NYC

Predicts which Citi Bike stations in NYC may run out of bikes or docks by the
end of the next hour. See the project spec (v2) for the full requirements.

## Overview (B)

## Setup and running (B)

## Architecture (B)

## Data sources and cleaning (A)

### Sources

| Data | Source | Code | Output file |
|---|---|---|---|
| Trips, Jul–Sep 2026 | Citi Bike monthly zips, `https://s3.amazonaws.com/tripdata/` (NYC files only, not `JC-`) | A1 `bikeflow/ingest/trips.py` | `data/raw/trips/YYYYMM/*.csv` |
| Stations | GBFS `station_information.json` | A2 `bikeflow/ingest/stations.py` | F1 `data/processed/stations.parquet` |
| Weather history | Open-Meteo archive API, one point in Manhattan (40.7128, −74.0060), `timezone=America/New_York` | A6 `bikeflow/ingest/weather.py` | F3 `data/processed/weather/weather_history.parquet` |
| Weather forecast | Open-Meteo forecast API, next 48 hours, same point | A6 | F4 `data/processed/weather/weather_forecast.parquet` |

All times are New York local time without a time zone. Trip times are
already local, so they are not converted.

### Station codes

`station_code` (for example `5980.10`) is the only station key in the
project. It is the trip files' `start_station_id` / `end_station_id`, and
also the GBFS `short_name`, after `normalize_station_code` (A3):

- strip spaces, then match `^(\d+)\.(\d{1,2})(_.*)?$`;
- pad a one-digit decimal part with a 0 (`5980.1` → `5980.10`);
- drop a suffix that starts with `_` (`5303.06_` → `5303.06`, `5348.06_old` → `5348.06`);
- anything else (e.g. `SYS016`, `Shop Morgan`, `5980.123`, empty) becomes null.

F1 drops GBFS stations whose `short_name` cannot be normalized. One real
conflict came up: `5303.06` and `5303.06_` both normalize to `5303.06`. The
team agreed to keep the station whose `short_name` is exactly the code (no
suffix) and drop the other; if a code ever has no such unique station, A2
stops with an error instead of guessing. F1 has **2,409 stations**, all
codes unique.

### Cleaning trips (A3 `clean_trips`)

In this order:

1. read every CSV of the month with all columns as strings;
2. drop duplicate `ride_id`s;
3. parse `started_at` / `ended_at` and keep rides that start in the month;
4. keep valid rides only: 1 ≤ duration ≤ 1,440 minutes;
5. normalize the start and end station ids to `start_code` / `end_code`;
6. codes that are not in F1 become null. The ride is kept, because it still
   counts at its other end.

### Station-hour table (A4 `build_station_hour`, file F2)

- `pickups` = valid rides that start at the station in that hour (by
  `started_at`); `dropoffs` = valid rides that end there in that hour (by
  `ended_at`). Drop-offs that end in the next month are not counted.
- The grid is complete: every station with at least one counted pickup or
  drop-off in the month × every hour of the month, with 0 where nothing
  happened. `(station_code, hour_start)` is unique.

| Month | Stations | Rows (= stations × hours) | Pickups | Drop-offs |
|---|---|---|---|---|
| 2026-07 | 2,276 | 1,693,344 (× 744) | 4,944,030 | 4,932,465 |
| 2026-08 | 2,323 | 1,728,312 (× 744) | 5,215,361 | 5,204,483 |
| 2026-09 | 2,361 | 1,699,920 (× 720) | 4,961,142 | 4,950,429 |

### Quality checks (A5 `run_checks`, file F5)

Each month writes `data/processed/quality/quality_YYYYMM.json`. The check
fails (and the Airflow task turns red) if the raw row count is outside
2–8 million, a matched rate is below 90%, a missing rate is above 2%, F2 has
a duplicate key, or `sum(pickups)` does not equal the number of valid rides
with a start code. All three months pass:

| Month | Raw rows | Valid rides | Dropped by duration | Start missing | Start matched | End missing | End matched |
|---|---|---|---|---|---|---|---|
| 2026-07 | 4,993,137 | 4,991,171 | 1,097 | 0.07% | 99.12% | 0.30% | 99.12% |
| 2026-08 | 5,246,236 | 5,243,546 | 1,236 | 0.06% | 99.53% | 0.27% | 99.53% |
| 2026-09 | 4,982,855 | 4,981,268 | 1,038 | 0.07% | 99.67% | 0.29% | 99.67% |

- *Missing* = the ride has no station id, or it does not normalize (e.g.
  system ids like `SYS016`). *Matched* = of the codes that normalize, the
  share found in F1.
- Raw rows − valid rides is larger than "dropped by duration" because
  duplicate `ride_id`s, unparsable times and rides starting outside the
  month are also removed.
- The unmatched codes are most likely stations that were removed or
  renumbered before our GBFS snapshot (`5886.13` and `6779.07` are in the
  top 4 every month). This would also explain why the matched rate rises
  from July to September: F1 is today's station list, closest to September.

### Weather (A6, files F3 and F4)

F3 has 2,208 hourly rows (2026-07-01 00:00 to 2026-09-30 23:00), no
duplicate hours and no missing values; 516 of those hours have rain
(`precipitation_mm > 0`). F4 is overwritten on every run with exactly 48
rows starting at the current hour. Both files are checked for units, row
count and duplicate hours before they are written.

### Tests

`tests/test_clean.py`, `test_aggregate.py`, `test_quality.py`,
`test_weather.py` and the other A tests run only on `tests/fixtures/`, with
no network: `trips_sample.csv` (the first 2,000 September rides plus every
station-code case and two invalid durations), `stations_sample.json` and
`weather_sample.json`. `tests/test_integration.py` (A8) runs the whole chain
on those fixtures (clean → F2 → weather → B's loaders) and checks that every
table has exactly the rows of its file. It writes into a throwaway schema,
so it never touches real data.

## SQL analysis (A)

Five queries in `sql/analysis/` (A7), run on the loaded July–September 2026
data. Each file starts with the question it answers. Peak hours are
`hour_start` 7 and 8 (07:00–09:00) and 17 and 18 (17:00–19:00). Run one with:

```bash
docker compose exec -T postgres psql -U bikeflow -d bikeflow < sql/analysis/01_peak_top_stations.sql
```

**1. Busiest stations in the peaks** (`01_peak_top_stations.sql`,
`RANK()` window function). Average pickups per peak hour:

| Rank | Morning (07–09) | Avg | Evening (17–19) | Avg |
|---|---|---|---|---|
| 1 | E 33 St & 1 Ave | 33.88 | 9 Ave & W 33 St | 64.89 |
| 2 | Park Ave & E 42 St | 33.24 | W 21 St & 6 Ave | 61.09 |
| 3 | Pier 61 at Chelsea Piers | 29.75 | Pier 61 at Chelsea Piers | 57.24 |
| 4 | W 43 St & 10 Ave | 27.85 | Broadway & W 25 St | 55.67 |
| 5 | W 44 St & 11 Ave | 26.41 | Park Ave & E 42 St | 53.18 |
| 6 | W 31 St & 7 Ave | 25.36 | E 17 St & Broadway | 49.66 |
| 7 | Riverside Blvd & W 67 St | 24.85 | W 31 St & 7 Ave | 46.56 |
| 8 | 11 Ave & W 41 St | 24.74 | 10 Ave & W 14 St | 46.36 |
| 9 | FDR Drive & E 35 St | 24.08 | Broadway & E 21 St | 44.41 |
| 10 | W 21 St & 6 Ave | 24.07 | 7 Ave & Central Park S | 44.18 |

The evening peak is about twice as busy as the morning peak. Park Ave &
E 42 St (next to Grand Central), Pier 61 and W 31 St & 7 Ave (next to Penn
Station) are in both lists.

**2. Largest net outflow on weekday mornings** (`02_weekday_morning_net_outflow.sql`,
CTE). Average drop-offs − pickups per hour, Monday–Friday 07–09:

| Station | Net per hour |
|---|---|
| W 43 St & 10 Ave | −26.92 |
| Riverside Blvd & W 67 St | −26.05 |
| W 44 St & 11 Ave | −24.90 |
| FDR Drive & E 35 St | −23.05 |
| E 10 St & Ave A | −21.83 |
| E 6 St & Ave B | −19.27 |
| E 2 St & Ave B | −19.05 |
| W 59 St & 10 Ave | −19.02 |
| E 7 St & Ave B | −18.77 |
| E 11 St & Ave B | −17.73 |

These are mostly residential areas (Hell's Kitchen west of 10 Ave, the Upper West
Side riverside, the East Village) where people ride out to work. This is
net outflow, not "most likely to run out": whether a station empties also
depends on how many bikes it starts with.

**3. Rain vs dry hours** (`03_rain_vs_dry.sql`). System-wide pickups per
hour:

| Weather | Hours | Avg pickups per hour |
|---|---|---|
| Dry | 1,692 | 7,062.3 |
| Rain (`precipitation_mm > 0`) | 516 | 6,145.4 |

Rainy hours have about 13% fewer pickups. This is a plain comparison, not a
causal estimate: it does not control for hour of day or day of week.

**4. Day of week × hour** (`04_weekday_hour_profile.sql`, 168 rows). Summary
of the system-wide average pickups per hour:

| Day | Busiest hour | Pickups | 08:00 | Quietest hour |
|---|---|---|---|---|
| Mon | 17:00 | 15,754 | 9,816 | 03:00 |
| Tue | 17:00 | 16,769 | 13,785 | 03:00 |
| Wed | 17:00 | 17,978 | 14,267 | 03:00 |
| Thu | 17:00 | 15,387 | 13,490 | 03:00 |
| Fri | 17:00 | 16,269 | 10,390 | 03:00 |
| Sat | 15:00 | 12,240 | 4,504 | 05:00 |
| Sun | 14:00 | 11,228 | 2,954 | 05:00 |

Weekdays have two commute peaks (08:00 and 17:00); weekends have one broad
afternoon peak. Monday and Friday mornings are lower than Tuesday–Thursday,
which may reflect hybrid work; Labor Day (Mon 9/7) also lowers the Monday average.

**5. Stations the model predicts worst** (`05_september_mae_by_station.sql`).
Pickup MAE of `poisson_v1` in September (C2's evaluation predictions in
`forecast_hour`) joined with the actual counts:

| Station | Pickup MAE | Avg actual pickups per hour |
|---|---|---|
| 7 Ave & Central Park S | 12.48 | 13.31 |
| 9 Ave & W 33 St | 11.29 | 20.81 |
| Park Ave & E 42 St | 10.74 | 20.18 |
| Central Park S & 6 Ave | 10.66 | 18.47 |
| Pier 61 at Chelsea Piers | 10.34 | 23.43 |
| W 22 St & 10 Ave | 9.48 | 6.52 |
| Lafayette St & Astor Pl | 9.14 | 15.80 |
| 10 Ave & W 28 St | 8.97 | 16.31 |
| W 21 St & 6 Ave | 8.95 | 24.03 |
| W 31 St & 7 Ave | 8.66 | 21.27 |

Each station has all 720 September hours. The overall pickup MAE is about
1.7 trips per station-hour, so these stations are 5–7 times worse than
average. Most are the busiest stations from query 1; the error grows with
volume. Two stand out: the Central Park South stations (12.5 and 10.7) are
probably leisure stations whose demand follows weather and weekends more
than the hour of day, and W 22 St & 10 Ave has an MAE above its own average
demand (9.5 vs 6.5), so its September demand may have changed from
July–August. These are the stations where the shared hour-of-day curve of
the Poisson model fits worst (see the Model section).

## Model (C)

## Dashboard (C)

## Undercurrents (all)

### Data governance (A)

- **One definition per term.** `station_code`, `hour_start`, valid ride,
  pickups and drop-offs each have one definition in the spec (section 2),
  and the code follows it. `normalize_station_code` is written once (A3) and
  imported everywhere else, including F1, so trip ids and GBFS ids cannot
  drift apart.
- **Fixed file contracts.** F1–F5 have fixed paths, columns and types (spec
  section 4). Producers build them with explicit schemas and tests check
  the schema, so a changed column type fails a test instead of reaching the
  database.
- **Quality gate with a record.** Every month's run writes
  `quality_YYYYMM.json` (row counts, missing and matched rates, top
  unmatched codes, reconciliation sums) and stops the pipeline if a
  threshold fails. The files stay in `data/processed/quality/`, so each
  month's numbers can be compared later.
- **Decisions are written down, not guessed.** When real data broke a rule
  (two GBFS stations normalizing to `5303.06`), A2 stopped with an error,
  the team chose a rule, and it was added to the spec. The code raises an
  error for any future conflict it cannot resolve the same way.
- **Raw data is never edited or committed.** Raw CSVs are only read;
  cleaning happens in code, so every output can be rebuilt from the source.
  `data/`, `artifacts/` and `.env` are in `.gitignore`.

### Efficiency (A)

- **Lazy, columnar processing.** `clean_trips` scans the month's CSVs with
  polars' lazy API, so filters and the `ride_id` dedup run in one optimized
  query over about 5 million rows instead of in Python loops.
- **Vectorized normalization.** Station codes are normalized with one polars
  regex expression over the whole column. The plain Python
  `normalize_station_code` is only used for single values (F1, tests), and a
  test checks that both give the same result.
- **Measured memory.** The heaviest step (A5 on a full month) peaks at about
  1.2 GB, which fits in the 4 GB Docker VM next to Postgres and Airflow.
- **Skip work that is already done.** A1 skips the download when the month's
  CSVs are already there, and extracts into a temp folder that only replaces
  the real one when it is complete, so a failed download never leaves half a
  month behind.
- **Small, compressed outputs.** F1–F4 are Parquet. The whole July–September
  F2 (about 5.1 million rows) plus F1, F3 and F4 is a few MB, and B's loaders
  move it into Postgres with `COPY` instead of row-by-row inserts.

## Limitations (all)

## Team roles and AI use (all)
