# BikeFlow NYC — Architecture

BikeFlow NYC turns four public data sources into an hourly "will this Citi Bike
station run out of bikes or docks?" map. The pipeline is split into three
stages with fixed contracts between them:

- **A — ingest & transform**: raw sources → cleaned Parquet files (F1–F5), with Polars
- **B — storage & orchestration**: files + live status → PostgreSQL, scheduled by Airflow, all in Docker
- **C — model & dashboard**: Poisson models write forecasts back to Postgres; a SQL view computes risk; Streamlit shows it

Each stage only talks to the next through files (section 3) or tables (section 4), so any
stage can be rewritten without touching the others.

## 1. Data flow

```mermaid
flowchart LR
    subgraph SRC["Data sources"]
        S1["Citi Bike trip data<br/>S3 monthly CSV, Jul–Sep 2026"]
        S2["GBFS station_information"]
        S3["GBFS station_status<br/>live"]
        S4["Open-Meteo weather<br/>history + 48 h forecast"]
    end

    subgraph AIR["Orchestrated by Airflow (except make train)"]
        subgraph A["A · ingest & transform (Polars)"]
            A1["download_month<br/>clean_trips · build_station_hour<br/>run_checks"]
            A2["fetch_stations"]
            A3["fetch_weather_history<br/>fetch_weather_forecast"]
        end

        F["Parquet files<br/>F1 stations · F2 station_hour<br/>F3/F4 weather · F5 quality JSON"]

        subgraph B["B · storage"]
            L["loaders.py<br/>COPY → temp table → upsert"]
            G["fetch_and_load_status<br/>every 15 min"]
            PG[("PostgreSQL 16<br/>dim_station · fact_station_hour<br/>weather_hour · station_status<br/>forecast_hour")]
            V["v_station_risk<br/>SQL view"]
        end

        P["predict_next_hours<br/>hourly"]
    end

    subgraph C["C · model & dashboard"]
        T["make train<br/>Poisson regression ×2<br/>(manual)"]
        ART["artifacts/<br/>models · station_means<br/>metrics.json"]
        D["Streamlit dashboard<br/>localhost:8501"]
    end

    subgraph B0["B0 · GBFS collector (separate repo)"]
        GH["GitHub Actions<br/>snapshot every 15 min<br/>→ csv.gz"]
        BF["make backfill"]
    end

    S1 --> A1
    S2 --> A2
    S4 --> A3
    A1 --> F
    A2 --> F
    A3 --> F
    F --> L --> PG
    S3 --> G --> PG
    S3 --> GH --> BF --> PG
    PG --> T --> ART
    ART --> P
    PG --> P --> PG
    PG --> V --> D
    ART --> D
```

**Why this shape**

- **Files between A and B.** A's output is plain Parquet on disk, so A can be developed and
  tested with no database, and every load is reproducible from the files.
- **The database is the only shared state for C.** C reads history and writes forecasts through
  `bikeflow.db.get_conn()`; the dashboard reads one view, so risk logic lives in SQL, in one place.
- **Two paths for live status.** The Airflow DAG collects while the stack is running; the B0
  collector keeps collecting in GitHub Actions even when nobody's laptop is on, and
  `make backfill` loads that history.

## 2. Orchestration (Airflow)

All four DAGs only call functions from `bikeflow/`; there is no business logic in `dags/`.
Each task returns a file path that the next task reads from XCom.

| DAG | Schedule | Tasks |
|---|---|---|
| `trips_monthly` | manual, param `month` (e.g. `2026-09`) | download_month → fetch_stations → load_stations → build_station_hour → run_checks → load_station_hour |
| `weather_history` | manual | fetch_weather_history → load_weather(is_forecast=False) |
| `gbfs_status` | every 15 min | fetch_and_load_status |
| `forecast_hourly` | minute 5 of every hour | fetch_weather_forecast → load_weather(is_forecast=True) → predict_next_hours |

```mermaid
flowchart LR
    subgraph M["trips_monthly (manual, per month)"]
        direction LR
        m1[download_month] --> m2[fetch_stations] --> m3[load_stations] --> m4[build_station_hour] --> m5[run_checks] --> m6[load_station_hour]
    end
    subgraph W["weather_history (manual)"]
        direction LR
        w1[fetch_weather_history] --> w2[load_weather]
    end
    subgraph S["gbfs_status (*/15 * * * *)"]
        s1[fetch_and_load_status]
    end
    subgraph H["forecast_hourly (5 * * * *)"]
        direction LR
        h1[fetch_weather_forecast] --> h2[load_weather] --> h3[predict_next_hours]
    end
```

- `run_checks` raises if a quality check fails, so a bad month turns the DAG red **before**
  `load_station_hour` writes anything.
- Schedules use America/New_York time; `catchup=False` and `max_active_runs=1` on every DAG.
- Model training (`make train`) is deliberately **not** in Airflow: it runs once on July–August
  history, and its artifacts are reused by every hourly prediction.

## 3. File contracts (A → B, C)

All paths are relative to the repo root; `data/` and `artifacts/` are not committed.

| File | Path | Written by | Read by |
|---|---|---|---|
| F1 | `data/processed/stations.parquet` | A2 `fetch_stations` | B3 → `dim_station` |
| F2 | `data/processed/station_hour/station_hour_YYYYMM.parquet` | A4 `build_station_hour` | B3 → `fact_station_hour` |
| F3 | `data/processed/weather/weather_history.parquet` | A6 `fetch_weather_history` | B3 → `weather_hour` |
| F4 | `data/processed/weather/weather_forecast.parquet` | A6 `fetch_weather_forecast` | B3 → `weather_hour` |
| F5 | `data/processed/quality/quality_YYYYMM.json` | A5 `run_checks` | people / README |
| — | `artifacts/` (models, `station_means.parquet`, `metrics.json`) | C2 `train` | C3, dashboard |

## 4. Database (ERD)

```mermaid
erDiagram
    dim_station ||--o{ fact_station_hour : "station_code"
    dim_station ||--o{ forecast_hour : "station_code"
    dim_station ||--o{ station_status : "gbfs_station_id (logical)"
    weather_hour ||--o{ fact_station_hour : "hour_start (logical)"

    dim_station {
        text station_code PK "normalized code, e.g. 5980.10"
        text gbfs_station_id UK "GBFS station_id"
        text name
        float lat
        float lon
        int capacity
    }
    fact_station_hour {
        text station_code PK, FK
        timestamp hour_start PK "NY local, no tz"
        int pickups
        int dropoffs
    }
    weather_hour {
        timestamp hour_start PK
        float temperature_c
        float precipitation_mm
        float wind_speed_kmh
        boolean is_forecast
    }
    station_status {
        text gbfs_station_id PK
        timestamp last_reported PK
        timestamp fetched_at
        int num_bikes_available
        int num_ebikes_available
        int num_docks_available
        boolean is_renting
        boolean is_returning
    }
    forecast_hour {
        text station_code PK, FK
        timestamp hour_start PK
        text model_version PK "poisson_v1"
        float pred_pickups
        float pred_dropoffs
        timestamp created_at
    }
```

| Table | Grain | Loaded by | Upsert key |
|---|---|---|---|
| `dim_station` | one row per station | B3 `load_stations` (F1) | `station_code` |
| `fact_station_hour` | station × hour, full grid (0 when no rides) | B3 `load_station_hour` (F2) | `(station_code, hour_start)` |
| `weather_hour` | one row per hour | B3 `load_weather` (F3/F4) | `hour_start` |
| `station_status` | one GBFS report per station | B4 `fetch_and_load_status`, `make backfill` | `(gbfs_station_id, last_reported)`; only `fetched_at` is updated on conflict |
| `forecast_hour` | station × hour × model | C2 `train`, C3 `predict_next_hours` | `(station_code, hour_start, model_version)` |

**Design notes**

- **Star-like layout.** `dim_station` is the dimension; the hourly facts and forecasts reference it
  with foreign keys, so a forecast can never point at an unknown station.
- **Two station keys.** Trip data uses short codes (`5980.10`), live GBFS uses `station_id`.
  `dim_station` stores both, which is the bridge between historical and live data.
  `station_status` has no foreign key on purpose: GBFS can report stations we dropped
  (e.g. Jersey City), and we still want to keep the raw feed.
- **All times are New York local time without a time zone**; conversion from UTC happens once,
  at ingest. The database timezone is also America/New_York, so `LOCALTIMESTAMP` in the view
  means "now in New York".
- **Every load is an upsert** (`COPY` into a temp table, then `INSERT … ON CONFLICT`), in one
  transaction, so re-running any task never duplicates rows and a failed load changes nothing.

**`v_station_risk` (view, C4)** — one row per `dim_station` station: takes each station's
newest valid snapshot from `station_status`, adds the expected pickups/dropoffs from
`forecast_hour` for the rest of the current hour and the next hour, and labels it
`empty_risk`, `full_risk`, `ok` or `no_data`. The dashboard reads only this view.

## 5. Deployment (Docker Compose)

```mermaid
flowchart LR
    subgraph HOST["Your machine"]
        ENV[".env<br/>POSTGRES_*"]
        subgraph DC["docker compose (make up)"]
            PGS[("postgres:16<br/>:5432<br/>volume pgdata<br/>sql/ → initdb")]
            AF["airflow standalone<br/>:8080<br/>dags/ bikeflow/ data/ artifacts/"]
            ST["streamlit<br/>:8501<br/>dashboard/ artifacts/"]
        end
    end
    ENV -.-> PGS & AF & ST
    AF -->|get_conn| PGS
    ST -->|get_conn| PGS
    AF <-->|artifacts/| ST
```

| Service | Image | Port | Notes |
|---|---|---|---|
| `postgres` | `postgres:16` | 5432 | `sql/schema.sql` then `sql/views.sql` run on an empty volume; timezone America/New_York |
| `airflow` | `docker/airflow.Dockerfile` (apache/airflow 2.10.5 + `requirements.txt`) | 8080 | `airflow standalone`; code mounted, so edits need no rebuild |
| `dashboard` | `docker/app.Dockerfile` (python 3.12-slim + `requirements.txt`) | 8501 | `streamlit run dashboard/app.py` |

All three read the same `.env`; no credentials are in the code or the images. Dependency
versions are pinned in `requirements.txt`, so every machine and CI builds the same environment.

## 6. CI (GitHub Actions)

`.github/workflows/ci.yml` runs on every pull request and every push to `main`:

```mermaid
flowchart LR
    c1[checkout] --> c2[Python 3.12<br/>pip install -r requirements.txt] --> c3[ruff check] --> c4["scripts/init_db.py<br/>timezone + schema.sql + views.sql"] --> c5[pytest]
    PGc[("postgres:16 service")] -.-> c4 & c5
```

The test database is built from the same `schema.sql` and `views.sql` as Docker, so CI tests
the real schema, including the end-to-end integration test (A8) and the risk-view cases (C4).
DAG structure tests run where Airflow is installed and are skipped in CI.
