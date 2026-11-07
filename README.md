# California Traffic Collision Data Warehouse & Analytics Platform

[![Python](https://img.shields.io/badge/Python-3.12%20%7C%203.13-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-blue.svg)](https://www.postgresql.org/)
[![Database](https://img.shields.io/badge/Database-test__warehouse-green.svg)](#database-configuration)
[![Batch Size](https://img.shields.io/badge/Batch%20Size-100%2C000%20rows-orange.svg)](#100000-row-batch-streaming)
[![Validation](https://img.shields.io/badge/Checks-19%2F19%20Passed-brightgreen.svg)](#warehouse-validation--integrity-checks)

An enterprise-grade, high-throughput analytical data warehouse and machine learning feature platform integrating over two decades of California traffic incidents with meteorological conditions and demographic context.

---

## Architecture & Data Sources

The platform combines three independent public data sources spanning **2001 through 2021**:

1. **California Statewide Integrated Traffic Records System (SWITRS)**
   - **9,424,334** collision records with party counts, primary collision factors, road conditions, injury/fatality counts, and geographic coordinates.
2. **NOAA Global Historical Climatology Network (GHCN-Daily)**
   - **3,082,346** station-day observations across **448** selected weather stations in California, tracking precipitation (`PRCP`), temperature minimums (`TMIN`), and temperature maximums (`TMAX`).
3. **U.S. Census Bureau Population Estimates Program (PEP)**
   - **1,218** annual county population records covering all **58** California counties across the entire 21-year period (Intercensal 2000–2010, Vintage 2020, Vintage 2021).

### Schema Design

The warehouse is deployed in PostgreSQL (`test_warehouse`) across five structured schemas:

```mermaid
graph TD
    subgraph Landing & Staging
        L[landing.files] --> S_C[staging.collision]
        L --> S_W[staging.weather_daily]
        L --> S_P[staging.population_annual]
    end

    subgraph Core Dimensional Model (dw)
        S_C --> FC[dw.fact_collision]
        S_W --> FW[dw.fact_weather_daily]
        S_P --> FP[dw.fact_population_annual]
        
        D_DATE[dw.dim_date] --> FC
        D_DATE --> FW
        D_TIME[dw.dim_time] --> FC
        D_COUNTY[dw.dim_county] --> FC
        D_COUNTY --> FP
        D_STATION[dw.dim_station] --> FW
        D_CONTEXT[dw.dim_context] --> FC
        D_YEAR[dw.dim_year] --> FP
        D_SRC[dw.dim_source] --> FC
        D_SRC --> FW
        D_SRC --> FP
    end

    subgraph Analytical Marts (mart)
        FC & FW & FP --> CC[mart.collision_context]
        CC --> SCY[mart.safety_county_year]
        CC --> CS[mart.collision_snowflake]
        CC --> SP[mart.severity_predictors]
        CC --> SR[mart.severity_retrospective]
        CC --> SC[mart.spatial_clustering]
    end

    subgraph Governance (audit)
        A_B[audit.batches]
        A_F[audit.files]
        A_Q[audit.quality]
        A_R[audit.reject_files]
    end
```

---

## Key Features & Ingestion Pipeline

### 100,000-Row Batch Streaming
- **Live Queryability**: Ingestion uses `psycopg` cursor `COPY` in committed batches of **100,000 rows** (`BATCH_SIZE = 100000`). Data is committed immediately to PostgreSQL so analysts can query incoming records live without waiting for multi-million-row pipelines to finish.
- **Upfront & Real-Time Progress Tracking**: Calculates total batches upfront and logs detailed real-time progress:
  ```text
  Loaded dw.fact_collision: batch 1 of 95 (100,000 rows, 100,000 / 9,424,334 total committed and queryable)
  ...
  Loaded dw.fact_collision: batch 95 of 95 (24,334 rows, 9,424,334 / 9,424,334 total committed and queryable)
  ```

### Smart Idempotency & Resumption
- **Download Reruns**: [download_sources.py](file:///d:/MU%20Projects/DWH-Mining/Python/download_sources.py) checks local file integrity (`is_valid_local_file`) and SHA-256 checksums, downloading **only missing or newly released** NOAA stations and Census tables.
- **Pipeline Reruns**: [pipeline.py](file:///d:/MU%20Projects/DWH-Mining/Python/pipeline.py) verifies existing records in `dw.fact_collision` and `mart.collision_context`. Any existing collision is skipped automatically, preventing duplicate keys and streaming only missing or updated records.
- **Safe Type Handling**: Automatic PostgreSQL type inspection and pandas `Int64` nullable integer casting prevents float representations (e.g. `"0.0"`) from failing `COPY` commands into integer columns.

---

## Database Configuration

The pipeline defaults to target database **`test_warehouse`** on `localhost:5432`.

### Environment Variables (`.env` & `Python/.env`)
Create or edit `.env` in the repository root or inside `Python/`:

```ini
PGHOST=localhost
PGPORT=5432
PGDATABASE=test_warehouse
PGUSER=postgres
PGPASSWORD=your_secure_password
```

> [!NOTE]
> All connection settings are protected by `sanitize_param()`, which automatically strips trailing commas, quotes, and whitespace (preventing PostgreSQL auth syntax errors).

---

## Quickstart Guide

For step-by-step instructions on a fresh workstation, see [New_Machine_Setup.md](file:///d:/MU%20Projects/DWH-Mining/New_Machine_Setup.md).

### 1. Prerequisites
- **Operating System**: Windows 10/11, macOS, or Linux
- **Python**: 3.12 (recommended) or 3.13 (64-bit)
- **PostgreSQL**: 14+ running on port 5432
- **SWITRS SQLite File**: Original collisions SQLite file placed in `Original_Data/switrs.sqlite` (or custom location)

### 2. Environment Setup
```powershell
# Navigate to project directory
cd "D:\MU Projects\DWH-Mining"

# Create virtual environment with Python 3.12
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r Python\requirements.txt
```

### 3. Initialize Sources & Database
```powershell
.\.venv\Scripts\python.exe Python\download_sources.py --setup --sqlite "Original_Data\switrs.sqlite" --database test_warehouse --user postgres
```
This downloads missing Census and NOAA stations, builds crosswalks, initializes `Support/Settings/config.json`, and ensures `test_warehouse` exists.

### 4. Run the Full Warehouse Pipeline
```powershell
.\.venv\Scripts\python.exe Python\pipeline.py --all
```
This executes:
1. Verification of source file hashes (`python_source_manifest.json`).
2. Unit tests on data cleaning rules (time, coordinates, weather flags, counts).
3. Weather cleaning and spatial k-d tree indexing (50 km matching radius).
4. Chunked collision cleaning and streaming in 100,000-row batches into PostgreSQL.
5. Primary keys, foreign keys, unique rules, and domain check constraints.
6. Analytical mart views, snowflake dimension views, and schema export via `pg_dump`.
7. Full 19-point warehouse validation check.

---

## Pipeline CLI Reference

[pipeline.py](file:///d:/MU%20Projects/DWH-Mining/Python/pipeline.py) provides multiple operational modes:

| Command Flag | Description |
| :--- | :--- |
| `--all` / `--run` | Full warehouse build or resumption; streams only missing/new records in 100,000-row batches. |
| `--daily` | Incremental daily sync; reloads recent days (configured lookback) plus new dates. |
| `--from <YYYY-MM-DD>` | Incremental sync from a specific date forward (supports optional `--to` and `--sqlite`). |
| `--demo-one` | Creates a single synthetic teaching record (`DEMO_PORTABLE_1`) to demo batch processing. |
| `--test` | Runs automated unit tests on data cleaning and matching logic without database changes. |
| `--test-batch` | Runs an isolated multi-case rollback and idempotency test in temporary schemas. |
| `--prepare` | Prepares dimension and fact staging CSVs without modifying PostgreSQL. |
| `--load` | Loads already-prepared staging CSVs into PostgreSQL in 100,000-row batches. |

---

## Benchmarking & Performance Analysis

[benchmark.py](file:///d:/MU%20Projects/DWH-Mining/Python/benchmark.py) evaluates warehouse performance across three structural designs:

1. **Baseline**: Unindexed warehouse facts.
2. **Indexed**: Specialized covering composite indexes (`idx_bench_fc_date_cover`, `idx_bench_fc_date_time_ctx`, `idx_bench_fc_county_date_killed`).
3. **Partitioned**: Declarative yearly range partitioning on `date_key` (`dw.fact_collision_part`).

```powershell
# Run baseline query measurements
.\.venv\Scripts\python.exe Python\benchmark.py --phase baseline

# Run indexed queries
.\.venv\Scripts\python.exe Python\benchmark.py --phase indexed

# Run partitioning comparison against indexed design
.\.venv\Scripts\python.exe Python\benchmark.py --phase partitioning
```

Each run executes 8 iterations per query (4 warm-up + 4 measured), records `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` execution plans, checks result row equality between designs, and exports timing summaries to `Support/Generated/Benchmarks/`.

---

## Warehouse Validation & Integrity Checks

At the conclusion of `--all`, the pipeline runs 19 automated assertions against independent source counts:

| Assertion | Expected Value | Status |
| :--- | :---: | :---: |
| `source_vs_collision_rows` | Exact match with SQLite collision count (9,424,334) | Passed |
| `accepted_plus_rejected` | Sum of clean and rejected rows equals raw total (9,424,334) | Passed |
| `integrated_one_row_per_case` | `mart.collision_context` matches `dw.fact_collision` (9,424,334) | Passed |
| `killed_total_reconciled` | Fatalities match SQLite source total (73,007) | Passed |
| `injured_total_reconciled` | Injuries match SQLite source total (5,290,670) | Passed |
| `loaded_weather_rows_match_python` | Staged weather rows match `dw.fact_weather_daily` (3,082,346) | Passed |
| `loaded_population_rows_match_python` | Staged Census rows match `dw.fact_population_annual` (1,218) | Passed |
| `population_58_counties_per_selected_year` | Exactly 58 California counties present for all 21 study years | Passed |
| `unknown_counties_zero` | Zero collisions assigned to unknown county (key 0) | Passed |
| `matched_weather_max_50km` | Zero matched collisions exceed the 50,000-meter radius limit | Passed |
| `temperature_order` | Zero records where `tmin_c > tmax_c` | Passed |
| `all_fact_constraints_validated` | All PK, FK, Unique, and Check constraints active and valid | Passed |
| `snowflake_links_valid` | Snowflake normalized hierarchy constraints validated | Passed |

---

## Project Structure

```text
D:\MU Projects\DWH-Mining\
├── .env                              # PostgreSQL connection configuration
├── .gitignore                        # Git exclusion rules
├── README.md                         # Project documentation and architecture guide
├── New_Machine_Setup.md              # Fresh workstation installation guide
├── Python\
│   ├── pipeline.py                   # Core ETL, batch streaming, and validation runner
│   ├── download_sources.py           # Setup script, NOAA/Census source downloader
│   ├── benchmark.py                  # EXPLAIN ANALYZE benchmark runner
│   ├── benchmark_queries.json        # Benchmark query definitions (Q1-Q5)
│   ├── requirements.txt              # Pinned Python package dependencies
│   └── .env                          # Local Python environment credentials
├── SQL\
│   ├── Benchmark_Preparation.sql     # Index and partitioned table DDL
│   └── Live_Demo_Checks.sql          # SQL queries for live demonstration
├── Original_Data\                    # Original SQLite, downloaded NOAA and Census data
├── Support\
│   ├── Settings\config.json          # Machine-specific paths and parameters
│   ├── ETL_Run.log                   # Detailed execution log of the latest run
│   └── Generated\
│       ├── Staging\                  # Prepared dimensional CSV datasets
│       ├── Checks\                   # Validation receipts, catalogs, and schema dumps
│       └── Benchmarks\               # Execution plans and timing benchmarks
└── Report\                           # Analytical charts and documentation
```

---

## License & Attribution

- **Traffic Data**: California Highway Patrol / SWITRS via UC Berkeley SafeTREC.
- **Meteorological Data**: NOAA GHCN-Daily public dataset (AWS S3 mirror).
- **Demographic Data**: U.S. Census Bureau County Population Totals (2000–2021).
- Designed for data warehousing, geospatial distance matching, and high-performance analytical mining.
