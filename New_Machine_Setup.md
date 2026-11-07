# Run the Warehouse on a New Windows Computer

This guide provides end-to-end instructions for deploying and running the California Traffic Collision Data Warehouse on a new computer.

---

## 1. Prerequisites

Before starting, ensure you have:
1. **Operating System**: 64-bit Windows 10/11 (or Linux / macOS).
2. **PostgreSQL 14+**: Installed and running (default port `5432`). Have your `postgres` superuser password ready.
3. **Python 3.12 (Recommended) or 3.13**: 64-bit Python installed with the Windows Python Launcher (`py`).
4. **Internet Access**: Required during initial setup to download public NOAA weather and Census population files.
5. **SWITRS SQLite File**: Your local copy of the California collisions SQLite file (e.g., `switrs.sqlite`).

---

## 2. Directory Structure & Files to Copy

The minimum project files required on the new machine:

```text
D:\DataWarehouse\
    New_Machine_Setup.md
    README.md
    .env                               # Database credentials
    Python\
        pipeline.py                    # ETL, batch streaming, and validation runner
        download_sources.py            # Setup and automated data downloader
        benchmark.py                   # Benchmark suite
        benchmark_queries.json         # Benchmark SQL queries
        requirements.txt               # Pinned Python package dependencies
    SQL\
        Benchmark_Preparation.sql      # Benchmark DDL & index setup
        Live_Demo_Checks.sql           # Demo validation queries
    Original_Data\
        switrs.sqlite                  # Original traffic collision database
```

> [!IMPORTANT]
> **What NOT to copy to a new machine**: Do **not** copy `.venv`, `Support/Settings/config.json`, `Support/Generated/`, or temporary logs. The setup and pipeline scripts will generate machine-specific paths, fresh downloads, and verified warehouse schemas automatically.

---

## 3. Create the Python Virtual Environment

Open PowerShell as a standard user in your chosen project directory:

```powershell
cd D:\DataWarehouse

# Create virtual environment with Python 3.12 (or 3.13)
py -3.12 -m venv .venv

# Upgrade pip and install pinned project dependencies
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r Python\requirements.txt
```

The installed packages include:
- `psycopg[binary]==3.3.6` (high-performance PostgreSQL 3 driver with cursor `COPY` streaming)
- `pandas==2.2.0` (data manipulation and nullable integer support)
- `numpy==2.0.0` (vectorized transformations)
- `scipy==1.15.0` (k-d tree spatial indexing for geodesic distance matching)
- `python-dotenv` (automated environment loading from `.env`)

---

## 4. Configure Database Credentials

The default target database name is **`test_warehouse`**.

Create a file named `.env` in the root folder (or inside `Python/.env`):

```ini
PGHOST=localhost
PGPORT=5432
PGDATABASE=test_warehouse
PGUSER=postgres
PGPASSWORD=your_postgres_password
```

> [!TIP]
> The scripts include `sanitize_param()` which automatically strips accidental trailing commas, quotes, or trailing whitespace from connection values.

---

## 5. Initialize Setup & Download External Sources

Ensure PostgreSQL is running, then execute the setup command once:

```powershell
.\.venv\Scripts\python.exe Python\download_sources.py --setup --sqlite "Original_Data\switrs.sqlite" --database test_warehouse --user postgres
```

### What Setup Does:
1. **Validates SQLite**: Inspects `collisions` table schema and required columns (`case_id`, `collision_date`, `county_location`).
2. **Creates Database**: Connects to administrative database `postgres` and creates `test_warehouse` if it does not already exist.
3. **Downloads Metadata & Census Data**:
   - Downloads 3 historical U.S. Census County Population CSVs (2000–2021).
   - Downloads NOAA GHCN-Daily station list (`ghcnd-stations.txt`) and inventory (`ghcnd-inventory.txt`).
4. **Selects Weather Stations**: Identifies 448 California stations with continuous 2001–2021 coverage for precipitation (`PRCP`), min temperature (`TMIN`), and max temperature (`TMAX`), then downloads their compressed observation files (`Original_Data/weather/*.csv.gz`).
5. **Generates Crosswalks**: Builds `Original_Data/county_crosswalk.csv` (mapping 58 California counties) and `Original_Data/selected_stations.json`.
6. **Writes Configuration**: Saves project paths, column mappings, and connection parameters to `Support/Settings/config.json`.

> [!NOTE]
> Completed download files are cached in `Original_Data/`. If setup is interrupted, simply re-run the command; existing files will be verified and skipped.

---

## 6. Run the Full Warehouse Pipeline

Execute the full end-to-end data pipeline:

```powershell
.\.venv\Scripts\python.exe Python\pipeline.py --all
```

### Pipeline Execution Architecture:
1. **Source Integrity Check**: Compares SHA-256 hashes of all input files.
2. **Data Cleaning Rules**: Executes unit tests on coordinate bounds, time normalization, and weather flags.
3. **100,000-Row Batch Streaming (`BATCH_SIZE = 100000`)**:
   - Cleans collisions in memory chunks of 100,000 rows.
   - Performs spatial geodesic nearest-neighbor matching against daily weather stations within 50 km using `scipy.spatial.cKDTree`.
   - Joins annual county population figures.
   - **Streams directly to PostgreSQL via `psycopg` cursor `COPY` in committed 100,000-row batches**.
   - **Rows are immediately queryable in `test_warehouse` during the run**; there is no need to wait for all 9.4M rows before analyzing incoming data.
   - Reports upfront batch counts and per-batch progress:
     ```text
     Processing traffic collisions: 9,424,334 rows across 95 batch(es) (100,000 rows per batch)
     ...
     Postgres batch 1 of 95 committed (100,000 new/missing rows): 100,000 total collisions now live and queryable
     ```
4. **Idempotent Resumption**:
   - On any rerun, the script queries `dw.fact_collision` and `mart.collision_context` for existing `case_id`s.
   - Any already-loaded collisions are automatically skipped. Only new or missing records are processed and committed.
5. **Relational Constraints & Schema Export**:
   - Adds primary keys, unique constraints, domain check rules, and foreign keys.
   - Refreshes analytical views and snowflake dimension hierarchies.
   - Exports schema DDL via `pg_dump` to `Support/Generated/Checks/Database_Schema.sql`.
6. **Automated Warehouse Validation**:
   - Executes 19 mathematical and relational assertions comparing warehouse rows to independent SQLite counts (verifying 9,424,334 collisions, 73,007 fatalities, and 5,290,670 injuries).

---

## 7. Verify the Warehouse in pgAdmin / psql

Connect to `test_warehouse` and run verification queries:

```sql
-- 1. Check core fact table totals
SELECT count(*) FROM dw.fact_collision;           -- Expected: 9,424,334
SELECT count(*) FROM dw.fact_population_annual;   -- Expected: 1,218
SELECT count(*) FROM dw.fact_weather_daily;       -- Expected: 3,082,346

-- 2. Inspect the consolidated analytical snowflake view
SELECT case_id, full_date, county_name, population, weather_matched, prcp_mm, tmax_c
FROM mart.collision_snowflake
LIMIT 10;

-- 3. Check casualty totals
SELECT sum(killed) AS total_killed, sum(injured) AS total_injured
FROM dw.fact_collision;                           -- Expected: 73,007 killed, 5,290,670 injured

-- 4. Check audit batch history
SELECT batch_id, started_at, status FROM audit.batches ORDER BY started_at DESC LIMIT 5;
```

---

## 8. Test the Pipeline with a Demo Single-Record Batch

To demonstrate incremental batch processing on a live system without altering your original SQLite data:

```powershell
.\.venv\Scripts\python.exe Python\pipeline.py --demo-one
```

### What this does:
1. Creates a synthetic teaching record (`DEMO_PORTABLE_1`) with the date set to the day following the latest collision in the database.
2. Ingests the record through the incremental date-batch pipeline.
3. Commits the record and updates both `dw.fact_collision` and `mart.collision_context`.

Verify the demo case in pgAdmin:
```sql
SELECT case_id, full_date, county_name, population, weather_matched
FROM mart.collision_snowflake
WHERE case_id = 'DEMO_PORTABLE_1';
```

Running `--demo-one` a second time reuses the existing case and demonstrates **duplicate prevention** (`new_cases = 0`, `changed_fact_rows = 0`).

To remove the demo case and return to pure original data, run:
```powershell
.\.venv\Scripts\python.exe Python\pipeline.py --all
```

---

## 9. Incremental & Daily Ingestion Modes

For ongoing warehouse maintenance:

```powershell
# Ingest collisions from a specific date forward (100,000-row committed batches)
.\.venv\Scripts\python.exe Python\pipeline.py --from 2021-06-01

# Ingest an explicit date window
.\.venv\Scripts\python.exe Python\pipeline.py --from 2021-06-01 --to 2021-06-03

# Run daily mode (reloads the last 2 configured lookback days plus newer dates)
.\.venv\Scripts\python.exe Python\pipeline.py --daily
```

---

## 10. Benchmarking Queries & Performance Optimization

To measure query execution times and compare physical database designs:

### Step 1: Baseline Measurements
Ensure no extra benchmark indexes exist, then run:
```powershell
.\.venv\Scripts\python.exe Python\benchmark.py --phase baseline
```

### Step 2: Index Optimization
In pgAdmin, run Section A of `SQL/Benchmark_Preparation.sql` to create specialized composite indexes, then run:
```powershell
.\.venv\Scripts\python.exe Python\benchmark.py --phase indexed
```

### Step 3: Range Partitioning Evaluation
In pgAdmin, run Section B of `SQL/Benchmark_Preparation.sql` to create `dw.fact_collision_part` partitioned yearly by `date_key`, then run:
```powershell
.\.venv\Scripts\python.exe Python\benchmark.py --phase partitioning
```

Results (raw timings, JSON query execution plans, partition pruning counts, and percentage comparisons) are saved automatically in `Support/Generated/Benchmarks/`.

---

## Summary of Generated Files & Logs

| Path | Description | Generated By |
| :--- | :--- | :--- |
| `Support/ETL_Run.log` | Detailed stdout/stderr execution log of the most recent pipeline run | `pipeline.py` |
| `Support/Generated/Checks/warehouse_validation.json` | 19-point mathematical and relational validation receipt | `pipeline.py --all` |
| `Support/Generated/Checks/Database_Schema.sql` | PostgreSQL schema DDL exported by `pg_dump` | `pipeline.py --all` |
| `Support/Generated/Checks/Coverage_By_Year.csv` | Weather and coordinate match fractions per year | `pipeline.py --all` |
| `Support/Generated/Checks/Source_Catalog.csv` | Cryptographic SHA-256 hashes and URLs of all input files | `download_sources.py` |
| `Support/Generated/Benchmarks/` | Execution plans, timing summaries, and partition pruning metrics | `benchmark.py` |
