# Run the warehouse on a new Windows computer

This guide starts with an installed PostgreSQL server and the original SWITRS SQLite file. You also need Python and an internet connection. The examples use D:\DataWarehouse. You can choose another folder; the scripts find their project folder automatically.

## 1. What to copy

The minimum working files are:

| File | Why it is needed |
|---|---|
| Python/pipeline.py | Cleans data, stages it, loads PostgreSQL, validates it, and updates the stars, snowflake and marts |
| Python/download_sources.py | Creates the initial setup and downloads NOAA and Census data |
| Python/requirements.txt | Installs the Python packages |
| Original_Data/switrs.sqlite | Your original traffic source; this cannot be recreated by the downloader |

Updated_Warehouse_Project.zip contains the two scripts, requirements, and this guide. Extract it into your chosen project folder. Copy your large SQLite file separately. For a NEW machine, do not copy the old config, password, libraries, logs, staging, validation files, weather files, Census files, or database export. For an EXISTING working project, keep its private config and source data; add only the new benchmark files and guides. Setup creates or downloads the required items.

After extracting and copying SQLite, the folder should look like this:

```text
D:\DataWarehouse\
    New_Machine_Setup.md
    Python\
        pipeline.py
        download_sources.py
        requirements.txt
    Original_Data\
        switrs.sqlite
```

The original SQLite has no automatic download in our program. If your file has another name or location, give its path in the setup command.

## 2. Install Python and its packages

Use 64-bit Python 3.12 or 3.13. These commands use Python 3.13; change the version argument to 3.12 if that is what you installed. Open PowerShell in the project folder and run:

```powershell
cd D:\DataWarehouse
py -3.13 -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r Python\requirements.txt
```

The .venv folder is a local Python environment. It is created on the new computer; it is not copied from the old computer. The commands below use it directly, so activation is not required. If py is not recognized, install Python with its Windows launcher, reopen PowerShell, and try again.

The fixed NumPy, pandas, SciPy and psycopg versions in requirements are the versions used by our project. These are the ETL packages used by the two scripts.

## 3. Set up PostgreSQL and download sources

Make sure PostgreSQL is running. Have the PostgreSQL username and password ready. For a normal local installation, the host is localhost, the port is 5432, and the user is postgres.

Run this once:

```powershell
.\.venv\Scripts\python.exe Python\download_sources.py --setup --sqlite "D:\DataWarehouse\Original_Data\switrs.sqlite" --database warehouse --user postgres
```

The script asks for the PostgreSQL password once and saves it in Support/Settings/config.json. Use the password from the NEW computer. Keep that config private; it contains the password in plain text. Do not submit or share it. Later runs use the saved value automatically. A TRAFFIC_PASSWORD environment variable can override it.

Setup does these steps:

1. Checks that the SQLite file has the expected SWITRS collisions fields.
2. Creates the project folders.
3. Connects to the PostgreSQL administration database named postgres. Creates warehouse if it does not exist. The account needs permission to create a database if the database is missing.
4. Downloads the three historical Census CSV files and NOAA station metadata and inventory.
5. Selects nearby NOAA stations with PRCP, TMIN and TMAX coverage spanning 2001 to 2021, then downloads their station history files.
6. Builds county_crosswalk.csv from the 58 California counties in the Census file. Creates selected_stations.json from the NOAA metadata.
7. Creates config.json with paths for this computer, selected columns, source release choices, weather distance limit, and database settings.

For another PostgreSQL port or host, add --port 5433 or --host your-host to that command. To use another database name, change --database. Use a dedicated project database: a full run replaces this project's landing, staging, dw, mart and audit schemas inside it.

Setup does not load the warehouse yet. A first download can take a long time because it retrieves station history files. If a transfer fails, fix the connection and run setup again. Completed files are reused. The config is written after setup succeeds. If config.json already exists, setup stops rather than overwriting it; edit it or run the full pipeline instead.

## 4. Run all records

```powershell
.\.venv\Scripts\python.exe Python\pipeline.py --all
```

This is the full warehouse process. It runs the cleaning checks, checks or downloads required source files, generates an independent traffic profile and file fingerprints, transforms data in Python, saves staging CSVs, loads replacement PostgreSQL schemas, validates the results, and publishes the warehouse.

You do not need to copy old validation JSON files. First-run checks compare the new warehouse with the local SQLite totals and the newly prepared Python data. Historical comparisons with an old SQL warehouse are optional and are skipped when that reference is absent.

The load creates the collision, daily weather and annual population stars, the small snowflake dimensions, the combined view, and the mining marts. Downloads can differ from an older run because public NOAA histories may change. Weather counts and matches should be read from the new run's validation rather than assumed to be identical to the old computer.

Support/ETL_Run.log contains the current run and is replaced on the next normal run. Support/Generated/Checks/warehouse_validation.json contains the full-load results. Source_Catalog.csv records file URLs and checksums. Staging CSVs are generated by the full run. An optional schema export is created if pg_dump is available.

The original full load took about 53 minutes on our earlier computer, excluding the first downloads. Allow substantial time and free disk space for the large SQLite, downloads, staging CSVs, database and indexes. Watch the run messages and use the log if it fails.



## 5. Check the warehouse in pgAdmin

Connect to your new warehouse database. Open the Query Tool and run:

```sql
SELECT count(*) FROM dw.fact_collision;
SELECT count(*) FROM dw.fact_population_annual;
SELECT count(*) FROM dw.fact_weather_daily;
SELECT * FROM mart.collision_snowflake LIMIT 10;
```

The collision count should equal accepted source collisions. For the unchanged original SQLite, our previous count was 9,424,334. The full-load validation must report all_checks_passed as true. The combined view keeps one row per collision. Do not sum its population column because the county-year population is repeated for each collision.

## 6. Create and process exactly one demo SQLite row

After the full load succeeds, run:

```powershell
.\.venv\Scripts\python.exe Python\pipeline.py --demo-one
```

This command does all the following:

1. Opens the original SQLite for reading only.
2. Copies the collisions table structure and one template row into a small, separate Original_Data/demo.sqlite file.
3. Sets case_id to DEMO_PORTABLE_1 and sets the collision date to the latest ORIGINAL source date plus one day. For our original file, this is 2021-06-04.
4. Sets killed and injured to zero and marks severity as property damage only. Coordinates are missing, so no weather location is invented. This is a synthetic teaching record, not a real collision.
5. Runs the normal date-batch ETL using only that demo file and only that date.
6. Updates the fact, collision mart, affected county-year summary, and snowflake dimensions. Prints the loaded demo result.

The command inserts ONE synthetic record into your chosen warehouse database. The original SWITRS SQLite stays unchanged. County-year population should match when that year has an estimate. Weather is missing for this demonstration because coordinates are missing.

Check it in pgAdmin:

```sql
SELECT case_id, full_date, county_name, population, weather_matched
FROM mart.collision_snowflake
WHERE case_id = 'DEMO_PORTABLE_1';
```

The result should contain one row. Support/Generated/Checks/batch_run.json should show selected = 1, accepted = 1 and new_cases = 1 on the first demo run. The full-load result remains a historical snapshot and is marked as needing a refresh after changes.

Run --demo-one again. It reuses the same case and date. The latest receipt should show selected = 1, new_cases = 0 and changed_fact_rows = 0. The database still has only one demo case.

## 7. Run that same row manually with date options

This is an alternative demonstration of the batch command. Run it AFTER --demo-one has created demo.sqlite. Use the date printed by the script; replace the example if your source ends on another date.

```powershell
.\.venv\Scripts\python.exe Python\pipeline.py --from 2021-06-04 --to 2021-06-04 --sqlite "D:\DataWarehouse\Original_Data\demo.sqlite" --dry-run
.\.venv\Scripts\python.exe Python\pipeline.py --from 2021-06-04 --to 2021-06-04 --sqlite "D:\DataWarehouse\Original_Data\demo.sqlite"
```

The first command previews one source row. The second processes it. It updates the same existing demo case, so it does not add a second case. --from and --to include both boundary dates. --sqlite selects the demo input for this batch; it does not change the original source path saved in config.json.

## 8. Remove the demo and return to original data

A full rebuild also removes fact_collision_part and benchmark indexes in dw. Export all benchmark evidence before rebuilding. Recreate benchmark objects afterwards if needed. The teaching row must be excluded from real dataset statistics.

Run the full process again using the original configured SQLite:

```powershell
.\.venv\Scripts\python.exe Python\pipeline.py --all
```

The rebuilt warehouse contains only collisions from the original SQLite, so DEMO_PORTABLE_1 is removed. Check:

```sql
SELECT count(*) FROM dw.fact_collision WHERE case_id = 'DEMO_PORTABLE_1';
```

The result should be zero. The separate demo.sqlite can remain for later practice or be deleted manually. Never put that file in the configured original source path.

## 9. Normal daily or date batches

For real additions or corrections to the configured SQLite:

```powershell
.\.venv\Scripts\python.exe Python\pipeline.py --daily
.\.venv\Scripts\python.exe Python\pipeline.py --from 2021-06-04
```

Daily mode reloads the last two loaded days and newer source dates by default. Older corrections need an earlier --from date. Source deletions need --all. These commands do not install a scheduler. Missing NOAA and configured Census files can be downloaded automatically. New population years are supported only through the configured published releases; unavailable future observations stay missing.

## Files created by the process

| Location | Downloaded or generated | Created when |
|---|---|---|
| Original_Data/*.csv and weather/*.csv.gz | Downloaded Census and NOAA files | Setup and required source refreshes |
| Original_Data/ghcnd-*.txt | Downloaded NOAA metadata | Setup |
| Original_Data/county_crosswalk.csv | Generated county mapping | Setup |
| Original_Data/selected_stations.json | Generated station selection | Setup |
| Support/Settings/config.json | Generated local settings and saved password | Setup |
| Support/Generated/Staging | Generated cleaned CSVs | Full run |
| Support/Generated/Checks | Generated profiles, checksums, receipts and validation | Full or date run |
| Support/ETL_Run.log | Generated current run log | Normal full or date run |
| Original_Data/demo.sqlite | Generated one-row teaching source | --demo-one |
| PostgreSQL tables and views | Generated warehouse, marts and snowflake | Full run; updated by date batches |

## Setup references

Python Windows installation: https://docs.python.org/3.12/using/windows.html
Python virtual environments: https://docs.python.org/3.12/library/venv.html
Pinned SciPy Python requirement and release files: https://pypi.org/project/scipy/1.18.1/
NOAA daily files: https://noaa-ghcn-pds.s3.amazonaws.com/
Census population datasets: https://www.census.gov/programs-surveys/popest/data/data-sets.html


## 10. New progress added to this package

The original pipeline.py, download_sources.py and requirements.txt are preserved. New files add benchmark automation, query-output validation, raw timings, measured JSON plans, environment/index/storage evidence, and live-demo steps. No extra Python dependency is required beyond existing psycopg.

For an EXISTING project: copy Python/benchmark.py, Python/benchmark_queries.json and the SQL/ and guide files into the same project root. Keep Support/Settings/config.json and Original_Data. Do not rerun setup or --all just to benchmark.

## 11. Benchmark preparation and phases

Run SQL/Benchmark_Preparation.sql manually in pgAdmin with Autocommit ON. Follow its stop/skip comments. CREATE and INSERT are only for an absent benchmark copy. VACUUM runs separately, never in a transaction block.

If no benchmark indexes exist, run baseline FIRST:

```powershell
.\.venv\Scripts\python.exe Python\benchmark.py --phase baseline
```

Then add indexes in SQL section A, refresh statistics, and run:

```powershell
.\.venv\Scripts\python.exe Python\benchmark.py --phase indexed
```

For an already indexed database, retain previous baseline/indexed evidence. The baseline command intentionally refuses a database with idx_bench indexes; it does not remove indexes.

Create the partitioned copy using SQL section B only if missing. Otherwise use the existing copy. Stop ETL and run:

```powershell
.\.venv\Scripts\python.exe Python\benchmark.py --phase partitioning
```

For a short live demonstration:

```powershell
.\.venv\Scripts\python.exe Python\benchmark.py --phase partitioning --query Q2
```

Each query/design is measured eight times, first four warm-ups and final four measured. Partitioning alternates B/C order and first validates equivalent returned query results. Q2/Q4 use deterministic tie-breakers; Q4 protects against zero population. New outputs are separate from old evidence, not silently merged.

The script uses the same config/env overrides as pipeline.py, never writes passwords into evidence, and does not modify warehouse tables or indexes. A consistent read-only snapshot is used for query validation/timing. Saved files under Support/Generated/Benchmarks/<timestamp_phase_query> include raw_timings.csv, execution_plans.csv, summary.csv, comparison.csv for partitioning, partitions_executed.csv, indexes.csv, storage.csv and manifest.json. A failed run has status failed; do not use partial timings as a completed comparison. Register server CPU/RAM/disk type manually.

## 12. Live demonstration using the actual pipeline

1. On a rehearsed demo project/database, show configuration database/port without showing the password. Open SQL/Live_Demo_Checks.sql in pgAdmin.
2. Run pipeline.py --demo-one. This deliberately loads ONE SYNTHETIC teaching record (DEMO_PORTABLE_1); never describe it as a genuine new collision. The original SQLite stays unchanged. Show the resulting integrated view and audit.batches receipt.
3. Run --demo-one again. Show demo_count still 1, new_cases 0 and changed_fact_rows 0 if the input/environment are unchanged. This demonstrates repeat processing without duplicate insertion.
4. Wait for ETL to finish, then run benchmark.py --phase partitioning --query Q2. Q2 uses 2015-2020, whereas the teaching row is after the last original source date; nevertheless validation must pass before timing.
5. Show summary.csv, comparison.csv and partitions_executed.csv. Six yearly partitions are expected for Q2 in the supplied configuration; report actual output. Plans and index choice can change with machine/settings.
6. Show historical indexing evidence separately; no need to drop indexes live. The core warehouse has 10 tables; the partitioned benchmark parent and leaf tables are extra objects, not extra production facts.
7. Export evidence before any full rebuild. If you use --all to remove the teaching record afterwards, recreate benchmark objects before the next demo.

## 13. Saved measured results and report updates

Evidence/ contains the original user-uploaded evidence, not fresh runs on this machine. Current partitioning rerun medians (ms): Q1 B 4120.683, C 4672.417, +13.39%; Q2 B 3056.485, C 1324.403, -56.67%; Q3 B 2573.708, C 2861.746, +11.19%; Q4 B 7986.774, C 8423.821, +5.47%; Q5 B 2046.921, C 2396.876, +17.10%. Negative means shorter execution time. Indexing evidence is from the earlier workbook and must be discussed separately.

Original total storage was reported as 2945 MB; the partitioned total was confirmed by the user as 2722 MB. This confirmation is not an included storage CSV; the new runner exports byte measurements. The original has the case_id PK index, the partitioned copy does not. Do not claim partitioning itself saves 223 MB. Three added fact indexes total about 737 MB; dim_date(year) adds 72 kB separately.

## 14. Submission and verification

Submit code, README/setup instructions and relevant CSV/plans. Do not submit config.json, passwords, .venv, or large source files unless the instructor requests them. Format the paper separately using the IEEE conference template; this code bundle is supporting material, not an IEEE-formatted paper.

The newly added runner was syntax-checked and its SQL generation/statistics helpers checked locally. It was NOT executed against your PostgreSQL database. Original pipeline/download scripts are preserved, not newly certified; package installation and full ETL require rehearsal on your computer. Existing source availability may change after a fresh download.
