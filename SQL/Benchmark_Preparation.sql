-- BENCHMARK PREPARATION. RUN SECTIONS MANUALLY IN pgAdmin, AUTOCOMMIT ON.
-- Database must match Support/Settings/config.json (or your PG environment overrides).
-- No reset/DROP commands. Stop ETL while preparing or timing.
-- For baseline measurement FIRST run Python/benchmark.py --phase baseline before
-- adding the benchmark indexes below. Existing indexed databases should retain
-- their old baseline evidence; do not drop indexes just to force another baseline.

-- A: inventory. If idx_bench* already exist, SKIP their CREATE statements.
SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='dw' AND tablename IN ('fact_collision','dim_date');
CREATE INDEX idx_bench_fc_date_cover ON dw.fact_collision (date_key) INCLUDE (county_key, killed, injured);
CREATE INDEX idx_bench_fc_date_time_ctx ON dw.fact_collision (date_key, time_key, context_key);
CREATE INDEX idx_bench_fc_county_date_killed ON dw.fact_collision (county_key, date_key) INCLUDE (killed);
CREATE INDEX idx_bench_dd_year ON dw.dim_date (year);
-- Execute each VACUUM alone.
VACUUM ANALYZE dw.fact_collision;
VACUUM ANALYZE dw.dim_date;
-- Now run: Python/benchmark.py --phase indexed

-- B: create partitioned copy ONLY if absent; otherwise validate/use the existing copy.
SELECT to_regclass('dw.fact_collision_part');
-- If the result is NOT NULL, SKIP creation/insertion below. Never append all rows again.
CREATE TABLE dw.fact_collision_part (LIKE dw.fact_collision INCLUDING DEFAULTS) PARTITION BY RANGE (date_key);

DO $$
BEGIN
  FOR y IN 2001..2021 LOOP
    EXECUTE format('CREATE TABLE dw.fc_p%s PARTITION OF dw.fact_collision_part FOR VALUES FROM (%s) TO (%s)',
                   y, y*10000+101, (y+1)*10000+101);
  END LOOP;
END $$;

CREATE TABLE dw.fc_pdefault PARTITION OF dw.fact_collision_part DEFAULT;

-- Takes a few minutes
INSERT INTO dw.fact_collision_part SELECT * FROM dw.fact_collision;

-- Same index types as the original (the partitioned copy has no primary key index)
CREATE INDEX ON dw.fact_collision_part (date_key, county_key);

CREATE INDEX ON dw.fact_collision_part (date_key) INCLUDE (county_key, killed, injured);

CREATE INDEX ON dw.fact_collision_part (date_key, time_key, context_key);

CREATE INDEX ON dw.fact_collision_part (county_key, date_key) INCLUDE (killed);

VACUUM ANALYZE dw.fact_collision_part;


ANALYZE dw.fact_collision_part;
-- Now run: Python/benchmark.py --phase partitioning
