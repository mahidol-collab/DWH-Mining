-- Run one statement at a time in the configured project database.
SELECT current_database(),current_setting('port');
-- Core dw tables: excludes partition leaves and the extra benchmark parent.
SELECT table_name FROM information_schema.tables WHERE table_schema='dw'
AND table_type='BASE TABLE' AND table_name<>'fact_collision_part'
AND table_name NOT LIKE 'fc_p%' ORDER BY table_name;
SELECT * FROM audit.batches ORDER BY started_at DESC LIMIT 5;
SELECT batch_id,status,details->>'selected' AS selected,
 details->>'accepted' AS accepted,details->>'new_cases' AS new_cases,
 details->>'changed_fact_rows' AS changed_fact_rows
FROM audit.batches ORDER BY started_at DESC LIMIT 5;
SELECT case_id,full_date,county_name,population,weather_matched
FROM mart.collision_snowflake WHERE case_id='DEMO_PORTABLE_1';
SELECT count(*) AS demo_count FROM dw.fact_collision WHERE case_id='DEMO_PORTABLE_1';
-- After one demo: 1 row. After repeating demo: still 1 row.
SELECT case_id,count(*) FROM dw.fact_collision GROUP BY case_id HAVING count(*)>1 LIMIT 10;
-- Expected zero duplicate case IDs. Run this during rehearsal if too slow live.
