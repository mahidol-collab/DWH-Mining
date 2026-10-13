# Updated warehouse project

Start with New_Machine_Setup.md. Existing working project: add benchmark.py and benchmark_queries.json under Python and use the SQL instructions; retain your local config/data. New computer: follow setup sections 1-5 before benchmarking.

Original ETL and downloader are included unchanged. Added: benchmark runner with validation/export, preparation SQL, actual audit/demo SQL, measured evidence and expanded setup/demo/report guidance. No database password or original SWITRS SQLite is bundled. No database operations are performed merely by extracting the ZIP.

The synthetic --demo-one record is for teaching only. Full --all rebuild removes benchmark objects in dw. Keep old/new experiment results distinct. benchmark.py --help describes options without requiring a database connection.
