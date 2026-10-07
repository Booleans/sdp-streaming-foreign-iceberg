# Local open source SDP streaming validation — 2026-10-07

Passed using Python 3.12.13, PySpark/SDP 4.1.0, Iceberg 1.11.0, and Temurin
17.0.20.1+1 on macOS arm64. Exact Python dependencies are in `uv.lock`; Iceberg
JAR hashes are in `scripts/iceberg-jars.sha256`. Proxy addresses, account
configuration, raw logs, snapshot IDs, and checkpoint IDs remain in ignored
local artifacts.

The pipeline uses the real `spark-pipelines` CLI and a `@dp.table` definition
returning `spark.readStream.format("iceberg")`. It reads the Glue source and
writes a separate local HadoopCatalog Iceberg table named
`local.demo.iceberg_stream_copy`.

| Test | Source rows | Destination rows | Missing / unexpected rows |
| --- | ---: | ---: | --- |
| Initial SDP update | 15,360 | 15,360 | 0 / 0 |
| First append and incremental update | 16,896 | 16,896 | 0 / 0 |
| Fresh process, same checkpoint, no new source data | 16,896 | 16,896 | 0 / 0 |
| New append after restart | 18,432 | 18,432 | 0 / 0 |

Every phase had distinct event counts equal to total row counts. Validation
compared complete rows with `exceptAll` in both directions, including ingestion
timestamps. Destination snapshots added exactly 15,360, 1,536, and 1,536 rows.
The no-data restart created no new destination snapshot. Checkpoint identity
was unchanged between the no-data restart and final incremental run. Source
append verification confirmed the complete deterministic prefix and continued
per-GPU sequences. All successful run/verify commands exited 0.

## Reproduce

Follow **Validate local SDP streaming** in the root README. Start with a fresh
source table containing 15,360 seeded rows and fresh local state. Use the same
state directory for every subsequent phase; never use full refresh in this test.
On the authoring laptop, source `artifacts/private-proxies.env`, set `JAVA_HOME`,
and use the frozen uv environment. Three generator tests passed, including
prefix preservation and unique IDs when extending the simulation by one minute.

## Scope

Each SDP update consumed available data and exited. The fresh-process restart
proves persisted checkpoint recovery across successful runs. Always-running
operation and a forced crash during a micro-batch were not tested. Databricks
Unity Catalog foreign-table streaming remains pending; this validates direct
Iceberg GlueCatalog access from open source SDP only.

During implementation, launch attempts caught two configuration errors before
processing source rows: `--master` conflicted with Spark Connect local mode, and
absolute library globs were unsupported. The runner now uses `--remote local[2]`
and a relative definitions path. Successful runs emitted a benign static-config
warning about `spark.sql.extensions`; Iceberg streaming and all row comparisons
completed successfully.
