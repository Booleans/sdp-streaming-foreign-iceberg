# SDP streaming from foreign Iceberg tables

Reproduction project demonstrating streaming reads from Apache Iceberg with open
source Spark Declarative Pipelines (SDP) on a laptop, then from a Unity Catalog
foreign Iceberg table in Databricks. AWS Glue provides the source catalog.
Synthetic GPU telemetry is an interchangeable sample dataset.
The goal is a small, repeatable engineering reproduction with inspectable source
snapshots, checkpoints, configuration, and results.

**Status:** deterministic generator and append-only Iceberg source writers are
implemented. SDP definitions and Databricks deployment are still pending. See
`results/` for verified source-table results; source creation alone does not
establish SDP streaming compatibility.

**Execution limitation on the authoring machine:** JDK 17 is available locally,
but Maven Central endpoints refused connections. The AWS source is populated
with PyIceberg 0.10.0, installed through the Databricks PyPI proxy. The optional
Spark writer requires the two pinned Iceberg JARs and remains unverified.

## Scenario and architecture

A fictional GPU fleet operations team collects telemetry from AWS, Azure, OCI,
and GCP. These are labels in synthetic records; no physical GPUs or accounts in
the other three clouds are required. This is modeled on NVIDIA DCGM telemetry,
not a description of NVIDIA's internal infrastructure.

```text
Local deterministic simulator
    -> local PyIceberg writer (optional Spark writer)
    -> S3 Iceberg data and metadata + AWS Glue Data Catalog
        -> local open source SDP reader (Iceberg Glue catalog)
        -> Databricks SDP reader (Unity Catalog foreign table)
```

Glue stores the table's catalog registration; S3 holds its data and Iceberg
metadata. The source writer creates the table through the Iceberg catalog and
appends data. A Glue crawler, Glue ETL job, Kafka deployment, and cloud GPU instances
are not required for this initial experiment.

## Reproducible local environment

Use [uv](https://docs.astral.sh/uv/) for Python installation, environments,
dependency resolution, and command execution. Run commands from the repository
root. Commit `uv.lock`; use `--locked` to prevent implicit lockfile changes.

The default package index is configured in `pyproject.toml` as
`a privately configured package index`. Access to the company proxy is
required for dependency downloads; direct public PyPI is not used as a fallback.
The lockfile also records proxy artifact URLs. Use this same index when updating
dependencies with `uv lock`.

```bash
uv python install 3.12.13
uv sync --locked
uv run --locked sdp-iceberg --config config/example.toml
```

The last command writes `artifacts/environment.json` without contacting AWS or
starting Spark. It records Python, installed package versions, Java availability,
uv version, Git commit/status, configuration, and the lockfile hash.

For the upcoming local Spark experiments, install a JDK 17 and set `JAVA_HOME`
to that installation, then install the local extra:

```bash
java -version
uv sync --locked --extra local
uv run --locked --extra local python -c 'import pyspark; from pyspark import pipelines; print(pyspark.__version__)'
uv run --locked --extra local sdp-iceberg --config config/example.toml
```

The import check verifies Python package availability; it does not start Spark
or prove Iceberg streaming works. Spark needs access to Maven repositories to
fetch the Iceberg JARs at first launch; Python packages are locked by uv, while
the Maven coordinates are pinned separately in `config/example.toml`.

| Component | Selected baseline |
| --- | --- |
| Python | 3.12.13 |
| PySpark / open source SDP | `pyspark[pipelines]==4.1.0` |
| Java | JDK 17; record vendor and exact patch version per run |
| Iceberg runtime | `iceberg-spark-runtime-4.1_2.13:1.11.0` |
| Iceberg AWS bundle | `iceberg-aws-bundle:1.11.0` |
| PyIceberg source writer | `pyiceberg[glue,pyarrow]==0.10.0` |
| Source table specification | Iceberg v2, Parquet |

This is the selected test baseline, not a tested compatibility matrix.
Databricks supplies its own Spark runtime: do not install this project's local
PySpark extra into a Databricks runtime. Record Databricks' actual runtime and
pipeline configuration separately. Pin a Java patch/distribution and archive
JAR checksums once the first successful execution establishes the baseline.

## AWS configuration

```bash
cp config/example.toml config/local.toml
```

Edit the AWS profile, region, and warehouse URI in `config/local.toml`. This file
is ignored by Git. Authenticate using your organization's AWS profile/SSO
workflow. Keep credentials in the AWS credential provider chain; never put
access keys or tokens in project configuration. The environment record includes
the configuration file's contents, so keep configuration free of secrets.

Required resources for the next stage:

1. A general-purpose S3 bucket/prefix for the Iceberg warehouse.
2. Glue database `sdp_iceberg_demo` and Iceberg table `gpu_samples`, created through the Iceberg writer.
3. A local AWS principal able to create/read/update those Glue objects and
   list/read/write the relevant S3 objects. Include KMS permissions if applicable
   and Lake Formation grants if that service governs the resources.

Prefer the Databricks workspace's AWS region. Keep account, region, bucket,
database, and table names explicit in each experiment. AWS storage and API usage
incur charges; the producer will have a bounded duration by default.

The planned Spark catalog configuration is:

```properties
spark.sql.catalog.gpu=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.gpu.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.gpu.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.gpu.warehouse=s3://<bucket>/gpu-telemetry/warehouse/
spark.sql.catalog.gpu.client.region=<region>
spark.sql.session.timeZone=UTC
```

## Dataset contract and simulation defaults

One row represents one GPU observation. Defaults: seed 42, fixed UTC start time,
32 GPUs per cloud (128 total), five-second sampling, sixty-second commits, and a
ten-minute run. With an exclusive end timestamp, a complete run should generate
15,360 observations across ten append batches of 1,536 rows each.

| Fields | Planned types / semantics |
| --- | --- |
| `event_id` | String, stable identity derived from GPU and observation sequence |
| `event_time`, `ingested_at` | UTC timestamps; distinguish observation from ingestion |
| `sequence_number` | Long, increasing per GPU |
| `cloud_provider`, `region`, `cluster_id`, `node_id` | Strings, synthetic fleet labels |
| `gpu_uuid`, `gpu_model` | Strings |
| `gpu_index` | Integer |
| `workload_id`, `workload_type` | Strings; training, inference, idle |
| `gpu_utilization_pct` | Double, 0–100 |
| `memory_used_mib`, `memory_total_mib` | Long, MiB |
| `power_draw_w`, `power_limit_w`, `gpu_temperature_c` | Doubles, explicit units |
| `ecc_uncorrectable_errors_total` | Long, cumulative counter |
| `last_xid_error` | Integer, most recently observed error code |
| `is_simulated`, `schema_version` | Boolean and integer |

The generator correlates load, power, and gradual thermal changes. First
establish a clean append-only baseline; then add deterministic fault scenarios
for late data, duplicates, gaps, idle GPUs, thermal stress, and error increases.
Synthetic values and thresholds are demonstration settings, not hardware guarantees.
Unavailable metrics should be null. Counter deltas require reset handling.

Logical observations should repeat with the same seed/configuration. Actual
ingestion times, Iceberg snapshot IDs, file names, and processing latency will
vary between runs and must be recorded rather than compared byte for byte.

## Generate and populate the source

After configuring `config/local.toml` and authenticating your AWS CLI profile:

```bash
uv sync --locked --extra source
uv run --locked --extra source python -m unittest discover -s tests -v
uv run --locked --extra source python -m sdp_streaming_foreign_iceberg.dataset generate --config config/local.toml --output artifacts/sample.json
uv run --locked --extra source python -m sdp_streaming_foreign_iceberg.source populate --config config/local.toml
uv run --locked --extra source python -m sdp_streaming_foreign_iceberg.source verify --config config/local.toml
uv run --locked --extra source sdp-iceberg --config config/local.toml
```

`populate` creates the namespace/table if absent, then writes ten append snapshots
in accelerated time by default. It refuses a nonempty table to avoid accidental
duplicate seeding. `verify` reads all rows back from S3 and compares every logical
field against the deterministic generator, ignoring actual ingestion timestamps.
The results are saved to `artifacts/source-result.json`.

For wall-clock pacing add `--realtime`: each observation still represents five
seconds, with sixty seconds between commits. To reproduce with a running SDP
reader, use a fresh table name and start the reader after the table exists; that
reader workflow has not yet been implemented. Existing source data can be read
as an initial stream snapshot once the reader is available.

For a new run change the table name; do not erase the existing table or snapshots.
The ten-minute test is intentionally bounded and holds 15,360 rows in memory.
The CLI exports refreshed AWS CLI login credentials in memory for its SDK; no
credentials are written into project files or logs. Long-running production
credential refresh is outside this bounded reproduction.

Optional Spark population uses the same generator and schema:

```bash
uv run --locked --extra local python -m sdp_streaming_foreign_iceberg.dataset populate --config config/local.toml --jars /path/to/iceberg-jars
```

Set `JAVA_HOME` and provide exactly the two pinned JARs in that directory.
Omitting `--jars` resolves the configured Maven coordinates. This path has not
been run successfully on the authoring laptop; PyIceberg is the verified source
writer. The public example configuration contains placeholders so every
reproducer selects their own AWS account and bucket.

## Implementation and verification sequence

1. Verify the deterministic simulator and schema: count, identities,
   sequence continuity, and metric bounds without AWS.
2. Create the Glue Iceberg table through PyIceberg, partitioned by `days(event_time)`.
   Seed one batch and verify a batch read and table metadata.
3. Start a checkpointed local SDP reader, append subsequent batches, and verify
   it consumes new snapshots. Stop/restart the reader with its checkpoint and
   verify recovery using distinct event IDs and sequence coverage.
4. Federate Glue into Unity Catalog. Configure the Glue connection, S3 external
   locations, authorized paths, and explicit catalog storage location required
   for Iceberg reads. Verify a batch read of the same source table.
5. Run the equivalent Databricks pipeline against the foreign table, record its
   precise runtime/settings, and compare the same event identities and counts.
6. Add anomalies and document successes or full failure reproductions.

Steps 1–2 have runnable commands below. Steps 3–6 remain planned; no pipeline
definitions are implemented yet.

Iceberg's open source streaming reader processes append snapshots. Overwrite
and delete snapshots fail by default; skipping them does not reproduce their
row-level changes downstream. Avoid overwrites, deletes, and aggressive snapshot
expiration during the baseline. Keep checkpoints isolated between independent
experiments and preserve them for restart tests. A successful direct Glue read
does not establish support through Databricks' foreign-table integration.

## Evidence for Databricks engineering

Follow [results/README.md](results/README.md) for every experiment. Include the Git
commit, exact commands, environment record, source snapshots, expected/observed
counts, checkpoint behavior, logs, and Databricks runtime details. Capture both
failures and successes. Review all evidence before publishing; raw artifacts and
local AWS configuration are ignored by Git.

## Repository layout

```text
pyproject.toml / uv.lock      Python environment and dependency lock
.python-version              Exact Python baseline
config/example.toml          Shared experiment defaults and Maven coordinates
src/sdp_streaming_foreign_iceberg/    Environment-record CLI; implementation will grow here
results/                     Reviewed experiment evidence
artifacts/                   Ignored local run outputs
```

## References

- [Spark 4.1 SDP guide](https://spark.apache.org/docs/4.1.0/declarative-pipelines-programming-guide.html)
- [Iceberg Spark quickstart](https://iceberg.apache.org/spark-quickstart/)
- [Iceberg AWS integration](https://iceberg.apache.org/docs/latest/aws/)
- [Iceberg streaming semantics](https://iceberg.apache.org/docs/1.8.0/spark-structured-streaming/)
- [Databricks Glue federation](https://docs.databricks.com/aws/en/query-federation/hms-federation-glue)
- [NVIDIA GPU telemetry](https://docs.nvidia.com/datacenter/cloud-native/gpu-telemetry/latest/index.html)

Public repository: [Booleans/sdp-streaming-foreign-iceberg](https://github.com/Booleans/sdp-streaming-foreign-iceberg).
