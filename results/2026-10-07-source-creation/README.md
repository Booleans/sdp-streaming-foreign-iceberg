# AWS Iceberg source creation — 2026-10-07

Created Glue table `sdp_iceberg_demo.gpu_samples` in `us-west-2`, backed by a
user-selected general-purpose S3 bucket. Glue reports `table_type=ICEBERG` and a
current Iceberg metadata location. Physical locations and account configuration
remain in ignored local artifacts. No AWS credentials are committed.

## Result

- Iceberg v2, Parquet, daily partition transform on `event_time`.
- 15,360 rows, 15,360 distinct event IDs, 128 GPUs, 3,840 rows per cloud.
- Ten append snapshots, each adding 1,536 rows, committed in accelerated time.
- Independent fresh catalog/table load matched every logical observation field.
- Zero missing or unexpected event IDs.
- Determinism, sequence coverage, metric bounds, and invalid-interval tests passed.
- Logical dataset SHA-256: `6e9f6fe21c9714f051bf506311b5a4866c1d680925a57717c638b36fb00a3705`.

Detailed snapshot IDs, timestamps, schema, and counts are kept in ignored local artifacts. This
is source creation evidence, not evidence of an SDP streaming read. There is no
reader checkpoint in this experiment. No Databricks compute was used.

## Commands

Configure `config/local.toml` from the example with your own AWS profile, region,
warehouse, database, and table. The run used seed 42 and the example simulation
settings. The AWS CLI was already authenticated.

```bash
uv sync --locked --extra source
uv run --locked --extra source python -m unittest discover -s tests -v
uv run --locked --extra source python -m sdp_streaming_foreign_iceberg.source populate --config config/local.toml
uv run --locked --extra source python -m sdp_streaming_foreign_iceberg.source verify --config config/local.toml --output artifacts/independent-verification.json
aws glue get-table --database-name sdp_iceberg_demo --name gpu_samples --region us-west-2
```

Successful population and independent verification exited 0. Use a fresh table
name for another population; the writer refuses nonempty tables.

## Runtime and limitations

Python 3.12.13 on macOS arm64; PyIceberg 0.10.0. Exact Python dependencies are
in `uv.lock`, sourced through the Databricks PyPI proxy. Actual ingestion times
and Iceberg snapshot IDs are intentionally nondeterministic.

The Spark source writer was implemented but not run: Maven Central and an
alternate mirror refused connections from this laptop. Temurin JDK
17.0.20.1+1 was downloaded into ignored artifacts; it was not used by PyIceberg.
The first PyIceberg attempt created the namespace then stopped before table
creation because the partition source ID did not match the inferred Arrow schema.
Explicit Iceberg field IDs fixed that setup issue; the subsequent run succeeded.
Local SDP and Databricks foreign-table reader validation remain pending.

## Local Spark read verification

After configuring the Databricks Maven proxy, the pinned Iceberg JARs downloaded
successfully. A fresh Spark 4.1.0 session using Temurin 17.0.20.1+1 read the
PyIceberg-created source through GlueCatalog and S3FileIO. It verified 15,360
rows, 15,360 distinct event IDs, 128 GPUs, and ten append snapshots with zero
missing or unexpected event IDs. Exit code: 0. Detailed read verification is kept in ignored local artifacts.

```bash
bash scripts/fetch-iceberg-jars.sh
export JAVA_HOME="$PWD/artifacts/runtime/jdk-17.0.20.1+1/Contents/Home"
uv run --locked --extra local --extra source python -m sdp_streaming_foreign_iceberg.dataset verify --config config/local.toml --jars artifacts/runtime/jars --output artifacts/spark-verification.json
```

The JAR hashes are committed in `scripts/iceberg-jars.sha256`. This validates
Spark batch reads; SDP streaming and Databricks foreign-table reads remain
pending. Spark population was not run.
