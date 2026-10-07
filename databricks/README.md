# Databricks comparison experiment

Status: test definitions prepared; remote execution pending workspace selection,
authentication, compute selection, and AWS/Unity Catalog access configuration.
No claim that native SDP fails or OSS Spark succeeds on Databricks has been established.

## Test A: native Databricks SDP and Unity Catalog foreign Iceberg

1. Federate the existing Glue source into Unity Catalog. Use an approved service
   credential for Glue and a storage credential/external location for the source
   bucket. Configure an explicit foreign catalog storage location for Iceberg.
2. Run `native_preflight.py` against the three-part foreign table name. Confirm
   that batch reads succeed and table metadata identifies a foreign Iceberg table.
   Permission, storage, or federation failures are setup failures, not evidence
   that streaming is unsupported.
3. Create a dedicated native SDP pipeline using `native_foreign_pipeline.py`,
   configuring `spark.demo.source_table` to that exact UC name and a separate
   managed target schema. Do not install OSS Iceberg JARs into native compute.
4. Execute the pipeline. Record the actual pipeline update ID, compute/runtime
   settings, and error class/message/stack in ignored local artifacts. If it
   succeeds, append one unique batch and validate incremental ingestion and
   checkpoint recovery. Do not assume the expected failure.

## Test B: isolated OSS Spark running on Databricks compute

Use suitable dedicated classic compute, selected by the user. Create a separate
uv environment and invoke its `spark-pipelines` executable as a subprocess,
with an independent Spark home, Python interpreter, JVM, destination warehouse,
and checkpoint location. Do not replace notebook PySpark or modify Databricks'
existing Spark session. Clear inherited Spark/Connect environment variables
before starting the isolated process.

Install the project's locked OSS SDP environment and the checksum-verified
Iceberg JARs. Package and Maven mirrors are configured privately on the compute;
no mirror/proxy addresses may enter Git. Use a role scoped to the demo Glue
objects and source S3 prefix for direct GlueCatalog access. Laptop AWS CLI login
credentials are not automatically available to the remote process. Do not upload
root credentials or commit credentials to work around this prerequisite.

Repeat the local test sequence with persistent test storage: initial ingestion,
a new append, no-data restart, and another append after restart. Persist outputs
and checkpoints beyond a job process if recovery spans jobs; driver-local files
alone do not survive cluster replacement. Report this as driver-local OSS Spark
unless an OSS distributed cluster has actually been configured and tested.

This path reads the same Iceberg source directly through GlueCatalog. It bypasses
Unity Catalog federation, so its success would demonstrate a workaround, not
native foreign-table streaming support. A separate OSS test through Unity
Catalog's Iceberg REST API would be a third experiment.

Detailed workspace URLs, credentials, cluster/pipeline IDs, raw errors, and
AWS account configuration stay in ignored local artifacts. Publish only reviewed
portable code and aggregate results after actual execution.
