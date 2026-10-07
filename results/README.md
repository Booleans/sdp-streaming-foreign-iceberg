# Experiment evidence

No AWS, Iceberg streaming, or Databricks experiments have been run yet.

For each experiment create a directory named after the run ID and record:

- Git commit and environment JSON produced by `gpu-iceberg`.
- Exact commands, sanitized configuration, start/end UTC times, and exit codes.
- Source table schema, properties, snapshot IDs and snapshot operations.
- Expected and observed row counts, distinct event counts, and sequence gaps.
- Checkpoint location and whether it was new or reused.
- Full sanitized exception and logs for failures.
- For Databricks: runtime version or pipeline channel, compute access mode,
  serverless/classic mode, pipeline settings, and foreign catalog configuration.

Use `artifacts/` for raw local outputs. Review files for credentials and private
account details before copying selected evidence here and committing it.
Do not publish a successful result based only on a batch query or a dry run.
