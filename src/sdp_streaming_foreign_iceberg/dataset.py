"""Deterministic observations and append-only Glue Iceberg population."""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import subprocess
import time
import tomllib

SCHEMA = """event_id string, event_time timestamp, ingested_at timestamp,
sequence_number long, cloud_provider string, region string, cluster_id string,
node_id string, gpu_uuid string, gpu_model string, gpu_index int,
workload_id string, workload_type string, gpu_utilization_pct double,
memory_used_mib long, memory_total_mib long, power_draw_w double,
power_limit_w double, gpu_temperature_c double, ecc_uncorrectable_errors_total long,
last_xid_error int, is_simulated boolean, schema_version int"""


def observations(config):
    """Yield ordered observations; end time is exclusive; ingestion is assigned by writer."""
    sim = config['simulation']
    step, commit, duration = (sim[k] for k in
        ('sample_interval_seconds', 'commit_interval_seconds', 'duration_seconds'))
    if min(step, commit, duration, sim['gpus_per_cloud']) <= 0 or commit % step or duration % commit:
        raise ValueError('Positive intervals required; commit must divide duration and sample must divide commit')
    start = datetime.fromisoformat(sim['start_time'].replace('Z', '+00:00'))
    if start.tzinfo is None:
        raise ValueError('start_time must include a timezone')
    start = start.astimezone(timezone.utc).replace(tzinfo=None)
    regions = {'aws': 'us-west-2', 'azure': 'westus2', 'oci': 'us-phoenix-1', 'gcp': 'us-west1'}
    rng = random.Random(sim['seed'])
    temperatures = {}
    for seq, offset in enumerate(range(0, duration, step)):
        for cloud in sim['clouds']:
            for index in range(sim['gpus_per_cloud']):
                gpu = f'GPU-{cloud}-{index:04d}'
                identity = f"{sim['seed']}:{start.isoformat()}:{gpu}:{seq}"
                workload = ('idle', 'inference', 'training')[index % 3]
                base = {'idle': 3, 'inference': 45, 'training': 85}[workload]
                utilization = round(max(0, min(100, base + 10 * math.sin(offset / 30 + index) + rng.uniform(-3, 3))), 3)
                power = round(55 + 5 * utilization, 3)
                target = 30 + 0.06 * power
                temp = temperatures.get(gpu, 30) + 0.1 * (target - temperatures.get(gpu, 30))
                temperatures[gpu] = temp
                yield {
                    'event_id': hashlib.sha256(identity.encode()).hexdigest(),
                    'event_time': start + timedelta(seconds=offset), 'ingested_at': None,
                    'sequence_number': seq, 'cloud_provider': cloud, 'region': regions[cloud],
                    'cluster_id': f'{cloud}-demo', 'node_id': f'{cloud}-node-{index // 8:03d}',
                    'gpu_uuid': gpu, 'gpu_model': 'H100-simulated', 'gpu_index': index % 8,
                    'workload_id': f'{cloud}-{workload}-{index // 8}', 'workload_type': workload,
                    'gpu_utilization_pct': utilization,
                    'memory_used_mib': int(81920 * (0.05 + 0.008 * utilization)),
                    'memory_total_mib': 81920, 'power_draw_w': power, 'power_limit_w': 700.0,
                    'gpu_temperature_c': round(temp, 3), 'ecc_uncorrectable_errors_total': 0,
                    'last_xid_error': 0, 'is_simulated': True, 'schema_version': 1,
                }


def spark_session(config, jars, extra_configuration=None):
    # AWS CLI login credentials may not be understood by the bundled Java SDK.
    # Export them in memory only; never put them in Spark properties or logs.
    aws = config['aws']
    credentials = json.loads(subprocess.check_output([
        'aws', 'configure', 'export-credentials', '--profile', aws['profile'], '--format', 'process'
    ], text=True))
    for field, env in [('AccessKeyId', 'AWS_ACCESS_KEY_ID'), ('SecretAccessKey', 'AWS_SECRET_ACCESS_KEY'),
                       ('SessionToken', 'AWS_SESSION_TOKEN')]:
        if credentials.get(field):
            os.environ[env] = credentials[field]
        else:
            os.environ.pop(env, None)
    os.environ['AWS_REGION'] = aws['region']
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.master('local[2]').appName('sdp-foreign-iceberg-source')
        .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2')
        .config('spark.ui.enabled', 'false')
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.sql.catalog.gpu', 'org.apache.iceberg.spark.SparkCatalog')
        .config('spark.sql.catalog.gpu.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
        .config('spark.sql.catalog.gpu.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .config('spark.sql.catalog.gpu.warehouse', aws['warehouse'])
        .config('spark.sql.catalog.gpu.client.region', aws['region']))
    if jars:
        paths = sorted(Path(jars).glob('*.jar'))
        if len(paths) != 2:
            raise ValueError('JAR directory must contain exactly the two pinned Iceberg JARs')
        builder = builder.config('spark.jars', ','.join(str(p.resolve()) for p in paths))
    else:
        runtime = config['runtime']
        builder = builder.config('spark.jars.packages', ','.join([
            runtime['iceberg_runtime'], runtime['iceberg_aws_bundle']]))
    for key, value in (extra_configuration or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['generate', 'populate', 'verify'])
    parser.add_argument('--config', type=Path, default=Path('config/local.toml'))
    parser.add_argument('--jars', type=Path)
    parser.add_argument('--output', type=Path, default=Path('artifacts/dataset-result.json'))
    parser.add_argument('--realtime', action='store_true', help='Wait one commit interval between batches')
    args = parser.parse_args()
    config = tomllib.loads(args.config.read_text())
    rows = list(observations(config))
    logical = hashlib.sha256(json.dumps(rows, default=str, sort_keys=True).encode()).hexdigest()
    report = {'expected_rows': len(rows), 'logical_sha256': logical, 'mode': args.mode}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.mode == 'generate':
        args.output.write_text(json.dumps(rows, default=str, indent=2) + '\n')
        print(json.dumps(report))
        return
    aws = config['aws']
    for identifier in (aws['database'], aws['table']):
        if not re.fullmatch('[a-z][a-z0-9_]*', identifier):
            raise ValueError('Database/table must be lowercase SQL identifiers')
    table = f"gpu.{aws['database']}.{aws['table']}"
    spark = spark_session(config, args.jars)
    try:
        if args.mode == 'populate':
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS gpu.{aws['database']}")
            spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({SCHEMA}) USING iceberg "
                      "PARTITIONED BY (days(event_time)) TBLPROPERTIES "
                      "('format-version'='2', 'write.format.default'='parquet')")
            if spark.table(table).limit(1).count():
                raise ValueError('Refusing to seed a nonempty table; use verify or a fresh table name')
            batch_size = len(config['simulation']['clouds']) * config['simulation']['gpus_per_cloud'] * (
                config['simulation']['commit_interval_seconds'] // config['simulation']['sample_interval_seconds'])
            for offset in range(0, len(rows), batch_size):
                if offset and args.realtime:
                    time.sleep(config['simulation']['commit_interval_seconds'])
                batch = rows[offset:offset + batch_size]
                ingested = datetime.now(timezone.utc).replace(tzinfo=None)
                for row in batch:
                    row['ingested_at'] = ingested
                spark.createDataFrame(batch, schema=SCHEMA).coalesce(1).writeTo(table).append()
                print(f'Committed batch {offset // batch_size + 1}: {len(batch)} rows', flush=True)
        from pyspark.sql import functions as F
        df = spark.table(table)
        summary = df.agg(F.count('*').alias('rows'), F.countDistinct('event_id').alias('distinct_events'),
                         F.countDistinct('gpu_uuid').alias('gpus')).first().asDict()
        ids = {r.event_id for r in df.select('event_id').collect()}
        expected = {r['event_id'] for r in rows}
        snapshots = [r.asDict() for r in spark.sql(
            f'SELECT committed_at, snapshot_id, parent_id, operation, summary FROM {table}.snapshots ORDER BY committed_at').collect()]
        report.update(summary, snapshots=snapshots, missing_events=len(expected - ids), unexpected_events=len(ids - expected),
                      table_properties=[r.asDict() for r in spark.sql(f'SHOW TBLPROPERTIES {table}').collect()],
                      cloud_counts=[r.asDict() for r in df.groupBy('cloud_provider').count().orderBy('cloud_provider').collect()])
        args.output.write_text(json.dumps(report, default=str, indent=2) + '\n')
        if summary['rows'] != len(rows) or len(ids) != len(rows) or ids != expected:
            raise ValueError(f'Dataset validation failed; inspect {args.output}')
        print(f"Verified {summary['rows']} rows, {summary['gpus']} GPUs, {len(snapshots)} snapshots")
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
