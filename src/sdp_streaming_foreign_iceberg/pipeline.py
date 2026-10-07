"""Run and verify local open source SDP using persistent local Iceberg storage."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib
from .dataset import spark_session


def local_configuration(root):
    return {
        'spark.sql.catalog.local': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.local.type': 'hadoop',
        'spark.sql.catalog.local.warehouse': (root / 'warehouse').as_uri(),
        'spark.sql.sources.default': 'iceberg',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['run', 'verify'])
    parser.add_argument('--config', type=Path, default=Path('config/local.toml'))
    parser.add_argument('--jars', type=Path, default=Path('artifacts/runtime/jars'))
    parser.add_argument('--state', type=Path, default=Path('artifacts/local-sdp'))
    parser.add_argument('--expected-rows', type=int)
    parser.add_argument('--output', type=Path, default=Path('artifacts/pipeline-verification.json'))
    args = parser.parse_args()
    config = tomllib.loads(args.config.read_text())
    root = args.state.resolve()
    root.mkdir(parents=True, exist_ok=True)
    extra = local_configuration(root)
    aws = config['aws']
    source = f"gpu.{aws['database']}.{aws['table']}"
    if args.mode == 'run':
        # Bootstrap namespaces with a regular Spark session, then launch SDP in a fresh process.
        spark = spark_session(config, args.jars, extra)
        spark.sql('CREATE NAMESPACE IF NOT EXISTS local.demo')
        spark.stop()
        import yaml
        configuration = dict(extra)
        configuration.update({
            'spark.sql.catalog.gpu': 'org.apache.iceberg.spark.SparkCatalog',
            'spark.sql.catalog.gpu.catalog-impl': 'org.apache.iceberg.aws.glue.GlueCatalog',
            'spark.sql.catalog.gpu.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
            'spark.sql.catalog.gpu.warehouse': aws['warehouse'],
            'spark.sql.catalog.gpu.client.region': aws['region'],
            'spark.sql.session.timeZone': 'UTC',
            'spark.sql.shuffle.partitions': '2',
            'spark.demo.source_table': source,
        })
        definitions = root / 'transformations'
        definitions.mkdir(exist_ok=True)
        (definitions / 'iceberg_copy.py').write_text(Path('pipelines/iceberg_copy.py').read_text())
        spec = {'name': 'iceberg-stream-copy', 'storage': (root / 'checkpoints').as_uri(),
                'catalog': 'local', 'database': 'demo', 'configuration': configuration,
                'libraries': [{'glob': {'include': 'transformations/iceberg_copy.py'}}]}
        spec_path = root / 'spark-pipeline.yml'
        spec_path.write_text(yaml.safe_dump(spec))
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
        command = [str(Path(sys.executable).parent / 'spark-pipelines'), 'run',
                   '--remote', 'local[2]', '--jars', ','.join(str(p.resolve()) for p in sorted(args.jars.glob('*.jar'))),
                   '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                   '--conf', 'spark.ui.enabled=false', '--spec', str(spec_path)]
        subprocess.run(command, check=True)
        return
    spark = spark_session(config, args.jars, extra)
    try:
        src, dst = spark.table(source), spark.table('local.demo.iceberg_stream_copy')
        source_rows, target_rows = src.count(), dst.count()
        distinct = dst.select('event_id').distinct().count()
        # Compare complete rows, including ingestion timestamps and duplicates.
        missing, unexpected = src.exceptAll(dst).count(), dst.exceptAll(src).count()
        destination_snapshots = [r.asDict() for r in spark.sql(
            'SELECT snapshot_id, operation, summary FROM local.demo.iceberg_stream_copy.snapshots ORDER BY committed_at').collect()]
        checkpoint_ids = [json.loads(p.read_text()) for p in (root / 'checkpoints').rglob('metadata') if p.is_file()]
        report = {'destination_snapshots': destination_snapshots, 'checkpoint_ids': checkpoint_ids, 'source_rows': source_rows, 'destination_rows': target_rows,
                  'distinct_events': distinct, 'missing_rows': missing, 'unexpected_rows': unexpected,
                  'checkpoint_files': len(list((root / 'checkpoints').rglob('*')))}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        if missing or unexpected or target_rows != source_rows or distinct != target_rows:
            raise ValueError(f'Source/destination mismatch: {report}')
        if args.expected_rows is not None and target_rows != args.expected_rows:
            raise ValueError(f'Expected {args.expected_rows} rows: {report}')
        print(json.dumps(report))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
