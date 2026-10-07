"""Create and verify the append-only Iceberg source using PyIceberg (no JVM)."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import tomllib
from .dataset import SCHEMA, observations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['populate', 'verify'])
    parser.add_argument('--config', type=Path, default=Path('config/local.toml'))
    parser.add_argument('--output', type=Path, default=Path('artifacts/source-result.json'))
    parser.add_argument('--realtime', action='store_true')
    args = parser.parse_args()
    config = tomllib.loads(args.config.read_text())
    aws, sim = config['aws'], config['simulation']
    for identifier in (aws['database'], aws['table']):
        if not re.fullmatch('[a-z][a-z0-9_]*', identifier):
            raise ValueError('Database/table must be lowercase SQL identifiers')
    # Export refreshed CLI credentials in memory; never persist or log them.
    credentials = json.loads(subprocess.check_output([
        'aws', 'configure', 'export-credentials', '--profile', aws['profile'], '--format', 'process'
    ], text=True))
    from pyiceberg.catalog import load_catalog
    import pyarrow as pa
    properties = {'type': 'glue', 'warehouse': aws['warehouse'],
                  'client.region': aws['region'], 's3.region': aws['region']}
    for prefix in ('client', 's3'):
        for name, key in [('access-key-id', 'AccessKeyId'), ('secret-access-key', 'SecretAccessKey'),
                          ('session-token', 'SessionToken')]:
            if credentials.get(key):
                properties[f'{prefix}.{name}'] = credentials[key]
    catalog = load_catalog('source', **properties)
    types = {'string': pa.string(), 'long': pa.int64(), 'int': pa.int32(),
             'timestamp': pa.timestamp('us'), 'double': pa.float64(), 'boolean': pa.bool_()}
    schema = pa.schema([pa.field(name, types[kind]) for name, kind in
                       (column.strip().split() for column in SCHEMA.split(','))])
    rows = list(observations(config))
    digest = hashlib.sha256(json.dumps(rows, default=str, sort_keys=True).encode()).hexdigest()
    identifier = (aws['database'], aws['table'])
    if args.mode == 'populate':
        catalog.create_namespace_if_not_exists(aws['database'])
        if catalog.table_exists(identifier):
            table = catalog.load_table(identifier)
        else:
            from pyiceberg.partitioning import PartitionSpec, PartitionField
            from pyiceberg.transforms import DayTransform
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField, StringType, LongType, IntegerType, TimestampType, DoubleType, BooleanType
            iceberg_types = {'string': StringType(), 'long': LongType(), 'int': IntegerType(),
                             'timestamp': TimestampType(), 'double': DoubleType(), 'boolean': BooleanType()}
            iceberg_schema = Schema(*[NestedField(i, name, iceberg_types[kind], required=False)
                for i, (name, kind) in enumerate(
                    (column.strip().split() for column in SCHEMA.split(',')), start=1)])
            table = catalog.create_table(identifier, schema=iceberg_schema,
                partition_spec=PartitionSpec(PartitionField(source_id=2, field_id=1000,
                    transform=DayTransform(), name='event_day')),
                properties={'format-version': '2', 'write.format.default': 'parquet'})
        if table.scan().count():
            raise ValueError('Refusing to seed a nonempty table; use verify or a fresh table name')
        size = len(sim['clouds']) * sim['gpus_per_cloud'] * (
            sim['commit_interval_seconds'] // sim['sample_interval_seconds'])
        for offset in range(0, len(rows), size):
            if offset and args.realtime:
                time.sleep(sim['commit_interval_seconds'])
            batch = rows[offset:offset + size]
            for row in batch:
                row['ingested_at'] = datetime.now(timezone.utc).replace(tzinfo=None)
            table.append(pa.Table.from_pylist(batch, schema=schema))
            print(f'Committed batch {offset // size + 1}: {len(batch)} rows', flush=True)
    table = catalog.load_table(identifier)
    actual = table.scan().to_arrow().to_pylist()
    ids, expected = {r['event_id'] for r in actual}, {r['event_id'] for r in rows}
    snapshots = [s.model_dump(mode='json') for s in table.snapshots()]
    report = {'writer': 'pyiceberg', 'format_version': table.metadata.format_version,
        'rows': len(actual), 'distinct_events': len(ids), 'expected_rows': len(rows),
        'logical_sha256': digest, 'missing_events': len(expected - ids),
        'unexpected_events': len(ids - expected), 'gpus': len({r['gpu_uuid'] for r in actual}),
        'cloud_counts': dict(Counter(r['cloud_provider'] for r in actual)),
        'snapshots': snapshots, 'schema': str(table.schema()), 'partition_spec': str(table.spec()),
        'validated_at': datetime.now(timezone.utc).isoformat()}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    actual_logical = [{**r, 'ingested_at': None} for r in actual]
    ordered = lambda data: sorted(data, key=lambda r: r['event_id'])
    if len(actual) != len(rows) or ids != expected or ordered(actual_logical) != ordered(rows):
        # Population sets ingested_at on rows; compare only logical observations.
        clean = [{**r, 'ingested_at': None} for r in rows]
        if len(actual) != len(rows) or ids != expected or ordered(actual_logical) != ordered(clean):
            raise ValueError(f'Dataset verification failed; inspect {args.output}')
    if any(s['summary']['operation'] != 'append' for s in snapshots):
        raise ValueError('Expected append snapshots only')
    print(f'Verified {len(actual)} rows, {len(ids)} distinct events, {len(snapshots)} append snapshots')


if __name__ == '__main__':
    main()
