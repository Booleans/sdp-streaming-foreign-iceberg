import unittest
import tomllib
from pathlib import Path
from sdp_streaming_foreign_iceberg.dataset import observations


class DatasetTests(unittest.TestCase):
    def test_repeatable_complete_fleet(self):
        config = tomllib.loads(Path('config/example.toml').read_text())
        rows = list(observations(config))
        self.assertEqual(rows, list(observations(config)))
        self.assertEqual(len(rows), 15360)
        self.assertEqual(len({r['event_id'] for r in rows}), len(rows))
        fleet = {}
        for r in rows:
            fleet.setdefault(r['gpu_uuid'], []).append(r['sequence_number'])
            self.assertLessEqual(r['memory_used_mib'], r['memory_total_mib'])
            self.assertTrue(0 <= r['gpu_utilization_pct'] <= 100)
        self.assertEqual(len(fleet), 128)
        self.assertTrue(all(seq == list(range(120)) for seq in fleet.values()))

    def test_invalid_intervals(self):
        config = tomllib.loads(Path('config/example.toml').read_text())
        config['simulation']['commit_interval_seconds'] = 7
        with self.assertRaises(ValueError):
            list(observations(config))
