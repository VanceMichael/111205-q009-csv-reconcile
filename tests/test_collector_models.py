import unittest
from decimal import Decimal

import context  # noqa: F401  python3 -I 下注入仓库根
from collector.config import SupplierConfig
from collector.models import build_snapshot, normalize_row


def make_config(**overrides):
    payload = {
        'supplier': 'vendor-x',
        'candidate_key': ['sku'],
        'compare_columns': ['sku', 'qty', 'price'],
        'ignored_columns': ['note'],
        'numeric_columns': ['qty', 'price'],
    }
    payload.update(overrides)
    return SupplierConfig.from_manifest(payload)


class ConfigTests(unittest.TestCase):
    def test_numeric_columns_compare_as_decimal(self):
        config = make_config()
        self.assertEqual(config.compare_value('qty', '1.50'), Decimal('1.50'))
        # 1.50 与 1.5 在十进制上相等
        self.assertEqual(config.compare_value('qty', '1.50'),
                         config.compare_value('qty', '1.5'))
        self.assertEqual(config.compare_value('sku', ' A '), ' A ')

    def test_invalid_config_rejected(self):
        with self.assertRaises(ValueError):
            make_config(candidate_key=['missing'])
        with self.assertRaises(ValueError):
            make_config(ignored_columns=['qty'])
        with self.assertRaises(ValueError):
            make_config(numeric_columns=['note'])
        with self.assertRaises(ValueError):
            make_config(compare_columns=[])


class NormalizationTests(unittest.TestCase):
    def test_ignored_column_not_in_fingerprint(self):
        config = make_config()
        row1 = normalize_row(config, {'sku': 'A', 'qty': '1', 'price': '2',
                                      'note': 'first'}, 1)
        row2 = normalize_row(config, {'sku': 'A', 'qty': '1', 'price': '2',
                                      'note': 'second'}, 2)
        self.assertEqual(row1.fingerprint, row2.fingerprint)

    def test_invalid_decimal_row_raises(self):
        config = make_config()
        with self.assertRaises(Exception):
            normalize_row(config, {'sku': 'A', 'qty': 'oops', 'price': '2'}, 1)

    def test_empty_candidate_key_raises(self):
        config = make_config()
        with self.assertRaises(ValueError):
            normalize_row(config, {'sku': '  ', 'qty': '1', 'price': '2'}, 1)

    def test_snapshot_keeps_multiplicity(self):
        config = make_config()
        rows = [
            normalize_row(config, {'sku': 'A', 'qty': '1', 'price': '2',
                                   'note': 'a'}, 1),
            normalize_row(config, {'sku': 'A', 'qty': '1', 'price': '2',
                                   'note': 'b'}, 2),
            normalize_row(config, {'sku': 'A', 'qty': '2', 'price': '2',
                                   'note': 'c'}, 3),
        ]
        snapshot = build_snapshot(config, rows, digest='d1', generation=1,
                                  source_name='f.csv')
        entries = snapshot.multisets[('A',)]
        self.assertEqual(sum(entries.values()), 3)  # 不是 set 去重后的 2
        roundtrip = build_snapshot(
            config,
            [normalize_row(config, r.evidence.raw, r.evidence.source_row)
             for r in rows],
            digest='d1', generation=1, source_name='f.csv',
        )
        self.assertEqual(roundtrip.multisets, snapshot.multisets)


if __name__ == '__main__':
    unittest.main()
