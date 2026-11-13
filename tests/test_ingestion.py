import unittest

import context  # noqa: F401  python3 -I 下注入仓库根
from collector.ingestion import ingest_bytes
from test_collector_models import make_config

HEADER = b'sku,qty,price,note\n'


class IngestionGateTests(unittest.TestCase):
    def test_accepted_snapshot_digest_matches_parse_manifest(self):
        from service import parse_manifest
        content = HEADER + b'A,1.00,2.50,x\n'
        digest, _ = parse_manifest(content)
        result = ingest_bytes(make_config(), content, generation=1,
                              source_name='f.csv', declared_rows=1)
        self.assertTrue(result.accepted)
        self.assertEqual(result.digest, digest)
        self.assertEqual(result.row_count, 1)

    def test_source_declared_incomplete_is_rejected(self):
        result = ingest_bytes(
            make_config(), HEADER + b'A,1,2,x\n',
            generation=2, source_name='half.csv',
            declared_rows=1, declared_complete=False,
        )
        self.assertTrue(result.rejected)
        self.assertEqual(result.reason, 'source_declared_incomplete')

    def test_declared_row_count_mismatch_is_rejected(self):
        result = ingest_bytes(
            make_config(), HEADER + b'A,1,2,x\n',
            generation=1, source_name='trunc.csv',
            declared_rows=3, declared_complete=True,
        )
        self.assertTrue(result.rejected)
        self.assertEqual(result.reason, 'row_count_mismatch')
        self.assertTrue(any('声明行数' in e for e in result.errors))

    def test_bad_decimal_row_is_rejected(self):
        content = HEADER + b'A,not-a-number,2.50,x\n'
        result = ingest_bytes(make_config(), content, generation=1,
                              source_name='bad.csv', declared_rows=1)
        self.assertTrue(result.rejected)
        self.assertEqual(result.reason, 'bad_row')

    def test_ragged_row_is_rejected(self):
        content = HEADER + b'A,1,2\n'  # 缺列
        result = ingest_bytes(make_config(), content, generation=1,
                              source_name='ragged.csv', declared_rows=1)
        self.assertTrue(result.rejected)
        self.assertEqual(result.reason, 'bad_row')


if __name__ == '__main__':
    unittest.main()
