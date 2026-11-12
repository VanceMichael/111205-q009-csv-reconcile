import unittest
from decimal import Decimal
from service import parse_manifest

class CsvReconcileTests(unittest.TestCase):
    def test_digest_and_rows(self):
        digest, rows = parse_manifest(b"item,qty\na,2\n")
        self.assertEqual(len(digest), 64)
        self.assertEqual(rows[0]['qty'], '2')

    def test_rejects_invalid_encoding(self):
        with self.assertRaises(UnicodeDecodeError):
            parse_manifest(b'\xff')


if __name__ == '__main__':
    unittest.main()
