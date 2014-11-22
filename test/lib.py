import unittest
import poque
import sys


def assert_is_conninfo(self, info):
    self.assertIsInstance(info, dict)
    for k, v in info.items():
        self.assertIsInstance(k, str)
        for item in v[:5]:
            self.assertIsInstance(item, (str, type(None)))
        self.assertIsInstance(v[5], int)


class TestLib(unittest.TestCase):

    def test_constants(self):
        self.assertIsInstance(poque.CONNECTION_OK, int)
        self.assertIsInstance(poque.CONNECTION_BAD, int)
        self.assertIsInstance(poque.CONNECTION_MADE, int)
        self.assertIsInstance(poque.CONNECTION_AWAITING_RESPONSE, int)
        self.assertIsInstance(poque.CONNECTION_STARTED, int)
        self.assertIsInstance(poque.CONNECTION_AUTH_OK, int)
        self.assertIsInstance(poque.CONNECTION_SSL_STARTUP, int)
        self.assertIsInstance(poque.CONNECTION_SETENV, int)

        self.assertIsInstance(poque.TRANS_IDLE, int)
        self.assertIsInstance(poque.TRANS_ACTIVE, int)
        self.assertIsInstance(poque.TRANS_INTRANS, int)
        self.assertIsInstance(poque.TRANS_INERROR, int)
        self.assertIsInstance(poque.TRANS_UNKNOWN, int)

        self.assertIsInstance(poque.POLLING_READING, int)
        self.assertIsInstance(poque.POLLING_WRITING, int)
        self.assertIsInstance(poque.POLLING_OK, int)

        self.assertIsInstance(poque.INVALID_OID, int)
        self.assertIsInstance(poque.INT4OID, int)
        self.assertIsInstance(poque.OIDOID, int)

    def test_conn_defaults(self):
        d = poque.conn_defaults()
        assert_is_conninfo(self, d)

    def test_conninfo_parse(self):
        d = poque.conninfo_parse('dbname=postgres')
        assert_is_conninfo(self, d)
        self.assertEqual(d['dbname'][2], 'postgres')

    def test_conninfo_parse_wrong(self):
        with self.assertRaises(poque.Error):
            poque.conninfo_parse('zut')

    def test_libversion(self):
        self.assertGreaterEqual(poque.libversion(), 90100)
