import unittest

from test.config import BaseExtensionTest, BaseCTypesTest


def assert_is_conninfo(self, info):
    self.assertIsInstance(info, dict)
    for k, v in info.items():
        self.assertIsInstance(k, str)
        for item in v[:5]:
            self.assertIsInstance(item, (str, type(None)))
        self.assertIsInstance(v[5], int)


class TestLib():

    def test_constants(self):
        self.assertIsInstance(self.poque.CONNECTION_OK, int)
        self.assertIsInstance(self.poque.CONNECTION_BAD, int)
        self.assertIsInstance(self.poque.CONNECTION_MADE, int)
        self.assertIsInstance(self.poque.CONNECTION_AWAITING_RESPONSE, int)
        self.assertIsInstance(self.poque.CONNECTION_STARTED, int)
        self.assertIsInstance(self.poque.CONNECTION_AUTH_OK, int)
        self.assertIsInstance(self.poque.CONNECTION_SSL_STARTUP, int)
        self.assertIsInstance(self.poque.CONNECTION_SETENV, int)

        self.assertIsInstance(self.poque.TRANS_IDLE, int)
        self.assertIsInstance(self.poque.TRANS_ACTIVE, int)
        self.assertIsInstance(self.poque.TRANS_INTRANS, int)
        self.assertIsInstance(self.poque.TRANS_INERROR, int)
        self.assertIsInstance(self.poque.TRANS_UNKNOWN, int)

        self.assertIsInstance(self.poque.POLLING_READING, int)
        self.assertIsInstance(self.poque.POLLING_WRITING, int)
        self.assertIsInstance(self.poque.POLLING_OK, int)

        self.assertIsInstance(self.poque.INVALID_OID, int)
        self.assertIsInstance(self.poque.INT4OID, int)
        self.assertIsInstance(self.poque.OIDOID, int)

        self.assertIsInstance(self.poque.NUMERICOID, int)
        self.assertIsInstance(self.poque.NUMERICARRAYOID, int)
        self.assertEqual(self.poque.JSONBOID, 3802)
        self.assertEqual(self.poque.JSONBARRAYOID, 3807)
        self.assertEqual(self.poque.LINEOID, 628)
        self.assertEqual(self.poque.LINEARRAYOID, 629)
        self.assertEqual(self.poque.BITOID, 1560)
        self.assertEqual(self.poque.VARBITOID, 1562)
        self.assertEqual(self.poque.BITARRAYOID, 1561)
        self.assertEqual(self.poque.VARBITARRAYOID, 1563)
        self.assertEqual(self.poque.TIMETZOID, 1266)

    def test_conn_defaults(self):
        d = self.poque.conn_defaults()
        assert_is_conninfo(self, d)

    def test_conninfo_parse(self):
        d = self.poque.conninfo_parse('dbname=postgres')
        assert_is_conninfo(self, d)
        self.assertEqual(d['dbname'][2], 'postgres')

    def test_conninfo_parse_wrong(self):
        with self.assertRaises(self.poque.Error):
            self.poque.conninfo_parse('zut')

    def test_libversion(self):
        self.assertGreaterEqual(self.poque.lib_version(), 90100)


class TestLibExtension(
        BaseExtensionTest, TestLib, unittest.TestCase):
    pass


class TestLibCtypes(
        BaseCTypesTest, TestLib, unittest.TestCase):
    pass
