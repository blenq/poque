import unittest
import sys

from test.config import BaseExtensionTest, BaseCTypesTest


def assert_is_conninfo(self, info):
    self.assertIsInstance(info, dict)
    for k, v in info.items():
        self.assertIsInstance(k, str)
        self.assertIsInstance(v, tuple)
#         self.assertNotEqual(type(v), tuple)
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
        poque = self.poque

        for var, val in [(poque.INT2OID, 21),
                         (poque.INT2ARRAYOID, 1005),
                         (poque.INT4OID, 23),
                         (poque.INT4ARRAYOID, 1007),
                         (poque.INT8OID, 20),
                         (poque.INT8ARRAYOID, 1016),
                         (poque.INT2VECTOROID, 22),
                         (poque.INT2VECTORARRAYOID, 1006),
                         (poque.BOOLARRAYOID, 1000),
                         (poque.JSONBOID, 3802),
                         (poque.JSONBARRAYOID, 3807),
                         (poque.LINEOID, 628),
                         (poque.LINEARRAYOID, 629),
                         (poque.BITOID, 1560),
                         (poque.VARBITOID, 1562),
                         (poque.BITARRAYOID, 1561),
                         (poque.VARBITARRAYOID, 1563),
                         (poque.TIMETZOID, 1266),
                         (poque.UUIDARRAYOID, 2951),
                         ]:
            self.assertEqual(var, val)

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

    def test_ssl_loaded(self):
        self.assertTrue('ssl' in sys.modules)

    def test_encrypt_password(self):
        self.assertEqual(
            self.poque.encrypt_password('password', 'user'),
            'md54d45974e13472b5a0be3533de4666414')
        self.assertEqual(
            self.poque.encrypt_password(user='user', password='password'),
            'md54d45974e13472b5a0be3533de4666414')


class TestLibExtension(
        BaseExtensionTest, TestLib, unittest.TestCase):
    pass


class TestLibCtypes(
        BaseCTypesTest, TestLib, unittest.TestCase):
    pass
