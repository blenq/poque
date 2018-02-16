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
        self.assertIsInstance(self.poque.POLLING_FAILED, int)

        self.assertIsInstance(self.poque.INVALID_OID, int)
        self.assertIsInstance(self.poque.INT4OID, int)
        self.assertIsInstance(self.poque.OIDOID, int)

        self.assertIsInstance(self.poque.NUMERICOID, int)
        self.assertIsInstance(self.poque.NUMERICARRAYOID, int)
        poque = self.poque

        for var, val in [(poque.INVALID_OID, 0),
                         (poque.BOOLOID, 16),
                         (poque.BYTEAOID, 17),
                         (poque.CHAROID, 18),
                         (poque.NAMEOID, 19),
                         (poque.INT8OID, 20),
                         (poque.INT2OID, 21),
                         (poque.INT2VECTOROID, 22),
                         (poque.INT4OID, 23),
                         (poque.REGPROCOID, 24),
                         (poque.TEXTOID, 25),
                         (poque.OIDOID, 26),
                         (poque.TIDOID, 27),
                         (poque.XIDOID, 28),
                         (poque.CIDOID, 29),
                         (poque.OIDVECTOROID, 30),
                         (poque.JSONOID, 114),
                         (poque.XMLOID, 142),
                         (poque.XMLARRAYOID, 143),
                         (poque.JSONARRAYOID, 199),
                         (poque.POINTOID, 600),
                         (poque.LSEGOID, 601),
                         (poque.PATHOID, 602),
                         (poque.BOXOID, 603),
                         (poque.POLYGONOID, 604),
                         (poque.LINEOID, 628),
                         (poque.LINEARRAYOID, 629),
                         (poque.CIDROID, 650),
                         (poque.CIDRARRAYOID, 651),
                         (poque.FLOAT4OID, 700),
                         (poque.FLOAT8OID, 701),
                         (poque.ABSTIMEOID, 702),
                         (poque.RELTIMEOID, 703),
                         (poque.TINTERVALOID, 704),
                         (poque.UNKNOWNOID, 705),
                         (poque.CIRCLEOID, 718),
                         (poque.CIRCLEARRAYOID, 719),
                         (poque.MACADDR8OID, 774),
                         (poque.MACADDR8ARRAYOID, 775),
                         (poque.CASHOID, 790),
                         (poque.CASHARRAYOID, 791),
                         (poque.MACADDROID, 829),
                         (poque.INETOID, 869),
                         (poque.BOOLARRAYOID, 1000),
                         (poque.BYTEAARRAYOID, 1001),
                         (poque.CHARARRAYOID, 1002),
                         (poque.NAMEARRAYOID, 1003),
                         (poque.INT2ARRAYOID, 1005),
                         (poque.INT2VECTORARRAYOID, 1006),
                         (poque.INT4ARRAYOID, 1007),
                         (poque.REGPROCARRAYOID, 1008),
                         (poque.TEXTARRAYOID, 1009),
                         (poque.TIDARRAYOID, 1010),
                         (poque.XIDARRAYOID, 1011),
                         (poque.CIDARRAYOID, 1012),
                         (poque.OIDVECTORARRAYOID, 1013),
                         (poque.BPCHARARRAYOID, 1014),
                         (poque.VARCHARARRAYOID, 1015),
                         (poque.INT8ARRAYOID, 1016),
                         (poque.POINTARRAYOID, 1017),
                         (poque.LSEGARRAYOID, 1018),
                         (poque.PATHARRAYOID, 1019),
                         (poque.BOXARRAYOID, 1020),
                         (poque.FLOAT4ARRAYOID, 1021),
                         (poque.FLOAT8ARRAYOID, 1022),
                         (poque.ABSTIMEARRAYOID, 1023),
                         (poque.RELTIMEARRAYOID, 1024),
                         (poque.TINTERVALARRAYOID, 1025),
                         (poque.POLYGONARRAYOID, 1027),
                         (poque.OIDARRAYOID, 1028),
                         (poque.MACADDRARRAYOID, 1040),
                         (poque.INETARRAYOID, 1041),
                         (poque.BPCHAROID, 1042),
                         (poque.VARCHAROID, 1043),
                         (poque.DATEOID, 1082),
                         (poque.TIMEOID, 1083),
                         (poque.TIMESTAMPOID, 1114),
                         (poque.TIMESTAMPARRAYOID, 1115),
                         (poque.DATEARRAYOID, 1182),
                         (poque.TIMEARRAYOID, 1183),
                         (poque.TIMESTAMPTZOID, 1184),
                         (poque.TIMESTAMPTZARRAYOID, 1185),
                         (poque.INTERVALOID, 1186),
                         (poque.INTERVALARRAYOID, 1187),
                         (poque.NUMERICARRAYOID, 1231),
                         (poque.CSTRINGARRAYOID, 1263),
                         (poque.TIMETZOID, 1266),
                         (poque.TIMETZARRAYOID, 1270),
                         (poque.BITOID, 1560),
                         (poque.BITARRAYOID, 1561),
                         (poque.VARBITOID, 1562),
                         (poque.VARBITARRAYOID, 1563),
                         (poque.NUMERICOID, 1700),
                         (poque.CSTRINGOID, 2275),
                         (poque.UUIDOID, 2950),
                         (poque.UUIDARRAYOID, 2951),
                         (poque.JSONBOID, 3802),
                         (poque.JSONBARRAYOID, 3807),
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
