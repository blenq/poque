import datetime
import sys
import unittest
import weakref

import poque

from . import config
from uuid import uuid4


class ResultTestBasic(unittest.TestCase):

    def setUp(self):
        cn = poque.Conn(config.conninfo())
        self.res = cn.execute("SELECT 1 AS yo")

    def test_execute(self):
        self.assertIsNotNone(self.res)

    def test_weakref(self):
        self.assertEquals(weakref.ref(self.res)(), self.res)

    def test_ntuples(self):
        self.assertEqual(self.res.ntuples, 1)

    def test_nfields(self):
        self.assertEqual(self.res.nfields, 1)

    def test_nparams(self):
        self.assertEqual(self.res.nparams, 0)

    def test_clear(self):
        self.res.clear()
        self.assertEqual(self.res.ntuples, 0)

    def test_fname(self):
        self.assertEqual(self.res.fname(0), 'yo')
        self.assertEqual(self.res.fname(column_number=0), 'yo')

    def test_invalid_fname(self):
        self.res.clear()
        self.assertIsNone(self.res.fname(0))

    def test_fnumber(self):
        self.assertEqual(self.res.fnumber('yo'), 0)
        self.assertEqual(self.res.fnumber('no'), -1)
        self.assertEqual(self.res.fnumber(column_name='yo'), 0)

    def test_invalid_fnumber(self):
        self.res.clear()
        self.assertEqual(self.res.fnumber('yo'), -1)

    def test_ftable(self):
        self.assertEqual(self.res.ftable(0), poque.INVALID_OID)
        self.assertEqual(self.res.ftable(column_number=0), poque.INVALID_OID)

    def test_invalid_ftable(self):
        self.res.clear()
        self.assertEqual(self.res.ftable(0), poque.INVALID_OID)

    def test_ftablecol(self):
        self.assertEqual(self.res.ftablecol(0), 0)
        self.assertEqual(self.res.ftablecol(column_number=0), 0)

    def test_invalid_ftablecol(self):
        self.res.clear()
        self.assertEqual(self.res.ftablecol(0), 0)

    def test_fformat(self):
        self.assertEqual(self.res.fformat(0), 1)
        self.assertEqual(self.res.fformat(column_number=0), 1)

    def test_invalid_fformat(self):
        self.res.clear()
        self.assertEqual(self.res.fformat(4), 0)
        with self.assertRaises(TypeError):
            self.res.fformat()

    def test_ftype(self):
        self.assertEqual(self.res.ftype(0), poque.INT4OID)
        self.assertEqual(self.res.ftype(3), 0)

    def test_fmod(self):
        self.assertEqual(self.res.fmod(0), -1)

    def test_fsize(self):
        self.assertEqual(self.res.fsize(0), 4)

    def test_getlength(self):
        self.assertEqual(self.res.getlength(0, 0), 4)

    def test_invalid_getlength(self):
        self.assertEqual(self.res.getlength(0, 1), 0)


class ResultTestValues(unittest.TestCase):

    def setUp(self):
        self.cn = poque.Conn(config.conninfo())

    def test_format(self):
        res = self.cn.execute(command="SELECT 1", format=0)
        self.assertEqual(res.fformat(0), 0)

    def test_formats(self):
        res = self.cn.execute(command="SELECT 6::int4", format=0)
        self.assertEqual(res.getvalue(0, 0), "6")
        self.assertEqual(res.getvalue(2, 3), "")
        res = self.cn.execute(command="SELECT 'hoi'", format=1)
        self.assertEqual(res.getvalue(0, 0), b"hoi")
        self.assertEqual(res.getvalue(2, 3), "")

    def test_int4_value_text(self):
        res = self.cn.execute(command="SELECT 6::int4", format=0)
        self.assertEqual(res.value(0, 0), 6)
        res = self.cn.execute(command="SELECT -1::int4", format=1)
        self.assertEqual(res.value(0, 0), -1)

    def test_int4_value_bin(self):
        res = self.cn.execute(command="SELECT 6::int4", format=1)
        self.assertEqual(res.value(0, 0), 6)
        res = self.cn.execute(command="SELECT -1::int4", format=1)
        self.assertEqual(res.value(0, 0), -1)

    def test_int2_value_text(self):
        res = self.cn.execute(command="SELECT 6::int2", format=0)
        self.assertEqual(res.value(0, 0), 6)
        self.assertEqual(res.ftype(0), poque.INT2OID)
        res = self.cn.execute(command="SELECT -1::int2", format=1)
        self.assertEqual(res.value(0, 0), -1)

    def test_int2_value_bin(self):
        res = self.cn.execute(command="SELECT 6::int2", format=1)
        self.assertEqual(res.value(0, 0), 6)
        res = self.cn.execute(command="SELECT -1::int2", format=1)
        self.assertEqual(res.value(0, 0), -1)

    def test_bool_value_str(self):
        res = self.cn.execute(command="SELECT true", format=0)
        self.assertIs(res.value(0, 0), True)
        res = self.cn.execute(command="SELECT false", format=0)
        self.assertIs(res.value(0, 0), False)

    def test_bool_value_bin(self):
        res = self.cn.execute(command="SELECT true", format=1)
        self.assertIs(res.value(0, 0), True)
        self.assertEqual(res.ftype(0), poque.BOOLOID)
        res = self.cn.execute(command="SELECT false", format=1)
        self.assertIs(res.value(0, 0), False)

    def test_bytea_value_str(self):
        self.cn.execute("SET bytea_output TO hex")
        res = self.cn.execute(command="SELECT 'hi'::bytea", format=0)
        self.assertEqual(res.value(0, 0), b'hi')
        self.assertEqual(res.ftype(0), poque.BYTEAOID)
        res = self.cn.execute(command="SELECT ''::bytea", format=0)
        self.assertEqual(res.value(0, 0), b'')
        self.assertEqual(res.ftype(0), poque.BYTEAOID)
        res = self.cn.execute(command="SELECT '\t'::bytea", format=0)
        self.assertEqual(res.value(0, 0), b'\t')
        self.assertEqual(res.ftype(0), poque.BYTEAOID)
        self.cn.execute("SET bytea_output TO escape")
        res = self.cn.execute(command="SELECT convert_to('\t \\', 'utf8')",
                              format=0)
        self.assertEqual(res.value(0, 0), b'\t \\')
        self.assertEqual(res.ftype(0), poque.BYTEAOID)

    def test_bytea_value_bin(self):
        res = self.cn.execute(command="SELECT 'hi'::bytea", format=1)
        self.assertEqual(res.value(0, 0), b'hi')
        self.assertEqual(res.ftype(0), poque.BYTEAOID)
        res = self.cn.execute(command="SELECT ''::bytea", format=1)
        self.assertEqual(res.value(0, 0), b'')
        self.assertEqual(res.ftype(0), poque.BYTEAOID)
        res = self.cn.execute(command="SELECT convert_to('\t \\', 'utf8')",
                              format=1)
        self.assertEqual(res.value(0, 0), b'\t \\')
        self.assertEqual(res.ftype(0), poque.BYTEAOID)

    def test_char_value_str(self):
        res = self.cn.execute(command="SELECT 'a'::\"char\"", format=0)
        self.assertEqual(res.value(0, 0), b'a')
        self.assertEqual(res.ftype(0), poque.CHAROID)
        res = self.cn.execute(command="SELECT '€'::\"char\"", format=0)
        self.assertEqual(res.value(0, 0), "€".encode()[:1])

    def test_char_value_bin(self):
        res = self.cn.execute(command="SELECT 'a'::\"char\"", format=1)
        self.assertEqual(res.value(0, 0), b'a')
        self.assertEqual(res.ftype(0), poque.CHAROID)
        res = self.cn.execute(command="SELECT '€'::\"char\"", format=1)
        self.assertEqual(res.value(0, 0), "€".encode()[:1])

    def test_name_value_bin(self):
        res = self.cn.execute(command="SELECT 'hel''lo €'::name", format=1)
        self.assertEqual(res.value(0, 0), "hel'lo €")
        self.assertEqual(res.ftype(0), poque.NAMEOID)

    def test_name_value_str(self):
        res = self.cn.execute(command="SELECT 'hel''lo €'::name", format=0)
        self.assertEqual(res.value(0, 0), "hel'lo €")
        self.assertEqual(res.ftype(0), poque.NAMEOID)

    def test_int8_value_bin(self):
        res = self.cn.execute(command="SELECT 6::int8", format=1)
        self.assertEqual(res.value(0, 0), 6)
        self.assertEqual(res.ftype(0), poque.INT8OID)
        res = self.cn.execute(command="SELECT -1::int8", format=1)
        self.assertEqual(res.value(0, 0), -1)
        self.assertEqual(res.getlength(0, 0), 8)

    def test_int8_value_str(self):
        res = self.cn.execute(command="SELECT 6::int8", format=0)
        self.assertEqual(res.value(0, 0), 6)
        self.assertEqual(res.ftype(0), poque.INT8OID)
        self.assertEqual(res.getlength(0, 0), 1)
        res = self.cn.execute(command="SELECT -1::int8", format=0)
        self.assertEqual(res.value(0, 0), -1)

    def test_null_value_str(self):
        res = self.cn.execute(command="SELECT NULL", format=0)
        self.assertIsNone(res.value(0, 0))
        self.assertEqual(res.getlength(0, 0), 0)

    def test_int2vector_value_bin(self):
        res = self.cn.execute(command="SELECT '6 8'::int2vector", format=1)
        self.assertEqual(res.value(0, 0), [6, 8])
        self.assertEqual(res.ftype(0), poque.INT2VECTOROID)

    def test_int4array_value_bin(self):
        # nested aray
        res = self.cn.execute(
            command="SELECT '{{1,NULL,3},{4,5,6}}'::int4[][]", format=1)
        self.assertEqual(res.value(0, 0), [[1, None, 3], [4, 5, 6]])
        self.assertEqual(res.ftype(0), poque.INT4ARRAYOID)

        # zero dimensions
        res = self.cn.execute(
            command="SELECT '{}'::int4[][][]", format=1)
        self.assertEqual(res.value(0, 0), [])
        self.assertEqual(res.ftype(0), poque.INT4ARRAYOID)

        # maximum number of dimensions
        res = self.cn.execute(
            command="SELECT '{{{{{{-1}}}}}}'::int4[][][][][]", format=1)
        self.assertEqual(res.value(0, 0), [[[[[[-1]]]]]])
        self.assertEqual(res.ftype(0), poque.INT4ARRAYOID)

        # maximum number of dimensions exceeded
        with self.assertRaises(poque.Error):
            self.cn.execute("SELECT '{{{{{{{-1}}}}}}}'::int4[][][][][][]",
                            format=1)

    def test_regproc_value_bin(self):
        res = self.cn.execute(
            "SELECT oid::int4, oid::regproc FROM pg_catalog.pg_proc "
            "WHERE proname='int4recv'", format=1)
        self.assertEqual(res.value(0, 0), res.value(0, 1))
        self.assertEqual(res.ftype(1), poque.REGPROCOID)
        res = self.cn.execute(
            "SELECT 1::regproc", format=1)
        self.assertEqual(res.value(0, 0), 1)
        self.assertEqual(res.ftype(0), poque.REGPROCOID)

    def test_regproc_value_str(self):
        res = self.cn.execute(
            "SELECT oid::regproc FROM pg_catalog.pg_proc "
            "WHERE proname='int4recv'", format=0)
        self.assertEqual(res.value(0, 0), 'int4recv')
        self.assertEqual(res.ftype(0), poque.REGPROCOID)
        res = self.cn.execute(
            "SELECT 1::regproc", format=0)
        self.assertEqual(res.value(0, 0), '1')
        self.assertEqual(res.ftype(0), poque.REGPROCOID)

    def test_text_value_bin(self):
        res = self.cn.execute("SELECT 'hello'::text", format=1)
        self.assertEqual(res.value(0, 0), 'hello')
        self.assertEqual(res.ftype(0), poque.TEXTOID)
        res = self.cn.execute("SELECT ''::text", format=1)
        self.assertEqual(res.value(0, 0), '')
        self.assertEqual(res.ftype(0), poque.TEXTOID)

    def test_text_value_str(self):
        res = self.cn.execute("SELECT 'hello'::text", format=0)
        self.assertEqual(res.value(0, 0), 'hello')
        self.assertEqual(res.ftype(0), poque.TEXTOID)
        res = self.cn.execute("SELECT E'he\nllo'::text", format=0)
        self.assertEqual(res.value(0, 0), 'he\nllo')
        self.assertEqual(res.ftype(0), poque.TEXTOID)
        res = self.cn.execute("SELECT ''::text", format=0)
        self.assertEqual(res.value(0, 0), '')
        self.assertEqual(res.ftype(0), poque.TEXTOID)

    def test_oid_value_bin(self):
        res = self.cn.execute("SELECT 3::oid", format=1)
        self.assertEqual(res.value(0, 0), 3)
        self.assertEqual(res.ftype(0), poque.OIDOID)

    def test_oid_value_str(self):
        res = self.cn.execute("SELECT 3::oid", format=0)
        self.assertEqual(res.value(0, 0), 3)
        self.assertEqual(res.ftype(0), poque.OIDOID)

    def test_tid_value_bin(self):
        res = self.cn.execute("SELECT '(3,4)'::tid", format=1)
        self.assertEqual(res.value(0, 0), (3, 4))
        self.assertEqual(res.ftype(0), poque.TIDOID)

    def test_tid_value_str(self):
        res = self.cn.execute("SELECT '(3, 4)'::tid", format=0)
        self.assertEqual(res.value(0, 0), (3, 4))
        self.assertEqual(res.ftype(0), poque.TIDOID)

    def test_xid_value_bin(self):
        res = self.cn.execute("SELECT '2147483648'::xid;", format=1)
        self.assertEqual(res.value(0, 0), 2147483648)
        self.assertEqual(res.ftype(0), poque.XIDOID)

    def test_xid_value_str(self):
        res = self.cn.execute("SELECT '2147483648'::xid;", format=0)
        self.assertEqual(res.value(0, 0), 2147483648)
        self.assertEqual(res.ftype(0), poque.XIDOID)

    def test_cid_value_bin(self):
        res = self.cn.execute("SELECT '2147483648'::cid;", format=1)
        self.assertEqual(res.value(0, 0), 2147483648)
        self.assertEqual(res.ftype(0), poque.CIDOID)

    def test_cid_value_str(self):
        res = self.cn.execute("SELECT '2147483648'::cid;", format=0)
        self.assertEqual(res.value(0, 0), 2147483648)
        self.assertEqual(res.ftype(0), poque.CIDOID)

    def test_oidvector_value_bin(self):
        res = self.cn.execute("SELECT '3 8 2147483648'::oidvector", format=1)
        self.assertEqual(res.value(0, 0), [3, 8, 2147483648])
        self.assertEqual(res.ftype(0), poque.OIDVECTOROID)

    def test_float8_value_bin(self):
        res = self.cn.execute("SELECT 1.4::float8", format=1)
        self.assertAlmostEqual(res.value(0, 0), 1.4)
        self.assertEqual(res.ftype(0), poque.FLOAT8OID)

    def test_float8_value_str(self):
        res = self.cn.execute("SELECT 1.4::float8", format=0)
        self.assertAlmostEqual(res.value(0, 0), 1.4)
        self.assertEqual(res.ftype(0), poque.FLOAT8OID)

    def test_float4_value_bin(self):
        res = self.cn.execute("SELECT 1.4::float4", format=1)
        self.assertAlmostEqual(res.value(0, 0), 1.4)
        self.assertEqual(res.ftype(0), poque.FLOAT4OID)

    def test_float4_value_str(self):
        res = self.cn.execute("SELECT 1.4::float4", format=0)
        self.assertAlmostEqual(res.value(0, 0), 1.4)
        self.assertEqual(res.ftype(0), poque.FLOAT4OID)

    def test_json_value_bin(self):
        res = self.cn.execute("SELECT '{\"hi\": 23}'::json", format=1)
        self.assertEqual(res.value(0, 0), {"hi": 23})
        self.assertEqual(res.ftype(0), poque.JSONOID)

    def test_json_value_str(self):
        res = self.cn.execute("SELECT '{\"hi\": 23}'::json", format=0)
        self.assertEqual(res.value(0, 0), {"hi": 23})
        self.assertEqual(res.ftype(0), poque.JSONOID)

    def test_json_array_value_str(self):
        res = self.cn.execute(
            "SELECT '{\"{\\\"hi\\\": 23}\",\"[3, 4]\"}'::json[];")
        self.assertEqual(res.value(0, 0), [{"hi": 23}, [3, 4]])
        self.assertEqual(res.ftype(0), poque.JSONARRAYOID)

    def test_xml_value_bin(self):
        res = self.cn.execute("SELECT '<el>hi</el>'::xml", format=1)
        self.assertEqual(res.value(0, 0), '<el>hi</el>')
        self.assertEqual(res.ftype(0), poque.XMLOID)

    def test_xml_value_str(self):
        res = self.cn.execute("SELECT '<el>hi</el>'::xml", format=0)
        self.assertEqual(res.value(0, 0), '<el>hi</el>')
        self.assertEqual(res.ftype(0), poque.XMLOID)

    def test_xml_array_value_bin(self):
        res = self.cn.execute("SELECT E'{<el>\n1</el>, <el>2</el>}'::xml[];",
                              format=1)
        self.assertEqual(res.value(0, 0), ['<el>\n1</el>', '<el>2</el>'])
        self.assertEqual(res.ftype(0), poque.XMLARRAYOID)

    def assert_tuple(self, val, length=2):
        self.assertIsInstance(val, tuple)
        self.assertEqual(len(val), length)

    def assert_point(self, point, vals):
        self.assert_tuple(point)
        self.assertAlmostEqual(point[0], vals[0])
        self.assertAlmostEqual(point[1], vals[1])

    def test_point_value_bin(self):
        res = self.cn.execute("SELECT '(1.24, 3.4)'::point;", format=1)
        val = res.value(0, 0)
        self.assert_point(val, (1.24, 3.4))
        self.assertEqual(res.ftype(0), poque.POINTOID)

    def test_lseg_value_bin(self):
        res = self.cn.execute("SELECT '((1.3, 3.45), (2, 5.6))'::lseg;",
                              format=1)
        val = res.value(0, 0)
        self.assert_tuple(val)
        self.assert_point(val[0], (1.3, 3.45))
        self.assert_point(val[1], (2, 5.6))
        self.assertEqual(res.ftype(0), poque.LSEGOID)

    def test_path_value_bin(self):
        res = self.cn.execute(
            "SELECT '((1.3, 3.45), (2, 5.6), (-1.3, -4))'::path;")
        val = res.value(0, 0)
        self.assertIsInstance(val, dict)
        self.assert_point(val['path'][0], (1.3, 3.45))
        self.assert_point(val['path'][1], (2, 5.6))
        self.assert_point(val['path'][2], (-1.3, -4))
        self.assertIs(val['closed'], True)
        del val['path']
        self.assertEqual(val, {'closed': True})
        self.assertEqual(res.ftype(0), poque.PATHOID)

    def test_box_value_bin(self):
        res = self.cn.execute("SELECT '((1.3, 3.45), (2, 5.6))'::box;")
        val = res.value(0, 0)
        self.assert_tuple(val)
        self.assert_point(val[1], (1.3, 3.45))
        self.assert_point(val[0], (2, 5.6))
        self.assertEqual(res.ftype(0), poque.BOXOID)

    def test_polygon_value_bin(self):
        res = self.cn.execute(
            "SELECT '((1.3, 3.45), (2, 5.6), (-1.3, -4))'::polygon;")
        val = res.value(0, 0)
        self.assertIsInstance(val, list)
        self.assertEqual(len(val), 3)
        self.assert_point(val[0], (1.3, 3.45))
        self.assert_point(val[1], (2, 5.6))
        self.assert_point(val[2], (-1.3, -4))
        self.assertEqual(res.ftype(0), poque.POLYGONOID)

    def test_abstime_value_bin(self):
        res = self.cn.execute("SELECT 1000000000::abstime;")
        val = res.value(0, 0)
        self.assertEqual(val, datetime.datetime.fromtimestamp(1000000000))
        self.assertEqual(res.ftype(0), poque.ABSTIMEOID)

    def test_reltime_value_bin(self):
        res = self.cn.execute("SELECT 100::reltime;")
        val = res.value(0, 0)
        self.assertEqual(val, datetime.timedelta(seconds=100))
        self.assertEqual(res.ftype(0), poque.RELTIMEOID)

    def test_tinterval_value_bin(self):
        res = self.cn.execute(
            "SELECT '[\"2014-01-01\" \"2015-01-01\"]'::tinterval")
        val = res.value(0, 0)
        self.assertEqual(val, (datetime.datetime(2014, 1, 1),
                               datetime.datetime(2015, 1, 1)))
        self.assertEqual(res.ftype(0), poque.TINTERVALOID)

    def test_unknown_value_bin(self):
        res = self.cn.execute("SELECT 'hello'::unknown")
        val = res.value(0, 0)
        self.assertEqual(val, "hello")
        self.assertEqual(res.ftype(0), poque.UNKNOWNOID)

    def test_circle_value_bin(self):
        res = self.cn.execute("SELECT '<(2.3, -4.5), 3.75>'::circle;")
        val = res.value(0, 0)
        self.assertIsInstance(val, tuple)
        self.assertEqual(len(val), 2)
        self.assertIsInstance(val[0], tuple)
        self.assertEqual(len(val[0]), 2)
        self.assertAlmostEqual(val[0][0], 2.3)
        self.assertAlmostEqual(val[0][1], -4.5)
        self.assertAlmostEqual(val[1], 3.75)
        self.assertEqual(res.ftype(0), poque.CIRCLEOID)

    def test_circle_array_value_bin(self):
        res = self.cn.execute(
            'SELECT \'{"<(2.3, -4.5), 3.75>", "<(2.3, -4.5), 3.75>"}\'::circle[];')
        value = res.value(0, 0)
        self.assertIsInstance(value, list)
        self.assertEqual(len(value), 2)
        for val in value:
            self.assertIsInstance(val, tuple)
            self.assertEqual(len(val), 2)
            self.assertIsInstance(val[0], tuple)
            self.assertEqual(len(val[0]), 2)
            self.assertAlmostEqual(val[0][0], 2.3)
            self.assertAlmostEqual(val[0][1], -4.5)
            self.assertAlmostEqual(val[1], 3.75)
            self.assertEqual(res.ftype(0), poque.CIRCLEARRAYOID)

    def test_money_value_bin(self):
        res = self.cn.execute("SELECT 3::money")
        value = res.value(0, 0)
        self.assertEqual(value, 300)
        self.assertEqual(res.ftype(0), poque.CASHOID)

    def test_money_array_value_bin(self):
        res = self.cn.execute("SELECT '{3.4, -52}'::numeric[]::money[];")
        val = res.value(0, 0)
        self.assertEqual(val, [340, -5200])
        self.assertEqual(res.ftype(0), poque.CASHARRAYOID)

    def test_mac_addr_value_bin(self):
        res = self.cn.execute("SELECT '24:0a:64:dd:58:c4'::macaddr;")
        val = res.value(0, 0)
        self.assertEqual(val, 0x240a64dd58c4)
        self.assertEqual(res.ftype(0), poque.MACADDROID)

    def test_uuid_value_bin(self):
        val = uuid4()
        res = self.cn.execute("SELECT '{0}'::uuid".format(val), format=1)
        v = res.value(0, 0)
        self.assertEqual(sys.getrefcount(v), 2)
        self.assertEqual(v, val)
        self.assertEqual(res.ftype(0), poque.UUIDOID)

    def test_uuid_value_str(self):
        val = uuid4()
        res = self.cn.execute("SELECT '{0}'::uuid".format(val), format=0)
        v = res.value(0, 0)
        self.assertEqual(sys.getrefcount(v), 2)
        self.assertEqual(v, val)
        self.assertEqual(res.ftype(0), poque.UUIDOID)


class ResultTest(unittest.TestCase):

    def setUp(self):
        cn = poque.Conn(config.conninfo())
        self.res = cn.execute("SELECT * FROM pg_type")

    def test_ftable(self):
        self.assertGreater(self.res.ftable(0), 0)

    def test_ftablecol(self):
        self.assertEqual(self.res.ftablecol(1), 2)

    def test_ftype(self):
        self.assertEqual(self.res.ftype(1), poque.OIDOID)
