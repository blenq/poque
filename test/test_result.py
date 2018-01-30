import datetime
from decimal import Decimal
from ipaddress import IPv4Interface, IPv6Interface, IPv4Network, IPv6Network
import unittest
import weakref

import poque

from test import config
from test.config import BaseExtensionTest, BaseCTypesTest

from uuid import uuid4, UUID
from poque.ctypes.constants import (
    LINEOID, LINEARRAYOID, FORMAT_BINARY, FORMAT_TEXT)


class ResultTestBasic():

    @classmethod
    def setUpClass(cls):
        cls.cn = cls.poque.Conn(config.conninfo())

    def setUp(self):
        self.res = self.cn.execute("SELECT 1 AS yo")

    @classmethod
    def tearDownClass(cls):
        cls.cn.finish()

    def test_execute(self):
        self.assertIsNotNone(self.res)

    def test_weakref(self):
        self.assertEqual(weakref.ref(self.res)(), self.res)

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
        self.assertEqual(self.res.ftable(0), self.poque.INVALID_OID)
        self.assertEqual(
            self.res.ftable(column_number=0), self.poque.INVALID_OID)

    def test_invalid_ftable(self):
        self.res.clear()
        self.assertEqual(self.res.ftable(0), self.poque.INVALID_OID)

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
        self.assertEqual(self.res.ftype(0), self.poque.INT4OID)
        self.assertEqual(self.res.ftype(column_number=3), 0)

    def test_fmod(self):
        self.assertEqual(self.res.fmod(0), -1)

    def test_fsize(self):
        self.assertEqual(self.res.fsize(0), 4)

    def test_getlength(self):
        self.assertEqual(self.res.getlength(0, 0), 4)

    def test_invalid_getlength(self):
        self.assertEqual(self.res.getlength(0, 1), 0)


class ResultTestBasicExtension(
        BaseExtensionTest, ResultTestBasic, unittest.TestCase):
    pass


class ResultTestBasicCtypes(
        BaseCTypesTest, ResultTestBasic, unittest.TestCase):
    pass


class ResultTestValues():

    @classmethod
    def setUpClass(cls):
        cls.cn = cls.poque.Conn(config.conninfo())

    @classmethod
    def tearDownClass(cls):
        cls.cn.finish()

    def setUp(self):
        self.cn.execute("BEGIN")

    def tearDown(self):
        self.cn.execute("ROLLBACK")

    def test_is_null(self):
        res = self.cn.execute("SELECT NULL")
        self.assertIs(res.getisnull(0, 0), True)
        res = self.cn.execute("SELECT 1")
        self.assertIs(res.getisnull(0, 0), False)

    def _test_value_and_type(self, command, value, type_oid, result_format):
        res = self.cn.execute(command, result_format=result_format)
        self.assertEqual(res.getvalue(0, 0), value)
        self.assertEqual(res.ftype(0), type_oid)

    def _test_value_and_type_bin(self, command, value, type_oid):
        self._test_value_and_type(command, value, type_oid, FORMAT_BINARY)

    def _test_value_and_type_str(self, command, value, type_oid):
        self._test_value_and_type(command, value, type_oid, FORMAT_TEXT)

    def test_format(self):
        res = self.cn.execute(command="SELECT 1", result_format=0)
        self.assertEqual(res.fformat(0), 0)

    def test_formats(self):
        res = self.cn.execute(command="SELECT 6::int4", result_format=0)
        self.assertEqual(res.pq_getvalue(0, 0), "6")
        self.assertEqual(res.pq_getvalue(2, 3), "")
        res = self.cn.execute(command="SELECT 'hoi'", result_format=1)
        self.assertEqual(res.pq_getvalue(0, 0), b"hoi")
        self.assertEqual(res.pq_getvalue(2, 3), "")

    def test_two_queries(self):
        res = self.cn.execute(command="SELECT 1;SELECT 2", result_format=0)
        self.assertEqual(res.getvalue(0, 0), 2)

    def _test_int_getvalue(self, fmt, typ, oid):
        res = self.cn.execute(command="SELECT 6::{0}".format(typ),
                              result_format=fmt)
        self.assertEqual(res.getvalue(0, 0), 6)
        res = self.cn.execute(command="SELECT -1::{0}".format(typ),
                              result_format=fmt)
        self.assertEqual(res.getvalue(0, 0), -1)
        self.assertEqual(res.ftype(0), oid)

    def test_int4_value_text(self):
        self._test_int_getvalue(0, 'int4', self.poque.INT4OID)

    def test_int4_value_bin(self):
        self._test_int_getvalue(1, 'int4', self.poque.INT4OID)

    def test_int4array_value_bin(self):
        # nested aray
        res = self.cn.execute(
            command="SELECT '{{1,NULL,3},{4,5,6}}'::int4[][]", result_format=1)
        self.assertEqual(res.getvalue(0, 0), [[1, None, 3], [4, 5, 6]])
        self.assertEqual(res.ftype(0), self.poque.INT4ARRAYOID)

        # zero dimensions
        res = self.cn.execute(
            command="SELECT '{}'::int4[][][]", result_format=1)
        self.assertEqual(res.getvalue(0, 0), [])
        self.assertEqual(res.ftype(0), self.poque.INT4ARRAYOID)

        # maximum number of dimensions
        res = self.cn.execute(
            command="SELECT '{{{{{{-1}}}}}}'::int4[][][][][]", result_format=1)
        self.assertEqual(res.getvalue(0, 0), [[[[[[-1]]]]]])
        self.assertEqual(res.ftype(0), self.poque.INT4ARRAYOID)

        # maximum number of dimensions exceeded
        with self.assertRaises(self.poque.Error):
            self.cn.execute("SELECT '{{{{{{{-1}}}}}}}'::int4[][][][][][]",
                            result_format=1)

    def test_int2_value_text(self):
        self._test_int_getvalue(0, 'int2', self.poque.INT2OID)

    def test_int2_value_bin(self):
        self._test_int_getvalue(1, 'int2', self.poque.INT2OID)

    def test_int2_array_value_bin(self):
        self._test_value_and_type_bin("SELECT '{6, NULL, -4}'::int2[]",
                                      [6, None, -4], self.poque.INT2ARRAYOID)

    def test_int8_value_bin(self):
        self._test_int_getvalue(1, 'int8', self.poque.INT8OID)

    def test_int8_value_str(self):
        self._test_int_getvalue(0, 'int8', self.poque.INT8OID)

    def test_int8_array_value_bin(self):
        self._test_value_and_type_bin("SELECT '{6, NULL, -1}'::int8[]",
                                      [6, None, -1], self.poque.INT8ARRAYOID)

    def test_bit_value_bin(self):
        self._test_value_and_type_bin("SELECT 23::BIT(5)",
                                      23, self.poque.BITOID)
        self._test_value_and_type_bin("SELECT 260::BIT(15)",
                                      260, self.poque.BITOID)
        self._test_value_and_type_bin("SELECT 260::BIT(16)",
                                      260, self.poque.BITOID)
        self._test_value_and_type_bin("SELECT 260::BIT(16)",
                                      260, self.poque.BITOID)
        val = '1' * 65
        self._test_value_and_type_bin("SELECT B'{0}'::BIT(65)".format(val),
                                      0x1FFFFFFFFFFFFFFFF, self.poque.BITOID)

    def test_bit_value_str(self):
        self._test_value_and_type_str("SELECT 23::BIT(5)",
                                      23, self.poque.BITOID)
        self._test_value_and_type_str("SELECT 260::BIT(15)",
                                      260, self.poque.BITOID)
        self._test_value_and_type_str("SELECT 260::BIT(16)",
                                      260, self.poque.BITOID)
        self._test_value_and_type_str("SELECT 260::BIT(16)",
                                      260, self.poque.BITOID)
        val = '1' * 65
        self._test_value_and_type_str("SELECT B'{0}'::BIT(65)".format(val),
                                      0x1FFFFFFFFFFFFFFFF, self.poque.BITOID)

    def test_bit_array_value_bin(self):
        self._test_value_and_type_bin("SELECT ARRAY[23::BIT(8), 200::BIT(8)]",
                                      [23, 200], self.poque.BITARRAYOID)

    def test_varbit_value_bin(self):
        self._test_value_and_type_bin("SELECT 23::BIT(5)::VARBIT",
                                      23, self.poque.VARBITOID)
        self._test_value_and_type_bin("SELECT 260::BIT(15)::VARBIT",
                                      260, self.poque.VARBITOID)
        self._test_value_and_type_bin("SELECT 260::BIT(16)::VARBIT",
                                      260, self.poque.VARBITOID)
        val = '1' * 65
        self._test_value_and_type_bin(
            "SELECT B'{0}'::BIT(65)::VARBIT".format(val),
            0x1FFFFFFFFFFFFFFFF, self.poque.VARBITOID)

    def test_varbit_value_str(self):
        self._test_value_and_type_str("SELECT 23::BIT(5)::VARBIT",
                                      23, self.poque.VARBITOID)
        self._test_value_and_type_str("SELECT 260::BIT(15)::VARBIT",
                                      260, self.poque.VARBITOID)
        self._test_value_and_type_str("SELECT 260::BIT(16)::VARBIT",
                                      260, self.poque.VARBITOID)
        val = '1' * 65
        self._test_value_and_type_str(
            "SELECT B'{0}'::BIT(65)::VARBIT".format(val),
            0x1FFFFFFFFFFFFFFFF, self.poque.VARBITOID)

    def test_varbit_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY[23::BIT(16)::VARBIT, 200::BIT(16)::VARBIT]",
            [23, 200], self.poque.VARBITARRAYOID)

    def assertNumericEqual(self, val1, val2):
        if val1.is_nan():
            self.assertTrue(val2.is_nan())
            return
        self.assertEqual(val1, val2)
        # at least as many digits as went in
        self.assertTrue(len(val1.as_tuple()[1]) >= len(val2.as_tuple()[1]))

    def _test_numeric_value(self, fmt):
        res = self.cn.execute(
            command="SELECT '123.456'::numeric(12, 5), 'NaN'::numeric, "
                    " '123456789012345678901234567890'::numeric, "
                    " '0.000000000000001230'::numeric, "
                    " '-123456789012345678901234567890'::numeric, "
                    " '-0.000000000000001230'::numeric, "
                    " '9990E+99'::numeric, "
                    " '9990E-98'::numeric, "
                    " '0'::numeric, "
                    " '0.000000'::numeric, "
                    " 'NaN'::numeric, "
                    " '1234567890.0987654321'::numeric;",
            result_format=fmt)
        self.assertNumericEqual(res.getvalue(0, 0), Decimal('123.45600'))
        self.assertTrue(res.getvalue(0, 1).is_nan())
        self.assertNumericEqual(
            res.getvalue(0, 2), Decimal('123456789012345678901234567890'))
        self.assertNumericEqual(
            res.getvalue(0, 3), Decimal('0.000000000000001230'))
        self.assertNumericEqual(
            res.getvalue(0, 4), Decimal('-123456789012345678901234567890'))
        self.assertNumericEqual(
            res.getvalue(0, 5), Decimal('-0.000000000000001230'))
        self.assertNumericEqual(
            res.getvalue(0, 6), Decimal('9990E+99'))
        self.assertNumericEqual(
            res.getvalue(0, 7), Decimal('9990E-98'))
        self.assertNumericEqual(
            res.getvalue(0, 8), Decimal('0'))
        self.assertNumericEqual(
            res.getvalue(0, 9), Decimal('0.000000'))
        self.assertNumericEqual(
            res.getvalue(0, 10), Decimal('NaN'))
        self.assertNumericEqual(
            res.getvalue(0, 11), Decimal('1234567890.0987654321'))

    def test_numeric_value_str(self):
        self._test_numeric_value(0)

    def test_numeric_value_bin(self):
        self._test_numeric_value(1)

    def test_numeric_array_value_bin(self):
        res = self.cn.execute(
            command="SELECT '{123.456, NULL, "
                    "123456789012345678901234567890}'::numeric[]",
            result_format=1)
        self.assertEqual(res.getvalue(0, 0), [
            Decimal('123.456'), None,
            Decimal('123456789012345678901234567890')])

    def test_bool_value_str(self):
        res = self.cn.execute(command="SELECT true", result_format=0)
        self.assertIs(res.getvalue(0, 0), True)
        res = self.cn.execute(command="SELECT false", result_format=0)
        self.assertIs(res.getvalue(0, 0), False)

    def test_bool_value_bin(self):
        self._test_value_and_type_bin("SELECT true", True, self.poque.BOOLOID)
        self._test_value_and_type_bin(
            "SELECT false", False, self.poque.BOOLOID)

    def test_bytea_value_str(self):
        self.cn.execute("SET bytea_output TO hex")
        res = self.cn.execute(command="SELECT 'hi'::bytea", result_format=0)
        self.assertEqual(res.getvalue(0, 0), b'hi')
        self.assertEqual(res.ftype(0), self.poque.BYTEAOID)
        res = self.cn.execute(command="SELECT ''::bytea", result_format=0)
        self.assertEqual(res.getvalue(0, 0), b'')
        self.assertEqual(res.ftype(0), self.poque.BYTEAOID)
        res = self.cn.execute(command="SELECT '\t'::bytea", result_format=0)
        self.assertEqual(res.getvalue(0, 0), b'\t')
        self.assertEqual(res.ftype(0), self.poque.BYTEAOID)
        self.cn.execute("SET bytea_output TO escape")
        res = self.cn.execute(command="SELECT convert_to('\t \\', 'utf8')",
                              result_format=0)
        self.assertEqual(res.getvalue(0, 0), b'\t \\')
        self.assertEqual(res.ftype(0), self.poque.BYTEAOID)
        res = self.cn.execute(command="SELECT ''::bytea", result_format=0)
        self.assertEqual(res.getvalue(0, 0), b'')
        res = self.cn.execute(r"SELECT 'h\\oi '::bytea", result_format=0)
        self.assertEqual(res.getvalue(0, 0), b'h\\oi ')
        self.assertEqual(res.ftype(0), self.poque.BYTEAOID)
        res = self.cn.execute(r"SELECT 'hoi \001'::bytea", result_format=0)
        self.assertEqual(res.getvalue(0, 0), b'hoi \x01')

    def test_bytea_value_bin(self):
        self._test_value_and_type_bin("SELECT 'hi'::bytea", b'hi',
                                      self.poque.BYTEAOID)
        self._test_value_and_type_bin(
            "SELECT ''::bytea", b'', self.poque.BYTEAOID)
        self._test_value_and_type_bin("SELECT convert_to('\t \\', 'utf8')",
                                      b'\t \\', self.poque.BYTEAOID)

    def test_char_value_str(self):
        res = self.cn.execute(command="SELECT 'a'::\"char\"", result_format=0)
        self.assertEqual(res.getvalue(0, 0), b'a')
        self.assertEqual(res.ftype(0), self.poque.CHAROID)
        res = self.cn.execute(command="SELECT '€'::\"char\"", result_format=0)
        self.assertEqual(res.getvalue(0, 0), "€".encode()[:1])

    def test_char_value_bin(self):
        self._test_value_and_type_bin("SELECT 'a'::\"char\"", b'a',
                                      self.poque.CHAROID)
        self._test_value_and_type_bin("SELECT '€'::\"char\"", "€".encode()[:1],
                                      self.poque.CHAROID)

    def test_char_array_value_bin(self):
        res = self.cn.execute(command="SELECT '{a, b}'::\"char\"[]")
        self.assertEqual(res.getvalue(0, 0), [b'a', b'b'])
        self.assertEqual(res.ftype(0), self.poque.CHARARRAYOID)

    def test_name_value_bin(self):
        res = self.cn.execute(command="SELECT 'hel''lo €'::name",
                              result_format=1)
        self.assertEqual(res.getvalue(0, 0), "hel'lo €")
        self.assertEqual(res.ftype(0), self.poque.NAMEOID)

    def test_name_value_str(self):
        res = self.cn.execute(command="SELECT 'hel''lo €'::name",
                              result_format=0)
        self.assertEqual(res.getvalue(0, 0), "hel'lo €")
        self.assertEqual(res.ftype(0), self.poque.NAMEOID)

    def test_name_array_value_bin(self):
        res = self.cn.execute(command="SELECT '{hello, hi}'::name[]")
        self.assertEqual(res.getvalue(0, 0), ['hello', 'hi'])
        self.assertEqual(res.ftype(0), self.poque.NAMEARRAYOID)

    def get_unknown_cmp_type(self):
        if (self.cn.server_version < 100000):
            return self.poque.UNKNOWNOID
        else:
            return self.poque.TEXTOID

    def test_unknown_value_str(self):
        self._test_value_and_type_str("SELECT 'hello'::unknown", 'hello',
                                      self.get_unknown_cmp_type())

    def test_unknown_value_bin(self):
        self._test_value_and_type_bin("SELECT 'hello'::unknown", 'hello',
                                      self.get_unknown_cmp_type())

    def test_null_value_str(self):
        res = self.cn.execute(command="SELECT NULL", result_format=0)
        self.assertIsNone(res.getvalue(0, 0))
        self.assertEqual(res.getlength(0, 0), 0)

    def test_int2vector_value_bin(self):
        res = self.cn.execute(command="SELECT '6 8'::int2vector",
                              result_format=1)
        self.assertEqual(res.getvalue(0, 0), [6, 8])
        self.assertEqual(res.ftype(0), self.poque.INT2VECTOROID)

    def test_int2vector_array_value_bin(self):
        res = self.cn.execute("SELECT '{1 2 6, NULL, 3 4}'::int2vector[];")
        self.assertEqual(res.getvalue(0, 0), [[1, 2, 6], None, [3, 4]])
        self.assertEqual(res.ftype(0), self.poque.INT2VECTORARRAYOID)

    def test_regproc_value_bin(self):
        res = self.cn.execute(
            "SELECT oid::int4, oid::regproc FROM pg_catalog.pg_proc "
            "WHERE proname='int4recv'", result_format=1)
        self.assertEqual(res.getvalue(0, 0), res.getvalue(0, 1))
        self.assertEqual(res.ftype(1), self.poque.REGPROCOID)
        res = self.cn.execute(
            "SELECT 1::regproc", result_format=1)
        self.assertEqual(res.getvalue(0, 0), 1)
        self.assertEqual(res.ftype(0), self.poque.REGPROCOID)

    def test_regproc_array_value_bin(self):
        res = self.cn.execute(
            "SELECT oid::int4, ARRAY[oid::regproc, NULL, oid::regproc] "
            "FROM pg_catalog.pg_proc "
            "WHERE proname='int4recv'")
        val = res.getvalue(0, 0)
        self.assertEqual(res.getvalue(0, 1), [val, None, val])
        self.assertEqual(res.ftype(1), self.poque.REGPROCARRAYOID)

    def test_regproc_value_str(self):
        res = self.cn.execute(
            "SELECT oid::regproc FROM pg_catalog.pg_proc "
            "WHERE proname='int4recv'", result_format=0)
        self.assertEqual(res.getvalue(0, 0), 'int4recv')
        self.assertEqual(res.ftype(0), self.poque.REGPROCOID)
        res = self.cn.execute(
            "SELECT 1::regproc", result_format=0)
        self.assertEqual(res.getvalue(0, 0), '1')
        self.assertEqual(res.ftype(0), self.poque.REGPROCOID)

    def test_text_value_bin(self):
        res = self.cn.execute("SELECT 'hello'::text", result_format=1)
        self.assertEqual(res.getvalue(0, 0), 'hello')
        self.assertEqual(res.ftype(0), self.poque.TEXTOID)
        res = self.cn.execute("SELECT ''::text", result_format=1)
        self.assertEqual(res.getvalue(0, 0), '')
        self.assertEqual(res.ftype(0), self.poque.TEXTOID)

    def test_text_value_str(self):
        res = self.cn.execute("SELECT 'hello'::text", result_format=0)
        self.assertEqual(res.getvalue(0, 0), 'hello')
        self.assertEqual(res.ftype(0), self.poque.TEXTOID)
        res = self.cn.execute("SELECT E'he\nllo'::text", result_format=0)
        self.assertEqual(res.getvalue(0, 0), 'he\nllo')
        self.assertEqual(res.ftype(0), self.poque.TEXTOID)
        res = self.cn.execute("SELECT ''::text", result_format=0)
        self.assertEqual(res.getvalue(0, 0), '')
        self.assertEqual(res.ftype(0), self.poque.TEXTOID)

    def test_text_array_value_str(self):
        res = self.cn.execute("SELECT ARRAY['hello', NULL, 'hi']::text[]")
        self.assertEqual(res.getvalue(0, 0), ['hello', None, 'hi'])
        self.assertEqual(res.ftype(0), self.poque.TEXTARRAYOID)

    def test_bpchar_value_bin(self):
        res = self.cn.execute("SELECT 'hello'::char(6)")
        self.assertEqual(res.getvalue(0, 0), 'hello ')
        self.assertEqual(res.ftype(0), self.poque.BPCHAROID)

    def test_bpchar_value_str(self):
        res = self.cn.execute("SELECT 'hello'::char(6)", result_format=0)
        self.assertEqual(res.getvalue(0, 0), 'hello ')
        self.assertEqual(res.ftype(0), self.poque.BPCHAROID)

    def test_bpchar_array_value_bin(self):
        res = self.cn.execute("SELECT '{hello, NULL, hi}'::char(4)[];")
        self.assertEqual(res.getvalue(0, 0), ['hell', None, 'hi  '])
        self.assertEqual(res.ftype(0), self.poque.BPCHARARRAYOID)

    def _test_varchar_getvalue(self, fmt):
        res = self.cn.execute("SELECT 'hello'::varchar(6)", result_format=fmt)
        self.assertEqual(res.getvalue(0, 0), 'hello')
        self.assertEqual(res.ftype(0), self.poque.VARCHAROID)

    def test_varchar_value_bin(self):
        self._test_varchar_getvalue(0)

    def test_varchar_value_str(self):
        self._test_varchar_getvalue(1)

    def test_varchar_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '{hello, NULL, hi}'::varchar(6)[]", ['hello', None, 'hi'],
            self.poque.VARCHARARRAYOID)

    def test_cstring_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT 'hello'::cstring", 'hello', self.poque.CSTRINGOID)

    def test_cstring_array_value_bin(self):
        self._test_value_and_type_bin("SELECT ARRAY['hello'::cstring]",
                                      ['hello'],
                                      self.poque.CSTRINGARRAYOID)

    def test_oid_value_bin(self):
        self._test_value_and_type_bin("SELECT 3::oid", 3, self.poque.OIDOID)

    def test_oid_value_str(self):
        res = self.cn.execute("SELECT 3::oid", result_format=0)
        self.assertEqual(res.getvalue(0, 0), 3)
        self.assertEqual(res.ftype(0), self.poque.OIDOID)

    def test_oid_array_value_bin(self):
        self._test_value_and_type_bin("SELECT ARRAY[3, NULL, 4]::oid[]",
                                      [3, None, 4], self.poque.OIDARRAYOID)

    def test_tid_value_bin(self):
        self._test_value_and_type_bin("SELECT '(3,4)'::tid", (3, 4),
                                      self.poque.TIDOID)

    def test_tid_value_str(self):
        res = self.cn.execute("SELECT '(3, 4)'::tid", result_format=0)
        self.assertEqual(res.getvalue(0, 0), (3, 4))
        self.assertEqual(res.ftype(0), self.poque.TIDOID)
        res = self.cn.execute("SELECT '(3, 4)'::tid", result_format=0)
        self.assertEqual(res.getvalue(0, 0), (3, 4))
        self.assertEqual(res.ftype(0), self.poque.TIDOID)

    def test_tid_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '{\"(3, 4)\", NULL, \"(6, 2)\"}'::tid[];",
            [(3, 4), None, (6, 2)], self.poque.TIDARRAYOID)

    def test_xid_value_bin(self):
        self._test_value_and_type_bin("SELECT '2147483648'::xid;", 2147483648,
                                      self.poque.XIDOID)

    def test_xid_value_str(self):
        res = self.cn.execute("SELECT '2147483648'::xid;", result_format=0)
        self.assertEqual(res.getvalue(0, 0), 2147483648)
        self.assertEqual(res.ftype(0), self.poque.XIDOID)

    def test_xid_array_value_bin(self):
        self._test_value_and_type_bin("SELECT '{2147483648, NULL, 3}'::xid[];",
                                      [2147483648, None, 3],
                                      self.poque.XIDARRAYOID)

    def test_cid_value_bin(self):
        self._test_value_and_type_bin("SELECT '2147483648'::cid;", 2147483648,
                                      self.poque.CIDOID)

    def test_cid_value_str(self):
        res = self.cn.execute("SELECT '2147483648'::cid;", result_format=0)
        self.assertEqual(res.getvalue(0, 0), 2147483648)
        self.assertEqual(res.ftype(0), self.poque.CIDOID)

    def test_cid_array_value_bin(self):
        self._test_value_and_type_bin("SELECT '{2147483648, NULL, 3}'::cid[];",
                                      [2147483648, None, 3],
                                      self.poque.CIDARRAYOID)

    def test_oidvector_value_bin(self):
        self._test_value_and_type_bin("SELECT '3 8 2147483648'::oidvector",
                                      [3, 8, 2147483648],
                                      self.poque.OIDVECTOROID)

    def test_oidvector_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '{3 8 2147483648, NULL, 7 3}'::oidvector[]",
            [[3, 8, 2147483648], None, [7, 3]], self.poque.OIDVECTORARRAYOID)

    def test_float8_value_bin(self):
        res = self.cn.execute("SELECT 1.4::float8", result_format=1)
        self.assertAlmostEqual(res.getvalue(0, 0), 1.4)
        self.assertEqual(res.ftype(0), self.poque.FLOAT8OID)

    def test_float8_value_str(self):
        res = self.cn.execute("SELECT 1.4::float8", result_format=0)
        self.assertAlmostEqual(res.getvalue(0, 0), 1.4)
        self.assertEqual(res.ftype(0), self.poque.FLOAT8OID)

    def test_float8_array_value_bin(self):
        res = self.cn.execute("SELECT '{1.4, NULL, 3, -2.5}'::float8[]")
        val = res.getvalue(0, 0)
        self.assertIsInstance(val, list)
        self.assertEqual(len(val), 4)
        self.assertAlmostEqual(val[0], 1.4)
        self.assertIsNone(val[1])
        self.assertAlmostEqual(val[2], 3)
        self.assertAlmostEqual(val[3], -2.5)
        self.assertEqual(res.ftype(0), self.poque.FLOAT8ARRAYOID)

    def test_float4_value_bin(self):
        res = self.cn.execute("SELECT 1.4::float4", result_format=1)
        self.assertAlmostEqual(res.getvalue(0, 0), 1.4)
        self.assertEqual(res.ftype(0), self.poque.FLOAT4OID)

    def test_float4_value_str(self):
        res = self.cn.execute("SELECT 1.4::float4", result_format=0)
        self.assertAlmostEqual(res.getvalue(0, 0), 1.4)
        self.assertEqual(res.ftype(0), self.poque.FLOAT4OID)

    def test_float4_array_value_bin(self):
        res = self.cn.execute("SELECT '{1.4, NULL, 3, -2.5}'::float4[]")
        val = res.getvalue(0, 0)
        self.assertIsInstance(val, list)
        self.assertEqual(len(val), 4)
        self.assertAlmostEqual(val[0], 1.4)
        self.assertIsNone(val[1])
        self.assertAlmostEqual(val[2], 3)
        self.assertAlmostEqual(val[3], -2.5)
        self.assertEqual(res.ftype(0), self.poque.FLOAT4ARRAYOID)

    def test_json_value_bin(self):
        self._test_value_and_type_bin("SELECT '{\"hi\": 23}'::json",
                                      {"hi": 23}, self.poque.JSONOID)

    def test_json_value_str(self):
        res = self.cn.execute("SELECT '{\"hi\": 23}'::json", result_format=0)
        self.assertEqual(res.getvalue(0, 0), {"hi": 23})
        self.assertEqual(res.ftype(0), self.poque.JSONOID)

    def test_json_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '{\"{\\\"hi\\\": 23}\",\"[3, 4]\"}'::json[];",
            [{"hi": 23}, [3, 4]], self.poque.JSONARRAYOID)

    def test_jsonb_value_bin(self):
        self._test_value_and_type_bin("SELECT '{\"hi\": 23}'::jsonb",
                                      {"hi": 23}, self.poque.JSONBOID)

    def test_jsonb_value_str(self):
        res = self.cn.execute("SELECT '{\"hi\": 23}'::jsonb", result_format=0)
        self.assertEqual(res.getvalue(0, 0), {"hi": 23})
        self.assertEqual(res.ftype(0), self.poque.JSONBOID)

    def test_jsonb_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '{\"{\\\"hi\\\": 23}\",\"[3, 4]\"}'::jsonb[];",
            [{"hi": 23}, [3, 4]], self.poque.JSONBARRAYOID)

    def test_xml_value_bin(self):
        self._test_value_and_type_bin("SELECT '<el>hi</el>'::xml",
                                      '<el>hi</el>', self.poque.XMLOID)

    def test_xml_value_str(self):
        res = self.cn.execute("SELECT '<el>hi</el>'::xml", result_format=0)
        self.assertEqual(res.getvalue(0, 0), '<el>hi</el>')
        self.assertEqual(res.ftype(0), self.poque.XMLOID)

    def test_xml_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT E'{<el>\n1</el>, <el>2</el>}'::xml[];",
            ['<el>\n1</el>', '<el>2</el>'], self.poque.XMLARRAYOID)

    def assert_tuple(self, val, length=2):
        self.assertIsInstance(val, tuple)
        self.assertEqual(len(val), length)

    def assert_point(self, point, vals):
        self.assert_tuple(point)
        self.assertAlmostEqual(point[0], vals[0])
        self.assertAlmostEqual(point[1], vals[1])

    def test_point_value_bin(self):
        res = self.cn.execute("SELECT '(1.24, 3.4)'::point;", result_format=1)
        val = res.getvalue(0, 0)
        self.assert_point(val, (1.24, 3.4))
        self.assertEqual(res.ftype(0), self.poque.POINTOID)

    def test_point_array_value_bin(self):
        res = self.cn.execute(
            "SELECT ARRAY['(1.24, 3.4)', NULL, '(-1.2, 5)']::point[]")
        val = res.getvalue(0, 0)
        self.assertEqual(len(val), 3)
        self.assertIsInstance(val, list)
        self.assert_point(val[0], (1.24, 3.4))
        self.assertEqual(val[1], None)
        self.assert_point(val[2], (-1.2, 5))
        self.assertEqual(res.ftype(0), self.poque.POINTARRAYOID)

    def test_line_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '{1.2, 3.0, 4}'::line;", (1.2, 3.0, 4), LINEOID)

    def test_linearray_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY['{1.2, 3.0, 4}', '{1.2, 3.0, 5}']::line[];",
            [(1.2, 3.0, 4), (1.2, 3.0, 5)], LINEARRAYOID)

    def assert_lseg(self, val, lseg):
        self.assert_tuple(val)
        self.assert_point(val[0], lseg[0])
        self.assert_point(val[1], lseg[1])

    def test_lseg_value_bin(self):
        res = self.cn.execute("SELECT '((1.3, 3.45), (2, 5.6))'::lseg;",
                              result_format=1)
        val = res.getvalue(0, 0)
        self.assert_lseg(val, ((1.3, 3.45), (2, 5.6)))
        self.assertEqual(res.ftype(0), self.poque.LSEGOID)

    def test_lseg_array_value_bin(self):
        res = self.cn.execute("""
            SELECT ARRAY['((1.3, 3.45), (2, 5.6))', NULL,
                         '((-1.1, 2), (3, 4.12))']::lseg[]""")
        val = res.getvalue(0, 0)
        self.assertIsInstance(val, list)
        self.assertEqual(len(val), 3)
        self.assert_lseg(val[0], ((1.3, 3.45), (2, 5.6)))
        self.assertIsNone(val[1])
        self.assert_lseg(val[2], ((-1.1, 2), (3, 4.12)))
        self.assertEqual(res.ftype(0), self.poque.LSEGARRAYOID)

    def assert_path(self, val, path):
        self.assertIsInstance(val, dict)
        val_path = val['path']
        self.assertIsInstance(val_path, list)
        self.assertEqual(len(val_path), len(path["path"]))
        for p1, p2 in zip(val_path, path["path"]):
            self.assert_point(p1, p2)
        del val['path']
        self.assertEqual(val, {'closed': path['closed']})

    def test_path_value_bin(self):
        res = self.cn.execute(
            "SELECT '((1.3, 3.45), (2, 5.6), (-1.3, -4))'::path;")
        val = res.getvalue(0, 0)
        self.assert_path(val, {'path': [(1.3, 3.45), (2, 5.6), (-1.3, -4)],
                               'closed': True})
        self.assertEqual(res.ftype(0), self.poque.PATHOID)

    def test_path_array_value_bin(self):
        res = self.cn.execute("""
            SELECT ARRAY['((1.3, 3.45), (2, 5.6), (-1.3, -4))', NULL,
                         '[(1.3, 3.45), (2, 5.6), (-1.3, -4)]']::path[];""")
        val = res.getvalue(0, 0)
        self.assertIsInstance(val, list)
        self.assertEqual(len(val), 3)
        self.assert_path(val[0], {'path': [(1.3, 3.45), (2, 5.6), (-1.3, -4)],
                                  'closed': True})
        self.assertIs(val[1], None)
        self.assert_path(val[2], {'path': [(1.3, 3.45), (2, 5.6), (-1.3, -4)],
                                  'closed': False})
        self.assertEqual(res.ftype(0), self.poque.PATHARRAYOID)

    def test_box_value_bin(self):
        res = self.cn.execute("SELECT '((1.3, 3.45), (2, 5.6))'::box;")
        val = res.getvalue(0, 0)
        self.assert_lseg(val, ((2, 5.6), (1.3, 3.45)))
        self.assertEqual(res.ftype(0), self.poque.BOXOID)

    def test_box_array_value_bin(self):
        res = self.cn.execute("""
            SELECT ARRAY['((1.3, 3.45), (2, 5.6))', NULL,
                         '((1.3, 3.45), (2, 5.6))']::box[];""")
        val = res.getvalue(0, 0)
        self.assertIsInstance(val, list)
        self.assertEqual(len(val), 3)
        self.assert_lseg(val[0], ((2, 5.6), (1.3, 3.45)))
        self.assertIs(val[1], None)
        self.assert_lseg(val[2], ((2, 5.6), (1.3, 3.45)))
        self.assertEqual(res.ftype(0), self.poque.BOXARRAYOID)

    def test_polygon_value_bin(self):
        res = self.cn.execute(
            "SELECT '((1.3, 3.45), (2, 5.6), (-1.3, -4))'::polygon;")
        val = res.getvalue(0, 0)
        self.assertIsInstance(val, list)
        self.assertEqual(len(val), 3)
        self.assert_point(val[0], (1.3, 3.45))
        self.assert_point(val[1], (2, 5.6))
        self.assert_point(val[2], (-1.3, -4))
        self.assertEqual(res.ftype(0), self.poque.POLYGONOID)

    def test_polygon_array_value_bin(self):
        res = self.cn.execute("""
            SELECT ARRAY['((1.3, 3.45), (2, 5.6), (-1.3, -4))'::polygon,
                         NULL]""")
        val = res.getvalue(0, 0)
        self.assertIsInstance(val, list)
        self.assertEqual(len(val), 2)
        self.assertIsNone(val[1])
        val = val[0]
        self.assertIsInstance(val, list)
        self.assertEqual(len(val), 3)
        self.assert_point(val[0], (1.3, 3.45))
        self.assert_point(val[1], (2, 5.6))
        self.assert_point(val[2], (-1.3, -4))
        self.assertEqual(res.ftype(0), self.poque.POLYGONARRAYOID)

    def test_abstime_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT 1000000000::abstime;",
            datetime.datetime.fromtimestamp(1000000000), self.poque.ABSTIMEOID)

    def test_abstime_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY[1000000000::abstime, 86400::abstime]",
            [datetime.datetime.fromtimestamp(1000000000),
             datetime.datetime.fromtimestamp(86400)], self.poque.ABSTIMEARRAYOID)

    def test_reltime_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT 100::reltime;", datetime.timedelta(seconds=100),
            self.poque.RELTIMEOID)

    def test_reltime_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY[100::reltime, 1000::reltime]",
            [datetime.timedelta(seconds=100),
             datetime.timedelta(seconds=1000)], self.poque.RELTIMEARRAYOID)

    def test_tinterval_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '[\"2014-01-01\" \"2015-01-01\"]'::tinterval",
            (datetime.datetime(2014, 1, 1), datetime.datetime(2015, 1, 1)),
            self.poque.TINTERVALOID)

    def test_tinterval_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY['[\"2014-01-01\" \"2015-01-01\"]'::tinterval, NULL]",
            [(datetime.datetime(2014, 1, 1), datetime.datetime(2015, 1, 1)),
             None], self.poque.TINTERVALARRAYOID)

    def test_circle_value_bin(self):
        res = self.cn.execute("SELECT '<(2.3, -4.5), 3.75>'::circle;")
        val = res.getvalue(0, 0)
        self.assertIsInstance(val, tuple)
        self.assertEqual(len(val), 2)
        self.assertIsInstance(val[0], tuple)
        self.assertEqual(len(val[0]), 2)
        self.assertAlmostEqual(val[0][0], 2.3)
        self.assertAlmostEqual(val[0][1], -4.5)
        self.assertAlmostEqual(val[1], 3.75)
        self.assertEqual(res.ftype(0), self.poque.CIRCLEOID)

    def test_circle_array_value_bin(self):
        res = self.cn.execute('''
            SELECT '{"<(2.3, -4.5), 3.75>", "<(2.3, -4.5), 3.75>"}'::circle[];
            ''')
        value = res.getvalue(0, 0)
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
            self.assertEqual(res.ftype(0), self.poque.CIRCLEARRAYOID)

    def test_money_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT 3::money", 300, self.poque.CASHOID)

    def test_money_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '{3.4, -52}'::numeric[]::money[];", [340, -5200],
            self.poque.CASHARRAYOID)

    def test_mac_addr_value_bin(self):
        self._test_value_and_type_bin("SELECT '24:0a:64:dd:58:c4'::macaddr;",
                                      0x240a64dd58c4, self.poque.MACADDROID)

    def test_mac_addr_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY['24:0a:64:dd:58:c4'::macaddr];", [0x240a64dd58c4],
            self.poque.MACADDRARRAYOID)

    def test_mac_addr8_value_bin(self):
        if self.cn.server_version >= 100000:
            self._test_value_and_type_bin(
                "SELECT '24:0a:64:dd:58:c4'::macaddr8;",
                0x240a64fffedd58c4,
                self.poque.MACADDR8OID)

    def test_mac_addr8_array_value_bin(self):
        if self.cn.server_version >= 100000:
            self._test_value_and_type_bin(
                "SELECT ARRAY['24:0a:64:dd:58:c4'::macaddr8];",
                [0x240a64fffedd58c4],
                self.poque.MACADDR8ARRAYOID)

    def test_ipv4_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '192.168.0.1'::inet", IPv4Interface('192.168.0.1'),
            self.poque.INETOID)
        self._test_value_and_type_bin(
            "SELECT '192.168.10.1/24'::inet", IPv4Interface('192.168.10.1/24'),
            self.poque.INETOID)

    def test_ipv4_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY['192.168.0.1'::inet, NULL]",
            [IPv4Interface('192.168.0.1'), None], self.poque.INETARRAYOID)

    def test_ipv6_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '2001:db8:85a3:0:0:8a2e:370:7334'::inet",
            IPv6Interface('2001:db8:85a3:0:0:8a2e:370:7334'),
            self.poque.INETOID)
        self._test_value_and_type_bin(
            "SELECT '2001:db8:85a3:0:0:8a2e:370:7334/64'::inet",
            IPv6Interface('2001:db8:85a3:0:0:8a2e:370:7334/64'),
            self.poque.INETOID)

    def test_ipv6_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY[NULL, '2001:db8:85a3:0:0:8a2e:370:7334'::inet]",
            [None, IPv6Interface('2001:db8:85a3:0:0:8a2e:370:7334')],
            self.poque.INETARRAYOID)

    def test_cidrv4_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '192.168.0.0/24'::cidr", IPv4Network('192.168.0.0/24'),
            self.poque.CIDROID)

    def test_cidrv4_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '{192.168.0.0/24}'::cidr[]",
            [IPv4Network('192.168.0.0/24')], self.poque.CIDRARRAYOID)

    def test_cidrv6_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '2001:db8:85a3:0:0:8a2e:0:0/96'::cidr",
            IPv6Network('2001:db8:85a3:0:0:8a2e:0:0/96'), self.poque.CIDROID)

    def test_bool_array_bin(self):
        self._test_value_and_type_bin("SELECT '{true, NULL, false}'::bool[]",
                                      [True, None, False],
                                      self.poque.BOOLARRAYOID)

    def test_bytea_array_bin(self):
        self._test_value_and_type_bin(
            r"SELECT '{\\x2020, NULL, \\x2020}'::bytea[]",
            [b'  ', None, b'  '], self.poque.BYTEAARRAYOID)

    def test_uuid_value_bin(self):
        val = uuid4()
        res = self.cn.execute("SELECT '{0}'::uuid".format(val),
                              result_format=1)
        v = res.getvalue(0, 0)
        self.assertEqual(v, val)
        self._test_value_and_type_bin(
            "SELECT '12345678123456781234567800345678'::uuid",
            UUID(hex='12345678123456781234567800345678'), self.poque.UUIDOID)

    def test_uuid_value_str(self):
        val = uuid4()
        res = self.cn.execute("SELECT '{0}'::uuid".format(val),
                              result_format=0)
        v = res.getvalue(0, 0)
        self.assertEqual(v, val)
        self.assertEqual(res.ftype(0), self.poque.UUIDOID)

    def test_date_value_bin(self):
        self._test_value_and_type_bin("SELECT '2014-03-01'::date",
                                      datetime.date(2014, 3, 1),
                                      self.poque.DATEOID)
        self._test_value_and_type_bin("SELECT '20140-03-01'::date",
                                      '20140-03-01', self.poque.DATEOID)
        self._test_value_and_type_bin("SELECT '500-03-01 BC'::date",
                                      '0500-03-01 BC', self.poque.DATEOID)
        self._test_value_and_type_bin("SELECT '0001-01-01'::date",
                                      datetime.date.min, self.poque.DATEOID)
        self._test_value_and_type_bin("SELECT '0001-12-31 BC'::date",
                                      '0001-12-31 BC', self.poque.DATEOID)

    def test_date_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY['2014-03-01'::date]", [datetime.date(2014, 3, 1)],
            self.poque.DATEARRAYOID)

    def test_time_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '13:09:25.123'::time", datetime.time(13, 9, 25, 123000),
            self.poque.TIMEOID)

    def test_time_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY[NULL, '13:09:25.123'::time]",
            [None, datetime.time(13, 9, 25, 123000)], self.poque.TIMEARRAYOID)

    def test_timestamp_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '2013-04-02 13:09:25.123'::timestamp",
            datetime.datetime(2013, 4, 2, 13, 9, 25, 123000),
            self.poque.TIMESTAMPOID)
        self._test_value_and_type_bin(
            "SELECT '500-04-02 13:09:25.123 BC'::timestamp",
            "0500-04-02 13:09:25.123000 BC", self.poque.TIMESTAMPOID)
        self._test_value_and_type_bin(
            "SELECT '20130-04-02 13:09:25.123'::timestamp",
            "20130-04-02 13:09:25.123000",
            self.poque.TIMESTAMPOID)
        self._test_value_and_type_bin(
            "SELECT 'infinity'::timestamp", "infinity",
            self.poque.TIMESTAMPOID)
        self._test_value_and_type_bin(
            "SELECT '-infinity'::timestamp", "-infinity",
            self.poque.TIMESTAMPOID)

    def test_timestamp_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY['2013-04-02 13:09:25.123'::timestamp, NULL]",
            [datetime.datetime(2013, 4, 2, 13, 9, 25, 123000), None],
            self.poque.TIMESTAMPARRAYOID)

    def test_timetz_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '14:12+00:30'::timetz",
            datetime.time(14, 12, tzinfo=datetime.timezone(
                datetime.timedelta(seconds=1800))),
            self.poque.TIMETZOID)

        self._test_value_and_type_bin(
            "SELECT '14:12-04:00'::timetz",
            datetime.time(14, 12, tzinfo=datetime.timezone(
                datetime.timedelta(hours=-4))),
            self.poque.TIMETZOID)

    def test_timestamptz_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '2013-04-02 13:09:25.123 +3'::timestamptz",
            datetime.datetime(
                2013, 4, 2, 10, 9, 25, 123000, tzinfo=datetime.timezone.utc),
            self.poque.TIMESTAMPTZOID)

    def test_timestamptz_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY['2013-04-02 13:09:25.123 +3'::timestamptz, NULL]",
            [datetime.datetime(
                2013, 4, 2, 10, 9, 25, 123000, tzinfo=datetime.timezone.utc),
             None],
            self.poque.TIMESTAMPTZARRAYOID)

    def test_interval_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT '1 century 4 month 2 days 3 hour'::interval;",
            (1204, datetime.timedelta(days=2, hours=3)),
            self.poque.INTERVALOID)
        self._test_value_and_type_bin(
            "SELECT '1 century 4 month 2 days 3 hour ago'::interval;",
            (-1204, datetime.timedelta(days=-2, hours=-3)),
            self.poque.INTERVALOID)

    def test_interval_array_value_bin(self):
        self._test_value_and_type_bin(
            "SELECT ARRAY['1 century 4 month 2 days 3 hour'::interval, NULL];",
            [(1204, datetime.timedelta(days=2, hours=3)), None],
            self.poque.INTERVALARRAYOID)


class ResultTestValuesExtension(
        BaseExtensionTest, ResultTestValues, unittest.TestCase):
    pass


class ResultTestValuesCtypes(
        BaseCTypesTest, ResultTestValues, unittest.TestCase):
    pass


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
