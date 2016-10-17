import datetime
from decimal import Decimal
import unittest
import uuid

from test.config import BaseExtensionTest, BaseCTypesTest, conninfo


class ResultTestParameters():

    def setUp(self):
        self.cn = self.poque.Conn(conninfo())

    def _test_param_val(self, val):
        res = self.cn.execute("SELECT $1", [val])
        self.assertEqual(res.getvalue(0, 0), val)

    def test_str_param(self):
        self._test_param_val('hi')

    def test_int_param(self):
        res = self.cn.execute(
            "SELECT $1, $2, $3", [3, 2147483648, 17000000000000000000])
        self.assertEqual(res.getvalue(0, 0), 3)
        self.assertEqual(res.getvalue(0, 1), 2147483648)
        self.assertEqual(res.getvalue(0, 2), "17000000000000000000")

    def test_int_array_param(self):
        res = self.cn.execute(
            "SELECT $1", ([3, 2147483648, 17000000000000000000],))
        self.assertEqual(
            res.getvalue(0, 0), ['3', '2147483648', '17000000000000000000'])
        self.assertEqual(res.ftype(0), self.poque.TEXTARRAYOID)
        res = self.cn.execute("SELECT $1", ([3, None, 12],))
        self.assertEqual(
            res.getvalue(0, 0), [3, None, 12])
        self.assertEqual(res.ftype(0), self.poque.INT4ARRAYOID)
        res = self.cn.execute("SELECT $1", ([3, None, 0x80000000],))
        self.assertEqual(
            res.getvalue(0, 0), [3, None, 0x80000000])
        self.assertEqual(res.ftype(0), self.poque.INT8ARRAYOID)

    def test_mixed_array_param(self):
        with self.assertRaises(ValueError):
            self.cn.execute("SELECT $1", ([3, 'hi'],))

    def test_all_none_array_param(self):
        res = self.cn.execute(
            "SELECT $1", ([[None, None], [None, None]],))
        self.assertEqual(
            res.getvalue(0, 0), [[None, None], [None, None]])
        self.assertEqual(res.ftype(0), self.poque.TEXTARRAYOID)

    def test_empty_array_param(self):
        res = self.cn.execute(
            "SELECT $1", ([[], []],))
        self.assertEqual(
            res.getvalue(0, 0), [])
        self.assertEqual(res.ftype(0), self.poque.TEXTARRAYOID)

    def test_wrong_array_param(self):
        with self.assertRaises(ValueError):
            self.cn.execute("SELECT $1", ([[2], [3, 4]],))

        with self.assertRaises(ValueError):
            self.cn.execute("SELECT $1", ([2, []],))

        with self.assertRaises(ValueError):
            self.cn.execute("SELECT $1", ([[2, 3], 5],))

        with self.assertRaises(ValueError):
            self.cn.execute("SELECT $1", ([[[]], []],))

        with self.assertRaises(ValueError):
            self.cn.execute("SELECT $1", ([[[[[[[101]]]]]]],))

    def test_float_param(self):
        self._test_param_val(3.24)

    def test_bool_param(self):
        res = self.cn.execute(
            "SELECT $1, $2", [True, False])
        self.assertIs(res.getvalue(0, 0), True)
        self.assertIs(res.getvalue(0, 1), False)

    def test_bytes_param(self):
        self._test_param_val(b'hoi')

    def test_none_param(self):
        res = self.cn.execute("SELECT $1", [None])
        self.assertIsNone(res.getvalue(0, 0))

    def test_uuid_param(self):
        self._test_param_val(uuid.uuid4())

    def test_date_param(self):
        self._test_param_val(datetime.date.today())

    def test_time_param(self):
        self._test_param_val(datetime.datetime.now().time())

    def test_datetime_param(self):
        self._test_param_val(datetime.datetime.now())

    def test_datetimetz_param(self):
        self._test_param_val(datetime.datetime.now(
            datetime.timezone(datetime.timedelta(hours=2))))

    def _test_param_decimal(self, val):
        res = self.cn.execute("SELECT $1", [val])
        val_out = res.getvalue(0, 0)
        if val.is_nan():
            self.assertTrue(val_out.is_nan())
            return
        self.assertEqual(res.getvalue(0, 0), val)
        # at least as many digits as went in
        self.assertTrue(len(val_out.as_tuple()[1]) >= len(val.as_tuple()[1]))

    def test_decimal_param(self):
        self._test_param_decimal(Decimal('123.45600'))
        self._test_param_decimal(Decimal('123456789012345678901234567890'))
        self._test_param_decimal(Decimal('0.000000000000001230'))
        self._test_param_decimal(Decimal('-123456789012345678901234567890'))
        self._test_param_decimal(Decimal('-0.000000000000001230'))
        self._test_param_decimal(Decimal('9999E+100'))
        self._test_param_decimal(Decimal('NaN'))
        self._test_param_decimal(Decimal('99E-100'))


class ResultTestParametersCtypes(
        BaseCTypesTest, ResultTestParameters, unittest.TestCase):
    pass
