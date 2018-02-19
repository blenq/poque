import datetime
from decimal import Decimal
from ipaddress import IPv4Interface, IPv6Interface, IPv4Network, IPv6Network
import unittest
import uuid

from test.config import BaseExtensionTest, BaseCTypesTest, conninfo


class ResultTestParameters():

    @classmethod
    def setUpClass(cls):
        cls.cn = cls.poque.Conn(conninfo())

    @classmethod
    def tearDownClass(cls):
        cls.cn.finish()

    def setUp(self):
        self.cn.execute("BEGIN")

    def tearDown(self):
        self.cn.execute("ROLLBACK")

    def _test_param_val(self, val, typ=None):
        res = self.cn.execute("SELECT $1", [val])
        out_val = res.getvalue(0, 0)
        self.assertEqual(out_val, val)
        self.assertEqual(type(out_val), typ or type(val))

    def test_empty_params(self):
        res = self.cn.execute("SELECT 1", [])
        self.assertEqual(res.getvalue(0, 0), 1)

    def test_int_param(self):
        res = self.cn.execute(
            "SELECT $1, $2, $3, $4, $5",
            [3, 2147483648, 17000000000000000000, -4, -2147483649])
        self.assertEqual(res.getvalue(0, 0), 3)
        self.assertEqual(res.getvalue(0, 1), 2147483648)
        self.assertEqual(res.getvalue(0, 2), "17000000000000000000")
        self.assertEqual(res.getvalue(0, 3), -4)
        self.assertEqual(res.getvalue(0, 4), -2147483649)

    def test_int_array_param(self):
        res = self.cn.execute(
            "SELECT $1", ([3, 17000000000000000000, 2147483648],))
        self.assertEqual(
            res.getvalue(0, 0), ['3', '17000000000000000000', '2147483648'])
        self.assertEqual(res.ftype(0), self.poque.TEXTARRAYOID)

        res = self.cn.execute(
            "SELECT $1", ([3, 2147483648, 17000000000000000000],))
        self.assertEqual(
            res.getvalue(0, 0), ['3', '2147483648', '17000000000000000000'])
        self.assertEqual(res.ftype(0), self.poque.TEXTARRAYOID)

        res = self.cn.execute(
            "SELECT $1", ([2147483648, 3, 17000000000000000000],))
        self.assertEqual(
            res.getvalue(0, 0), ['2147483648', '3', '17000000000000000000'])
        self.assertEqual(res.ftype(0), self.poque.TEXTARRAYOID)

        res = self.cn.execute(
            "SELECT $1", ([2147483648, 17000000000000000000, 3],))
        self.assertEqual(
            res.getvalue(0, 0), ['2147483648', '17000000000000000000', '3'])
        self.assertEqual(res.ftype(0), self.poque.TEXTARRAYOID)

        res = self.cn.execute("SELECT $1", ([3, None, 12],))
        self.assertEqual(
            res.getvalue(0, 0), [3, None, 12])
        self.assertEqual(res.ftype(0), self.poque.INT4ARRAYOID)
        res = self.cn.execute("SELECT $1", ([3, None, 0x80000000],))
        self.assertEqual(
            res.getvalue(0, 0), [3, None, 0x80000000])
        self.assertEqual(res.ftype(0), self.poque.INT8ARRAYOID)
        res = self.cn.execute("SELECT $1", ([[[[[[101]]]]]],))
        self.assertEqual(
            res.getvalue(0, 0), [[[[[[101]]]]]])

    def test_str_param(self):
        self._test_param_val('hi')
        self._test_param_val('')

    def test_str_array_param(self):
        self._test_param_val(['hi', None, 'hello'])
        self._test_param_val(['hi', 'hello'])
        self._test_param_val(['', 'hello'])

    def test_float_param(self):
        self._test_param_val(3.24)

    def test_float_array_param(self):
        self._test_param_val([3.24, None, 234.765])

    def test_bytes_param(self):
        self._test_param_val(b'hoi', memoryview)
        self._test_param_val(b'', memoryview)

    def test_bytes_array_param(self):
        self._test_param_val([b'hoi', b'ha\0llo'])
        self._test_param_val([b'', b'hoi'])

    def test_bool_param(self):
        res = self.cn.execute(
            "SELECT $1, $2", [True, False])
        self.assertIs(res.getvalue(0, 0), True)
        self.assertIs(res.getvalue(0, 1), False)

    def test_bool_array_param(self):
        val = [True, False]
        self._test_param_val(val)

    def test_uuid_param(self):
        self._test_param_val(uuid.uuid4())

    def test_uuid_array_param(self):
        val = [uuid.uuid4(), uuid.uuid4()]
        self._test_param_val(val)

    def test_date_param(self):
        self._test_param_val(datetime.date.today())

    def test_date_array_param(self):
        self._test_param_val([
            datetime.date.today(),
            datetime.date.today() - datetime.timedelta(days=1),
            None
        ])

    def test_time_param(self):
        self._test_param_val(datetime.datetime.now().time())

    def test_time_array_param(self):
        self._test_param_val([
            datetime.datetime.now().time(),
            datetime.datetime.now().time()
        ])

    def test_datetime_param(self):
        self._test_param_val(datetime.datetime.now())

    def test_datetime_array_param(self):
        self._test_param_val([datetime.datetime.now(),
                              datetime.datetime.now()])

    def test_datetimetz_param(self):
        self._test_param_val(datetime.datetime.now(
            datetime.timezone(datetime.timedelta(hours=2))))

    def test_datetime_mixed_array_param(self):
        with self.assertRaises(ValueError):
            self.cn.execute("SELECT $1", ([
                datetime.datetime.now(),
                datetime.datetime.now(
                    datetime.timezone(datetime.timedelta(hours=2)))],))

    def test_datetimetz_array_param(self):
        self._test_param_val([datetime.datetime.now(
            datetime.timezone(datetime.timedelta(hours=2)))])

    def _test_param_decimal(self, val):
        res = self.cn.execute("SELECT $1", [val])
        val_out = res.getvalue(0, 0)
        if val.is_nan():
            self.assertTrue(val_out.is_nan())
            return
        self.assertEqual(val_out, val)
        # at least as many digits as went in
        self.assertTrue(len(val_out.as_tuple()[1]) >= len(val.as_tuple()[1]))

    def test_decimal_param(self):
        self._test_param_decimal(Decimal('123.45600'))
        self._test_param_decimal(Decimal('1'))
        self._test_param_decimal(Decimal('12'))
        self._test_param_decimal(Decimal('123'))
        self._test_param_decimal(Decimal('1234'))
        self._test_param_decimal(Decimal('12.34'))
        self._test_param_decimal(Decimal('12345'))
        self._test_param_decimal(Decimal('0'))
        self._test_param_decimal(Decimal('123456789012345678901234567890'))
        self._test_param_decimal(Decimal('0.000000000000001230'))
        self._test_param_decimal(Decimal('-123456789012345678901234567890'))
        self._test_param_decimal(Decimal('-0.000000000000001230'))
        self._test_param_decimal(Decimal('9999E+100'))
        self._test_param_decimal(Decimal('NaN'))
        self._test_param_decimal(Decimal('99E-100'))
        self._test_param_decimal(Decimal('123000000'))
        self._test_param_decimal(Decimal('123000000000.0'))
        self._test_param_decimal(Decimal('12300000000400000000.005'))
        self._test_param_decimal(Decimal('1E-16383'))
        with self.assertRaises(ValueError):
            self._test_param_decimal(Decimal('1E-16384'))
        self._test_param_decimal(Decimal('1E131071'))
        with self.assertRaises(ValueError):
            self._test_param_decimal(Decimal('1E131072'))
        with self.assertRaises(ValueError):
            self._test_param_decimal(Decimal('Inf'))

    def test_decimal_array_param(self):
        self._test_param_val([
            Decimal('123.45600'), Decimal('99E-100')])

    def test_ipinterface_param(self):
        self._test_param_val(IPv4Interface('192.168.0.1'))
        self._test_param_val(IPv4Interface('192.168.0.1/24'))
        self._test_param_val(IPv6Interface('2001:db8:85a3:0:0:8a2e:370:7334'))
        self._test_param_val(
            IPv6Interface('2001:db8:85a3:0:0:8a2e:370:7334/64'))

    def test_ipinterface_array_param(self):
        self._test_param_val([
            IPv4Interface('192.168.0.1'),
            IPv4Interface('192.168.0.1/24'),
            IPv6Interface('2001:db8:85a3:0:0:8a2e:370:7334'),
            IPv6Interface('2001:db8:85a3:0:0:8a2e:370:7334/64')
        ])
        self._test_param_val([
            IPv6Interface('2001:db8:85a3:0:0:8a2e:370:7334'),
            IPv6Interface('2001:db8:85a3:0:0:8a2e:370:7334/64'),
            IPv4Interface('192.168.0.1'),
            IPv4Interface('192.168.0.1/24'),
        ])

    def test_ipnetwork_param(self):
        self._test_param_val(IPv4Network('192.168.0.0/24'))
        self._test_param_val(IPv6Network('2001:db8:85a3:0:0:8a2e:0:0/96'))

    def test_ipnetwork_array_param(self):
        self._test_param_val([IPv4Network('192.168.0.0/24'),
                              IPv6Network('2001:db8:85a3:0:0:8a2e:0:0/96')])
        self._test_param_val([IPv6Network('2001:db8:85a3:0:0:8a2e:0:0/96'),
                              IPv4Network('192.168.0.0/24')])

    def test_mixed_array_param(self):
        with self.assertRaises(ValueError):
            self.cn.execute("SELECT $1", ([3, 'hi'],))

    def test_none_param(self):
        res = self.cn.execute("SELECT $1", [None])
        self.assertIsNone(res.getvalue(0, 0))

    def test_unknown_param(self):
        res = self.cn.execute("SELECT $1", [set([1])])
        self.assertEqual(res.getvalue(0, 0), '{1}')

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


class ResultTestParametersExtension(
        BaseExtensionTest, ResultTestParameters, unittest.TestCase):
    pass


class ResultTestParametersCtypes(
        BaseCTypesTest, ResultTestParameters, unittest.TestCase):
    pass
