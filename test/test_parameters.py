import datetime
import unittest
import uuid

from test.config import BaseExtensionTest, BaseCTypesTest, conninfo


class ResultTestParameters():

    def setUp(self):
        self.cn = self.poque.Conn(conninfo())

    def test_str_param(self):
        res = self.cn.execute("SELECT $1", ['hi'])
        self.assertEqual(res.getvalue(0, 0), 'hi')

    def test_int_param(self):
        res = self.cn.execute(
            "SELECT $1, $2, $3", [3, 2147483648, 17000000000000000000])
        self.assertEqual(res.getvalue(0, 0), 3)
        self.assertEqual(res.getvalue(0, 1), 2147483648)
        self.assertEqual(res.getvalue(0, 2), "17000000000000000000")

    def test_float_param(self):
        res = self.cn.execute(
            "SELECT $1", [3.24])
        self.assertEqual(res.getvalue(0, 0), 3.24)

    def test_bool_param(self):
        res = self.cn.execute(
            "SELECT $1, $2", [True, False])
        self.assertIs(res.getvalue(0, 0), True)
        self.assertIs(res.getvalue(0, 1), False)

    def test_bytes_param(self):
        res = self.cn.execute("SELECT $1", [b'hoi'])
        self.assertEqual(res.getvalue(0, 0), b'hoi')

    def test_none_param(self):
        res = self.cn.execute("SELECT $1", [None])
        self.assertEqual(res.getvalue(0, 0), None)

    def test_uuid_param(self):
        val = uuid.uuid4()
        res = self.cn.execute("SELECT $1", [val])
        self.assertEqual(res.getvalue(0, 0), val)

    def test_date_param(self):
        val = datetime.date.today()
        res = self.cn.execute("SELECT $1", [val])
        self.assertEqual(res.getvalue(0, 0), val)

    def test_time_param(self):
        val = datetime.datetime.now().time()
        res = self.cn.execute("SELECT $1", [val])
        self.assertEqual(res.getvalue(0, 0), val)

    def test_datetime_param(self):
        val = datetime.datetime.now()
        res = self.cn.execute("SELECT $1", [val])
        self.assertEqual(res.getvalue(0, 0), val)

    def test_datetimetz_param(self):
        val = datetime.datetime.now(
            datetime.timezone(datetime.timedelta(hours=2)))
        res = self.cn.execute("SELECT $1", [val])
        self.assertEqual(res.getvalue(0, 0), val)


class ResultTestParametersCtypes(
        BaseCTypesTest, ResultTestParameters, unittest.TestCase):
    pass
