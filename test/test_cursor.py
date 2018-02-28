from decimal import Decimal
import unittest

from test.config import BaseExtensionTest, BaseCTypesTest, conninfo


class CursorTest():

    @classmethod
    def setUpClass(cls):
        cls.cn = cls.poque.Conn(conninfo())

    @classmethod
    def tearDownClass(cls):
        cls.cn.finish()

    def get_unknown_cmp_type(self):
        if (self.cn.server_version < 100000):
            return self.poque.UNKNOWNOID
        else:
            return self.poque.TEXTOID

    def get_unknown_fsize(self):
        if (self.cn.server_version < 100000):
            return -2
        else:
            return None

    def test_new_cursor(self):
        cr = self.cn.cursor()
        self.assertIsNotNone(cr)

    def test_cursor_conn(self):
        cr = self.cn.cursor()
        self.assertIs(self.cn, cr.connection)

    def test_close(self):
        cr = self.cn.cursor()
        cr.close()
        self.assertIsNone(cr.connection)
        with self.assertRaises(self.poque.InterfaceError):
            cr.execute("SELECT 1")
        cn = self.poque.Conn(conninfo())
        cr = cn.cursor()
        cn.close()
        with self.assertRaises(self.poque.InterfaceError):
            cr.execute("SELECT 1")

    two_row_query = """
    SELECT 1 as first,
           'hello' as second,
           '2.3'::decimal(4,2) as third,
           '4.7'::decimal as fourth
    UNION
    SELECT 2 as first,
           'hi' as second,
           '6.32'::decimal(4,2) as third,
           '5.7'::decimal as fourth
    """

    def test_fetchone(self):
        cr = self.cn.cursor()
        with self.assertRaises(self.poque.InterfaceError):
            cr.fetchone()
        cr.execute("SET bytea_output=hex")
        with self.assertRaises(self.poque.InterfaceError):
            cr.fetchone()
        cr.execute(self.two_row_query)
        self.assertEqual(cr.fetchone(),
                         (1, 'hello', Decimal('2.3'), Decimal('4.7')))
        self.assertEqual(cr.fetchone(),
                         (2, 'hi', Decimal('6.32'), Decimal('5.7')))
        self.assertIsNone(cr.fetchone())
        cr.close()
        with self.assertRaises(self.poque.InterfaceError):
            cr.fetchone()

    def test_execute_params(self):
        cr = self.cn.cursor()
        cr.execute("SELECT $2, $1", (4, 5))
        self.assertEqual(cr.fetchone(), (5, 4))
        cr.execute(parameters=(4, 5), operation="SELECT $2, $1")
        self.assertEqual(cr.fetchone(), (5, 4))

    def test_fetchall(self):
        cr = self.cn.cursor()
        cr.execute(self.two_row_query)
        self.assertEqual(cr.fetchall(), [
            (1, 'hello', Decimal('2.3'), Decimal('4.7')),
            (2, 'hi', Decimal('6.32'), Decimal('5.7')),
        ])
        self.assertEqual(cr.fetchall(), [])
        cr.execute(self.two_row_query)
        cr.fetchone()
        self.assertEqual(cr.fetchall(), [
            (2, 'hi', Decimal('6.32'), Decimal('5.7')),
        ])
        cr.close()
        with self.assertRaises(self.poque.InterfaceError):
            cr.fetchall()

    def test_execute_many(self):
        cr = self.cn.cursor()
        cr.execute("CREATE TEMPORARY TABLE ya (val1 int, val2 int)")
        cr.executemany("INSERT INTO ya VALUES ($1, $2)", [(1, 2), (3, 4)])
        cr.execute("SELECT * FROM ya")
        self.assertEqual(cr.fetchall(), [(1, 2), (3, 4)])

    def test_inputsizes(self):
        cr = self.cn.cursor()
        cr.setinputsizes([])

    def test_outputsize(self):
        cr = self.cn.cursor()
        cr.setoutputsize(3)
        cr.setoutputsize(3, 1)

    def test_arraysize(self):
        cr = self.cn.cursor()
        self.assertEqual(cr.arraysize, 1)

    def test_rownumber(self):
        cr = self.cn.cursor()
        self.assertIsNone(cr.rownumber)
        cr.execute(self.two_row_query)
        self.assertEqual(cr.rownumber, 0)
        cr.fetchone()
        self.assertEqual(cr.rownumber, 1)
        cr.fetchall()
        self.assertEqual(cr.rownumber, 2)
        cr.execute("SET bytea_output=hex")
        self.assertIsNone(cr.rownumber)


class CursorTestExtension(
        BaseExtensionTest, CursorTest, unittest.TestCase):
    pass


class CursorTestCtypes(BaseCTypesTest, CursorTest, unittest.TestCase):

    def test_cursor_description(self):
        cr = self.cn.cursor()
        self.assertIsNone(cr.description)
        cr.execute("""
            SELECT
                1 as first, 'hello' as second,
                '2.3'::decimal(4,2) as third,
                '4.7'::decimal as fourth""")
        self.assertEqual(cr.description, [
            ('first', self.poque.INT4OID, None, 4, None, None, None),
            ('second', self.get_unknown_cmp_type(), None,
             self.get_unknown_fsize(), None, None, None),
            ('third', self.poque.NUMERICOID, None, None, 4, 2, None),
            ('fourth', self.poque.NUMERICOID, None, None, None, None, None),
        ])
        cr.close()
        with self.assertRaises(self.poque.InterfaceError):
            cr.description

    def test_cursor_rowcount(self):
        cr = self.cn.cursor()
        self.assertEqual(cr.rowcount, -1)
        cr.execute("SELECT 1 UNION SELECT 2")
        self.assertEqual(cr.rowcount, 2)
        cr.execute("CREATE TEMPORARY TABLE yo (val int)")
        cr.execute("INSERT INTO yo VALUES (1)")
        self.assertEqual(cr.rowcount, 1)
        cr.close()
        with self.assertRaises(self.poque.InterfaceError):
            cr.rowcount

    def test_iter(self):
        cr = self.cn.cursor()
        cr.execute(self.two_row_query)
        self.assertEqual([r for r in cr], [
            (1, 'hello', Decimal('2.3'), Decimal('4.7')),
            (2, 'hi', Decimal('6.32'), Decimal('5.7')),
        ])
        cr.execute(self.two_row_query)
        it = (r for r in cr)
        next(it)
        cr.close()
        with self.assertRaises(self.poque.InterfaceError):
            next(it)
        with self.assertRaises(self.poque.InterfaceError):
            list(cr)

    def test_fetchmany(self):
        cr = self.cn.cursor()
        cr.execute(self.two_row_query)
        self.assertEqual(cr.fetchmany(), [
            (1, 'hello', Decimal('2.3'), Decimal('4.7')),
        ])
        self.assertEqual(cr.fetchone(),
                         (2, 'hi', Decimal('6.32'), Decimal('5.7')))
        cr.execute(self.two_row_query)
        cr.arraysize = 4
        self.assertEqual(cr.fetchmany(), [
            (1, 'hello', Decimal('2.3'), Decimal('4.7')),
            (2, 'hi', Decimal('6.32'), Decimal('5.7')),
        ])
        cr.execute(self.two_row_query)
        self.assertEqual(cr.fetchmany(3), [
            (1, 'hello', Decimal('2.3'), Decimal('4.7')),
            (2, 'hi', Decimal('6.32'), Decimal('5.7')),
        ])
        cr.execute(self.two_row_query)
        cr.fetchone()
        self.assertEqual(cr.fetchmany(2), [
            (2, 'hi', Decimal('6.32'), Decimal('5.7')),
        ])

    def test_scroll(self):
        cr = self.cn.cursor()
        with self.assertRaises(self.poque.InterfaceError):
            cr.scroll(2)
        cr.execute("SELECT * FROM (VALUES (1), (2), (3), (4)) t;")
        cr.fetchone()
        cr.scroll(2)
        self.assertEqual(cr.fetchone(), (4,))
        cr.scroll(1, "absolute")
        cr.scroll(1, "relative")
        self.assertEqual(cr.fetchone(), (3,))
        cr.scroll(0, "absolute")
        self.assertEqual(cr.fetchone(), (1,))
        with self.assertRaises(self.poque.InterfaceError):
            cr.scroll(0, "somewhere")
        with self.assertRaises(self.poque.InterfaceError):
            cr.scroll(8)
        with self.assertRaises(IndexError):
            cr.scroll(8)
        cr.execute("SET bytea_output=hex")
        with self.assertRaises(self.poque.InterfaceError):
            cr.scroll(0, "absolute")
        cr.execute("SELECT * FROM (VALUES (1), (2), (3), (4)) t;")
        cr.close()
        with self.assertRaises(self.poque.InterfaceError):
            cr.scroll(0, "absolute")
