import select
import unittest
import weakref


from test import config
from test.config import BaseExtensionTest, BaseCTypesTest
from test.test_lib import assert_is_conninfo


class TestConnectionOpen():

    def test_connect_params(self):
        kwargs = config.connparams()
        cn = self.poque.connect(**kwargs)
        self.assertEqual(cn.status, self.poque.CONNECTION_OK)

    def test_connect_params_expand(self):
        connstring = config.conninfo()
        cn = self.poque.Conn(dbname=connstring, expand_dbname=True)
        self.assertEqual(cn.status, self.poque.CONNECTION_OK)

    def test_connect_conninfo(self):
        connstring = config.conninfo()
        cn = self.poque.Conn(connstring)
        self.assertEqual(cn.status, self.poque.CONNECTION_OK)

    def test_connect_url(self):
        connstring = config.connurl()
        cn = self.poque.Conn(connstring)
        self.assertEqual(cn.status, self.poque.CONNECTION_OK)

    def test_connect_params_invalid_arg(self):
        kwargs = config.connstringparams()
        kwargs['yoyo'] = None
        cn = self.poque.Conn(**kwargs)
        self.assertEqual(cn.status, self.poque.CONNECTION_OK)

    def test_connect_wrong_params(self):
        with self.assertRaises(self.poque.Error):
            self.poque.Conn(zut='zut')


class TestConnectionOpenExtension(
        BaseExtensionTest, TestConnectionOpen, unittest.TestCase):
    pass


class TestConnectionOpenCtypes(
        BaseCTypesTest, TestConnectionOpen, unittest.TestCase):
    pass


class TestConnectionBasic():

    def setUp(self):
        self.cn = self.poque.Conn(config.conninfo())

    def test_db(self):
        self.assertEqual(self.cn.db, config.connparams()['dbname'])

    def test_user(self):
        self.assertIsInstance(self.cn.user, str)

    def test_password(self):
        self.assertIsInstance(self.cn.password, str)

    def test_port(self):
        self.assertIsInstance(self.cn.port, str)

    def test_host(self):
        self.assertIsInstance(self.cn.host, (str, type(None)))

    def test_options(self):
        self.assertIsInstance(self.cn.options, str)

    def test_error_message(self):
        self.assertIsInstance(self.cn.error_message, str)

    def test_protocol_version(self):
        self.assertIn(self.cn.protocol_version, [2, 3])

    def test_transaction_status(self):
        self.assertEqual(self.cn.transaction_status, self.poque.TRANS_IDLE)

    def test_server_version(self):
        self.assertIsInstance(self.cn.server_version, int)

    def test_fileno(self):
        self.assertIsInstance(self.cn.fileno(), int)

    def test_backend_pid(self):
        self.assertIsInstance(self.cn.backend_pid, int)

    def test_parameter_status(self):
        self.assertEqual(self.cn.parameter_status('client_encoding'), 'UTF8')

    def test_parameter_status_wrong(self):
        self.assertIsNone(self.cn.parameter_status('nonsense'))

    def test_parameter_status_wrong_args(self):
        with self.assertRaises(TypeError):
            self.cn.parameter_status(1)

    def test_info(self):
        assert_is_conninfo(self, self.cn.info())

    def test_reset(self):
        self.cn.reset()
        self.assertEqual(self.cn.status, self.poque.CONNECTION_OK)

    def test_reset_async(self):
        cn = self.cn
        cn.reset_start()
        state = self.poque.POLLING_WRITING
        while state != self.poque.POLLING_OK:
            if state == self.poque.POLLING_WRITING:
                select.select([], [cn], [])
            elif state == self.poque.POLLING_READING:
                select.select([cn], [], [])
            state = cn.reset_poll()
        self.assertEqual(cn.status, self.poque.CONNECTION_OK)

    def test_escape_literal(self):
        self.assertEqual(self.cn.escape_literal("h'oi"), "'h''oi'")
        self.assertEqual(self.cn.escape_literal(literal="h'oi"), "'h''oi'")
        with self.assertRaises(TypeError):
            self.cn.escape_literal(lit="h'oi")

    def test_escape_identifier(self):
        self.assertEqual(self.cn.escape_identifier('Hello'), '"Hello"')
        self.assertEqual(self.cn.escape_identifier(identifier='Hello'),
                         '"Hello"')
        with self.assertRaises(TypeError):
            self.cn.escape_identifier(lit="h'oi")

    def test_finish(self):
        self.cn.finish()
        self.assertEqual(self.cn.status, self.poque.CONNECTION_BAD)

    def test_weakref(self):
        self.assertEqual(weakref.ref(self.cn)(), self.cn)


class TestConnectionBasicExtension(
        BaseExtensionTest, TestConnectionBasic, unittest.TestCase):
    pass


class TestConnectionBasicCtypes(
        BaseCTypesTest, TestConnectionBasic, unittest.TestCase):
    pass


class TestConnectionClosed():

    def setUp(self):
        self.cn = self.poque.Conn(config.conninfo())
        self.cn.finish()

    def test_db(self):
        self.assertIsNone(self.cn.db)

    def test_user(self):
        self.assertIsNone(self.cn.user)

    def test_password(self):
        self.assertIsNone(self.cn.password)

    def test_port(self):
        self.assertIsNone(self.cn.port)

    def test_host(self):
        self.assertIsNone(self.cn.host)

    def test_error_message(self):
        self.assertIsInstance(self.cn.error_message, str)

    def test_options(self):
        self.assertIsNone(self.cn.options)

    def test_protocol_version(self):
        self.assertEqual(self.cn.protocol_version, 0)

    def test_transaction_status(self):
        self.assertEqual(self.cn.transaction_status, self.poque.TRANS_UNKNOWN)

    def test_server_version(self):
        self.assertEqual(self.cn.server_version, 0)

    def test_fileno(self):
        with self.assertRaises(ValueError):
            self.cn.fileno()

    def test_info(self):
        self.assertIs(self.cn.info(), None)

    def test_parameter_status(self):
        self.assertIsNone(self.cn.parameter_status('client_encoding'))

    def test_backend_pid(self):
        self.assertEqual(self.cn.backend_pid, 0)

    def test_reset(self):
        with self.assertRaises(ValueError):
            self.cn.reset()

    def test_reset_async(self):
        with self.assertRaises(ValueError):
            self.cn.reset_start()

    def test_reset_poll(self):
        with self.assertRaises(ValueError):
            self.cn.reset_poll()

    def test_escape_literal(self):
        with self.assertRaises(ValueError):
            self.cn.escape_literal("h'oi")

    def test_escape_identifier(self):
        with self.assertRaises(ValueError):
            self.cn.escape_identifier('Hello')

    def test_finish(self):
        self.cn.finish()
        self.assertEqual(self.cn.status, self.poque.CONNECTION_BAD)


class TestConnectionClosedExtension(
        BaseExtensionTest, TestConnectionClosed, unittest.TestCase):
    pass


class TestConnectionClosedCtypes(
        BaseCTypesTest, TestConnectionClosed, unittest.TestCase):
    pass


class TestConnectionAsync():

    def async_connect(self, cn):
        state = self.poque.POLLING_WRITING
        while state != self.poque.POLLING_OK:
            if state == self.poque.POLLING_WRITING:
                select.select([], [cn], [])
            elif state == self.poque.POLLING_READING:
                select.select([cn], [], [])
            state = cn.connect_poll()

    def test_async(self):
        cn = self.poque.Conn(config.conninfo(), True)
        self.async_connect(cn)
        self.assertEqual(cn.status, self.poque.CONNECTION_OK)

    def test_async_wrong(self):
        dbparams = config.connstringparams()
        dbparams['dbname'] = 'nonsense'
        cn = self.poque.Conn(blocking=False, **dbparams)
        with self.assertRaises(self.poque.Error):
            self.async_connect(cn)
        self.assertEqual(cn.status, self.poque.CONNECTION_BAD)


class TestConnectionAsyncExtension(
        BaseExtensionTest, TestConnectionAsync, unittest.TestCase):
    pass


class TestConnectionAsyncCtypes(
        BaseCTypesTest, TestConnectionAsync, unittest.TestCase):
    pass
