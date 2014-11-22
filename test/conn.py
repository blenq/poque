import select
import unittest
import weakref

import poque

from . import config
from .lib import assert_is_conninfo


class TestConnectionOpen(unittest.TestCase):

    def test_connect_params(self):
        kwargs = config.connparams()
        cn = poque.connect(**kwargs)
        self.assertEquals(cn.status, poque.CONNECTION_OK)

    def test_connect_params_expand(self):
        connstring = config.conninfo()
        cn = poque.Conn(dbname=connstring, expand_dbname=True)
        self.assertEquals(cn.status, poque.CONNECTION_OK)

    def test_connect_conninfo(self):
        connstring = config.conninfo()
        cn = poque.Conn(connstring)
        self.assertEquals(cn.status, poque.CONNECTION_OK)

    def test_connect_url(self):
        connstring = config.connurl()
        cn = poque.Conn(connstring)
        self.assertEquals(cn.status, poque.CONNECTION_OK)

    def test_connect_params_invalid_arg(self):
        kwargs = config.connstringparams()
        kwargs['yoyo'] = None
        cn = poque.Conn(**kwargs)
        self.assertEquals(cn.status, poque.CONNECTION_OK)

    def test_connect_wrong_params(self):
        with self.assertRaises(poque.Error):
            poque.Conn(zut='zut')


class TestConnectionBasic(unittest.TestCase):

    def setUp(self):
        self.cn = poque.Conn(config.conninfo())

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
        self.assertEquals(self.cn.transaction_status, poque.TRANS_IDLE)

    def test_server_version(self):
        self.assertIsInstance(self.cn.server_version, int)

    def test_fileno(self):
        self.assertIsInstance(self.cn.fileno(), int)

    def test_backend_pid(self):
        self.assertIsInstance(self.cn.backend_pid, int)

    def test_parameter_status(self):
        self.assertEquals(self.cn.parameter_status('client_encoding'), 'UTF8')

    def test_parameter_status_wrong(self):
        self.assertIsNone(self.cn.parameter_status('nonsense'))

    def test_parameter_status_wrong_args(self):
        with self.assertRaises(TypeError):
            self.cn.parameter_status(1)

    def test_info(self):
        assert_is_conninfo(self, self.cn.info())

    def test_reset(self):
        self.cn.reset()
        self.assertEquals(self.cn.status, poque.CONNECTION_OK)

    def test_reset_async(self):
        cn = self.cn
        cn.reset_start()
        state = poque.POLLING_WRITING
        while state != poque.POLLING_OK:
            if state == poque.POLLING_WRITING:
                select.select([], [cn], [])
            elif state == poque.POLLING_READING:
                select.select([cn], [], [])
            state = cn.reset_poll()
        self.assertEquals(cn.status, poque.CONNECTION_OK)

    def test_finish(self):
        self.cn.finish()
        self.assertEquals(self.cn.status, poque.CONNECTION_BAD)

    def test_weakref(self):
        self.assertEquals(weakref.ref(self.cn)(), self.cn)


class TestConnectionBasicClosed(unittest.TestCase):

    def setUp(self):
        self.cn = poque.Conn(config.conninfo())
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
        self.assertEquals(self.cn.protocol_version, 0)

    def test_transaction_status(self):
        self.assertEquals(self.cn.transaction_status, poque.TRANS_UNKNOWN)

    def test_server_version(self):
        self.assertEquals(self.cn.server_version, 0)

    def test_fileno(self):
        with self.assertRaises(ValueError):
            self.cn.fileno()

    def test_parameter_status(self):
        self.assertIsNone(self.cn.parameter_status('client_encoding'))

    def test_backend_pid(self):
        self.assertEquals(self.cn.backend_pid, 0)

    def test_reset(self):
        with self.assertRaises(ValueError):
            self.cn.reset()

    def test_reset_async(self):
        with self.assertRaises(ValueError):
            self.cn.reset_start()

    def test_reset_poll(self):
        with self.assertRaises(ValueError):
            self.cn.reset_poll()

    def test_finish(self):
        self.cn.finish()
        self.assertEquals(self.cn.status, poque.CONNECTION_BAD)


class TestConnectionAsync(unittest.TestCase):

    def async_connect(self, cn):
        state = poque.POLLING_WRITING
        while state != poque.POLLING_OK:
            if state == poque.POLLING_WRITING:
                select.select([], [cn], [])
            elif state == poque.POLLING_READING:
                select.select([cn], [], [])
            state = cn.connect_poll()

    def test_async(self):
        cn = poque.Conn(config.conninfo(), True)
        self.async_connect(cn)
        self.assertEqual(cn.status, poque.CONNECTION_OK)

    def test_async_wrong(self):
        dbparams = config.connstringparams()
        dbparams['dbname'] = 'nonsense'
        cn = poque.Conn(async=True, **dbparams)
        with self.assertRaises(poque.Error):
            self.async_connect(cn)
        self.assertEqual(cn.status, poque.CONNECTION_BAD)
