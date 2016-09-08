from ctypes import c_void_p, c_char_p, POINTER, c_int, Structure, c_uint
import functools

from .pq import pq, check_string, PQconninfoOptions, check_info_options
from .constants import CONNECTION_BAD, BAD_RESPONSE, FATAL_ERROR
from .lib import Error
from .result import Result


def new_connstring(connstring, async=False):
    return [connstring], async


class Property():

    def __init__(self, callable):
        self.callable = callable

    def __get__(self, instance, owner):
        return self.callable(instance)


class Method():

    def __init__(self, callable):
        self.callable = callable

    def __get__(self, instance, owner):
        def func(*args, **kwargs):
            return self.callable(instance, *args, **kwargs)
        return func


class Conn(c_void_p):

    def __new__(cls, *args, **kwargs):
        if args:
            names = ['dbname']
            values, async = new_connstring(*args, **kwargs)
            expand_dbname = True
        else:
            kws = kwargs.copy()
            expand_dbname = bool(kws.pop('expand_dbname', False))
            async = kws.pop('async', False)
            names, values = list(zip(*kws.items()))
        plen = len(names) + 1
        pq_names = (c_char_p * plen)()
        pq_values = (c_char_p * plen)()
        for i, (name, value) in enumerate(zip(names, values)):
            if name is not None:
                name = name.encode()
            pq_names[i] = name
            if value is not None:
                value = value.encode()
            pq_values[i] = value
        if async:
            return pq.PQconnectStartParams(pq_names, pq_values, expand_dbname)
        return pq.PQconnectdbParams(pq_names, pq_values, expand_dbname)

    def __init__(self, *args, **kwargs):
        pass

    connect_poll = Method(pq.PQconnectPoll)

    backend_pid = Property(pq.PQbackendPID)
    transaction_status = Property(pq.PQtransactionStatus)
    protocol_version = Property(pq.PQprotocolVersion)
    server_version = Property(pq.PQserverVersion)
    status = Property(pq.PQstatus)
    db = Property(pq.PQdb)
    error_message = Property(pq.PQerrorMessage)
    user = Property(pq.PQuser)
    password = Property(pq.PQpass)
    port = Property(pq.PQport)
    host = Property(pq.PQhost)
    options = Property(pq.PQoptions)
    info = Method(pq.PQconninfo)
    _fileno = Property(pq.PQsocket)

    def fileno(self):
        fileno = self._fileno
        if fileno == -1:
            raise ValueError("Connection is closed")
        return fileno

    def parameter_status(self, param_name):
        if param_name is not None:
            if not isinstance(param_name, str):
                raise TypeError("param_name must be str")
            param_name = param_name.encode()
        return pq.PQparameterStatus(self, param_name)

    def reset(self):
        pq.PQreset(self)

    def reset_start(self):
        return pq.PQresetStart(self)

    def reset_poll(self):
        return pq.PQresetPoll(self)

    def finish(self):
        pq.PQfinish(self)
        self.value = 0

    def _raise_error(self):
        if self:
            raise Error(self.error_message)
        raise ValueError("Connection is closed")

    def __del__(self):
        self.finish()

    def execute(self, command, fmt=1):
        return pq.PQexecParams(
            self, command.encode(), 0, None, None, None, None, fmt)


def check_connect(conn, func, args):
    if conn is None:
        raise MemoryError()
    if conn.status == CONNECTION_BAD:
        conn._raise_error()
    return conn


pq.PQconnectdbParams.restype = Conn
pq.PQconnectdbParams.argtypes = [
    POINTER(c_char_p), POINTER(c_char_p), c_int]
pq.PQconnectdbParams.errcheck = check_connect

pq.PQconnectStartParams.restype = Conn
pq.PQconnectStartParams.argtypes = [
    POINTER(c_char_p), POINTER(c_char_p), c_int]
pq.PQconnectStartParams.errcheck = check_connect


def check_async(res, func, args):
    if res == 0:
        conn = args[0]
        conn._raise_error()
    return res

pq.PQconnectPoll.argtypes = [Conn]
pq.PQconnectPoll.errcheck = check_async

pq.PQfinish.restype = None
pq.PQfinish.argtypes = [Conn]


def check_reset(res, func, args):
    conn = args[0]
    if conn.status == CONNECTION_BAD:
        conn._raise_error()

pq.PQreset.restype = None
pq.PQreset.argtypes = [Conn]
pq.PQreset.errcheck = check_reset

pq.PQresetStart.argtypes = [Conn]
pq.PQresetStart.errcheck = check_async

pq.PQresetPoll.argtypes = [Conn]
pq.PQresetPoll.errcheck = check_async

pq.PQstatus.restype = c_int
pq.PQstatus.argtypes = [Conn]

pq.PQerrorMessage.restype = c_char_p
pq.PQerrorMessage.argtypes = [Conn]

pq.PQbackendPID.restype = int
pq.PQbackendPID.argtypes = [Conn]

pq.PQtransactionStatus.restype = int
pq.PQtransactionStatus.argtypes = [Conn]

pq.PQprotocolVersion.restype = int
pq.PQprotocolVersion.argtypes = [Conn]

pq.PQserverVersion.restype = int
pq.PQserverVersion.argtypes = [Conn]

pq.PQsocket.restype = int
pq.PQsocket.argtypes = [Conn]

pq.PQdb.restype = c_char_p
pq.PQdb.argtypes = [Conn]
pq.PQdb.errcheck = check_string

pq.PQerrorMessage.restype = c_char_p
pq.PQerrorMessage.argtypes = [Conn]
pq.PQerrorMessage.errcheck = check_string

pq.PQhost.restype = c_char_p
pq.PQhost.argtypes = [Conn]
pq.PQhost.errcheck = check_string

pq.PQoptions.restype = c_char_p
pq.PQoptions.argtypes = [Conn]
pq.PQoptions.errcheck = check_string

pq.PQpass.restype = c_char_p
pq.PQpass.argtypes = [Conn]
pq.PQpass.errcheck = check_string

pq.PQuser.restype = c_char_p
pq.PQuser.argtypes = [Conn]
pq.PQuser.errcheck = check_string

pq.PQport.restype = c_char_p
pq.PQport.argtypes = [Conn]
pq.PQport.errcheck = check_string

pq.PQconninfo.restype = PQconninfoOptions
pq.PQconninfo.argtypes = [Conn]
pq.PQconninfo.errcheck = check_info_options

pq.PQparameterStatus.restype = c_char_p
pq.PQparameterStatus.argtypes = [Conn, c_char_p]
pq.PQparameterStatus.errcheck = check_string


def check_exec_params(res, func, args):
    if res is None:
        conn = args[0]
        conn._raise_error()
    if res.status in [BAD_RESPONSE, FATAL_ERROR]:
        raise Error(res.error_message)
    return res


pq.PQexecParams.restype = Result
pq.PQexecParams.argtypes = [
    Conn, c_char_p, c_int, POINTER(c_uint), POINTER(c_void_p), POINTER(c_int),
    POINTER(c_int), c_int]
pq.PQexecParams.errcheck = check_exec_params
