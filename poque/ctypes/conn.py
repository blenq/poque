from ctypes import c_void_p, c_char_p, POINTER, c_int, c_uint, c_size_t
from uuid import UUID

from .pq import pq, check_string, PQconninfoOptions, check_info_options
from .constants import (
    CONNECTION_BAD, BAD_RESPONSE, FATAL_ERROR, TEXTOID,
    BOOLOID, BYTEAOID, UUIDOID, FORMAT_BINARY,
    FORMAT_TEXT)
from .dt import get_date_time_param_converters
from .numeric import get_numeric_param_converters
from .lib import Error, _get_property, get_method
from .result import Result


def new_connstring(connstring, blocking=True):
    return [connstring], blocking


def _get_str_param(val):
    val = val.encode()
    return TEXTOID, c_char_p(val), len(val), FORMAT_BINARY


def _get_bool_param(val):
    return BOOLOID, c_char_p(b'\x01' if val else b'\0'), 1, FORMAT_BINARY


def _get_bytes_param(val):
    return BYTEAOID, c_char_p(val), len(val), FORMAT_BINARY


def _get_none_param(val):
    return TEXTOID, 0, 0, FORMAT_BINARY


def _get_uuid_param(val):
    return UUIDOID, c_char_p(val.bytes), 16, FORMAT_BINARY


class Conn(c_void_p):

    def __new__(cls, *args, **kwargs):
        if args:
            names = ['dbname']
            values, blocking = new_connstring(*args, **kwargs)
            expand_dbname = True
        else:
            kws = kwargs.copy()
            expand_dbname = bool(kws.pop('expand_dbname', False))
            blocking = kws.pop('blocking', True)
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
        if blocking:
            return pq.PQconnectdbParams(pq_names, pq_values, expand_dbname)
        return pq.PQconnectStartParams(pq_names, pq_values, expand_dbname)

    def __init__(self, *args, **kwargs):
        pass

    connect_poll = get_method(pq.PQconnectPoll)

    backend_pid = _get_property(pq.PQbackendPID)
    transaction_status = _get_property(pq.PQtransactionStatus)
    protocol_version = _get_property(pq.PQprotocolVersion)
    server_version = _get_property(pq.PQserverVersion)
    status = _get_property(pq.PQstatus)
    db = _get_property(pq.PQdb)
    error_message = _get_property(pq.PQerrorMessage)
    user = _get_property(pq.PQuser)
    password = _get_property(pq.PQpass)
    port = _get_property(pq.PQport)
    host = _get_property(pq.PQhost)
    options = _get_property(pq.PQoptions)

    info = get_method(pq.PQconninfo)
    fileno = get_method(pq.PQsocket)

    def parameter_status(self, param_name):
        if param_name is not None:
            if not isinstance(param_name, str):
                raise TypeError("param_name must be str")
            param_name = param_name.encode()
        return pq.PQparameterStatus(self, param_name)

    reset = get_method(pq.PQreset)
    reset_start = get_method(pq.PQresetStart)
    reset_poll = get_method(pq.PQresetPoll)
    _finish = get_method(pq.PQfinish)

    def escape_literal(self, literal):
        literal = literal.encode()
        return pq.PQescapeLiteral(self, literal, len(literal))

    def escape_identifier(self, identifier):
        identifier = identifier.encode()
        return pq.PQescapeIdentifier(self, identifier, len(identifier))

    def finish(self):
        self._finish()
        self.value = 0

    def _raise_error(self):
        if self:
            raise Error(self.error_message)
        raise ValueError("Connection is closed")

    def __del__(self):
        self.finish()

    _param_converters = {
        str: _get_str_param,
        bool: _get_bool_param,
        bytes: _get_bytes_param,
        type(None): _get_none_param,
        UUID: _get_uuid_param,
    }
    _param_converters.update(get_date_time_param_converters())
    _param_converters.update(get_numeric_param_converters())

    def execute(self, command, parameters=None, result_format=FORMAT_BINARY):
        if parameters is None:
            if result_format == FORMAT_TEXT:
                return pq.PQexec(self, command.encode())
            return pq.PQexecParams(self, command.encode(), 0, None, None, None,
                                   None, result_format)
        num_params = len(parameters)
        oids = (c_uint * num_params)()
        values = (c_char_p * num_params)()
        lengths = (c_int * num_params)()
        formats = (c_int * num_params)()
        for i, param in enumerate(parameters):
            conv = self._param_converters.get(type(param))
            if conv is None:
                param = str(param)
                conv = _get_str_param
            oids[i], values[i], lengths[i], formats[i] = conv(param)
        return pq.PQexecParams(
            self, command.encode(), num_params, oids, values, lengths,
            formats, result_format)


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


def check_fileno(res, func, args):
    if res == -1:
        raise ValueError("Connection is closed")
    return res

pq.PQsocket.restype = int
pq.PQsocket.argtypes = [Conn]
pq.PQsocket.errcheck = check_fileno

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


def check_exec(res, func, args):
    if res is None:
        conn = args[0]
        conn._raise_error()
    if res.status in [BAD_RESPONSE, FATAL_ERROR]:
        raise Error(res.error_message)
    return res


pq.PQexecParams.restype = Result
pq.PQexecParams.argtypes = [
    Conn, c_char_p, c_int, POINTER(c_uint), POINTER(c_char_p), POINTER(c_int),
    POINTER(c_int), c_int]
pq.PQexecParams.errcheck = check_exec


pq.PQexec.restype = Result
pq.PQexec.argtypes = [Conn, c_char_p]
pq.PQexec.errcheck = check_exec


def check_string_and_free(res, func, args):
    if res is None:
        conn = args[0]
        conn._raise_error()
    ret = c_char_p(res).value
    pq.PQfreemem(res)
    return ret.decode()

pq.PQescapeLiteral.restype = c_void_p
pq.PQescapeLiteral.argtypes = [Conn, c_char_p, c_size_t]
pq.PQescapeLiteral.errcheck = check_string_and_free

pq.PQescapeIdentifier.restype = c_void_p
pq.PQescapeIdentifier.argtypes = [Conn, c_char_p, c_size_t]
pq.PQescapeIdentifier.errcheck = check_string_and_free
