from ctypes import (c_void_p, c_char_p, POINTER, c_int, c_uint, c_size_t,
                    c_char, cast, CFUNCTYPE)
from datetime import date, time, datetime
from decimal import Decimal
from functools import reduce
from ipaddress import IPv4Interface, IPv6Interface, IPv4Network, IPv6Network
import operator
from uuid import UUID

from .pq import pq, check_string, PQconninfoOptions, check_info_options
from .constants import *  # noqa
from .common import get_struct
from .dt import (
    DateParameterHandler, TimeParameterHandler, DateTimeParameterHandler)
from .numeric import (
    IntParameterHandler, FloatParameterHandler, DecimalParameterHandler,
    BoolParameterHandler)
from .lib import Error, get_property, get_method, InterfaceError
from .result import Result
from .text import TextParameterHandler, BytesParameterHandler
from .various import (
    UuidParameterHandler, InterfaceParameterHandler, NetworkParameterHandler)


def new_connstring(connstring, blocking=True):
    return [connstring], blocking


class ArrayParameterHandler(object):

    has_null = False
    type = None
    max_depth = 0
    converter = None

    def walk_list(self, val, dims, depth=0):
        # A nested list is not the same as a multidimensional array. Therefore
        # some checks to make sure all lists of a certain dimension have the
        # correct length

        # get length of current dimension
        try:
            dim_length = dims[depth]
        except IndexError:
            dims.append(len(val))
        else:
            # compare dimension length with current list length
            if len(val) != dim_length:
                raise ValueError("Invalid list length")

        depth += 1
        for item in val:
            if isinstance(item, list):
                # a child list

                if self.max_depth and depth == self.max_depth:
                    # we already found non list values at this depth. a list
                    # must not appear here
                    raise ValueError("Invalid nesting")

                # postgres supports up to 6 dimensions
                if len(dims) == 6:
                    raise ValueError("Too deep nested")

                # repeat ourselves for the child list
                self.walk_list(item, dims, depth)
            else:
                # non list item

                if self.max_depth == 0:
                    # set the depth at which non list values are found
                    self.max_depth = depth
                elif depth != self.max_depth:
                    # all non list values must be found at the same depth
                    raise ValueError("Invalid nesting")

                # check for None (NULL)
                if item is None:
                    self.has_null = True
                    continue

                # get the type
                item_type = type(item)
                if self.type is None:
                    # set the type and get the converter
                    self.type = item_type
                    self.converter = Conn._param_handlers.get(
                        item_type, TextParameterHandler)()
                elif (item_type != self.type and
                      not self.converter.type_allowed(item_type)):
                    # all non-list items must be of the same type
                    raise ValueError("Can not mix types")

                # give the converter the opportunity to examine the value
                self.converter.examine(item)

    def write(self, fmt, *args):
        self.fmt.append(fmt)
        self.vals.extend(args)

    def write_values(self, val):
        # walk the list again to write the values
        for item in val:
            if type(item) == list:
                self.write_values(item)
            elif item is None:
                # NULLs are represented by a length of -1
                self.write("i", -1)
            else:
                fmt, values = self.converter.encode_value(item)
                stc = get_struct(fmt)
                self.write("i", stc.size)
                self.write(fmt, *values)

    def encode(self, val):
        self.fmt = []
        self.vals = []

        # walk the list to set up structure and converter
        dims = []
        self.walk_list(val, dims)
        dim_len = len(dims)

        # calculate length: 12 bytes header, 8 bytes per dimension and
        # 4 bytes (length) per value
        length = 12 + dim_len * 8 + reduce(operator.mul, dims, 1) * 4
        if self.converter:
            cv = self.converter
            length += cv.get_size()  # add length of values
            elem_oid = cv.oid
            self.oid = cv.array_oid
        else:
            elem_oid = TEXTOID
            self.oid = TEXTARRAYOID

        self.write("IiI", dim_len, self.has_null, elem_oid)
        for dim in dims:
            self.write("ii", dim, 1)
        # actually write the values
        self.write_values(val)
        return ''.join(self.fmt), self.vals


PQnoticeReceiver = CFUNCTYPE(None, c_void_p, Result)


class Conn(c_void_p):

    def __new__(cls, *args, **kwargs):
        # Use __new__ to instantiate object. This way we can inherit from
        # c_void_p and actually be the connection pointer instead of
        # wrapping it and creating another level of indirection.
        # The c_void_p class is alreay a wrapper round a C pointer
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

    # Necessary to prevent the base __init__ to be called which errors out.
    # The pointer value is already correctly set by the __new__ method, so it
    # does not need to be called anyway
    def __init__(self, *args, **kwargs):
        pass

    connect_poll = get_method(pq.PQconnectPoll)

    backend_pid = get_property(pq.PQbackendPID)
    transaction_status = get_property(pq.PQtransactionStatus)
    protocol_version = get_property(pq.PQprotocolVersion)
    server_version = get_property(pq.PQserverVersion)
    status = get_property(pq.PQstatus)
    db = get_property(pq.PQdb)
    error_message = get_property(pq.PQerrorMessage)
    user = get_property(pq.PQuser)
    password = get_property(pq.PQpass)
    port = get_property(pq.PQport)
    host = get_property(pq.PQhost)
    options = get_property(pq.PQoptions)
    client_encoding = get_property(pq.PQclientEncoding)

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

    _warning_msg = None

    def _notice_proc(self, p, result):
        try:
            sql_state = result.error_field(SQLSTATE)
            if not sql_state or sql_state.startswith("01"):
                self._warning_msg = result.error_message
        finally:
            # Result will be cleared by libpq. Set pointer to zero to prevent
            # a preliminary PQClear by the Result destructor
            result.value = 0

    def finish(self):
        self._finish()
        self.value = 0

    def _raise_error(self):
        if self:
            raise InterfaceError(self.error_message)
        raise InterfaceError("Connection is closed")

    def __del__(self):
        self.finish()

    _param_handlers = {
        int: IntParameterHandler,
        float: FloatParameterHandler,
        UUID: UuidParameterHandler,
        bool: BoolParameterHandler,
        bytes: BytesParameterHandler,
        date: DateParameterHandler,
        time: TimeParameterHandler,
        datetime: DateTimeParameterHandler,
        Decimal: DecimalParameterHandler,
        list: ArrayParameterHandler,
        IPv4Interface: InterfaceParameterHandler,
        IPv6Interface: InterfaceParameterHandler,
        IPv4Network: NetworkParameterHandler,
        IPv6Network: NetworkParameterHandler,
    }

    def execute(self, command, parameters=None, result_format=FORMAT_BINARY):
        command = command.encode()
        if not parameters:
            if result_format == FORMAT_TEXT:
                res = pq.PQexec(self, command)
            else:
                res = pq.PQexecParams(self, command, 0, None, None, None,
                                      None, result_format)
            res.conn = self
            return res

        num_params = len(parameters)
        oids = (c_uint * num_params)()
        values = (c_char_p * num_params)()
        lengths = (c_int * num_params)()
        formats = (c_int * num_params)()
        for i, param in enumerate(parameters):
            formats[i] = FORMAT_BINARY
            if param is None:
                # Bind None as SQL NULL
                oids[i], values[i], lengths[i] = (
                    TEXTOID, 0, 0)
                continue

            handler = self._param_handlers.get(type(param),
                                               TextParameterHandler)()
            fmt, param_vals = handler.encode(param)
            stc = get_struct(fmt)
            length = stc.size
            value = (c_char * length)()
            stc.pack_into(value, 0, *param_vals)
            values[i] = cast(value, c_char_p)
            oids[i] = handler.oid
            lengths[i] = length

        res = pq.PQexecParams(self, command, num_params, oids, values,
                               lengths, formats, result_format)
        res.conn = self
        return res


def check_connect(conn, func, args):
    if conn is None:
        raise MemoryError()
    if conn.status == CONNECTION_BAD:
        conn._raise_error()
    conn._notice_proc = PQnoticeReceiver(conn._notice_proc)
    pq.PQsetNoticeReceiver(conn, conn._notice_proc, conn)
    return conn


pq.PQconnectdbParams.restype = Conn
pq.PQconnectdbParams.argtypes = [
    POINTER(c_char_p), POINTER(c_char_p), c_int]
pq.PQconnectdbParams.errcheck = check_connect

pq.PQconnectStartParams.restype = Conn
pq.PQconnectStartParams.argtypes = [
    POINTER(c_char_p), POINTER(c_char_p), c_int]
pq.PQconnectStartParams.errcheck = check_connect

pq.PQsetNoticeReceiver.restype = PQnoticeReceiver
pq.PQsetNoticeReceiver.argtypes = [Conn, PQnoticeReceiver, c_void_p]


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

pq.PQclientEncoding.restype = int
pq.PQclientEncoding.argtypes = [Conn]


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
    conn = args[0]
    if not res:
        # no result returned. Use connection to raise Exception
        conn._raise_error()
    res._views = []
    res._conn = conn
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
