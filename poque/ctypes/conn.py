from collections import deque
from ctypes import (c_void_p, c_char_p, POINTER, c_int, c_uint, c_size_t,
                    c_char, cast)
from functools import reduce
import operator
from uuid import UUID

from .pq import pq, check_string, PQconninfoOptions, check_info_options
from .constants import (
    CONNECTION_BAD, BAD_RESPONSE, FATAL_ERROR, TEXTOID, INT4OID, INT8OID,
    BOOLOID, BYTEAOID, UUIDOID, FORMAT_BINARY, INT4ARRAYOID, INT8ARRAYOID,
    TEXTARRAYOID, FORMAT_TEXT)
from .common import get_struct
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


class IntArrayParameterHandler(object):

    def __init__(self):
        self.values = []
        self.oid = INT4OID
        self.encode_value = self.encode_int4_value
        self.get_length = self.get_int4_length

    def check_value(self, val):
        self.values.append(val)

        if self.oid == INT4OID:
            if -0x80000000 <= val <= 0x7FFFFFFF:
                return
            self.oid = INT8OID
            self.encode_value = self.encode_int8_value
            self.get_length = self.get_int8_length

        if self.oid == INT8OID:
            if -0x8000000000000000 <= val <= 0x7FFFFFFFFFFFFFFF:
                return
            self.oid = TEXTOID
            self.encode_value = self.encode_text_value
            self.get_length = self.get_text_length

    def get_int4_length(self):
        return len(self.values) * 4

    def encode_int4_value(self, param, val):
        return "i", val

    def get_int8_length(self):
        return len(self.values) * 8

    def encode_int8_value(self, param, val):
        return "q", val

    def get_text_length(self):
        self.values = deque(str(v).encode() for v in self.values)
        return sum(len(v) for v in self.values)

    def encode_text_value(self, param, val):
        val = self.values.popleft()
        return "{0}s".format(len(val)), val

    def get_array_oid(self):
        if self.oid == INT4OID:
            return INT4ARRAYOID
        if self.oid == INT8OID:
            return INT8ARRAYOID
        return TEXTARRAYOID


class ArrayParameter(object):

    def __init__(self, val):
        self.val = val
        self.type = None
        self.dims = []
        self.max_depth = 0
        self.has_null = False
        self.converter = None

    _array_handlers = {
        int: IntArrayParameterHandler,
    }

    def walk_list(self, val, depth=0):
        # A nested list is not the same as a multidimensional array. Therefore
        # some checks to make sure all lists of a certain dimension have the
        # correct length

        # get length of current dimension
        val_len = len(val)
        try:
            dim_length = self.dims[depth]
        except IndexError:
            dim_length = val_len
            self.dims.append(dim_length)

        # compare dimension length with current list length
        if val_len != dim_length:
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
                if len(self.dims) == 6:
                    raise ValueError("Too deep nested")

                # repeat ourselves for the child list
                self.walk_list(item, depth)
            else:
                # non list item

                if self.max_depth == 0:
                    # set the depth at which non list values are found
                    self.max_depth = depth

                # all non list values must be found at the same depth
                if depth != self.max_depth:
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
                    self.converter = self._array_handlers[item_type]()

                # all non-list items must be of the same type
                if item_type != self.type:
                    raise ValueError("Can not mix types")

                # give the converter the opportunity to examine tha value
                self.converter.check_value(item)

    def _write(self, stc, *args):
        stc.pack_into(self.buf, self.pos, *args)
        self.pos += stc.size

    def write(self, fmt, *args):
        stc = get_struct(fmt)
        self._write(stc, *args)

    def write_values(self, val):
        # walk the list again to write the values
        for item in val:
            if type(item) == list:
                self.write_values(item)
            elif item is None:
                # NULLs are represented by a length of -1
                self.write("!i", -1)
            else:
                values = self.converter.encode_value(self, item)
                stc = get_struct("!" + values[0])
                self.write("!i", stc.size)
                self._write(stc, *values[1:])

    def get_value(self):
        # walk the list to set up structure and converter
        self.walk_list(self.val)

        dim_len = len(self.dims)

        # calculate length: 12 bytes header, 8 bytes per dimension and
        # 4 bytes (length) per value
        length = 12 + dim_len * 8 + reduce(operator.mul, self.dims, 1) * 4
        if self.converter:
            length += self.converter.get_length()  # add length of values
            elem_type = self.converter.oid
            array_oid = self.converter.get_array_oid()
        else:
            elem_type = TEXTOID
            array_oid = TEXTARRAYOID
        self.buf = (c_char * length)()

        # initialize writer
        self.pos = 0

        # write header
        self.write("!IiI", dim_len, self.has_null, elem_type)

        # Write for each dimension the length and the lower bound (1). Python
        # does not have a lower bound for lists other than zero, so we choose
        # the default lower bound for PostgreSQL, which is one.
        for dim in self.dims:
            self.write("!ii", dim, 1)

        # actually write the values
        self.write_values(self.val)

        return array_oid, cast(self.buf, c_char_p), length, FORMAT_BINARY


def _get_array_param(val):
    param = ArrayParameter(val)
    return param.get_value()


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
        list: _get_array_param,
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
