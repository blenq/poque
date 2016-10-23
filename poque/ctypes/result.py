from ctypes import (c_void_p, c_int, c_char_p, c_uint, string_at)
import ipaddress
import json
from uuid import UUID

from .pq import pq, check_string
from .constants import *
from .cursor import ValueCursor
from .dt import get_date_time_converters
from .numeric import get_numeric_converters
from .lib import Error, _get_property, get_method
from codecs import getdecoder


def _read_float_text(crs):
    return float(crs.advance_text())


def _read_int_text(crs):
    return int(crs.advance_text())


def _read_bool_text(crs):
    return crs.advance_view(1) == b't'


def _read_tid_text(crs):
    tid = crs.advance_text()
    if tid[0] != '(' or tid[-1] != ')':
        raise Error("Invalid value")
    tid1, tid2 = tid[1:-1].split(',')
    return int(tid1), int(tid2)


def read_text(crs):
    return crs.advance_text()


def read_bytes(crs):
    return crs.advance_bytes()


hexdecoder = getdecoder('hex')


def _read_bytea_text(crs):
    if crs.length - crs.idx >= 2:
        prefix = crs.advance_view(2)
        if prefix == b"\\x":
            output, length = hexdecoder(crs.data[crs.idx:])
            crs.idx += length
            return output

    backslash = ord(b'\\')

    def get_bytes(crs):
        biter = iter(crs.data.obj)
        for b in biter:
            if b != backslash:
                # regular byte
                yield b

            b = next(biter)
            if b == backslash:
                # backslash
                yield b

            # octal value
            b2 = next(biter)
            b3 = next(biter)
            yield (b - 48) * 64 + (b2 - 48) * 8 + (b3 - 48)

    ret = bytes(get_bytes(crs))
    crs.idx = crs.length
    return ret


def read_float4_bin(crs):
    return crs.advance_single("!f")


def read_float8_bin(crs):
    return crs.advance_single("!d")


def read_bool_bin(crs):
    return crs.advance_single("!?")


def read_int2_bin(crs):
    return crs.advance_single("!h")


def read_int4_bin(crs):
    return crs.advance_single("!i")


def read_uint4_bin(crs):
    return crs.advance_single("!I")


def read_int8_bin(crs):
    return crs.advance_single("!q")


def _read_uuid_text(crs):
    return UUID(crs.advance_text())


def _read_uuid_bin(crs):
    return UUID(bytes=crs.advance_bytes())


def _read_tid_bin(crs):
    return crs.advance_struct_format("!IH")


def _read_point_bin(crs):
    return crs.advance_struct_format("!2d")


def _read_line_bin(crs):
    return crs.advance_struct_format("!3d")


def _read_lseg_bin(crs):
    x1, y1, x2, y2 = crs.advance_struct_format("!4d")
    return ((x1, y1), (x2, y2))


def _read_polygon_bin(crs):
    npoints = read_int4_bin(crs)
    fmt = "!{0}d".format(npoints * 2)
    coords = crs.advance_struct_format(fmt)
    args = [iter(coords)] * 2
    return list(zip(*args))


def _read_path_bin(crs):
    closed = read_bool_bin(crs)
    path = _read_polygon_bin(crs)
    return {"closed": closed, "path": path}


def _read_circle_bin(crs):
    x, y, r = crs.advance_struct_format("!3d")
    return ((x, y), r)

PGSQL_AF_INET = 2
PGSQL_AF_INET6 = PGSQL_AF_INET + 1


def _read_inet_bin(crs):
    family, mask, is_cidr, size = crs.advance_struct_format("!4B")

    if family == PGSQL_AF_INET:
        correct_size = 4
        fmt = "!4B"
        addr_format = "{0}.{1}.{2}.{3}/{4}"
        if is_cidr:
            cls = ipaddress.IPv4Network
        else:
            cls = ipaddress.IPv4Interface
    elif family == PGSQL_AF_INET6:
        correct_size = 16
        fmt = "!8H"
        addr_format = "{0:x}:{1:x}:{2:x}:{3:x}:{4:x}:{5:x}:{6:x}:{7:x}/{8}"
        if is_cidr:
            cls = ipaddress.IPv6Network
        else:
            cls = ipaddress.IPv6Interface
    else:
        raise Error("Invalid address family")
    if size != correct_size:
        raise Error("Invalid address size")
    parts = crs.advance_struct_format(fmt)
    addr_string = addr_format.format(*(parts + (mask,)))
    return cls(addr_string)


def _read_mac_bin(crs):
    mac1, mac2 = crs.advance_struct_format("!HI")
    return (mac1 << 32) + mac2


def _read_json_bin(crs):
    return json.loads(crs.advance_text())


def _read_jsonb_bin(crs):
    version = crs.advance_single("!B")
    return _read_json_bin(crs)


def _read_bit_text(crs):
    """ Reads a bitstring as a Python integer

    Format:
        * a string composed of '1' and '0' characters

    """
    val = 0
    one = ord(b'1')
    zero = ord(b'0')
    for char in crs.advance_bytes():
        if char not in (zero, one):
            raise Error('Invalid character in bit string')
        val = (val << 1) | (char - zero)
    return val


def _read_bit_bin(crs):
    """ Reads a bitstring as a Python integer

    Format:
        * signed int: number of bits (bit_len)
        * bytes: All the bits left aligned

    """
    # first get the number of bits in the bit string
    bit_len = read_int4_bin(crs)

    # calculate number of data bytes
    quot, rest = divmod(bit_len, 8)
    byte_len = quot + bool(rest)

    # add the value byte by byte, python ints have no upper limit, so this
    # works even for bitstrings longer than 64 bits
    val = 0
    for char in crs.advance_bytes(byte_len):
        val = (val << 8) | char

    if rest:
        # correct for the fact that the bitstring is left aligned
        val >>= (8 - rest)

    return val


def _get_array_value(crs, array_dims, reader):
    if array_dims:
        # get an array of (nested) values
        dim = array_dims[0]
        return [_get_array_value(crs, array_dims[1:], reader)
                for i in range(dim)]
    else:
        # get a single value, either NULL or an actual value prefixed by a
        # length
        item_len = read_int4_bin(crs)
        if item_len == -1:
            return None
        cursor = crs.cursor(item_len)
        val = reader(cursor)
        if cursor.idx != cursor.length:
            # we're not at the end, something must have gone wrong
            raise Error("Invalid data format")
        return val


def get_array_bin_reader(elem_oid):
    def read_array_bin(crs, length=None):

        dims, flags, elem_type = crs.advance_struct_format("!IiI")

        if elem_type != elem_oid:
            raise Error("Unexpected element type")
        if dims > 6:
            raise Error("Number of dimensions exceeded")
        if flags & 1 != flags:
            raise Error("Invalid value for array flags")
        if dims == 0:
            return []
        reader = Result._converters[elem_type][1]
        array_dims = []
        for i in range(dims):
            array_dims.append(read_int4_bin(crs))
            crs.advance(4)
        return _get_array_value(crs, array_dims, reader)
    return read_array_bin


def _get_result_column_method(res_func):
    def result_method(self, column_number):
        return res_func(self, column_number)
    return result_method


def _get_result_row_column_method(res_func):
    def result_method(self, row_number, column_number):
        return res_func(self, row_number, column_number)
    return result_method


class Result(c_void_p):

    status = _get_property(pq.PQresultStatus)
    ntuples = _get_property(pq.PQntuples)
    nfields = _get_property(pq.PQnfields)
    nparams = _get_property(pq.PQnparams)
    error_message = _get_property(pq.PQresultErrorMessage)

    fformat = _get_result_column_method(pq.PQfformat)
    fmod = _get_result_column_method(pq.PQfmod)
    fname = _get_result_column_method(pq.PQfname)
    fsize = _get_result_column_method(pq.PQfsize)
    ftable = _get_result_column_method(pq.PQftable)
    ftablecol = _get_result_column_method(pq.PQftablecol)
    ftype = _get_result_column_method(pq.PQftype)

    def fnumber(self, column_name):
        return pq.PQfnumber(self, column_name.encode())

    getlength = _get_result_row_column_method(pq.PQgetlength)
    getisnull = _get_result_row_column_method(pq.PQgetisnull)
    _pq_getvalue = _get_result_row_column_method(pq.PQgetvalue)

    clear = get_method(pq.PQclear)

    def __del__(self):
        self.clear()

    def pq_getvalue(self, row_number, column_number):
        data = self._pq_getvalue(row_number, column_number)
        length = self.getlength(row_number, column_number)
        data = string_at(data, length)
        return data if self.fformat(column_number) == 1 else data.decode()

    _converters = {
        FLOAT4OID: (_read_float_text, read_float4_bin),
        FLOAT4ARRAYOID: (None, get_array_bin_reader(FLOAT4OID)),
        FLOAT8OID: (_read_float_text, read_float8_bin),
        FLOAT8ARRAYOID: (None, get_array_bin_reader(FLOAT8OID)),
        INT2OID: (_read_int_text, read_int2_bin),
        INT2ARRAYOID: (None, get_array_bin_reader(INT2OID)),
        INT2VECTOROID: (None, get_array_bin_reader(INT2OID)),
        INT2VECTORARRAYOID: (None, get_array_bin_reader(INT2VECTOROID)),
        INT4OID: (_read_int_text, read_int4_bin),
        INT4ARRAYOID: (None, get_array_bin_reader(INT4OID)),
        INT8OID: (_read_int_text, read_int8_bin),
        INT8ARRAYOID: (None, get_array_bin_reader(INT8OID)),
        BOOLOID: (_read_bool_text, read_bool_bin),
        BOOLARRAYOID: (None, get_array_bin_reader(BOOLOID)),
        BPCHAROID: (None, read_text),
        BPCHARARRAYOID: (None, get_array_bin_reader(BPCHAROID)),
        XMLOID: (None, read_text),
        XMLARRAYOID: (None, get_array_bin_reader(XMLOID)),
        XIDOID: (_read_int_text, read_uint4_bin),
        XIDARRAYOID: (None, get_array_bin_reader(XIDOID)),
        VARCHAROID: (None, read_text),
        VARCHARARRAYOID: (None, get_array_bin_reader(VARCHAROID)),
        TEXTOID: (None, read_text),
        TEXTARRAYOID: (None, get_array_bin_reader(TEXTOID)),
        NAMEOID: (None, read_text),
        NAMEARRAYOID: (None, get_array_bin_reader(NAMEOID)),
        CSTRINGOID: (None, read_text),
        CSTRINGARRAYOID: (None, get_array_bin_reader(CSTRINGOID)),
        UNKNOWNOID: (None, read_text),
        CIDOID: (_read_int_text, read_uint4_bin),
        CIDARRAYOID: (None, get_array_bin_reader(CIDOID)),
        OIDOID: (_read_int_text, read_uint4_bin),
        OIDARRAYOID: (None, get_array_bin_reader(OIDOID)),
        OIDVECTOROID: (None, get_array_bin_reader(OIDOID)),
        OIDVECTORARRAYOID: (None, get_array_bin_reader(OIDVECTOROID)),
        ABSTIMEARRAYOID: (None, get_array_bin_reader(ABSTIMEOID)),
        TINTERVALARRAYOID: (None, get_array_bin_reader(TINTERVALOID)),
        RELTIMEARRAYOID: (None, get_array_bin_reader(RELTIMEOID)),
        CHAROID: (read_bytes, read_bytes),
        CHARARRAYOID: (None, get_array_bin_reader(CHAROID)),
        UUIDOID: (_read_uuid_text, _read_uuid_bin),
        UUIDARRAYOID: (None, get_array_bin_reader(UUIDOID)),
        BYTEAOID: (_read_bytea_text, read_bytes),
        BYTEAARRAYOID: (None, get_array_bin_reader(BYTEAOID)),
        TIDOID: (_read_tid_text, _read_tid_bin),
        TIDARRAYOID: (None, get_array_bin_reader(TIDOID)),
        DATEARRAYOID: (None, get_array_bin_reader(DATEOID)),
        TIMEARRAYOID: (None, get_array_bin_reader(TIMEOID)),
        TIMESTAMPARRAYOID: (None, get_array_bin_reader(TIMESTAMPOID)),
        TIMESTAMPTZARRAYOID: (None, get_array_bin_reader(TIMESTAMPTZOID)),
        INTERVALARRAYOID: (None, get_array_bin_reader(INTERVALOID)),
        POINTOID: (None, _read_point_bin),
        POINTARRAYOID: (None, get_array_bin_reader(POINTOID)),
        BOXOID: (None, _read_lseg_bin),
        BOXARRAYOID: (None, get_array_bin_reader(BOXOID)),
        LSEGOID: (None, _read_lseg_bin),
        LSEGARRAYOID: (None, get_array_bin_reader(LSEGOID)),
        POLYGONOID: (None, _read_polygon_bin),
        POLYGONARRAYOID: (None, get_array_bin_reader(POLYGONOID)),
        PATHOID: (None, _read_path_bin),
        PATHARRAYOID: (None, get_array_bin_reader(PATHOID)),
        CIRCLEOID: (None, _read_circle_bin),
        CIRCLEARRAYOID: (None, get_array_bin_reader(CIRCLEOID)),
        CIDROID: (None, _read_inet_bin),
        CIDRARRAYOID: (None, get_array_bin_reader(CIDROID)),
        INETOID: (None, _read_inet_bin),
        INETARRAYOID: (None, get_array_bin_reader(INETOID)),
        MACADDROID: (None, _read_mac_bin),
        MACADDRARRAYOID: (None, get_array_bin_reader(MACADDROID)),
        REGPROCOID: (None, read_uint4_bin),
        REGPROCARRAYOID: (None, get_array_bin_reader(REGPROCOID)),
        CASHOID: (None, read_int8_bin),
        CASHARRAYOID: (None, get_array_bin_reader(CASHOID)),
        JSONOID: (_read_json_bin, _read_json_bin),
        JSONARRAYOID: (None, get_array_bin_reader(JSONOID)),
        JSONBOID: (_read_json_bin, _read_jsonb_bin),
        JSONBARRAYOID: (None, get_array_bin_reader(JSONBOID)),
        LINEOID: (None, _read_line_bin),
        LINEARRAYOID: (None, get_array_bin_reader(LINEOID)),
        BITOID: (_read_bit_text, _read_bit_bin),
        BITARRAYOID: (None, get_array_bin_reader(BITOID)),
        VARBITOID: (_read_bit_text, _read_bit_bin),
        VARBITARRAYOID: (None, get_array_bin_reader(VARBITOID)),
        NUMERICARRAYOID: (None, get_array_bin_reader(NUMERICOID)),
    }
    _converters.update(get_date_time_converters())
    _converters.update(get_numeric_converters())

    def getvalue(self, row_number, column_number):
        # first check for NULL values
        if self.getisnull(row_number, column_number):
            return None

        # get the type oid
        toid = self.ftype(column_number)

        try:
            # try to find the proper converters
            readers = self._converters[toid]
        except KeyError:
            # no problemm, just return the actual contents
            pass
        else:
            # get the proper reader for text or binary
            reader = readers[self.fformat(column_number)]
            if reader:
                # create a cursor from the address and length
                data = self._pq_getvalue(row_number, column_number)
                length = self.getlength(row_number, column_number)
                crs = ValueCursor(data, length)

                # convert the data in the cursor
                value = reader(crs)
                if crs.idx != crs.length:
                    # we're not at the end, something must have gone wrong
                    raise Error("Invalid data format")
                return value

        # return the original content as it is
        return self.pq_getvalue(row_number, column_number)


pq.PQresultStatus.argtypes = [Result]
pq.PQntuples.argtypes = [Result]
pq.PQnfields.argtypes = [Result]
pq.PQnparams.argtypes = [Result]


pq.PQfformat.argtypes = [Result, c_int]
pq.PQfmod.argtypes = [Result, c_int]
pq.PQfsize.argtypes = [Result, c_int]
pq.PQftablecol.argtypes = [Result, c_int]

pq.PQfname.argtypes = [Result, c_int]
pq.PQfname.restype = c_char_p
pq.PQfname.errcheck = check_string

pq.PQfnumber.argtypes = [Result, c_char_p]

pq.PQftype.argtypes = [Result, c_int]
pq.PQftype.restype = c_uint

pq.PQftable.argtypes = [Result, c_int]
pq.PQftable.restype = c_uint

pq.PQgetlength.argtypes = [Result, c_int, c_int]


def check_bool(res, func, args):
    return bool(res)

pq.PQgetisnull.argtypes = [Result, c_int, c_int]
pq.PQgetisnull.errcheck = check_bool

pq.PQgetvalue.argtypes = [Result, c_int, c_int]
pq.PQgetvalue.restype = c_void_p

pq.PQresultErrorMessage.argtypes = [Result]
pq.PQresultErrorMessage.restype = c_char_p
pq.PQresultErrorMessage.errcheck = check_string


def check_clear(res, func, args):
    result = args[0]
    if result:
        result.value = 0

pq.PQclear.argtypes = [Result]
pq.PQclear.restype = None
pq.PQclear.errcheck = check_clear
