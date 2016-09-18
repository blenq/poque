from collections import deque
from ctypes import (
    c_void_p, c_int, c_char_p, c_uint, c_char, string_at)
from datetime import date, datetime, time, MAXYEAR, MINYEAR, timedelta
from decimal import Decimal
import ipaddress
import json
import struct
from uuid import UUID

from .pq import pq, check_string
from .constants import *
from .lib import Error
from codecs import getdecoder

NUMERIC_NAN = 0xC000
NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000


class _crs():
    """ Cursor object to traverse through postgresql data value """

    def __init__(self, data, length):
        # a ctypes fixed length array implements the buffer interface, and
        # is therefore accessible by the struct module
        self.data = (c_char * length).from_address(data)

        self.length = length
        self.idx = 0

    def advance(self, length=None):
        if length is None:
            # just advance until the end
            length = self.length - self.idx
        elif self.idx + length > self.length:
            # check
            raise Error("Item length exceeds data length")

        # actually advance the cursor and return the previous index
        ret = self.idx
        self.idx += length
        return ret

    def advance_struct_format(self, fmt):
        # read values from the cursor according to the provided struct format
        length = struct.calcsize(fmt)
        return struct.unpack_from(fmt, self.data, offset=self.advance(length))

    def advance_struct(self, sct):
        return sct.unpack_from(self.data, offset=self.advance(sct.size))

    def advance_bytes(self, length=None):
        if length is None:
            length = self.length - self.idx
        idx = self.advance(length)
        return self.data[idx:idx + length]

    def advance_text(self, length=None):
        return self.advance_bytes(length).decode()


def _get_unpacker(fmt):
    unp = struct.Struct(fmt)
    struct_length = unp.size

    def unpacker(crs, length=struct_length):
        if length != struct_length:
            raise Error("Invalid value")
        return crs.advance_struct(unp)[0]
    return unpacker


def _read_float_text(crs):
    return float(crs.advance_text())


def _read_int_text(crs):
    return int(crs.advance_text())


def _read_bool_text(crs):
    return crs.advance_bytes() == b't'


def _read_tid_text(crs):
    tid = crs.advance_text()
    if tid[0] != '(' or tid[-1] != ')':
        raise Error("Invalid value")
    tid1, tid2 = tid[1:-1].split(',')
    return int(tid1), int(tid2)


def _read_text(crs, length=None):
    return crs.advance_text(length)


def _read_text_bin(crs, length=None):
    return crs.advance_bytes(length)


def _read_bytea_text(crs):
    if crs.length - crs.idx >= 2:
        prefix = crs.advance_bytes(2)
        if prefix == b"\\x":
            dec = getdecoder('hex')
            output, length = dec(memoryview(crs.data)[crs.idx:])
            crs.idx += length
            return output

    def get_bytes(crs):
        biter = iter(crs.data)
        for b in biter:
            if b != b'\\':
                # regular byte
                yield ord(b)
            else:
                b = next(biter)
                if b == b'\\':
                    # backslash
                    yield ord(b)
                else:
                    # octal value
                    b2 = next(biter)
                    b3 = next(biter)
                    val = int(b + b2 + b3, 8)
                    yield val

    ret = bytes(get_bytes(crs))
    crs.idx = crs.length
    return ret

_read_float4_bin = _get_unpacker("!f")
_read_float8_bin = _get_unpacker("!d")
_read_int2_bin = _get_unpacker("!h")
_read_uint2_bin = _get_unpacker("!H")
_read_int4_bin = _get_unpacker("!i")
_read_uint4_bin = _get_unpacker("!I")
_read_int8_bin = _get_unpacker("!q")
_read_bool_bin = _get_unpacker("!?")


INVALID_ABSTIME = 0x7FFFFFFE


def _read_abstime_bin(crs, length=4):
    if length != 4:
        raise Error("Invalid value")
    seconds = _read_int4_bin(crs)
    return datetime.fromtimestamp(seconds)


def _read_tinterval_bin(crs, length=12):
    if length != 12:
        raise Error("Invalid value")

    status, dt1, dt2 = crs.advance_struct_format("!3i")
    if dt1 == INVALID_ABSTIME or dt2 == INVALID_ABSTIME:
        st = 0
    else:
        st = 1
    if st != status:
        raise Error("Invalid value")
    return (datetime.fromtimestamp(dt1),
            datetime.fromtimestamp(dt2))


def _read_reltime_bin(crs, length=4):
    return timedelta(seconds=_read_int4_bin(crs, length=length))


def _read_uuid_text(crs):
    return UUID(crs.advance_text())


def _read_uuid_bin(crs, length=16):
    if length != 16:
        raise Error("Invalid value")
    return UUID(bytes=crs.advance_bytes(16))


def _read_tid_bin(crs, length=6):
    if length != 6:
        raise Error("Invalid value")
    return crs.advance_struct_format("!IH")

USECS_PER_SEC = 1000000
USECS_PER_MINUTE = 60 * USECS_PER_SEC
USECS_PER_HOUR = 60 * USECS_PER_MINUTE
USECS_PER_DAY = 24 * USECS_PER_HOUR

POSTGRES_EPOCH_JDATE = 2451545


def _date_vals_from_int(jd):

    # julian day magic to retrieve day, month and year, shamelessly copied
    # from postgres server code
    julian = jd + POSTGRES_EPOCH_JDATE + 32044
    quad, extra = divmod(julian, 146097)
    extra = extra * 4 + 3
    julian += 60 + quad * 3 + extra // 146097
    quad, julian = divmod(julian, 1461)
    y = julian * 4 // 1461
    julian = ((julian + 305) % 365 if y else (julian + 306) % 366) + 123
    y += quad * 4
    year = y - 4800
    quad = julian * 2141 // 65536
    day = julian - 7834 * quad // 256
    month = (quad + 10) % 12 + 1
    return year, month, day


def _time_vals_from_int(tm):
    hour, tm = divmod(tm, USECS_PER_HOUR)
    if tm < 0 or hour > 23:
        raise Error("Invalid time value")
    minute, tm = divmod(tm, USECS_PER_MINUTE)
    second, usec = divmod(tm, USECS_PER_SEC)
    return hour, minute, second, usec


def _read_date_bin(crs, length=4):
    if length != 4:
        raise Error("Invalid value")
    jd = _read_int4_bin(crs)

    year, month, day = _date_vals_from_int(jd)

    # if outside python date range convert to a string
    if year > MAXYEAR:
        fmt = "{0}-{1:02}-{2:02}"
    elif year < MINYEAR:
        fmt = "{0:04}-{1:02}-{2:02} BC"
        year = -1 * (year - 1)
    else:
        return date(year, month, day)
    return fmt.format(year, month, day)


def _read_time_bin(crs, length=8):
    if length != 8:
        raise Error("Invalid value")
    return time(*_time_vals_from_int(_read_int8_bin(crs)))


def _read_timestamp_bin(crs, length=8):
    if length != 8:
        raise Error("Invalid value")
    value = _read_int8_bin(crs)
    dt, tm = divmod(value, USECS_PER_DAY)
    if tm < 0:
        tm += USECS_PER_DAY
        dt -= 1

    year, month, day = _date_vals_from_int(dt)
    time_vals = _time_vals_from_int(tm)
    if year > MAXYEAR:
        fmt = "{0}-{1:02}-{2:02} {3:02}:{4:02}:{5:02}.{6:06}"
    elif year < MINYEAR:
        year = -1 * (year - 1)  # There is no year zero
        fmt = "{0:04}-{1:02}-{2:02} {3:02}:{4:02}:{5:02}.{6:06} BC"
    else:
        return datetime(year, month, day, *time_vals)
    return fmt.format(year, month, day, *time_vals)


def _read_interval_bin(crs, length=16):
    if length != 16:
        raise ValueError("Invalid value")
    usecs, days, months = crs.advance_struct_format("!qii")
    value = timedelta(days, *divmod(usecs, USECS_PER_SEC))
    return months, value


def _read_point_bin(crs, length=16):
    if length != 16:
        raise ValueError("Invalid value")
    return crs.advance_struct_format("!dd")


def _read_line_bin(crs, length=24):
    if length != 24:
        raise ValueError("Invalid value")
    return crs.advance_struct_format("!ddd")


def _read_lseg_bin(crs, length=32):
    if length != 32:
        raise ValueError("Invalid value")
    x1, y1, x2, y2 = crs.advance_struct_format("!4d")
    return ((x1, y1), (x2, y2))


def _read_polygon_bin(crs, length=None):
    npoints = _read_int4_bin(crs)
    fmt = "!{0}d".format(npoints * 2)
    coords = crs.advance_struct_format(fmt)
    args = [iter(coords)] * 2
    return list(zip(*args))


def _read_path_bin(crs, length=None):
    closed = _read_bool_bin(crs)
    path = _read_polygon_bin(crs)
    return {"closed": closed, "path": path}


def _read_circle_bin(crs, length=24):
    if length != 24:
        raise ValueError("Invalid value")
    x, y, r = crs.advance_struct_format("!3d")
    return ((x, y), r)

PGSQL_AF_INET = 2
PGSQL_AF_INET6 = PGSQL_AF_INET + 1


def _read_inet_bin(crs, length=None):
    uchar_4 = struct.Struct("!4B")
    family, mask, is_cidr, size = crs.advance_struct(uchar_4)

    if is_cidr > 1:
        raise Error("Invalid value for is_cidr")

    if family == PGSQL_AF_INET:
        if size != 4:
            raise Error("Invalid address size")
        parts = crs.advance_struct(uchar_4)
        addr_string = "{0}.{1}.{2}.{3}/{4}".format(*(parts + (mask,)))
        if is_cidr:
            cls = ipaddress.IPv4Network
        else:
            cls = ipaddress.IPv4Interface
    elif family == PGSQL_AF_INET6:
        if size != 16:
            raise Error("Invalid address size")
        parts = crs.advance_struct_format("!8H")
        addr_string = (
            "{0:x}:{1:x}:{2:x}:{3:x}:{4:x}:{5:x}:{6:x}:{7:x}/{8}".format(
                *(parts + (mask,))))
        if is_cidr:
            cls = ipaddress.IPv6Network
        else:
            cls = ipaddress.IPv6Interface
    else:
        raise Error("Unknown network family")
    return cls(addr_string)


def _read_mac_bin(crs, length=6):
    if length != 6:
        raise Error("Invalid value")
    mac1, mac2 = crs.advance_struct_format("!HI")
    return (mac1 << 32) + mac2


def _read_json_bin(crs, length=None):
    value = _read_text(crs, length)
    return json.loads(value)


def _read_jsonb_bin(crs, length=None):
    version = crs.advance_struct_format("!B")[0]
    if version != 1:
        raise Error("Invalid version")
    return _read_json_bin(crs, length - 1 if length else None)


def _read_numeric_str(crs, length=None):
    value = _read_text(crs, length)
    return Decimal(value)


def _read_numeric_bin(crs, length=None):
    """ Reads a binary numeric/decimal value """

    # Read field values: number of digits, weight, sign, display scale.
    npg_digits, weight, sign, dscale = crs.advance_struct_format("!HhHH")
    if npg_digits:
        pg_digits = crs.advance_struct_format("!" + 'H' * npg_digits)
    else:
        pg_digits = []
    if sign == NUMERIC_NAN:
        return Decimal('NaN')
    if sign == NUMERIC_NEG:
        sign = 1
    elif sign != NUMERIC_POS:
        raise Exception('Bad value')

    # number of digits
    ndigits = dscale + (weight + 1) * 4

    # fill digits
    j = 0
    digits = []
    for dg in pg_digits:
        if dg > 9999:
            raise Error("Invalid value")
        # a postgres digit contains 4 decimal digits
        for val in [dg // 1000, (dg // 100) % 10, (dg // 10) % 10, dg % 10]:
            if j == ndigits:
                # the pg value can have more zeroes than the display scale
                # indicates
                break
            digits.append(val)
            j += 1

    # add extra zeroes indicated by display scale that are not in the actual
    # value
    digits.extend([0] * (ndigits - j))

    # now create the decimal
    return Decimal((sign, digits, -dscale))


def _read_bit_bin(crs, length=None):
    """ Reads a bitstring as a Python integer

    Format:
        * signed int: number of bits (bit_len)
        * bytes: All the bits left aligned

    """
    # first get the number of bits in the bit string
    bit_len = _read_int4_bin(crs)

    # calculate number of data bytes
    quot, rest = divmod(bit_len, 8)
    byte_len = quot + (1 if rest else 0)

    # add the value byte by byte, python ints have no upper limit, so this
    # works even for bitstrings longer than 64 bits
    val = 0
    loop = iter(range(byte_len))
    while next(loop, None) is not None:
        val <<= 8
        val |= crs.advance_struct_format("!B")[0]

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
        item_len = _read_int4_bin(crs)
        if item_len == -1:
            return None
        return reader(crs, item_len)


def _read_array_bin(crs, length=None):

    dims, flags, elem_type = crs.advance_struct_format("!IiI")

    if dims > 6:
        raise Error("Number of dimensions exceeded")
    if flags & 1 != flags:
        raise Error("Invalid value for array flags")
    if dims == 0:
        return []
    reader = Result._converters[elem_type][1]
    array_dims = []
    for i in range(dims):
        array_dims.append(_read_int4_bin(crs))
        crs.advance(4)
    return _get_array_value(crs, array_dims, reader)


class Result(c_void_p):

    @property
    def status(self):
        return pq.PQresultStatus(self)

    @property
    def ntuples(self):
        return pq.PQntuples(self)

    @property
    def nfields(self):
        return pq.PQnfields(self)

    @property
    def nparams(self):
        return pq.PQnparams(self)

    @property
    def error_message(self):
        return pq.PQresultErrorMessage(self)

    _fformat = pq.PQfformat

    def fformat(self, column_number):
        return self._fformat(self, column_number)

    def fmod(self, column_number):
        return pq.PQfmod(self, column_number)

    def fname(self, column_number):
        return pq.PQfname(self, column_number)

    def fsize(self, column_number):
        return pq.PQfsize(self, column_number)

    def ftable(self, column_number):
        return pq.PQftable(self, column_number)

    def ftablecol(self, column_number):
        return pq.PQftablecol(self, column_number)

    _ftype = pq.PQftype

    def ftype(self, column_number):
        return self._ftype(self, column_number)

    def fnumber(self, column_name):
        return pq.PQfnumber(self, column_name.encode())

    _getlength = pq.PQgetlength

    def getlength(self, row_number, column_number):
        return self._getlength(self, row_number, column_number)

    _getisnull = pq.PQgetisnull

    def getisnull(self, row_number, column_number):
        return bool(self._getisnull(self, row_number, column_number))

    def clear(self):
        self._clear(self)
        self.value = 0

    def __del__(self):
        self.clear()

    _clear = pq.PQclear

    def pq_getvalue(self, row_number, column_number):
        data = pq.PQgetvalue(self, row_number, column_number)
        length = self._getlength(self, row_number, column_number)
        data = string_at(data, length)
        if self._fformat(self, column_number) == 1:
            return data
        return data.decode()

    _converters = {
        FLOAT4OID: (_read_float_text, _read_float4_bin),
        FLOAT4ARRAYOID: (None, _read_array_bin),
        FLOAT8OID: (_read_float_text, _read_float8_bin),
        FLOAT8ARRAYOID: (None, _read_array_bin),
        INT2OID: (_read_int_text, _read_int2_bin),
        INT2ARRAYOID: (None, _read_array_bin),
        INT2VECTOROID: (None, _read_array_bin),
        INT2VECTORARRAYOID: (None, _read_array_bin),
        INT4OID: (_read_int_text, _read_int4_bin),
        INT4ARRAYOID: (None, _read_array_bin),
        INT8OID: (_read_int_text, _read_int8_bin),
        INT8ARRAYOID: (None, _read_array_bin),
        BOOLOID: (_read_bool_text, _read_bool_bin),
        BOOLARRAYOID: (None, _read_array_bin),
        BPCHAROID: (None, _read_text),
        BPCHARARRAYOID: (None, _read_array_bin),
        XMLOID: (None, _read_text),
        XMLARRAYOID: (None, _read_array_bin),
        XIDOID: (_read_int_text, _read_uint4_bin),
        XIDARRAYOID: (None, _read_array_bin),
        VARCHAROID: (None, _read_text),
        VARCHARARRAYOID: (None, _read_array_bin),
        TEXTOID: (None, _read_text),
        TEXTARRAYOID: (None, _read_array_bin),
        NAMEOID: (None, _read_text),
        NAMEARRAYOID: (None, _read_array_bin),
        CSTRINGOID: (None, _read_text),
        CSTRINGARRAYOID: (None, _read_array_bin),
        UNKNOWNOID: (None, _read_text),
        CIDOID: (_read_int_text, _read_uint4_bin),
        CIDARRAYOID: (None, _read_array_bin),
        OIDOID: (_read_int_text, _read_uint4_bin),
        OIDARRAYOID: (None, _read_array_bin),
        OIDVECTOROID: (None, _read_array_bin),
        OIDVECTORARRAYOID: (None, _read_array_bin),
        ABSTIMEOID: (None, _read_abstime_bin),
        ABSTIMEARRAYOID: (None, _read_array_bin),
        TINTERVALOID: (None, _read_tinterval_bin),
        TINTERVALARRAYOID: (None, _read_array_bin),
        RELTIMEOID: (None, _read_reltime_bin),
        RELTIMEARRAYOID: (None, _read_array_bin),
        CHAROID: (_read_text_bin, _read_text_bin),
        CHARARRAYOID: (None, _read_array_bin),
        UUIDOID: (_read_uuid_text, _read_uuid_bin),
        BYTEAOID: (_read_bytea_text, _read_text_bin),
        BYTEAARRAYOID: (None, _read_array_bin),
        TIDOID: (_read_tid_text, _read_tid_bin),
        TIDARRAYOID: (None, _read_array_bin),
        DATEOID: (None, _read_date_bin),
        DATEARRAYOID: (None, _read_array_bin),
        TIMEOID: (None, _read_time_bin),
        TIMEARRAYOID: (None, _read_array_bin),
        TIMESTAMPOID: (None, _read_timestamp_bin),
        TIMESTAMPARRAYOID: (None, _read_array_bin),
        TIMESTAMPTZOID: (None, _read_timestamp_bin),
        TIMESTAMPTZARRAYOID: (None, _read_array_bin),
        INTERVALOID: (None, _read_interval_bin),
        INTERVALARRAYOID: (None, _read_array_bin),
        POINTOID: (None, _read_point_bin),
        POINTARRAYOID: (None, _read_array_bin),
        BOXOID: (None, _read_lseg_bin),
        BOXARRAYOID: (None, _read_array_bin),
        LSEGOID: (None, _read_lseg_bin),
        LSEGARRAYOID: (None, _read_array_bin),
        POLYGONOID: (None, _read_polygon_bin),
        POLYGONARRAYOID: (None, _read_array_bin),
        PATHOID: (None, _read_path_bin),
        PATHARRAYOID: (None, _read_array_bin),
        CIRCLEOID: (None, _read_circle_bin),
        CIRCLEARRAYOID: (None, _read_array_bin),
        CIDROID: (None, _read_inet_bin),
        CIDRARRAYOID: (None, _read_array_bin),
        INETOID: (None, _read_inet_bin),
        INETARRAYOID: (None, _read_array_bin),
        MACADDROID: (None, _read_mac_bin),
        MACADDRARRAYOID: (None, _read_array_bin),
        REGPROCOID: (None, _read_uint4_bin),
        REGPROCARRAYOID: (None, _read_array_bin),
        CASHOID: (None, _read_int8_bin),
        CASHARRAYOID: (None, _read_array_bin),
        JSONOID: (_read_json_bin, _read_json_bin),
        JSONARRAYOID: (None, _read_array_bin),
        JSONBOID: (_read_json_bin, _read_jsonb_bin),
        JSONBARRAYOID: (None, _read_array_bin),
        NUMERICOID: (_read_numeric_str, _read_numeric_bin),
        NUMERICARRAYOID: (None, _read_array_bin),
        LINEOID: (None, _read_line_bin),
        LINEARRAYOID: (None, _read_array_bin),
        BITOID: (None, _read_bit_bin),
    }

    _getvalue = pq.PQgetvalue

    def getvalue(self, row_number, column_number):
        # first check for NULL values
        if self._getisnull(self, row_number, column_number):
            return None

        # get the type oid
        toid = self._ftype(self, column_number)

        try:
            # try to find the proper converters
            readers = self._converters[toid]
        except KeyError:
            # no problemm, just return the actual contents
            pass
        else:
            # get the proper reader for text or binary
            reader = readers[self._fformat(self, column_number)]
            if reader:
                # create a cursor from the address and length
                data = self._getvalue(self, row_number, column_number)
                length = self._getlength(self, row_number, column_number)
                crs = _crs(data, length)

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

pq.PQgetisnull.argtypes = [Result, c_int, c_int]

pq.PQgetvalue.argtypes = [Result, c_int, c_int]
pq.PQgetvalue.restype = c_void_p

pq.PQresultErrorMessage.argtypes = [Result]
pq.PQresultErrorMessage.restype = c_char_p
pq.PQresultErrorMessage.errcheck = check_string

pq.PQclear.argtypes = [Result]
pq.PQclear.restype = None
