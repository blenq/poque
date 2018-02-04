from collections import deque
from decimal import Decimal
from itertools import repeat, islice
from struct import calcsize

from .common import (BaseParameterHandler, get_array_bin_reader,
                     get_single_reader)
from .constants import (
    INT4OID, INT4ARRAYOID, INT8OID, INT8ARRAYOID, TEXTOID, TEXTARRAYOID,
    FLOAT8OID, FLOAT8ARRAYOID, BOOLOID, BOOLARRAYOID, NUMERICOID,
    NUMERICARRAYOID, FLOAT4OID, FLOAT4ARRAYOID, INT2OID, INT2ARRAYOID,
    INT2VECTOROID, INT2VECTORARRAYOID, XIDOID, XIDARRAYOID, OIDOID,
    OIDARRAYOID, CIDOID, CIDARRAYOID, OIDVECTOROID, OIDVECTORARRAYOID,
    REGPROCOID, REGPROCARRAYOID, CASHOID, CASHARRAYOID)
from .lib import Error


def read_bool_text(crs):
    return crs.advance_view(1) == b't'


class BoolParameterHandler(BaseParameterHandler):

    oid = BOOLOID
    array_oid = BOOLARRAYOID
    fmt = "?"


def read_int_text(crs):
    return int(crs.advance_text())


class IntParameterHandler(BaseParameterHandler):

    oid = INT4OID
    array_oid = INT4ARRAYOID
    fmt = "i"
    int8fmt = "q"
    int8size = calcsize(int8fmt)

    def __init__(self):
        self.values = []

    def examine(self, val):
        self.values.append(val)
        self.examine_item(val)

    def examine_int4(self, val):
        if -0x80000000 <= val <= 0x7FFFFFFF:
            return
        self.oid = INT8OID
        self.array_oid = INT8ARRAYOID
        self.item_size = self.int8size
        self.fmt = self.int8fmt
        self.examine_item = self.examine_int8
        self.examine_item(val)

    examine_item = examine_int4

    def examine_int8(self, val):
        if -0x8000000000000000 <= val <= 0x7FFFFFFFFFFFFFFF:
            return
        self.oid = TEXTOID
        self.array_oid = TEXTARRAYOID
        self.encode_value = self.encode_text_value
        self.get_size = self.get_text_size
        self.examine_item = self.examine_text

    def examine_text(self, val):
        pass

    def get_size(self):
        return len(self.values) * self.item_size

    def get_text_size(self):
        self.values = deque(str(v).encode() for v in self.values)
        return sum(len(v) for v in self.values)

    def encode_text_value(self, val):
        val = self.values.popleft()
        return "{0}s".format(len(val)), val


def _read_float_text(crs):
    return float(crs.advance_text())


class FloatParameterHandler(BaseParameterHandler):

    oid = FLOAT8OID
    array_oid = FLOAT8ARRAYOID
    fmt = "d"


NUMERIC_NAN = 0xC000
NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000


def _read_numeric_str(crs):
    value = crs.advance_text()
    return Decimal(value)


def _read_numeric_bin(crs):
    """ Reads a binary numeric/decimal value """

    # Read field values: number of pg digits, weight, sign, display scale.
    npg_digits, weight, sign, dscale = crs.advance_struct_format("HhHH")

    if sign == NUMERIC_NAN:
        sign = 0
        exp = 'n'
        digits = []
    else:
        if sign == NUMERIC_NEG:
            sign = 1
        elif sign != NUMERIC_POS:
            raise Exception('Bad value')
        exp = -dscale

        ndigits = dscale + (weight + 1) * 4

        def get_digits():
            for _ in range(npg_digits):
                dg = crs.advance_single('H')
                if dg > 9999:
                    raise Error("Invalid value")
                # a postgres digit contains 4 decimal digits
                q, r = divmod(dg, 1000)
                yield q
                q, r = divmod(r, 100)
                yield q
                q, r = divmod(r, 10)
                yield q
                yield r
            # yield zeroes until caller is done
            yield from repeat(0)

        digits = [dg for dg in islice(get_digits(), ndigits)]
    return Decimal((sign, digits, exp))


def get_numeric_converters():
    return {
        BOOLOID: (read_bool_text, get_single_reader("?")),
        BOOLARRAYOID: (
            None, get_array_bin_reader(BOOLOID)),
        NUMERICOID: (_read_numeric_str, _read_numeric_bin),
        NUMERICARRAYOID: (
            None, get_array_bin_reader(NUMERICOID)),
        FLOAT4OID: (_read_float_text, get_single_reader("f")),
        FLOAT4ARRAYOID: (
            None, get_array_bin_reader(FLOAT4OID)),
        FLOAT8OID: (_read_float_text, get_single_reader("d")),
        FLOAT8ARRAYOID: (
            None, get_array_bin_reader(FLOAT8OID)),
        INT2OID: (read_int_text, get_single_reader("h")),
        INT2ARRAYOID: (
            None, get_array_bin_reader(INT2OID)),
        INT2VECTOROID: (
            None, get_array_bin_reader(INT2OID)),
        INT2VECTORARRAYOID: (
            None, get_array_bin_reader(INT2VECTOROID)),
        INT4OID: (read_int_text, get_single_reader("i")),
        INT4ARRAYOID: (
            None, get_array_bin_reader(INT4OID)),
        INT8OID: (read_int_text, get_single_reader("q")),
        INT8ARRAYOID: (
            None, get_array_bin_reader(INT8OID)),
        XIDOID: (read_int_text, get_single_reader("I")),
        XIDARRAYOID: (None, get_array_bin_reader(XIDOID)),
        CIDOID: (read_int_text, get_single_reader("I")),
        CIDARRAYOID: (None, get_array_bin_reader(CIDOID)),
        OIDOID: (read_int_text, get_single_reader("I")),
        OIDARRAYOID: (None, get_array_bin_reader(OIDOID)),
        OIDVECTOROID: (None, get_array_bin_reader(OIDOID)),
        OIDVECTORARRAYOID: (
            None, get_array_bin_reader(OIDVECTOROID)),
        REGPROCOID: (None, get_single_reader("I")),
        REGPROCARRAYOID: (
            None, get_array_bin_reader(REGPROCOID)),
        CASHOID: (None, get_single_reader("q")),
        CASHARRAYOID: (
            None, get_array_bin_reader(CASHOID)),
    }


class DecimalParameterHandler(BaseParameterHandler):

    oid = NUMERICOID
    array_oid = NUMERICARRAYOID

    def __init__(self):
        self.values = deque()
        self.size = 0

    header_fmt = "HhHH"
    header_size = calcsize(header_fmt)
    digit_fmt = "H"
    digit_size = calcsize(digit_fmt)

    def examine(self, val):
        if val.is_infinite():
            raise ValueError("PostgreSQL does not support decimal infinites")
        if (val.is_nan()):
            pg_digits = []
            pg_weight = 0
            pg_sign = NUMERIC_NAN
            dscale = 0
        else:
            sign, digits, exp = val.as_tuple()
            pg_sign = NUMERIC_POS if sign == 0 else NUMERIC_NEG
            dscale = 0 if exp > 0 else -exp

            # "len(digits) + exp", i.e. the number of digits plus the exponent
            # is the 10 based exponent of the first decimal digit
            # pg_weight is 10000 based exponent of first pg_digit minus one
            q, r = divmod(len(digits) + exp, 4)
            pg_weight = q + bool(r) - 1

            def get_pg_digits(i):
                pg_digit = 0
                for dg in digits:
                    # add decimal digit
                    pg_digit *= 10
                    pg_digit += dg
                    i += 1

                    if i == 4:
                        # we have a pg digit, yield it and reset for the next
                        yield pg_digit
                        pg_digit = 0
                        i = 0
                if pg_digit:
                    yield pg_digit * 10 ** (4 - i)

            pg_digits = list(get_pg_digits((4 - r) % 4))

        npg_digits = len(pg_digits)
        self.values.append((npg_digits, pg_weight, pg_sign, dscale) +
                           tuple(pg_digits))
        self.size += self.header_size + npg_digits * self.digit_size

    def encode_value(self, val):
        val = self.values.popleft()
        return (self.header_fmt + self.digit_fmt * val[0],) + val
