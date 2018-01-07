from collections import deque
from decimal import Decimal
from struct import calcsize

from .common import (BaseParameterHandler, get_array_bin_reader,
                     get_single_reader)
from . import constants
from .constants import (
    INT4OID, INT4ARRAYOID, INT8OID, INT8ARRAYOID, TEXTOID, TEXTARRAYOID,
    FLOAT8OID, FLOAT8ARRAYOID)
from .lib import Error


def read_bool_text(crs):
    return crs.advance_view(1) == b't'


class BoolParameterHandler(BaseParameterHandler):

    oid = constants.BOOLOID
    array_oid = constants.BOOLARRAYOID
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

    def check_int8(self, val):
        return -0x8000000000000000 <= val <= 0x7FFFFFFFFFFFFFFF

    def examine(self, val):
        self.values.append(val)

        if self.oid == INT4OID:
            if -0x80000000 <= val <= 0x7FFFFFFF:
                return
            if self.check_int8(val):
                self.oid = INT8OID
                self.array_oid = INT8ARRAYOID
                self.item_size = self.int8size
                self.fmt = self.int8fmt
                return

        if self.oid == TEXTOID or self.check_int8(val):
            return
        self.oid = TEXTOID
        self.array_oid = TEXTARRAYOID
        self.encode_value = self.encode_text_value
        self.get_size = self.get_text_size

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

    # Read field values: number of digits, weight, sign, display scale.
    npg_digits, weight, sign, dscale = crs.advance_struct_format("HhHH")
    if npg_digits:
        pg_digits = crs.advance_struct_format('{}H'.format(npg_digits))
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
    digits = []
    for dg in pg_digits:
        if dg > 9999:
            raise Error("Invalid value")
        # a postgres digit contains 4 decimal digits
        digits.extend([dg // 1000, (dg // 100) % 10, (dg // 10) % 10, dg % 10])

    len_diff = ndigits - len(digits)
    if len_diff < 0:
        # the pg value can have more zeroes than the display scale indicates
        del digits[len_diff:]
    else:
        # add extra zeroes indicated by display scale that are not in the
        # actual value
        digits.extend([0] * (len_diff))

    # now create the decimal
    return Decimal((sign, digits, -dscale))


def get_numeric_converters():
    return {
        constants.BOOLOID: (read_bool_text, get_single_reader("?")),
        constants.BOOLARRAYOID: (
            None, get_array_bin_reader(constants.BOOLOID)),
        constants.NUMERICOID: (_read_numeric_str, _read_numeric_bin),
        constants.NUMERICARRAYOID: (
            None, get_array_bin_reader(constants.NUMERICOID)),
        constants.FLOAT4OID: (_read_float_text, get_single_reader("f")),
        constants.FLOAT4ARRAYOID: (
            None, get_array_bin_reader(constants.FLOAT4OID)),
        constants.FLOAT8OID: (_read_float_text, get_single_reader("d")),
        constants.FLOAT8ARRAYOID: (
            None, get_array_bin_reader(constants.FLOAT8OID)),
        constants.INT2OID: (read_int_text, get_single_reader("h")),
        constants.INT2ARRAYOID: (
            None, get_array_bin_reader(constants.INT2OID)),
        constants.INT2VECTOROID: (
            None, get_array_bin_reader(constants.INT2OID)),
        constants.INT2VECTORARRAYOID: (
            None, get_array_bin_reader(constants.INT2VECTOROID)),
        constants.INT4OID: (read_int_text, get_single_reader("i")),
        constants.INT4ARRAYOID: (
            None, get_array_bin_reader(constants.INT4OID)),
        constants.INT8OID: (read_int_text, get_single_reader("q")),
        constants.INT8ARRAYOID: (
            None, get_array_bin_reader(constants.INT8OID)),
        constants.XIDOID: (read_int_text, get_single_reader("I")),
        constants.XIDARRAYOID: (None, get_array_bin_reader(constants.XIDOID)),
        constants.CIDOID: (read_int_text, get_single_reader("I")),
        constants.CIDARRAYOID: (None, get_array_bin_reader(constants.CIDOID)),
        constants.OIDOID: (read_int_text, get_single_reader("I")),
        constants.OIDARRAYOID: (None, get_array_bin_reader(constants.OIDOID)),
        constants.OIDVECTOROID: (None, get_array_bin_reader(constants.OIDOID)),
        constants.OIDVECTORARRAYOID: (
            None, get_array_bin_reader(constants.OIDVECTOROID)),
        constants.REGPROCOID: (None, get_single_reader("I")),
        constants.REGPROCARRAYOID: (
            None, get_array_bin_reader(constants.REGPROCOID)),
        constants.CASHOID: (None, get_single_reader("q")),
        constants.CASHARRAYOID: (
            None, get_array_bin_reader(constants.CASHOID)),
    }


class DecimalParameterHandler(BaseParameterHandler):

    oid = constants.NUMERICOID
    array_oid = constants.NUMERICARRAYOID

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
        pg_digits = []
        if (val.is_nan()):
            weight = 0
            pg_sign = NUMERIC_NAN
            exponent = 0
        else:
            sign, digits, exponent = val.as_tuple()
            pg_sign = NUMERIC_POS if sign == 0 else NUMERIC_NEG
            if exponent > 0:
                digits += (0,) * exponent
                exponent = 0

            quot, i = divmod(len(digits) + exponent, 4)
            weight = quot + bool(i) - 1  # calculate weight in pg_digits

            # fill pg_digits
            if i > 0:
                # add a pg_digit when we don't start on an exact 4 byte
                # boundary
                pg_digits.append(0)
            for dg in digits:
                if i == 0:
                    # add a new pg_digit
                    pg_digits.append(0)
                    i = 4
                i -= 1
                pg_digits[-1] += dg * 10 ** i

        npg_digits = len(pg_digits)
        self.values.append((npg_digits, weight, pg_sign, -exponent) +
                           tuple(pg_digits))
        self.size += self.header_size + npg_digits * self.digit_size

    def encode_value(self, val):
        val = self.values.popleft()
        return (self.header_fmt + self.digit_fmt * val[0],) + val
