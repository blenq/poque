from collections import deque
from decimal import Decimal

from .common import BaseParameterHandler
from .constants import (
    INT4OID, INT4ARRAYOID, INT8OID, INT8ARRAYOID, TEXTOID, TEXTARRAYOID,
    FLOAT8OID, NUMERICOID, NUMERICARRAYOID, FLOAT8ARRAYOID)
from .lib import Error


class IntArrayParameterHandler(object):

    oid = INT4OID
    array_oid = INT4ARRAYOID

    def __init__(self):
        self.values = []

    def check_value(self, val):
        self.values.append(val)

        if self.oid == INT4OID:
            if -0x80000000 <= val <= 0x7FFFFFFF:
                return
            self.oid = INT8OID
            self.array_oid = INT8ARRAYOID
            self.encode_value = self.encode_int8_value
            self.get_length = self.get_int8_length

        if self.oid == INT8OID:
            if -0x8000000000000000 <= val <= 0x7FFFFFFFFFFFFFFF:
                return
            self.oid = TEXTOID
            self.array_oid = TEXTARRAYOID
            self.encode_value = self.encode_text_value
            self.get_length = self.get_text_length

    def get_length(self):
        return len(self.values) * 4

    def encode_value(self, val):
        return "i", val

    def get_int8_length(self):
        return len(self.values) * 8

    def encode_int8_value(self, val):
        return "q", val

    def get_text_length(self):
        self.values = deque(str(v).encode() for v in self.values)
        return sum(len(v) for v in self.values)

    def encode_text_value(self, val):
        val = self.values.popleft()
        return "{0}s".format(len(val)), val

    def get_array_oid(self):
        if self.oid == INT4OID:
            return INT4ARRAYOID
        if self.oid == INT8OID:
            return INT8ARRAYOID
        return TEXTARRAYOID


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
        NUMERICOID: (_read_numeric_str, _read_numeric_bin),
    }


class DecimalArrayParameterHandler(object):

    oid = NUMERICOID
    array_oid = NUMERICARRAYOID

    def __init__(self):
        self.values = deque()

    def check_value(self, val):
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

    def get_length(self):
        return sum(8 + v[0] * 2 for v in self.values)

    def encode_value(self, val):
        val = self.values.popleft()
        return ("HhHH" + 'H' * val[0],) + val
