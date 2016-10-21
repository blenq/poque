from ctypes import create_string_buffer, cast, c_char_p
from decimal import Decimal
from struct import pack_into


from .constants import (
    INT4OID, INT8OID, TEXTOID, FLOAT8OID, NUMERICOID, FORMAT_BINARY)
from .lib import Error


def _get_int_param(val):
    if val >= -0x80000000 and val <= 0x7FFFFFFF:
        length = 4
        fmt = "!i"
        oid = INT4OID
    elif val >= -0x8000000000000000 and val <= 0x7FFFFFFFFFFFFFFF:
        length = 8
        fmt = "!q"
        oid = INT8OID
    else:
        val = str(val).encode()
        return TEXTOID, c_char_p(val), len(val), FORMAT_BINARY
    ret = create_string_buffer(length)
    pack_into(fmt, ret, 0, val)
    return oid, cast(ret, c_char_p), length, FORMAT_BINARY


def _get_float_param(val):
    ret = create_string_buffer(8)
    pack_into("!d", ret, 0, val)
    return FLOAT8OID, cast(ret, c_char_p), 8, FORMAT_BINARY


NUMERIC_NAN = 0xC000
NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000


def _read_numeric_str(crs, length):
    value = crs.advance_text(length)
    return Decimal(value)


def _read_numeric_bin(crs, length):
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


def write_decimal_bin(val):
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
            # add a pg_digit when we don't start on an exact 4 byte boundary
            pg_digits.append(0)
        for dg in digits:
            if i == 0:
                # add a new pg_digit
                pg_digits.append(0)
                i = 4
            i -= 1
            pg_digits[-1] += dg * 10 ** i

    npg_digits = len(pg_digits)
    msg_len = 8 + npg_digits * 2
    ret = create_string_buffer(msg_len)
    pack_into("!HhHH" + 'H' * npg_digits, ret, 0, npg_digits, weight, pg_sign,
              -exponent, *pg_digits)
    return NUMERICOID, cast(ret, c_char_p), msg_len, FORMAT_BINARY


def get_numeric_param_converters():
    return {
        int: _get_int_param,
        float: _get_float_param,
        Decimal: write_decimal_bin,
    }
