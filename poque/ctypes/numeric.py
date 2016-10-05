from ctypes import create_string_buffer, cast, c_char_p
from decimal import Decimal
from struct import pack_into


from .constants import NUMERICOID, FORMAT_BINARY
from .lib import Error

NUMERIC_NAN = 0xC000
NUMERIC_POS = 0x0000
NUMERIC_NEG = 0x4000


def _read_numeric_str(crs, length):
    value = crs.advance_text(length)
    return Decimal(value)


def _read_numeric_bin(crs, length):
    """ Reads a binary numeric/decimal value """

    # Read field values: number of digits, weight, sign, display scale.
    npg_digits, weight, sign, dscale = crs.advance_struct_format_HhHH()
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


def get_numeric_converters():
    return {
        NUMERICOID: (_read_numeric_str, _read_numeric_bin),
    }


def write_decimal_bin(val):
    pg_digits = []
    if (val.is_nan()):
        weight = 0
        pg_sign = NUMERIC_NAN
        dscale = 0
    else:
        sign, digits, exponent = val.as_tuple()
        if sign == 0:
            pg_sign = NUMERIC_POS
        else:
            pg_sign = NUMERIC_NEG
        dscale = -exponent
        if dscale < 0:
            digits += (0,) * exponent
            dscale = 0

        weight = len(digits) - dscale
        quot, rest = divmod(weight, 4)
        weight = quot + bool(rest) - 1
        if rest > 0:
            pg_digits.append(0)
            i = 4 - rest
        else:
            i = 0
        for dg in digits:
            if i == 0:
                pg_digits.append(1000 * dg)
            elif i == 1:
                pg_digits[-1] += 100 * dg
            elif i == 2:
                pg_digits[-1] += 10 * dg
            elif i == 3:
                pg_digits[-1] += dg
                i = 0
                continue
            i += 1
    npg_digits = len(pg_digits)
    msg_len = 8 + npg_digits * 2
    ret = create_string_buffer(msg_len)
    pack_into("!HhHH" + 'H' * npg_digits, ret, 0, npg_digits, weight, pg_sign,
              dscale, *pg_digits)
    return NUMERICOID, cast(ret, c_char_p), msg_len, FORMAT_BINARY


def get_numeric_param_converters():
    return {
        Decimal: write_decimal_bin,
    }
