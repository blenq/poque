from codecs import getdecoder
from collections import deque

from .common import BaseParameterHandler, get_array_bin_reader
from . import constants


def read_text(crs):
    return crs.advance_text()


def read_bytes(crs):
    return crs.advance_bytes()


hexdecoder = getdecoder('hex')


def read_bytea_text(crs):
    prefix = crs.peek_view(2)
    if prefix == b"\\x":
        # hex encoding
        crs.advance(2)
        return hexdecoder(crs.advance_view())[0]

    # escape encoding

    def next_or_fail(iterator):
        try:
            ret = next(iterator)
        except StopIteration:
            raise ValueError("Invalid bytea value")
        return ret

    def get_bytes():
        backslash = ord(b'\\')
        biter = iter(crs)
        for b in biter:
            if b != backslash:
                # regular byte
                yield b
                continue

            b = next_or_fail(biter)
            if b == backslash:
                # backslash
                yield b
                continue

            # octal value
            b2 = next_or_fail(biter) - 48
            b3 = next_or_fail(biter) - 48
            yield (b - 48) * 64 + b2 * 8 + b3

    return bytes(get_bytes())


def get_text_converters():
    return {
        constants.BPCHAROID: (None, read_text),
        constants.BPCHARARRAYOID: (
            None, get_array_bin_reader(constants.BPCHAROID)),
        constants.VARCHAROID: (None, read_text),
        constants.VARCHARARRAYOID: (
            None, get_array_bin_reader(constants.VARCHAROID)),
        constants.TEXTOID: (None, read_text),
        constants.TEXTARRAYOID: (
            None, get_array_bin_reader(constants.TEXTOID)),
        constants.NAMEOID: (None, read_text),
        constants.NAMEARRAYOID: (
            None, get_array_bin_reader(constants.NAMEOID)),
        constants.CSTRINGOID: (None, read_text),
        constants.CSTRINGARRAYOID: (
            None, get_array_bin_reader(constants.CSTRINGOID)),
        constants.CHAROID: (read_bytes, read_bytes),
        constants.CHARARRAYOID: (
            None, get_array_bin_reader(constants.CHAROID)),
        constants.BYTEAOID: (read_bytea_text, read_bytes),
        constants.BYTEAARRAYOID: (
            None, get_array_bin_reader(constants.BYTEAOID)),
    }


class BytesParameterHandler(BaseParameterHandler):

    oid = constants.BYTEAOID
    array_oid = constants.BYTEAARRAYOID

    def get_item_size(self, val):
        return len(val)

    def get_format(self, val):
        return "{0}s".format(len(val))


class TextParameterHandler(BaseParameterHandler):

    oid = constants.TEXTOID
    array_oid = constants.TEXTARRAYOID

    def __init__(self):
        super(TextParameterHandler, self).__init__()
        self.values = deque()

    def examine(self, val):
        val = str(val).encode()
        self.values.append(val)
        self.size += len(val)

    def binary_value(self, val):
        return self.values.popleft()

    def get_format(self, val):
        return "{0}s".format(len(val))
