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
