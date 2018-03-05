from codecs import getdecoder
from collections import deque

from .common import BaseParameterHandler
from .constants import *  # noqa


def read_text(crs):
    return crs.advance_text()


def read_bytes(crs):
    return crs.advance_view()


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
    return [
        (BPCHAROID, BPCHARARRAYOID, None, read_text),
        (VARCHAROID, VARCHARARRAYOID, None, read_text),
        (TEXTOID, TEXTARRAYOID, read_text, read_text),
        (NAMEOID, NAMEARRAYOID, read_text, read_text),
        (CSTRINGOID, CSTRINGARRAYOID, None, read_text),
        (CHAROID, CHARARRAYOID, read_bytes, read_bytes),
        (BYTEAOID, BYTEAARRAYOID, read_bytea_text,
         read_bytes),
    ]


class BytesParameterHandler(BaseParameterHandler):

    oid = BYTEAOID
    array_oid = BYTEAARRAYOID

    def get_item_size(self, val):
        return len(val)

    def get_format(self, val):
        return "{0}s".format(len(val))


class TextParameterHandler(BaseParameterHandler):

    oid = TEXTOID
    array_oid = TEXTARRAYOID

    def __init__(self):
        super(TextParameterHandler, self).__init__()
        self.values = deque()

    def get_item_size(self, val):
        return len(self.values[-1])

    def examine(self, val):
        val = str(val).encode()
        self.values.append(val)
        return super().examine(val)

    def get_format(self, val):
        return "{0}s".format(len(self.values[0]))

    def binary_value(self, val):
        return self.values.popleft()
