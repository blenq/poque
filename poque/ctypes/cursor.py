from codecs import decode
from ctypes import c_byte

from .common import get_struct
from .lib import Error


class ValueCursor():
    """ Cursor object to traverse through postgresql data value """

    def __init__(self, data, length):
        self.data = memoryview((c_byte * length).from_address(data))
        self.idx = 0

    def at_end(self):
        return self.idx == len(self.data)

    def advance(self, length=None):
        ret = self.idx

        if length is None:
            self.idx = len(self.data)
        else:
            if ret + length > len(self.data):
                # check
                raise Error("Item length exceeds data length")

            # actually advance the cursor
            self.idx += length

        # return the previous index
        return ret

    def advance_struct_format(self, fmt):
        # read values from the cursor according to the provided struct format
        stc = get_struct(fmt)
        return stc.unpack_from(self.data, offset=self.advance(stc.size))

    def __iter__(self):
        length = len(self.data)
        while self.idx < length:
            yield self.data.obj[self.idx]
            self.idx += 1

    def advance_view(self, length=None):
        return self.data[self.advance(length):self.idx]

    def peek_view(self, length):
        return self.data[self.idx:self.idx + length]

    def advance_bytes(self, length=None):
        return bytes(self.advance_view(length))

    def advance_text(self):
        return decode(self.advance_view())

    def advance_single(self, fmt):
        return self.advance_struct_format(fmt)[0]

    def cursor(self, length):
        # create sub cursor
        return SubCursor(self.advance_view(length))


class SubCursor(ValueCursor):

    def __init__(self, data):
        self.data = data
        self.idx = 0
