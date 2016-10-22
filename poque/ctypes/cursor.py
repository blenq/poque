from codecs import decode
from ctypes import c_byte

from .common import get_struct
from .lib import Error


class ValueCursor():
    """ Cursor object to traverse through postgresql data value """

    def __init__(self, data, length):
        if type(data) == memoryview:
            self.data = data
        else:
            # a ctypes fixed length array implements the buffer interface, and
            # is therefore accessible as a memoryview
            self.data = memoryview((c_byte * length).from_address(data))

        self.length = length
        self.idx = 0

    def advance(self, length=None):
        ret = self.idx

        if length is None:
            self.idx = self.length
        else:
            if ret + length > self.length:
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

    def advance_view(self, length=None):
        idx = self.advance(length)
        return self.data[idx:self.idx]

    def advance_bytes(self, length=None):
        return bytes(self.advance_view(length))

    def advance_text(self):
        return decode(self.advance_view())

    def advance_single(self, fmt):
        return self.advance_struct_format(fmt)[0]

    def cursor(self, length):
        # create subcursor
        return ValueCursor(self.advance_view(length), length)
