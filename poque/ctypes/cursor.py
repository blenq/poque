from codecs import decode
from ctypes import c_byte
from struct import Struct, calcsize, unpack_from

from .lib import Error


def get_struct_advancer(fmt):
    sct = Struct(fmt)
    sct_size = sct.size
    unpack_from = sct.unpack_from

    def advance_struct(self, length=None):
        if length is not None and length != sct_size:
            raise Error("Invalid length for item")
        return unpack_from(self.data, offset=self.advance(sct_size))
    return advance_struct


def get_struct_single_advancer(fmt):
    func = get_struct_advancer(fmt)

    def advance_struct(self, length=None):
        return func(self, length)[0]
    return advance_struct


class ValueCursor():
    """ Cursor object to traverse through postgresql data value """

    def __init__(self, data, length):
        # a ctypes fixed length array implements the buffer interface, and
        # is therefore accessible as a memoryview
        self.data = memoryview((c_byte * length).from_address(data))

        self.length = length
        self.idx = 0

    def advance(self, length):
        ret = self.idx
        if ret + length > self.length:
            # check
            raise Error("Item length exceeds data length")

        # actually advance the cursor
        self.idx += length
        # return the previous index
        return ret

    def advance_struct_format(self, fmt):
        # read values from the cursor according to the provided struct format
        calc_length = calcsize(fmt)
        return unpack_from(fmt, self.data, offset=self.advance(calc_length))

    def advance_view(self, length):
        idx = self.advance(length)
        return self.data[idx:self.idx]

    def advance_bytes(self, length):
        return bytes(self.advance_view(length))

    def advance_text(self, length):
        return decode(self.advance_view(length))

    advance_bool = get_struct_single_advancer("!?")
    advance_ubyte = get_struct_single_advancer("!B")
    advance_char = get_struct_single_advancer("!c")
    advance_int2 = get_struct_single_advancer("!h")
    advance_int4 = get_struct_single_advancer("!i")
    advance_uint4 = get_struct_single_advancer("!I")
    advance_int8 = get_struct_single_advancer("!q")
    advance_float4 = get_struct_single_advancer("!f")
    advance_float8 = get_struct_single_advancer("!d")

    advance_struct_format_IiI = get_struct_advancer("!IiI")
    advance_struct_format_2d = get_struct_advancer("!2d")
    advance_struct_format_3d = get_struct_advancer("!3d")
    advance_struct_format_4d = get_struct_advancer("!4d")
    advance_struct_format_4B = get_struct_advancer("!4B")
    advance_struct_format_8H = get_struct_advancer("!8H")
    advance_struct_format_HI = get_struct_advancer("!HI")
    advance_struct_format_IH = get_struct_advancer("!IH")
    advance_struct_format_qi = get_struct_advancer("!qi")
    advance_struct_format_qii = get_struct_advancer("!qii")
    advance_struct_format_HhHH = get_struct_advancer("!HhHH")
