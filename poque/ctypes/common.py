from functools import lru_cache
from struct import Struct, calcsize

from .constants import FORMAT_BINARY
from .lib import Error


@lru_cache()
def get_struct(fmt):
    return Struct("!" + fmt)


def get_single_reader(fmt):

    def reader(crs):
        return crs.advance_single(fmt)

    return reader


class BaseParameterHandlerMeta(type):

    def __new__(cls, name, bases, namespace, **kwds):
        handler = type.__new__(cls, name, bases, namespace)

        # set the item_size based on format
        try:
            fmt = handler.fmt
        except AttributeError:
            pass
        else:
            handler.item_size = calcsize(fmt)
        return handler


class BaseParameterHandler(object, metaclass=BaseParameterHandlerMeta):
    """ Helper class to implement paramater handlers """

    def __init__(self):
        self.size = 0

    def get_item_size(self, val):
        return self.item_size

    def examine(self, val):
        self.size += self.get_item_size(val)

    def get_size(self):
        return self.size

    def get_format(self, val):
        return self.fmt

    def binary_value(self, val):
        return val

    def binary_values(self, val):
        return (self.binary_value(val),)

    def encode_value(self, val):
        return (self.get_format(val),) + self.binary_values(val)

    def type_allowed(self, typ):
        return False


result_converters = {}


def _get_array_value(crs, array_dims, reader):
    if array_dims:
        # get an array of (nested) values
        dim = array_dims[0]
        return [_get_array_value(crs, array_dims[1:], reader)
                for _ in range(dim)]
    else:
        # get a single value, either NULL or an actual value prefixed by a
        # length
        item_len = crs.advance_single("i")
        if item_len == -1:
            return None
        cursor = crs.cursor(item_len)
        val = reader(cursor)
        if not cursor.at_end():
            # we're not at the end, something must have gone wrong
            raise Error("Invalid data format")
        return val


def get_array_bin_reader(elem_oid):
    def read_array_bin(crs):

        dims, flags, elem_type = crs.advance_struct_format("IiI")

        if elem_type != elem_oid:
            raise Error("Unexpected element type")
        if dims > 6:
            raise Error("Number of dimensions exceeded")
        if flags & 1 != flags:
            raise Error("Invalid value for array flags")
        if dims == 0:
            return []
        reader = result_converters[elem_type][FORMAT_BINARY]
        array_dims = [crs.advance_struct_format("ii")[0] for _ in range(dims)]
        return _get_array_value(crs, array_dims, reader)
    return read_array_bin
