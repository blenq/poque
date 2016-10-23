from functools import lru_cache
from struct import Struct


@lru_cache()
def get_struct(fmt):
    return Struct(fmt)


class BaseParameterHandlerMeta(type):

    def __new__(cls, name, bases, namespace, **kwds):
        handler = type.__new__(cls, name, bases, namespace)

        # set the item_size based on format
        try:
            fmt = handler.fmt
        except AttributeError:
            pass
        else:
            handler.item_size = get_struct(fmt).size
        return handler


class BaseParameterHandler(object, metaclass=BaseParameterHandlerMeta):
    """ Helper class to implement paramater handlers """

    def __init__(self):
        self.length = 0

    def get_item_size(self, val):
        return self.item_size

    def check_value(self, val):
        self.length += self.get_item_size(val)

    def get_length(self):
        return self.length

    def get_format(self, val):
        return self.fmt

    def binary_value(self, val):
        return val

    def encode_value(self, val):
        val = self.binary_value(val)
        return self.get_format(val), val
