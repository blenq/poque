from functools import lru_cache
from struct import Struct


@lru_cache()
def get_struct(fmt):
    return Struct(fmt)
