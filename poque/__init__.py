from ._poque import *


def connect(*args, **kwargs):
    return Conn(*args, **{
        k: v if k in ['expand_dbname', 'async'] or v is None else str(v)
        for k, v in kwargs.items()})
