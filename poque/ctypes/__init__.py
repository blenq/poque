import ssl

from .conn import Conn
from .constants import *
from .lib import (
    Error, conn_defaults, conninfo_parse, lib_version, encrypt_password)


def connect(*args, **kwargs):
    return Conn(*args, **{
        k: v if k in ['expand_dbname', 'async'] or v is None else str(v)
        for k, v in kwargs.items()})
