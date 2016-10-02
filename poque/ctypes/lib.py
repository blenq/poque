from ctypes import c_char_p, POINTER, c_int
from .pq import pq, PQconninfoOptions, check_info_options


class Error(Exception):
    pass


pq.PQconndefaults.argtypes = []
pq.PQconndefaults.restype = PQconninfoOptions
pq.PQconndefaults.errcheck = check_info_options

conn_defaults = pq.PQconndefaults

pq.PQconninfoParse.argtypes = [c_char_p, POINTER(c_char_p)]
pq.PQconninfoParse.restype = PQconninfoOptions
pq.PQconninfoParse.errcheck = check_info_options


def conninfo_parse(conninfo):
    err_msg = c_char_p()
    options = pq.PQconninfoParse(conninfo.encode(), err_msg)
    if options is None:
        if err_msg:
            error_msg = err_msg.value.decode()
            pq.PQfreemem(err_msg)
            raise Error(error_msg)
        else:
            raise MemoryError()
    return options


pq.PQlibVersion.argtypes = []
pq.PQlibVersion.restype = c_int

lib_version = pq.PQlibVersion


def _get_property(res_func):
    def result_method(self):
        return res_func(self)
    return property(result_method)


def get_method(func):
    def method(self):
        return func(self)
    return method
