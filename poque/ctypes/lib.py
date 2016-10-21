from ctypes import c_char_p, POINTER, c_void_p

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
lib_version = pq.PQlibVersion


def _get_property(res_func):
    def result_method(self):
        return res_func(self)
    return property(result_method)


def get_method(func):
    def method(self):
        return func(self)
    return method


def check_encrypt_password(res, func, args):
    if res is None:
        raise MemoryError()
    ret = c_char_p(res).value
    pq.PQfreemem(res)
    return ret.decode()

pq.PQencryptPassword.restype = c_void_p
pq.PQencryptPassword.argtypes = [c_char_p, c_char_p]
pq.PQencryptPassword.errcheck = check_encrypt_password


def encrypt_password(password, user):
    return pq.PQencryptPassword(password.encode(), user.encode())
