from ctypes import (c_void_p, c_int, c_char_p, c_uint, string_at, c_ubyte)
import codecs

from .pq import pq, check_string
from .common import result_converters, register_result_converters
from .cursor import ValueCursor
from .dt import get_date_time_converters
from .numeric import get_numeric_converters
from .lib import Error, get_property, get_method
from .text import get_text_converters
from .various import get_various_converters
from poque.ctypes.constants import FORMAT_TEXT

register_result_converters(get_numeric_converters())
register_result_converters(get_text_converters())
register_result_converters(get_date_time_converters())
register_result_converters(get_various_converters())


def _get_result_column_method(res_func):
    def result_method(self, column_number):
        return res_func(self, column_number)
    return result_method


def _get_result_row_column_method(res_func):
    def result_method(self, row_number, column_number):
        return res_func(self, row_number, column_number)
    return result_method


class Result(c_void_p):

    status = get_property(pq.PQresultStatus)
    ntuples = get_property(pq.PQntuples)
    nfields = get_property(pq.PQnfields)
    nparams = get_property(pq.PQnparams)
    error_message = get_property(pq.PQresultErrorMessage)

    fformat = _get_result_column_method(pq.PQfformat)
    fmod = _get_result_column_method(pq.PQfmod)
    fname = _get_result_column_method(pq.PQfname)
    fsize = _get_result_column_method(pq.PQfsize)
    ftable = _get_result_column_method(pq.PQftable)
    ftablecol = _get_result_column_method(pq.PQftablecol)
    ftype = _get_result_column_method(pq.PQftype)

    def fnumber(self, column_name):
        return pq.PQfnumber(self, column_name.encode())

    getlength = _get_result_row_column_method(pq.PQgetlength)
    getisnull = _get_result_row_column_method(pq.PQgetisnull)
    _pq_getvalue = _get_result_row_column_method(pq.PQgetvalue)

    clear = get_method(pq.PQclear)

    def __del__(self):
        self.clear()

    def pq_getvalue(self, row_number, column_number):
        data = self._pq_getvalue(row_number, column_number)
        if data is not None:
            if self.fformat(column_number) == FORMAT_TEXT:
                return codecs.decode(data, "utf-8")
            self._views.append(data)
        return data

    def getvalue(self, row_number, column_number):
        # first check for NULL values
        if self.getisnull(row_number, column_number):
            return None

        # get the type oid
        toid = self.ftype(column_number)

        try:
            # try to find the proper converters
            readers = result_converters[toid]
        except KeyError:
            # no problem, just return the actual contents
            pass
        else:
            # get the proper reader for text or binary
            reader = readers[self.fformat(column_number)]
            if reader is not None:
                # create a cursor from the address and length
                data = self._pq_getvalue(row_number, column_number)
                crs = ValueCursor(data)

                # convert the data in the cursor
                value = reader(crs)
                if not crs.at_end():
                    # we're not at the end, something must have gone wrong
                    raise Error("Invalid data format")
                return value

        # return the original content as it is
        return self.pq_getvalue(row_number, column_number)


pq.PQresultStatus.argtypes = [Result]
pq.PQntuples.argtypes = [Result]
pq.PQnfields.argtypes = [Result]
pq.PQnparams.argtypes = [Result]


pq.PQfformat.argtypes = [Result, c_int]
pq.PQfmod.argtypes = [Result, c_int]
pq.PQfsize.argtypes = [Result, c_int]
pq.PQftablecol.argtypes = [Result, c_int]

pq.PQfname.argtypes = [Result, c_int]
pq.PQfname.restype = c_char_p
pq.PQfname.errcheck = check_string

pq.PQfnumber.argtypes = [Result, c_char_p]

pq.PQftype.argtypes = [Result, c_int]
pq.PQftype.restype = c_uint

pq.PQftable.argtypes = [Result, c_int]
pq.PQftable.restype = c_uint

pq.PQgetlength.argtypes = [Result, c_int, c_int]


def check_bool(res, func, args):
    return bool(res)


pq.PQgetisnull.argtypes = [Result, c_int, c_int]
pq.PQgetisnull.errcheck = check_bool


def check_value(res, func, args):
    if res is not None:
        pg_res, row, col = args
        length = pg_res.getlength(row, col)
        return memoryview((c_ubyte * length).from_address(res))
#         return ValueCursor(data)
    return res


pq.PQgetvalue.argtypes = [Result, c_int, c_int]
pq.PQgetvalue.restype = c_void_p
pq.PQgetvalue.errcheck = check_value

pq.PQresultErrorMessage.argtypes = [Result]
pq.PQresultErrorMessage.restype = c_char_p
pq.PQresultErrorMessage.errcheck = check_string


def check_clear(res, func, args):
    result = args[0]
    if result:
        result.value = 0
        try:
            views = result._views
        except AttributeError:
            pass
        else:
            for v in views:
                v.release()


pq.PQclear.argtypes = [Result]
pq.PQclear.restype = None
pq.PQclear.errcheck = check_clear
