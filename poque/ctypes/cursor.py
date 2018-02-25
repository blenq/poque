from itertools import islice

from .constants import *  # noqa
from .lib import InterfaceError, InterfaceIndexError
from builtins import property


class Cursor():

    def __init__(self, cn):
        self._cn = cn
        self._res = None
        self.arraysize = 1
        self._pos = 0

    def _check_closed(self):
        if self._cn is None:
            raise InterfaceError("Cursor is closed")

    @property
    def connection(self):
        return self._cn

    @property
    def rowcount(self):
        self._check_closed()
        res = self._res
        if res is None:
            return -1
        rc = res.cmd_tuples
        if rc == -1:
            return res.ntuples
        return rc

    @property
    def rownumber(self):
        res = self._res
        if res is None or res.nfields == 0:
            return None
        return self._pos

    @property
    def description(self):
        self._check_closed()
        res = self._res
        if res is None:
            return None

        ret = []
        for i in range(self._res.nfields):
            oid = res.ftype(i)
            internal_size = res.fsize(i)
            if internal_size == -1:
                internal_size = None
            precision = None
            scale = None
            if oid == NUMERICOID:
                mod = res.fmod(i) - 4
                if mod >= 0:
                    precision = mod // 0xffff
                    scale = mod & 0xffff
            elif oid == FLOAT8OID:
                precision = 53
            elif oid == FLOAT4OID:
                precision = 24
            ret.append((
                res.fname(i), oid, None, internal_size, precision, scale,
                None
            ))
        return ret

    def execute(self, operation, *args, **kwargs):
        self._check_closed()
        cn = self._cn
        if not cn.autocommit and cn.transaction_status == TRANS_IDLE:
            cn.execute("BEGIN")
        self._res = cn.execute(operation, *args, **kwargs)
        self._pos = 0

    def executemany(self, operation, seq_of_parameters, *args, **kwargs):
        for parameters in seq_of_parameters:
            self.execute(operation, parameters, *args, **kwargs)
        self._res = None

    def _check_fetch(self):
        res = self._res
        if res is None:
            raise InterfaceError("Invalid cursor state")
        if res.nfields == 0:
            raise InterfaceError("No result set")

    def fetchone(self):
        self._check_fetch()
        res = self._res
        pos = self._pos
        if pos < res.ntuples:
            ret = tuple(res.getvalue(pos, i) for i in range(res.nfields))
            self._pos += 1
            return ret
        return None

    def __iter__(self):
        return self

    def __next__(self):
        ret = self.fetchone()
        if ret is None:
            raise StopIteration
        return ret

    def fetchall(self):
        self._check_fetch()
        return self._fetchmany(self._res.ntuples)

    def _fetchmany(self, n):
        res = self._res
        nfields = res.nfields
        ret = [tuple(res.getvalue(pos, i) for i in range(nfields))
               for pos in range(self._pos, n)]
        self._pos = n
        return ret

    def __length_hint__(self):
        res = self._res
        if res is None:
            return 0
        return res.ntuples - self._pos

    def fetchmany(self, size=None):
        self._check_fetch()
        if size is None:
            size = self.arraysize
        return self._fetchmany(min(self._res.ntuples, self._pos + size))

    def scroll(self, value, mode="relative"):
        if self._res is None or self._res.nfields == 0:
            raise InterfaceError("No result set")
        if mode == "relative":
            pos = self._pos + value
        elif mode == "absolute":
            pos = value
        else:
            raise InterfaceError("Invalid mode")
        if pos < 0 or pos > self._res.ntuples:
            # Weird one. According to pep this should be an IndexError.
            raise InterfaceIndexError("Position out of range")
        self._pos = pos

    def close(self):
        # not actually closing anything, just removing references
        self._cn = None
        self._res = None

    def setinputsizes(self, *args, **kwargs):
        pass

    def setoutputsize(self, *args, **kwargs):
        pass
