from collections import namedtuple
from ctypes import CDLL, Structure, c_char_p, c_int, POINTER, c_void_p
from ctypes.util import find_library

pq_path = find_library('pq')
if not pq_path:
    pq_path = find_library('libpq')
pq = CDLL(pq_path)


class PQconninfoOption(Structure):
    _fields_ = [("keyword", c_char_p),
                ("envvar", c_char_p),
                ("compiled", c_char_p),
                ("val", c_char_p),
                ("label", c_char_p),
                ("dispchar", c_char_p),
                ("dispsize", c_int),
                ]

PQconninfoOptions = POINTER(PQconninfoOption)

pq.PQconninfoFree.restype = None
pq.PQconninfoFree.argtypes = [PQconninfoOptions]


def check_string(char, *args):
    return None if char is None else char.decode()


InfoOption = namedtuple(
    "InfoOption", [f[0] for f in PQconninfoOption._fields_[1:]])


def check_info_options(options, func, args):
    if not options:
        return None
    ret = {}
    i = 0
    while True:
        option = options[i]
        if not option.keyword:
            break
        ret[option.keyword.decode()] = InfoOption(
            envvar=check_string(option.envvar),
            compiled=check_string(option.compiled),
            val=check_string(option.val),
            label=check_string(option.label),
            dispchar=check_string(option.dispchar),
            dispsize=option.dispsize,
        )
        i += 1
    pq.PQconninfoFree(options)
    return ret


pq.PQfreemem.restype = None
pq.PQfreemem.argtypes = [c_void_p]
