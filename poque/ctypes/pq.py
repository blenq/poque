from ctypes import CDLL, Structure, c_char_p, c_int, POINTER, c_void_p
from ctypes.util import find_library

pq_path = find_library('pq')
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
    if char is None:
        return None
    return char.decode()


def check_info_options(options, func, args):
    if not options:
        return None
    ret = {}
    i = 0
    while True:
        option = options[i]
        if not option.keyword:
            break
        ret[option.keyword.decode()] = (
            check_string(option.envvar),
            check_string(option.compiled),
            check_string(option.val),
            check_string(option.label),
            check_string(option.dispchar),
            option.dispsize,
        )
        i += 1
    pq.PQconninfoFree(options)
    return ret


pq.PQfreemem.restype = None
pq.PQfreemem.argtypes = [c_void_p]
