try:
    from .extension import *
except ImportError as ex:
    if ex.name == 'poque._poque':
        from .ctypes import *
    else:
        raise
