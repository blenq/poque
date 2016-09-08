import os

import poque.extension
import poque.ctypes


class BaseExtensionTest():
    poque = poque.extension


class BaseCTypesTest():
    poque = poque.ctypes


params = {
    param: os.environ.get('POQUE_TEST_' + param.upper(), default)
    for param, default in [('dbname', 'poque_test'),
                           ('host', None),
                           ('user', None),
                           ('port', 5432),
                           ('password', None)]
}


def conninfo():
    """ converts connection params into connection string """
    return ' '.join([
        "{0}='{1}'".format(k, str(v).replace('\\', '\\\\').replace("'", "\\'"))
        for k, v in params.items() if v is not None])


def connparams():
    return {k: v for k, v in params.items() if v is not None}


def connstringparams():
    return {k: str(v) for k, v in params.items() if v is not None}


def connurl():
    url = ['postgresql://']
    user = params.get('user')
    if user:
        url.append(user)
        password = params.get('password')
        if password:
            url.extend([':', password])
        url.append('@')
    host = params.get('host')
    if host:
        url.append(host)
    port = params.get('port')
    if port:
        url.extend([':', str(port)])
    dbname = params.get('dbname')
    if dbname:
        url.extend(['/', dbname])
    return ''.join(url)
