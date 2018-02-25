#!/usr/bin/env python
from setuptools import setup, Extension
from subprocess import Popen, PIPE


def execute_pg_config(option):
    pg_config = Popen(
        ['pg_config', '--' + option], stdout=PIPE, stderr=PIPE)
    out, err = pg_config.communicate()
    if pg_config.returncode != 0:
        raise Exception('Error executing pg_config: {0}'.format(err or ''))
    return out.decode().strip()


pq_incdir = execute_pg_config("includedir")
pq_libdir = execute_pg_config("libdir")

poque_ext = Extension('poque._poque',
                      sources=['extension/poque.c',
                               'extension/conn.c',
                               'extension/result.c',
                               'extension/val_crs.c',
                               'extension/poque_type.c',
                               'extension/numeric.c',
                               'extension/text.c',
                               'extension/uuid.c',
                               'extension/datetime.c',
                               'extension/network.c',
                               'extension/geometric.c',
                               'extension/cursor.c'],
                      depends=['extension/poque.h',
                               'extension/val_crs.h',
                               'extension/numeric.h',
                               'extension/poque_type.h',
                               'extension/uuid.h',
                               'extension/text.h',
                               'extension/datetime.h',
                               'extension/network.h',
                               'extension/geometric.h',
                               'extension/cursor.h'],
                      include_dirs=[pq_incdir],
                      library_dirs=[pq_libdir],
                      libraries=['pq'])

setup(name='poque',
      version='0.1',
      description='PostgreSQL client library',
      ext_modules=[poque_ext],
      packages=['poque', 'poque.ctypes'])
