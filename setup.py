#!/usr/bin/env python
from distutils.core import setup, Extension
from subprocess import Popen, PIPE

pg_config = Popen(
    ['pg_config', '--includedir'], stdout=PIPE, stderr=PIPE)
out, err = pg_config.communicate()
if pg_config.returncode != 0:
    raise Exception('Error executing pg_config: {0}'.format(err or ''))
pq_incdir = out.decode().strip()

pg_config = Popen(
    ['pg_config', '--libdir'], stdout=PIPE, stderr=PIPE)
out, err = pg_config.communicate()
if pg_config.returncode != 0:
    raise Exception('Error executing pg_config: {0}'.format(err or ''))
pq_libdir = out.decode().strip()


poque_ext = Extension('poque._poque',
                      sources=['extension/poque.c',
                               'extension/conn.c',
                               'extension/result.c',
                               'extension/cursor.c',
                               'extension/poque_type.c',
                               'extension/numeric.c'],
                      depends=['extension/poque.h',
                               'extension/cursor.h',
                               'extension/numeric.h'],
                      include_dirs=[pq_incdir],
                      library_dirs=[pq_libdir],
                      libraries=['pq'])

setup(name='poque',
      version='0.1',
      description='PostgreSQL client library',
      ext_modules=[poque_ext],
      packages=['poque', 'poque.ctypes'])
