#ifndef _POQUE_TEXT_H_
#define _POQUE_TEXT_H_

#include "poque.h"
#include "cursor.h"

PyObject *bytea_binval(data_crs* crs);
PyObject *bytea_strval(data_crs *crs);

PyObject *char_binval(data_crs* crs);

#endif
