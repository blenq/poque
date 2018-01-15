#ifndef _POQUE_NUMERIC_H_
#define _POQUE_NUMERIC_H_

#include "cursor.h"

PyObject *int16_binval(data_crs *crs);
PyObject *uint32_binval(data_crs *crs);
PyObject *int32_binval(data_crs *crs);
PyObject *int64_binval(data_crs *crs);
PyObject *int_strval(data_crs *crs);
PyObject *bool_binval(data_crs *crs);
PyObject *bool_strval(data_crs *crs);
PyObject *float64_binval(data_crs *crs);
PyObject *float32_binval(data_crs *crs);
PyObject *float_strval(data_crs *crs);
PyObject *numeric_binval(data_crs *crs);
extern PyObject *PyDecimal;

#endif
