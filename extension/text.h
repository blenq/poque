#ifndef _POQUE_TEXT_H_
#define _POQUE_TEXT_H_

#include "poque_type.h"

int init_text(void);
PyObject *text_val(data_crs* crs);
PyObject *bytea_binval(data_crs* crs);
param_handler *new_text_param_handler(int num_param);


#endif
