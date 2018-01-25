#ifndef _POQUE_UUID_H_
#define _POQUE_UUID_H_

#include "poque.h"
#include "poque_type.h"
#include "cursor.h"


int init_uuid(void);
PyObject *uuid_binval(data_crs *crs);
PyObject *uuid_strval(data_crs *crs);
//param_handler *new_uuid_param_handler(int num_param);


#endif
