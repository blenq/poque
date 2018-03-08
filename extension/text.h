#ifndef _POQUE_TEXT_H_
#define _POQUE_TEXT_H_

#include "poque_type.h"

int init_text(void);
PyObject *text_val(PoqueResult *result, char *data, int len, Oid el_oid);
PyObject *bytea_binval(PoqueResult *result, char *data, int len, Oid el_oid);
param_handler *new_text_param_handler(int num_param);


#endif
