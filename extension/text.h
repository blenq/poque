#ifndef _POQUE_TEXT_H_
#define _POQUE_TEXT_H_

#include "poque_type.h"

int init_text(void);
PyObject *text_val(
    PoqueResult *result, char *data, int len, PoqueValueHandler *el_handler);
PyObject *bytea_binval(
    PoqueResult *result, char *data, int len, PoqueValueHandler *el_handler);
param_handler *new_text_param_handler(int num_param);


extern PoqueValueHandler text_val_handler;
extern PoqueValueHandler char_val_handler;
extern PoqueValueHandler bytea_val_handler;

extern PoqueValueHandler textarray_val_handler;
extern PoqueValueHandler chararray_val_handler;
extern PoqueValueHandler byteaarray_val_handler;

#endif
