#ifndef _POQUE_CURSOR_H_
#define _POQUE_CURSOR_H_

#include "poque.h"

typedef struct {
    char *data;
    int len;
    int idx;
    Oid el_oid;
} data_crs;


#define crs_at_end(crs)	((crs)->len == (crs)->idx)
#define crs_len(crs) (crs)->len
#define crs_end(crs) (crs)->data + (crs)->len
#define crs_remaining(crs) (crs)->len - (crs)->idx
#define crs_el_oid(crs) (crs)->el_oid

void crs_init(data_crs *crs, char *data, int len, Oid el_oid);
char * crs_advance(data_crs *crs, int len);
char * crs_advance_end(data_crs *crs);
int crs_read_char(data_crs *crs, char *value);
#define crs_read_uchar(crs, value) crs_read_char(crs, (char *)value)
int crs_read_uint16(data_crs *crs, poque_uint16 *value);
#define crs_read_int16(crs, value) crs_read_uint16(crs, (poque_uint16 *) value)
int crs_read_uint32(data_crs *crs, PY_UINT32_T *value);
#define crs_read_int32(crs, value) crs_read_uint32(crs, (PY_UINT32_T *) value)
int crs_read_uint64(data_crs *crs, PY_UINT64_T *value);
#define crs_read_int64(crs, value) crs_read_uint64(crs, (PY_UINT64_T *) value)
int crs_read_float(data_crs *crs, double *value);
int crs_read_double(data_crs *crs, double *value);


#endif
