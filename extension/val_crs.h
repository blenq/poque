#ifndef _POQUE_VALCRS_H_
#define _POQUE_VALCRS_H_

#include "poque.h"

typedef struct {
    char *data;
    int len;
    int idx;
    Oid el_oid;
    PoqueResult *result;
} ValueCursor;


#define crs_at_end(crs)	((crs)->len == (crs)->idx)
#define crs_len(crs) (crs)->len
#define crs_end(crs) (crs)->data + (crs)->len
#define crs_remaining(crs) (crs)->len - (crs)->idx
#define crs_el_oid(crs) (crs)->el_oid

void crs_init(ValueCursor *crs, char *data, int len, Oid el_oid,
              PoqueResult *result);
char * crs_advance(ValueCursor *crs, int len);
char * crs_advance_end(ValueCursor *crs);
int crs_read_char(ValueCursor *crs, char *value);
#define crs_read_uchar(crs, value) crs_read_char(crs, (char *)value)
int crs_read_uint16(ValueCursor *crs, poque_uint16 *value);
#define crs_read_int16(crs, value) crs_read_uint16(crs, (poque_uint16 *) value)
int crs_read_uint32(ValueCursor *crs, PY_UINT32_T *value);
#define crs_read_int32(crs, value) crs_read_uint32(crs, (PY_UINT32_T *) value)
int crs_read_uint64(ValueCursor *crs, PY_UINT64_T *value);
#define crs_read_int64(crs, value) crs_read_uint64(crs, (PY_UINT64_T *) value)
int crs_read_float(ValueCursor *crs, double *value);
int crs_read_double(ValueCursor *crs, double *value);


#endif
