#ifndef _POQUE_VALCRS_H_
#define _POQUE_VALCRS_H_

#include "poque.h"

typedef struct _ValueCursor {
    char *data;
    int len;
    int idx;
    Oid el_oid;
    PoqueResult *result;
} ValueCursor;

#define crs_init(crs, d, l, e, r) (crs)->data = (d);\
    (crs)->len = (l);\
    (crs)->idx = 0;\
    (crs)->el_oid = (e);\
    (crs)->result = (r)


#define crs_at_end(crs)	((crs)->len == (crs)->idx)
#define crs_len(crs) (crs)->len
#define crs_end(crs) (crs)->data + (crs)->len
#define crs_remaining(crs) (crs)->len - (crs)->idx
#define crs_el_oid(crs) (crs)->el_oid

#define crs_advance_end(crs) ((crs)->data + (crs)->idx); (crs)->idx = (crs)->len


char * crs_advance(ValueCursor *crs, int len);
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

#define _read_uint16(d) (((d)[0] << 8) | (d)[1])
#define read_uint16(d) _read_uint16((unsigned char *)(d))
#define read_int16(d) (poque_int16)read_uint16(d)

#define _read_uint32(d) (((PY_UINT32_T)(d)[0] << 24) | ((d)[1] << 16) | ((d)[2] << 8) | (d)[3])
#define read_uint32(d) _read_uint32((unsigned char *)(d))
#define read_int32(d) (PY_INT32_T)read_uint32(d)

#define _read_uint64(d) (((PY_UINT64_T)(d)[0] << 56) | \
		((PY_UINT64_T)(d)[1] << 48) | \
		((PY_UINT64_T)(d)[2] << 40) | \
		((PY_UINT64_T)(d)[3] << 32) | \
		((PY_UINT64_T)(d)[4] << 24) | \
		((d)[5] << 16) | ((d)[6] << 8) | (d)[7])
#define read_uint64(d) _read_uint64((unsigned char *)(d))
#define read_int64(d) (PY_INT64_T)read_uint64(d)

#endif
