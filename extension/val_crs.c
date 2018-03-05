#include "val_crs.h"


char *
crs_advance(ValueCursor *crs, int len) {
    char *data;

	if (crs->idx > crs->len - len) {
		PyErr_SetString(PoqueError, "Item length exceeds data length");
		return NULL;
	}
    data = crs->data + crs->idx;
	crs->idx += len;
    return data;
}


int
crs_read_char(ValueCursor *crs, char *value) {

	char *data;

	data = crs_advance(crs, 1);
	if (data == NULL)
		return -1;
    *value = data[0];
    return 0;
}


int
crs_read_uint16(ValueCursor *crs, poque_uint16 *value)
{
    unsigned char *data;

    data = (unsigned char *)crs_advance(crs, 2);
    if (data == NULL)
        return -1;

    *value = (data[0] << 8) | data[1];
    return 0;
}


int
crs_read_uint32(ValueCursor *crs, PY_UINT32_T *value)
{
    unsigned char *data;

    data = (unsigned char *)crs_advance(crs, 4);
    if (data == NULL)
        return -1;

    *value = ((PY_UINT32_T)data[0] << 24) | (data[1] << 16) |
             (data[2] << 8) | data[3];

    return 0;
}


int
crs_read_uint64(ValueCursor *crs, PY_UINT64_T *value)
{
    unsigned char *data;

    data = (unsigned char *)crs_advance(crs, 8);
    if (data == NULL)
        return -1;
    *value = ((PY_UINT64_T)data[0] << 56) | ((PY_UINT64_T)data[1] << 48) |
             ((PY_UINT64_T)data[2] << 40) | ((PY_UINT64_T)data[3] << 32) |
             ((PY_UINT64_T)data[4] << 24) | ((PY_UINT64_T)data[5] << 16) |
             ((PY_UINT64_T)data[6] << 8) | data[7];
    return 0;
}

/* ----------- floating point read functions ---------------------------------
 *
 * This is using undocumented python API functions _PyFloat_Unpack8 and
 * _PyFloat_Unpack4.
 *
 * Reasons:
 * * No hassle, especially for 64 bit, converting endiannes in a cross
 *   platform manner
 * * This converts even when the client platform does not use IEEE floating
 *   point. (Can anyone name any?)
 */
int
crs_read_double(ValueCursor *crs, double *value)
{
	char *data;

	data = crs_advance(crs, 8);
    if (data == NULL)
        return -1;
    *value = _PyFloat_Unpack8((unsigned char *)data, 0);
    if (*value == -1.0 && PyErr_Occurred())
        return -1;
    return 0;
}


int
crs_read_float(ValueCursor *crs, double *value)
{
	char *data;

	data = crs_advance(crs, 4);
    if (data == NULL)
        return -1;
    *value = _PyFloat_Unpack4((unsigned char *)data, 0);
    if (*value == -1.0 && PyErr_Occurred())
        return -1;
    return 0;
}
