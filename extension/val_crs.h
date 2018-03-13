#ifndef _POQUE_VALCRS_H_
#define _POQUE_VALCRS_H_


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

#define CHECK_LENGTH_LT(l, s, t, r) if ((l) < (s)) {\
		PyErr_SetString(PoqueError, "Invalid data for " t " type.");\
		return (r);\
	}

#define CHECK_LENGTH_EQ(l, s, t, r) if ((l) != (s)) {\
		PyErr_SetString(PoqueError, "Invalid data for " t " type.");\
		return (r);\
	}

#define ADVANCE_DATA(d, l, s) d += (s); l -= s

#endif
