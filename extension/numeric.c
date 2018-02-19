#include "poque_type.h"


/* ==== int parameter handler =============================================== */

/* Python ints will be converted to pg int4, int8 or text, depending on size.
 *
 * For an array the largest (or smallest) value determine the actual type.
 */


/* struct for storing the parameter values */
typedef struct _IntParam {
    union {
        int int4;           /* int4 value */
        long long int8;     /* int8 value */
        char *string;       /* text value */
    } value;
    Py_ssize_t size;        /* size of text value */
    PyObject *ref;          /* reference to python object */
} IntParam;


typedef struct _IntParamHandler {
    param_handler handler;      /* base handler */
    int num_params;             /* number of parameters */
    int examine_pos;            /* where to examine next */
    int encode_pos;             /* where to encode next */
    /* this union is used to prevent an extra malloc in case of a single
     * value
     */
    union {
        IntParam param;
        IntParam *params;
    } params;                   /* cache of parameter values */
} IntParamHandler;


static int
int_examine_text(IntParamHandler *handler, PyObject *param) {
    /* value(s) too large for PG integer, convert to PG text (i.e. char *) */

    char *val;
    Py_ssize_t size;
    IntParam *ip;

    /* execute equivalent of "str(param)" */
    param = PyObject_Str(param);
    if (param == NULL) {
        return -1;
    }

    /* get access to raw UTF 8 pointer and size in bytes */
    val = PyUnicode_AsUTF8AndSize(param, &size);
    if (val == NULL) {
        Py_DECREF(param);
        return -1;
    }

#if SIZEOF_SIZE_T > SIZEOF_INT
    if (size > INT32_MAX) {
        Py_DECREF(param);
        PyErr_SetString(PyExc_ValueError,
                        "String too long for postgresql");
        return -1;
    }
#endif

    /* set parameter values */
    ip = get_current_examine_param(handler);
    ip->ref = param;    /* keep reference for decrementing refcount later */
    ip->size = size;
    ip->value.string = val;
    return (int)size;
}


static int
int_encode_text(IntParamHandler *handler, PyObject *param, char **loc) {
    IntParam *ip;

    /* just return the earlier stored pointer to the UTF-8 encoded char buffer
     */
    ip = get_current_encode_param(handler);
    *loc = ip->value.string;
    return 0;
}


static int
int_encode_at_text(IntParamHandler *handler, PyObject *param, char *loc) {
    IntParam *ip;
    int size;

    /* copy the earlier retrieved UTF-8 encoded char buffer to location */
    ip = get_current_encode_param(handler);
    size = (int)ip->size;
    memcpy(loc, ip->value.string, size);
    return size;
}


static int
int_total_size_text(IntParamHandler *handler) {
    /* calculate total size using previously retrieved values */
    int i, ret=0;

    for (i = 0; i < handler->examine_pos; i++) {
        IntParam *p;
        p = &handler->params.params[i];
        ret += (int)p->size;
    }
    return ret;
}


static int
int_set_examine_text(IntParamHandler *handler, PyObject *param) {
    int i;
    int examine_pos;

    /* set up handler for text values */
    handler->handler.oid = TEXTOID;
    handler->handler.array_oid = TEXTARRAYOID;
    handler->handler.encode_at = (ph_encode_at)int_encode_at_text;

    if (handler_single(handler)) {
        /* one less malloc and copy */
        handler->handler.encode = (ph_encode)int_encode_text;
    }
    else {
        /* set up stuff for multiple values */
        handler->handler.examine = (ph_examine)int_examine_text;
        if (handler->examine_pos) {
            /* Other values have been examined earlier. First set up size
             * calculation because the running total of the previous examine
             * results gives the wrong number.
             */
            handler->handler.total_size = (ph_total_size)int_total_size_text;

            /* and then rewrite the cached values */
            examine_pos = handler->examine_pos;
            handler->examine_pos = 0;
            for (i = 0; i < examine_pos; i++) {
                IntParam *p;
                p = &handler->params.params[i];
                if (int_examine_text(handler, p->ref) < 0) {
                    return -1;
                }
            }
        }
    }
    return int_examine_text(handler, param);
}

static int
int_examine_int8(IntParamHandler *handler, PyObject *param) {
    long long val;
    int overflow;
    IntParam *ip;

    val = PyLong_AsLongLongAndOverflow(param, &overflow);
    if (overflow) {
        /* value does not fit into 64 bits integer, use text */
        return int_set_examine_text(handler, param);
    }
    ip = get_current_examine_param(handler);
    ip->ref = param;
    ip->value.int8 = val;
    return 8;
}


static int
int_encode_at_int8(IntParamHandler *handler, PyObject *param, char *loc) {
    IntParam *ip;

    ip = get_current_encode_param(handler);
    write_uint64(&loc, ip->value.int8);
    return 8;
}


static int
int_total_size_int8(IntParamHandler *handler) {
    /* returns the total size for 64 bits integer values */
    return handler->examine_pos * 8;
}


static int
int_set_examine_int8(IntParamHandler *handler, PyObject *param) {
    int i;

    /* set up handler for pg type INT8 */
    handler->handler.oid = INT8OID;
    handler->handler.array_oid = INT8ARRAYOID;
    handler->handler.encode_at = (ph_encode_at)int_encode_at_int8;

    /* if this is the only parameter, don't bother about the rest */
    if (!handler_single(handler)) {
        handler->handler.examine = (ph_examine)int_examine_int8;
        if (handler->examine_pos) {
            /* Other values have been examined earlier. First set up size
             * calculation because the running total of previous examine results
             * gives the wrong number.
             */
            handler->handler.total_size = (ph_total_size)int_total_size_int8;
            /* and then rewrite the cached values */
            for (i = 0; i < handler->examine_pos; i++) {
                IntParam *p;
                p = &handler->params.params[i];
                p->value.int8 = p->value.int4;
            }
        }
    }
    return int_examine_int8(handler, param);
}

static int
int_examine(IntParamHandler *handler, PyObject *param) {
    long val;
    int overflow;
    IntParam *ip;

    val = PyLong_AsLongAndOverflow(param, &overflow);
#if SIZEOF_LONG == 4    /* for example on windows or 32 bits linux */
    if (overflow) {
        /* value does not fit in 32 bits, try with 64 bit integer instead */
        return int_set_examine_int8(handler, param);
    }
#else                   /* for example 64 bits linux */
    if (overflow) {
        /* value does not fit in 64 bits, use text instead */
        return int_set_examine_text(handler, param);
    }
    if (val < INT32_MIN || val > INT32_MAX) {
        /* value outside 32 bit range, use 64 bit integer instead */
        return int_set_examine_int8(handler, param);
    }
#endif
    /* value fits in 32 bits, set up parameter */
    ip = get_current_examine_param(handler);
    ip->ref = param;
    ip->value.int4 = val;
    return 4;
}


static int
int_encode_at(IntParamHandler *handler, PyObject *param, char *loc)
{
    IntParam *ip;

    ip = get_current_encode_param(handler);
    write_uint32(&loc, ip->value.int4);
    return 4;
}


static void
int_handler_free(IntParamHandler *handler)
{
    int i;

    if (handler_single(handler)) {
        if (handler->handler.oid == TEXTOID) {
            /* clean up cached PyUnicode value */
            Py_XDECREF(handler->params.param.ref);
        }
    }
    else {
        if (handler->handler.oid == TEXTOID) {
            /* clean up cached PyUnicode value */
            for (i = 0; i < handler->examine_pos; i++) {
                Py_DECREF(handler->params.params[i].ref);
            }
        }
        /* deallocate param array */
        PyMem_Free(handler->params.params);
    }

    /* and free ourselves */
    PyMem_Free(handler);
}


IntParamHandler *
new_int_param_handler(int num_params) {
    static IntParamHandler def_handler = {
        {
            (ph_examine)int_examine,        /* examine */
            NULL,                           /* total_size */
            NULL,                           /* encode */
            (ph_encode_at)int_encode_at,    /* encode_at */
            (ph_free)int_handler_free,      /* free */
            INT4OID,                        /* oid */
            INT4ARRAYOID,                   /* array_oid */
        },
        0
    }; /* static initialized handler */
	return_param_var_handler(def_handler, IntParamHandler, IntParam);
}


static PyObject *
int16_binval(data_crs *crs)
{
    poque_int16 value;
    if (crs_read_int16(crs, &value) < 0)
        return NULL;
    return PyLong_FromLong(value);
}


static PyObject *
uint32_binval(data_crs *crs)
{
    PY_UINT32_T value;
    if (crs_read_uint32(crs, &value) < 0)
        return NULL;
    return PyLong_FromLongLong(value);
}


static PyObject *
int32_binval(data_crs *crs)
{
    PY_INT32_T value;
    if (crs_read_int32(crs, &value) < 0)
        return NULL;
    return PyLong_FromLong(value);
}


static PyObject *
int64_binval(data_crs *crs)
{
    PY_INT64_T value;

    if (crs_read_int64(crs, &value) < 0)
        return NULL;
    return PyLong_FromLongLong((PY_INT64_T)value);
}


static PyObject *
int_strval(data_crs *crs)
{
    char *data, *pend;
    PyObject *value;

    data = crs_advance_end(crs);

    value = PyLong_FromString(data, &pend, 10);
    if (value != NULL && pend != crs_end(crs)) {
        PyErr_SetString(PoqueError, "Invalid value for text integer value");
        return NULL;
    }
    return value;
}


static PyObject *
bool_binval(data_crs *crs) {
    char data;

    if (crs_read_char(crs, &data) < 0)
    	return NULL;
    return PyBool_FromLong(data);
}


static PyObject *
bool_strval(data_crs *crs) {
    char data;

    if (crs_read_char(crs, &data) < 0)
    	return NULL;
    return PyBool_FromLong(data == 't');
}


static int
bool_examine(param_handler *handler, PyObject *param) {
    return 1;
}


static int
bool_encode_at(
        param_handler *handler, PyObject *param, char *loc) {

    loc[0] = (param == Py_True);
    return 1;
}


static param_handler bool_param_handler = {
    bool_examine,   /* examine */
    NULL,           /* total_size */
    NULL,           /* encode */
    bool_encode_at, /* encode_at */
    NULL,           /* free */
    BOOLOID,        /* oid */
    BOOLARRAYOID    /* array_oid */
}; /* static initialized handler */


static param_handler *
new_bool_param_handler(int num_param) {
    return &bool_param_handler;
}


static PyObject *
float64_binval(data_crs *crs)
{
    double val;
    if (crs_read_double(crs, &val) < 0)
        return NULL;
    return PyFloat_FromDouble(val);
}


static PyObject *
float32_binval(data_crs *crs)
{
	double val;
	if (crs_read_float(crs, &val) < 0)
		return NULL;
	return PyFloat_FromDouble(val);
}


static PyObject *
float_strval(data_crs *crs)
{
	double val;
	char *data, *pend;

	data = crs_advance_end(crs);
	errno = 0;
	val = PyOS_string_to_double(data, &pend, PoqueError);
	if (val == -1.0 && PyErr_Occurred())
		return NULL;
	if (pend != crs_end(crs)) {
		PyErr_SetString(PoqueError, "Invalid floating point value");
		return NULL;
	}
	return PyFloat_FromDouble(val);
}


static int
float_examine(param_handler *handler, PyObject *param) {
	return SIZEOF_DOUBLE;
}

static int
float_encode_at(
		param_handler *handler, PyObject *param, char *loc) {
	double v;

	v = PyFloat_AS_DOUBLE(param);
	if (_PyFloat_Pack8(v, (unsigned char *)loc, 0) < 0) {
		return -1;
	}
	return SIZEOF_DOUBLE;
}


static param_handler float_param_handler = {
    float_examine,
    NULL,
    NULL,
    float_encode_at,
    NULL,
    FLOAT8OID,
    FLOAT8ARRAYOID
}; /* static initialized handler */


static param_handler *
new_float_param_handler(int num_param) {
    return &float_param_handler;
}


/* struct for storing the parameter values */
typedef struct _DecimalParam {
    char *data;       /* encoded value */
    int size;  /* size of encoded value */
} DecimalParam;


typedef struct _DecimalParamHandler {
    param_handler handler;      /* base handler */
    int num_params;             /* number of parameters */
    int examine_pos;            /* where to examine next */
    int encode_pos;             /* where to encode next */
    /* this union is used to prevent an extra malloc in case of a single
     * value
     */
    union {
        DecimalParam param;
        DecimalParam *params;
    } params;                   /* cache of parameter values */
} DecimalParamHandler;


static inline int
decimal_check_infinite(PyObject *decimal) {
    PyObject *py_obj;
    int ret = 0;

    py_obj = PyObject_CallMethod(decimal, "is_infinite", NULL);
    if (py_obj == NULL) {
        return -1;
    }
    if (PyObject_IsTrue(py_obj)) {
        PyErr_SetString(PyExc_ValueError,
                        "PostgreSQL does not support decimal infinites");
        ret = -1;
    }
    Py_DECREF(py_obj);
    return ret;
}

static inline int
decimal_check_nan(PyObject *decimal) {
    PyObject *py_obj;
    int ret = 0;

    py_obj = PyObject_CallMethod(decimal, "is_nan", NULL);
    if (py_obj == NULL) {
        return -1;
    }
    ret = PyObject_IsTrue(py_obj);
    Py_DECREF(py_obj);
    return ret;
}


#define MAX_PG_WEIGHT 0x7FFF
#define MAX_DEC_WEIGHT ((MAX_PG_WEIGHT + 1) * 4)

static int
decimal_examine(DecimalParamHandler *handler, PyObject *param)
{
    PyObject *py_obj, *val = NULL;
    DecimalParam *np;
    int i, j, size=-1, isnan;
    char *data, *pos;
    poque_uint16 pg_sign, dscale, pg_digit;
    Py_ssize_t npg_digits, ndigits;
    poque_int16 pg_weight;

    /* Check if it is an infinite Decimal. PostgreSQL does not support
     * infinite.
     */
    if (decimal_check_infinite(param) == -1) {
        return -1;
    }

    /*  Check if it is a NaN */
    isnan = decimal_check_nan(param);
    if (isnan == -1) {
        return -1;
    }

    if (isnan) {
        /* it is a NaN */
        pg_weight = 0;
        pg_sign = NUMERIC_NAN;
        dscale = 0;
        npg_digits = 0;
        ndigits = 0;
        j = 0;
    }
    else {
        /* normal decimal */
        Py_ssize_t exp;
        int dec_weight, q, r;

        /* get sign, digits and exponent */
        val = PyObject_CallMethod(param, "as_tuple", NULL);
        if (val == NULL) {
            return -1;
        }

        /* get pg sign */
        py_obj = PyTuple_GET_ITEM(val, 0);
        pg_sign = PyLong_AsLong(py_obj) ? NUMERIC_NEG : NUMERIC_POS;

        /* get pg dscale */
        py_obj = PyTuple_GET_ITEM(val, 2);
        exp = PyLong_AsSsize_t(py_obj);
        if ((exp == -1 && PyErr_Occurred()) || exp < -0x3FFF) {
            /* In case of positive overflow the exponent is too large too
             * translate to a PG exponent (pg_weight, see later)
             *
             * negative exponent is the same as the positive pg dscale
             * Maximum value for dscale is 0x3FFF.
             *
             *
             */
            Py_DECREF(val);
            PyErr_SetString(PyExc_ValueError,
                            "Exponent out of PostgreSQL range");
            return -1;
        }
        dscale = exp > 0 ? 0 : -exp;

        /* now get the pg digits */
        py_obj = PyTuple_GET_ITEM(val, 1);
        ndigits = PyTuple_GET_SIZE(py_obj);

        /* calculate pg_weight */
        if (exp - MAX_DEC_WEIGHT > -ndigits) {
            /* overflow safe version of
             *
             * if (ndigits + exp > MAX_DEC_WEIGHT)
             *
             * ndigits will be equal to or greater than zero
             * exp will not be less than -0x3fff (-16383)
             */
            Py_DECREF(val);
            PyErr_SetString(PyExc_ValueError,
                            "Decimal out of PostgreSQL range");
            return -1;
        }

        dec_weight = ndigits + exp;
        q = dec_weight / 4;
        r = dec_weight % 4;
        if (r < 0) {
            /* correct for negative values */
            r += 4;
            q--;
        }
        pg_weight = q + (r > 0) - 1;

        /* Calculate number of pg digits
         * Decimal digits are grouped with four of them in one pg_digit. The
         * decimal digits are aligned around the decimal point.
         *
         * For example the value 12.34 will be stored in two pg digits
         * (0012 3400) because of alignment.
         *
         * Calculation works as follows. First divide (integral) the number of
         * decimal digits by 4. This is the base. Because of alignment, the
         * first pg digit might consist of 'r' decimal digits if any. If 'r' is
         * non zero, we'll need an extra pg_digit.
         * If there are more decimal digits left, i.e. the remainder of the
         * earlier division has more decimal digits than are present in the
         * 'r' digit, we'll need yet an extra pg_digit. For example 12345.67
         * needs 3 digits: 0001 2345 6700
         */
        npg_digits = ndigits / 4 + (r > 0) + (r < ndigits % 4);

        j = (4 - r) % 4;                /* set up for loop */
    }

    /* we know the number of digits, now we know the memory size */
    data = PyMem_Calloc(1, 8 + npg_digits * 2);
    if (data == NULL) {
        Py_XDECREF(val);
        PyErr_NoMemory();
        return -1;
    }

    /* write the pg digits */
    pos = data + 8;     /* position past header */
    pg_digit = 0;
    for (i = 0; i < ndigits; i++) {
        int digit;

        digit = PyLong_AsLong(PyTuple_GET_ITEM(py_obj, i));
        pg_digit *= 10;
        pg_digit += digit;
        if (++j == 4) {
            write_uint16(&pos, pg_digit);
            pg_digit = 0;
            j = 0;
        }
    }
    if (j) {
        for (i = 0; i < (4 - j); i++) {
            pg_digit *= 10;
        }
        write_uint16(&pos, pg_digit);
    }
    Py_XDECREF(val);

    /* write header */
    pos = data;
    write_uint16(&pos, (poque_uint16)npg_digits);
    write_uint16(&pos, pg_weight);
    write_uint16(&pos, pg_sign);
    write_uint16(&pos, dscale);

    /* set parameter values */
    np = get_current_examine_param(handler);
    np->data = data;
    size = 8 + npg_digits * 2;
    np->size = size;
    return size;
}


static int
decimal_encode(DecimalParamHandler *handler, PyObject *param, char **loc)
{
    DecimalParam *np;

    /* return the earlier encoded buffer */
    np = get_current_encode_param(handler);
    *loc = np->data;
    return 0;
}


static int
decimal_encode_at(DecimalParamHandler *handler, PyObject *param, char *loc) {
    DecimalParam *np;
    int size;

    /* copy the earlier encoded buffer to location */
    np = get_current_encode_param(handler);
    size = np->size;
    memcpy(loc, np->data, size);
    return size;
}


static void
decimal_handler_free(DecimalParamHandler *handler)
{
    int i;

    if (handler_single(handler)) {
        PyMem_Free(handler->params.param.data);
    }
    else {
        /* clean up cached values */
        for (i = 0; i < handler->examine_pos; i++) {
            PyMem_Free(handler->params.params[i].data);
        }
        /* deallocate param array */
        PyMem_Free(handler->params.params);
    }

    /* and free ourselves */
    PyMem_Free(handler);
}


DecimalParamHandler *
new_decimal_param_handler(int num_params) {
    static DecimalParamHandler def_handler = {
        {
            (ph_examine)decimal_examine,        /* examine */
            NULL,                               /* total_size */
            (ph_encode)decimal_encode,          /* encode */
            (ph_encode_at)decimal_encode_at,    /* encode_at */
            (ph_free)decimal_handler_free,      /* free */
            NUMERICOID,                         /* oid */
            NUMERICARRAYOID,                    /* array_oid */
        },
        0
    }; /* static initialized handler */

	return_param_var_handler(def_handler, DecimalParamHandler, DecimalParam);
}


/* reference to decimal.Decimal */
static PyObject *PyDecimal;

static PyObject *
numeric_strval(data_crs *crs) {
    /* Create a Decimal from a text value */
    char *data;

    data = crs_advance_end(crs);
    return PyObject_CallFunction(PyDecimal, "s#", data, crs_len(crs));
}


static int
numeric_set_digit(PyObject *digit_tuple, int val, Py_ssize_t idx) {
    PyObject *digit;

    /* create the digit */
    digit = PyLong_FromLong(val);
    if (digit == NULL)
        return -1;

    /* Add it to the tuple */
    PyTuple_SET_ITEM(digit_tuple, idx, digit);
    return 0;
}


static PyObject *
numeric_binval(data_crs *crs) {
    /* Create a Decimal from a pg numeric binary value
     *
     * PG numerics are not entirely the same as Python decimals, but they are
     * close enough. Major difference is the precision.
     * In Python the precision is determined from the number of significant
     * digits. For example, the literal '9.9E6' has a precision of 2.
     * PostgreSQL numerics have a precision that is number of digits before the
     * decimal point plus the optionally available number of digits after the
     * decimal point. The literal '9.9E6' will therefore have a precision of
     * 7 and will be rendered in text mode as '9900000'. Execute the following
     * in psql for a demonstration: SELECT '9.9E6'::numeric;
     *
     * On the wire, trailing zeroes are not present, but can be deduced from the
     * weight, which is like the decimal exponent, but than with a 10000 base
     * instead of 10.
     *
     * This conversion function will add the zeroes, so the precision of both
     * the binary as the text conversion deliver the same result
     *
     */
    poque_uint16 npg_digits, sign, dscale;
    poque_int16 weight;
    PyObject *digits, *ret=NULL, *zero=NULL;
    Py_ssize_t ndigits, j, i;

    /* Get the field values */
    if (crs_read_uint16(crs, &npg_digits) < 0)
        return NULL;
    if (crs_read_int16(crs, &weight) < 0)
        return NULL;
    if (crs_read_uint16(crs, &sign) < 0)
        return NULL;
    if (crs_read_uint16(crs, &dscale) < 0)
        return NULL;
    /* TODO check valid scale like postgres does */

    /* Check sign */
    if (sign == NUMERIC_NAN) {
        /* We're done it's a NaN */
        return PyObject_CallFunction(PyDecimal, "s", "NaN");
    }
    if (sign == NUMERIC_NEG) {
        sign = 1;
    }
    else if (sign != NUMERIC_POS) {
        PyErr_SetString(PoqueError, "Invalid value for numeric sign");
        return NULL;
    }

    /* create a tuple to hold the digits */
    ndigits = dscale + (weight + 1) * 4;
    digits = PyTuple_New(ndigits);
    if (digits == NULL) {
    	return NULL;
    }

    /* fill the digits from pg digits */
    j = 0;
    for (i = 0; i < npg_digits; i++) {
        /* fill from postgres digits. A postgres digit contains 4 decimal
         * digits */
        poque_uint16 pg_digit;

        if (crs_read_uint16(crs, &pg_digit) < 0)
            goto end;
        if (pg_digit > 9999) {
            PyErr_SetString(PoqueError, "Invalid numeric value");
            goto end;
        }
        if (numeric_set_digit(digits, pg_digit / 1000, j++) < 0)
            goto end;
        if (j == ndigits) {
            break;
        }
        pg_digit = pg_digit % 1000;
        if (numeric_set_digit(digits, pg_digit / 100, j++) < 0)
            goto end;
        if (j == ndigits) {
            break;
        }
        pg_digit = pg_digit % 100;
        if (numeric_set_digit(digits, pg_digit / 10, j++) < 0)
            goto end;
        if (j == ndigits) {
            break;
        }
        if (numeric_set_digit(digits, pg_digit % 10, j++) < 0)
            goto end;
        if (j == ndigits) {
            break;
        }
    }

    if (j < ndigits) {
        /* add trailing zeroes */
        zero = PyLong_FromLong(0);
        if (zero == NULL) {
            goto end;
        }
        for (; j < ndigits; j++) {
            Py_INCREF(zero);
            PyTuple_SET_ITEM(digits, j, zero);
        }
        Py_DECREF(zero);
    }

    /* create the Decimal now */
    ret = PyObject_CallFunction(
        PyDecimal, "((HOi))", sign, digits, -(int)dscale);

end:
    Py_DECREF(digits);
    return ret;
}

static PoqueTypeEntry numeric_value_handlers[] = {
    {INT4OID, int32_binval, int_strval, InvalidOid, NULL},
    {INT8OID, int64_binval, int_strval, InvalidOid, NULL},
    {FLOAT8OID, float64_binval, float_strval, InvalidOid, NULL},
    {INT2OID, int16_binval, int_strval, InvalidOid, NULL},
    {BOOLOID, bool_binval, bool_strval, InvalidOid, NULL},
    {NUMERICOID, numeric_binval, numeric_strval, InvalidOid, NULL},
    {FLOAT4OID, float32_binval, float_strval, InvalidOid, NULL},
    {CASHOID, int64_binval, NULL, InvalidOid, NULL},
    {OIDOID, uint32_binval, int_strval, InvalidOid, NULL},
    {XIDOID, uint32_binval, int_strval, InvalidOid, NULL},
    {CIDOID, uint32_binval, int_strval, InvalidOid, NULL},
    {REGPROCOID, uint32_binval, NULL, InvalidOid, NULL},

    {INT4ARRAYOID, array_binval, NULL, INT4OID, NULL},
    {INT8ARRAYOID, array_binval, NULL, INT8OID, NULL},
    {FLOAT8ARRAYOID, array_binval, NULL, FLOAT8OID, NULL},
    {INT2ARRAYOID, array_binval, NULL, INT2OID, NULL},
    {BOOLARRAYOID, array_binval, NULL, BOOLOID, NULL},
    {NUMERICARRAYOID, array_binval, NULL, NUMERICOID, NULL},
    {FLOAT4ARRAYOID, array_binval, NULL, FLOAT4OID, NULL},
    {CASHARRAYOID, array_binval, NULL, CASHOID, NULL},
    {OIDARRAYOID, array_binval, NULL, OIDOID, NULL},
    {XIDARRAYOID, array_binval, NULL, XIDOID, NULL},
    {CIDARRAYOID, array_binval, NULL, CIDOID, NULL},
    {REGPROCARRAYOID, array_binval, NULL, REGPROCOID, NULL},
    {InvalidOid}
};

int
init_numeric(void) {

    PyDecimal = load_python_object("decimal", "Decimal");
    if (PyDecimal == NULL)
        return -1;

    register_value_handler_table(numeric_value_handlers);

    register_parameter_handler(&PyLong_Type, (ph_new)new_int_param_handler);
    register_parameter_handler(&PyFloat_Type, new_float_param_handler);
    register_parameter_handler(&PyBool_Type, new_bool_param_handler);
    register_parameter_handler((PyTypeObject *)PyDecimal,
                               (ph_new)new_decimal_param_handler);

    return 0;
}
