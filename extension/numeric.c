#include "poque_type.h"


/* ==== int parameter handler =============================================== */

/* Python ints will be converted to pg int4, int8 or text, depending on size.
 *
 * For an array the largest (or smallest) value determine the actual type.
 */

#define handler_single(h) ((h)->num_params == 1)

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


static inline IntParam *
int_get_current_param(IntParamHandler *handler, int *pos) {
    /* gets the param value to work with and advances the index in case of
     * multiple value
     */
    if (handler_single(handler)) {
        return &handler->params.param;
    } else {
        return &handler->params.params[(*pos)++];
    }
}


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

    /* set parameter values */
    ip = int_get_current_param(handler, &handler->examine_pos);
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
    ip = int_get_current_param(handler, &handler->encode_pos);
    *loc = ip->value.string;
    return 0;
}


static int
int_encode_at_text(IntParamHandler *handler, PyObject *param, char *loc) {
    IntParam *ip;
    int size;

    /* copy the earlier retrieved UTF-8 encoded char buffer to location */
    ip = int_get_current_param(handler, &handler->encode_pos);
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
    ip = int_get_current_param(handler, &handler->examine_pos);
    ip->ref = param;
    ip->value.int8 = val;
    return 8;
}


static int
int_encode_at_int8(IntParamHandler *handler, PyObject *param, char *loc) {
    IntParam *ip;

    ip = int_get_current_param(handler, &handler->encode_pos);
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
    ip = int_get_current_param(handler, &handler->examine_pos);
    ip->ref = param;
    ip->value.int4 = val;
    return 4;
}


static int
int_encode_at(IntParamHandler *handler, PyObject *param, char *loc)
{
    IntParam *ip;

    ip = int_get_current_param(handler, &handler->encode_pos);
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


param_handler *
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
    IntParamHandler *handler;

    /* create new handler identical to static one */
    handler = (IntParamHandler *)new_param_handler(
        (param_handler *)&def_handler, sizeof(IntParamHandler));
    if (handler == NULL) {
        return NULL;
    }

    /* initialize IntParamHandler specifics */
    handler->num_params = num_params;
    if (!handler_single(handler)) {
        handler->params.params = PyMem_Calloc(num_params, sizeof(IntParam));
        if (handler->params.params == NULL) {
            return (param_handler *)PyErr_NoMemory();
        }
    }

    return (param_handler *)handler;
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
	return sizeof(double);
}

static int
float_encode_at(
		param_handler *handler, PyObject *param, char *loc) {
	double v;

	v = PyFloat_AsDouble(param);
	if (_PyFloat_Pack8(v, (unsigned char *)loc, 0) < 0) {
		return -1;
	}
	return sizeof(double);
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
typedef struct _NumericParam {
    char *data;       /* encoded value */
    int size;  /* size of encoded value */
} NumericParam;


typedef struct _NumericParamHandler {
    param_handler handler;      /* base handler */
    int num_params;             /* number of parameters */
    int examine_pos;            /* where to examine next */
    int encode_pos;             /* where to encode next */
    /* this union is used to prevent an extra malloc in case of a single
     * value
     */
    union {
        NumericParam param;
        NumericParam *params;
    } params;                   /* cache of parameter values */
} NumericParamHandler;


static inline NumericParam *
numeric_get_current_param(NumericParamHandler *handler, int *pos) {
    /* gets the param value to work with and advances the index in case of
     * multiple value
     */
    if (handler_single(handler)) {
        return &handler->params.param;
    } else {
        return &handler->params.params[(*pos)++];
    }
}


static int
numeric_examine(NumericParamHandler *handler, PyObject *param)
{
    PyObject *py_obj, *val = NULL;
    NumericParam *np;
    int i, j, ndigits, size=-1, isnan;
    char *data, *pos;
    poque_uint16 npg_digits, sign, dscale, pg_digit;
    poque_int16 pg_weight;

    /* Check if it is an infinite Decimal. PostgreSQL does not support
     * infinite.
     */
    py_obj = PyObject_CallMethod(param, "is_infinite", NULL);
    if (py_obj == NULL) {
        return -1;
    }
    if (PyObject_IsTrue(py_obj)) {
        PyErr_SetString(PyExc_ValueError,
                        "PostgreSQL does not support decimal infinites");
        Py_DECREF(py_obj);
        return -1;
    }
    Py_DECREF(py_obj);

    /* First check if it is a NaN */
    py_obj = PyObject_CallMethod(param, "is_nan", NULL);
    if (py_obj == NULL) {
        return -1;
    }
    isnan = PyObject_IsTrue(py_obj);
    Py_DECREF(py_obj);

    if (isnan) {
        /* it is a NaN */
        pg_weight = 0;
        sign = NUMERIC_NAN;
        dscale = 0;
        npg_digits = 0;
        ndigits = 0;
    }
    else {
        /* normal decimal */
        long exp;
        int dec_weight, q, r;

        /* get sign, digits and exponent */
        val = PyObject_CallMethod(param, "as_tuple", NULL);
        if (val == NULL) {
            return -1;
        }

        /* get pg sign */
        py_obj = PyTuple_GET_ITEM(val, 0);
        sign = PyLong_AsLong(py_obj) ? NUMERIC_NEG : NUMERIC_POS;

        /* get pg dscale */
        py_obj = PyTuple_GET_ITEM(val, 2);
        exp = PyLong_AsLong(py_obj);
        dscale = exp > 0 ? 0 : -exp;

        /* now get the pg digits */
        py_obj = PyTuple_GET_ITEM(val, 1);
        ndigits = PyTuple_GET_SIZE(py_obj);

        dec_weight = ndigits + exp;
        q = dec_weight / 4;
        r = dec_weight % 4;
        pg_weight = q + (r > 0) - 1;

        npg_digits = ndigits / 4 + 2;
        j = (4 - r) % 4;
    }

    /* we know the number of digits, now we know the memory size */
    data = PyMem_Malloc(8 + npg_digits * 2);
    if (data == NULL) {
        Py_XDECREF(val);
        PyErr_NoMemory();
        return -1;
    }

    /* write the pg digits */
    pos = data + 8;     /* position past header */
    pg_digit = 0;
    npg_digits = 0;
    for (i = 0; i < ndigits; i++) {
        int digit;

        digit = PyLong_AsLong(PyTuple_GET_ITEM(py_obj, i));
        pg_digit *= 10;
        pg_digit += digit;
        if (++j == 4) {
            write_uint16(&pos, pg_digit);
            npg_digits += 1;
            pg_digit = 0;
            j = 0;
        }
    }
    if (pg_digit) {
        for (i = 0; i < (4 - j); i++) {
            pg_digit *= 10;
        }
        write_uint16(&pos, pg_digit);
        npg_digits += 1;
    }
    Py_XDECREF(val);

    /* write header */
    pos = data;
    write_uint16(&pos, npg_digits);
    write_uint16(&pos, pg_weight);
    write_uint16(&pos, sign);
    write_uint16(&pos, dscale);

    /* set parameter values */
    np = numeric_get_current_param(handler, &handler->examine_pos);
    np->data = data;
    size = 8 + npg_digits * 2;
    np->size = size;
    return size;
}


static int
numeric_encode(NumericParamHandler *handler, PyObject *param, char **loc)
{
    NumericParam *np;

    /* copy the earlier retrieved encoded buffer to location */
    np = numeric_get_current_param(handler, &handler->encode_pos);
    *loc = np->data;
    return 0;
}


static int
numeric_encode_at(NumericParamHandler *handler, PyObject *param, char *loc) {
    NumericParam *np;
    int size;

    /* copy the earlier retrieved encoded buffer to location */
    np = numeric_get_current_param(handler, &handler->encode_pos);
    size = np->size;
    memcpy(loc, np->data, size);
    return size;
}


static void
numeric_handler_free(NumericParamHandler *handler)
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


param_handler *
new_numeric_param_handler(int num_params) {
    static NumericParamHandler def_handler = {
        {
            (ph_examine)numeric_examine,        /* examine */
            NULL,                               /* total_size */
            (ph_encode)numeric_encode,          /* encode */
            (ph_encode_at)numeric_encode_at,    /* encode_at */
            (ph_free)numeric_handler_free,      /* free */
            NUMERICOID,                         /* oid */
            NUMERICARRAYOID,                    /* array_oid */
        },
        0
    }; /* static initialized handler */
    NumericParamHandler *handler;

    /* create new handler identical to static one */
    handler = (NumericParamHandler *)new_param_handler(
        (param_handler *)&def_handler, sizeof(NumericParamHandler));
    if (handler == NULL) {
        return NULL;
    }

    /* initialize NumericParamHandler specifics */
    handler->num_params = num_params;
    if (!handler_single(handler)) {
        handler->params.params = PyMem_Calloc(num_params, sizeof(NumericParam));
        if (handler->params.params == NULL) {
            return (param_handler *)PyErr_NoMemory();
        }
    }

    return (param_handler *)handler;
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
    /* Create a Decimal from a binary value */
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

    register_parameter_handler(&PyLong_Type, new_int_param_handler);
    register_parameter_handler(&PyFloat_Type, new_float_param_handler);
    register_parameter_handler(&PyBool_Type, new_bool_param_handler);
    register_parameter_handler((PyTypeObject *)PyDecimal, new_numeric_param_handler);

    return 0;
}
