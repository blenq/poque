#include "poque_type.h"


#define int_handler_single(h) ((h)->num_params == 1)

typedef struct _IntParam {
    union {
        int int4;
        long long int8;
        char *string;
    } value;
    Py_ssize_t size;
    PyObject *ref;
    PyObject *s;
} IntParam;


typedef struct _IntParamHandler {
    param_handler handler;
    int num_params;
    int examine_pos;
    int encode_pos;
    /* this union is used to prevent an extra malloc in case of a single
     * value
     */
    union {
        IntParam param;
        IntParam *params;
    } params;
} IntParamHandler;


static inline IntParam *
int_get_current_param(IntParamHandler *handler, int *pos) {
    if (int_handler_single(handler)) {
        return &handler->params.param;
    } else {
        return &handler->params.params[(*pos)++];
    }
}


static int
int_examine_text(IntParamHandler *handler, PyObject *param) {
    char *val;
    Py_ssize_t size;
    IntParam *ip;

    param = PyObject_Str(param);
    if (param == NULL) {
        return -1;
    }
    val = PyUnicode_AsUTF8AndSize(param, &size);
    if (val == NULL) {
        Py_DECREF(param);
        return -1;
    }
    ip = int_get_current_param(handler, &handler->examine_pos);
    ip->ref = param; /* keep reference for decrementing refcount later */
    ip->size = size;
    ip->value.string = val;
    return size;
}


static int
int_encode_text(IntParamHandler *handler, PyObject *param, char **loc) {
    IntParam *ip;

    ip = int_get_current_param(handler, &handler->encode_pos);
    *loc = ip->value.string;
    return 0;
}


static int
int_encode_at_text(IntParamHandler *handler, PyObject *param, char *loc) {
    IntParam *ip;
    int size;

    ip = int_get_current_param(handler, &handler->encode_pos);
    size = ip->size;
    memcpy(loc, ip->value.string, size);
    return size;
}


static int
int_total_size_text(IntParamHandler *handler) {
    /* calculate total size using previously cached values */
    int i, ret=0;

    for (i = 0; i < handler->examine_pos; i++) {
        IntParam *p;
        p = &handler->params.params[i];
        ret += p->size;
    }
    return ret;
}


static int
int_set_examine_text(IntParamHandler *handler, PyObject *param) {
    int i;

    handler->handler.oid = TEXTOID;
    handler->handler.array_oid = TEXTARRAYOID;
    handler->handler.encode_at = (ph_encode_at)int_encode_at_text;

    if (int_handler_single(handler)) {
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
            for (i = 0; i < handler->examine_pos; i++) {
                IntParam *p;
                char *val;
                PyObject *py_str;

                p = &handler->params.params[i];

                /* replace ref with string version */
                py_str = PyObject_Str(p->ref);
                if (py_str == NULL) {
                    /* free uses examine_pos in decref loop */
                    handler->examine_pos = i;
                    return -1;
                }
                p->ref = py_str;

                /* get char pointer and size and store for later */
                val = PyUnicode_AsUTF8AndSize(p->ref, &p->size);
                if (val == NULL) {
                    Py_DECREF(py_str);
                    /* free uses examine_pos in decref loop */
                    handler->examine_pos = i;
                    return -1;
                }
                p->value.string = val;
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
        return int_set_examine_text(handler, param);
    }
    ip = int_get_current_param(handler, &handler->examine_pos);
    ip->ref = param;
    ip->value.int8 = val;
    return 8;
}


void
write_uint64(char **p, PY_UINT64_T val) {
    int i;
    unsigned char *q = (unsigned char *)*p;

    for (i = 7; i >=0; i--) {
        *(q + i) = (unsigned char)(val & 0xffL);
         val >>= 8;
    }
    *p += 8;
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
    if (!int_handler_single(handler)) {
        handler->handler.examine = (ph_examine)int_examine_int8;
        if (handler->examine_pos) {
            /* Other values have been examined earlier. First set up size
             * calculation because the summing the previous examine results
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
#if SIZEOF_LONG == 4
    if (overflow) {
        return int_set_examine_int8(handler, param);
    }
#else
    if (overflow) {
        return int_set_examine_text(handler, param);
    }
    if (val < INT32_MIN || val > INT32_MAX) {
        return int_set_examine_int8(handler, param);
    }
#endif
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

    if (int_handler_single(handler)) {
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
    if (!int_handler_single(handler)) {
        handler->params.params = PyMem_Calloc(num_params, sizeof(IntParam));
        if (handler->params.params == NULL) {
            return NULL;
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
	float val;
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


static int
numeric_set_digit(PyObject *digit_tuple, int val, int idx) {
    PyObject *digit;

    /* create the digit */
    digit = PyLong_FromLong(val);
    if (digit == NULL)
        return -1;

    /* Add it to the tuple */
    PyTuple_SET_ITEM(digit_tuple, idx, digit);
    return 0;
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


static PyObject *
numeric_binval(data_crs *crs) {
    /* Create a Decimal from a binary value */
    poque_uint16 npg_digits, sign, dscale;
    poque_int16 weight;
    int ndigits, i, j;
    PyObject *digits, *ret=NULL;

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
    } else if (sign == NUMERIC_NEG) {
        sign = 1;
    } else if (sign != NUMERIC_POS) {
        PyErr_SetString(PoqueError, "Invalid value for numeric sign");
        return NULL;
    }

    /* calculate number of digits of the Python Decimal */
    ndigits = dscale + (weight + 1) * 4;

    /* create a tuple to hold the digits */
    digits = PyTuple_New(ndigits);
    if (digits == NULL) {
    	return NULL;
    }

    /* fill the digits */
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

        if (j == ndigits)
        	continue;
        if (numeric_set_digit(digits, pg_digit / 1000, j++) < 0)
            goto end;

        if (j == ndigits)
        	continue;
        pg_digit = pg_digit % 1000;
        if (numeric_set_digit(digits, pg_digit / 100, j++) < 0)
            goto end;

        if (j == ndigits)
        	continue;
        pg_digit = pg_digit % 100;
        if (numeric_set_digit(digits, pg_digit / 10, j++) < 0)
            goto end;

        if (j == ndigits)
        	continue;
        if (numeric_set_digit(digits, pg_digit % 10, j++) < 0)
            goto end;
    }
    /* add extra zeroes if necessary */
    for (i = j; i < ndigits; i++) {
        if (numeric_set_digit(digits, 0, i) < 0) {
            goto end;
        }
    }

    /* create the Decimal now */
    ret = PyObject_CallFunction(
    	PyDecimal, "((HOi))", sign, digits, -((int)dscale));

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

    return 0;
}
