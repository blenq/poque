#include "poque_type.h"

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

    register_parameter_handler(&PyFloat_Type, new_float_param_handler);
    register_parameter_handler(&PyBool_Type, new_bool_param_handler);

    return 0;
}
