#include "poque.h"
#include "numeric.h"

PyObject *
int16_binval(data_crs *crs)
{
    poque_int16 value;
    if (crs_read_int16(crs, &value) < 0)
        return NULL;
    return PyLong_FromLong(value);
}


PyObject *
uint32_binval(data_crs *crs)
{
    PY_UINT32_T value;
    if (crs_read_uint32(crs, &value) < 0)
        return NULL;
    return PyLong_FromLongLong(value);
}


PyObject *
int32_binval(data_crs *crs)
{
    PY_INT32_T value;
    if (crs_read_int32(crs, &value) < 0)
        return NULL;
    return PyLong_FromLong(value);
}


PyObject *
int64_binval(data_crs *crs)
{
    PY_INT64_T value;

    if (crs_read_int64(crs, &value) < 0)
        return NULL;
    return PyLong_FromLongLong((PY_INT64_T)value);
}


PyObject *int_strval(data_crs *crs)
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


PyObject *bool_binval(data_crs *crs) {
    char data;

    if (crs_read_char(crs, &data) < 0)
    	return NULL;
    return PyBool_FromLong(data);
}


PyObject *bool_strval(data_crs *crs) {
    char data;

    if (crs_read_char(crs, &data) < 0)
    	return NULL;
    return PyBool_FromLong(data == 't');
}



PyObject *
float64_binval(data_crs *crs)
{
    union {
        PY_UINT64_T int_val;
        double dbl_val;
    } value;
    if (crs_read_uint64(crs, &value.int_val) < 0)
        return NULL;
    return PyFloat_FromDouble(value.dbl_val);
}


PyObject *
float32_binval(data_crs *crs)
{
    union {
        PY_UINT32_T int_val;
        float dbl_val;
    } value;
    if (crs_read_uint32(crs, &value.int_val) < 0)
        return NULL;
    return PyFloat_FromDouble(value.dbl_val);
}


PyObject *
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


PyObject *
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
        div_t q;

        if (crs_read_uint16(crs, &pg_digit) < 0)
            goto end;
        if (pg_digit > 9999) {
            PyErr_SetString(PoqueError, "Invalid numeric value");
            goto end;
        }

        if (j == ndigits)
        	continue;
        q = div(pg_digit, 1000);
        if (numeric_set_digit(digits, q.quot, j++) < 0)
            goto end;

        if (j == ndigits)
        	continue;
        q = div(q.rem, 100);
        if (numeric_set_digit(digits, q.quot, j++) < 0)
            goto end;

        if (j == ndigits)
        	continue;
        q = div(q.rem, 10);
        if (numeric_set_digit(digits, q.quot, j++) < 0)
            goto end;

        if (j == ndigits)
        	continue;
        if (numeric_set_digit(digits, q.rem, j++) < 0)
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
