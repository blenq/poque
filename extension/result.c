#include "poque.h"


/* ===== PoqueValue ========================================================= */

/* A PoqueValue is a minimal object that wraps a data pointer retrieved by
 * PQgetValue. It is used to expose it without copying as a memoryview to the
 * consumer (using buffer protocol)  and at the same time keep a reference to
 * the result object.
 *
 * The problem it solves is the availability of the data. The data will be
 * cleared automatically by libpq when the PGresult is cleared. Therefore it is
 * necessary to keep the PGresult alive while the memoryview is in use.
 * The PGresult will only be cleared by the destructor of the PoqueResult
 * when there are no more references to it.
 *
 * The resulting reference chain looks like:
 *
 * memoryview -> PoqueValue -> PoqueResult -> PGresult
 */

typedef struct {
    PyObject_HEAD
    PyObject *wr_list;
	poque_Result *result;
    char *data;
    int len;
} PoqueValue;


static int
PoqueValue_GetBuffer(PyObject *exporter, Py_buffer *view, int flags)
{
	/* fills in buffer from data pointer */
	PoqueValue *self;

	self = (PoqueValue *)exporter;
	return PyBuffer_FillInfo(
			view, exporter, self->data, self->len, 1, flags);
}


static void
PoqueValue_dealloc(PoqueValue *self) {
	/* standard destructor, clear weakrefs, break ref chain and free */
    if (self->wr_list != NULL)
        PyObject_ClearWeakRefs((PyObject *) self);
    Py_DECREF(self->result);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyBufferProcs PoqueValue_BufProcs = {
		PoqueValue_GetBuffer,
		NULL
};


PyTypeObject PoqueValueType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "poque.Value",                              /* tp_name */
    sizeof(PoqueValue),                         /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)PoqueValue_dealloc,             /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash  */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    &PoqueValue_BufProcs,                       /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /* tp_flags */
    PyDoc_STR("poque value object"),            /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    offsetof(PoqueValue, wr_list),              /* tp_weaklistoffset */
    0,                                          /* tp_iter */
};


static PoqueValue *
PoqueValue_New(poque_Result *result, char *data, int len)
{
	/* PoqueValue constructor */
	PoqueValue *value;

	value = PyObject_New(PoqueValue, &PoqueValueType);
	if (value == NULL) {
		return NULL;
	}
    value->wr_list = NULL;
	Py_INCREF(result);
	value->result = result;
	value->data = data;
	value->len = len;
	return value;
}


/* ===== PoqueResult ======================================================== */

poque_Result *
PoqueResult_New(PGresult *res, poque_Conn *conn) {
    poque_Result *result;

    result = PyObject_New(poque_Result, &poque_ResultType);
    if (result == NULL) {
        return NULL;
    }
    result->result = res;
    result->wr_list = NULL;
    Py_INCREF(conn);
    result->conn = conn;
    return result;
}


static void
Result_dealloc(poque_Result *self) {
    PQclear(self->result);
    Py_DECREF(self->conn);
    if (self->wr_list != NULL)
        PyObject_ClearWeakRefs((PyObject *) self);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static int
parse_column_number(PyObject *args, PyObject *keywds, int *column)
{
    static char *kwlist[] = {"column_number", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, keywds, "i", kwlist, column))
        return -1;
    return 0;
}


static int
Result_check_warnings(poque_Result *self) {
    char *warning_msg;
    if (self->conn == NULL) {
        return 0;
    }
    warning_msg = self->conn->warning_msg;
    if (warning_msg != NULL) {
        self->conn->warning_msg = NULL;
        return PyErr_WarnEx(PoqueWarning, warning_msg, 1);
    }
    return 0;
}


static PyObject *
Result_fname(poque_Result *self, PyObject *args, PyObject *keywds)
{
    int column;
    char *fname;

    if (parse_column_number(args, keywds, &column) == -1)
        return NULL;

    fname = PQfname(self->result, column);
    if (Result_check_warnings(self) == -1) {
        return NULL;
    }
    if (fname) {
        return PyUnicode_FromString(fname);
    }
    Py_RETURN_NONE;
}


static PyObject *
Result_fnumber(poque_Result *self, PyObject *args, PyObject *keywds)
{
    int fnumber;
    char *fname;
    static char *kwlist[] = {"column_name", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, keywds, "s", kwlist, &fname))
        return NULL;

    fnumber = PQfnumber(self->result, fname);
    /* PQfnumber does not trigger notices at the moment of writing */
    if (Result_check_warnings(self) == -1) {
        return NULL;
    }
    return PyLong_FromLong(fnumber);
}


static PyObject *
Result_column_oidproperty(
    poque_Result *self, PyObject *args, PyObject *keywds,
    Oid (*func)(const PGresult *, int))
{
    int column;
    Oid oid;

    if (parse_column_number(args, keywds, &column) == -1)
        return NULL;

    oid = func(self->result, column);
    if (Result_check_warnings(self) == -1) {
        return NULL;
    }
    return PyLong_FromUnsignedLong(oid);
}


static PyObject *
Result_ftable(poque_Result *self, PyObject *args, PyObject *keywds)
{
    return Result_column_oidproperty(self, args, keywds, PQftable);
}


static PyObject *
Result_ftype(poque_Result *self, PyObject *args, PyObject *keywds)
{
    return Result_column_oidproperty(self, args, keywds, PQftype);
}


static PyObject *
Result_column_intproperty(
    poque_Result *self, PyObject *args, PyObject *keywds,
    int (*func)(const PGresult *, int))
{
    int column, value;

    if (parse_column_number(args, keywds, &column) == -1)
        return NULL;

    value = func(self->result, column);
    if (Result_check_warnings(self) == -1) {
        return NULL;
    }
    return PyLong_FromLong(value);
}


static PyObject *
Result_ftablecol(poque_Result *self, PyObject *args, PyObject *keywds)
{
    return Result_column_intproperty(self, args, keywds, PQftablecol);
}


static PyObject *
Result_fformat(poque_Result *self, PyObject *args, PyObject *keywds)
{
    return Result_column_intproperty(self, args, keywds, PQfformat);
}


static PyObject *
Result_fmod(poque_Result *self, PyObject *args, PyObject *keywds)
{
    return Result_column_intproperty(self, args, keywds, PQfmod);
}


static PyObject *
Result_fsize(poque_Result *self, PyObject *args, PyObject *keywds)
{
    return Result_column_intproperty(self, args, keywds, PQfsize);
}


static int
Result_colrow_args(PyObject *args, PyObject *kwds, int *row, int *column)
{
    static char *kwlist[] = {"row_number", "column_number", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "ii", kwlist, row, column))
        return -1;
    return 0;
}


PyObject *
Result_getview(poque_Result *self, char *data, int len)
{
	PyObject *view;
	PoqueValue *value;


	value = PoqueValue_New(self, data, len);
	if (value == NULL) {
		return NULL;
	}
	view = PyMemoryView_FromObject((PyObject *)value);
	Py_DECREF(value);
	return view;
}


static PyObject *
Result_getvalue(poque_Result *self, PyObject *args, PyObject *kwds)
{
    int row, column, len;
    char *data;

    if (Result_colrow_args(args, kwds, &row, &column) == -1) {
        return NULL;
    }
    data = PQgetvalue(self->result, row, column);
    if (Result_check_warnings(self) == -1) {
        return NULL;
    }
    if (data == NULL) {
        Py_RETURN_NONE;
    }
    len = PQgetlength(self->result, row, column);
    if (PQfformat(self->result, column) == FORMAT_BINARY) {
        return Result_getview(self, data, len);
    }
    return PyUnicode_FromStringAndSize(data, len);
}

static PyObject *
Result_getlength(poque_Result *self, PyObject *args, PyObject *kwds) {
    int row, column, length;

    if (Result_colrow_args(args, kwds, &row, &column) == -1) {
        return NULL;
    }
    length = PQgetlength(self->result, row, column);
    if (Result_check_warnings(self) == -1) {
        return NULL;
    }
    return PyLong_FromLong(length);
}

static PyObject *
Result_value(poque_Result *self, PyObject *args, PyObject *kwds)
{
    int row, column;

    if (Result_colrow_args(args, kwds, &row, &column) == -1) {
        return NULL;
    }
    if (PQgetisnull(self->result, row, column))
        Py_RETURN_NONE;
    return Poque_value(self,
                       PQftype(self->result, column),
                       PQfformat(self->result, column),
                       PQgetvalue(self->result, row, column),
                       PQgetlength(self->result, row, column));
}

static PyObject *
Result_isnull(poque_Result *self, PyObject *args, PyObject *kwds)
{
    int row, column;

    if (Result_colrow_args(args, kwds, &row, &column) == -1) {
        return NULL;
    }
    if (PQgetisnull(self->result, row, column))
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}


static PyObject *
Result_intprop(poque_Result *self, int (*func)(PGresult *))
{
    /* Generic function to get int properties */
    return PyLong_FromLong(func(self->result));
}

static PyGetSetDef Result_getset[] = {{
        "ntuples",
        (getter)Result_intprop,
        NULL,
        PyDoc_STR("number of tuples"),
        PQntuples
    }, {
        "nfields",
        (getter)Result_intprop,
        NULL,
        PyDoc_STR("number of fields"),
        PQnfields
    }, {
        "nparams",
        (getter)Result_intprop,
        NULL,
        PyDoc_STR("number of parameters"),
        PQnparams
    }, {
        NULL
}};

static PyMethodDef Result_methods[] = {{
        "fname", (PyCFunction)Result_fname, METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("field name")
    },  {
        "fnumber", (PyCFunction)Result_fnumber, METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("field number")
    },  {
        "ftable", (PyCFunction)Result_ftable, METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("field table identifier")
    },  {
        "ftablecol", (PyCFunction)Result_ftablecol,
        METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("field table column")
    },  {
        "fformat", (PyCFunction)Result_fformat,
        METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("field format")
    },  {
        "ftype", (PyCFunction)Result_ftype, METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("field table identifier")
    },  {
        "fmod", (PyCFunction)Result_fmod, METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("field modifier")
    },  {
        "fsize", (PyCFunction)Result_fsize, METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("field size")
    }, {
        "pq_getvalue", (PyCFunction)Result_getvalue, METH_VARARGS | METH_KEYWORDS,
        PyDoc_STR("raw value")
    }, {
        "getlength", (PyCFunction)Result_getlength, METH_VARARGS | METH_KEYWORDS,
        PyDoc_STR("length of value")
    }, {
        "getvalue", (PyCFunction)Result_value, METH_VARARGS | METH_KEYWORDS,
        PyDoc_STR("Python value")
    }, {
        "getisnull", (PyCFunction)Result_isnull, METH_VARARGS | METH_KEYWORDS,
        PyDoc_STR("is null")
    },  {
        NULL
}};

PyTypeObject poque_ResultType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "poque.Result",                             /* tp_name */
    sizeof(poque_Result),                       /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)Result_dealloc,                 /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_reserved */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash  */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /* tp_flags */
    PyDoc_STR("poque result object"),           /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    offsetof(poque_Result, wr_list),            /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    Result_methods,                             /* tp_methods */
    0,                                          /* tp_members */
    Result_getset,                              /* tp_getset */
    0
};
