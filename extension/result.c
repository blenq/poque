#include "poque.h"


poque_Result *
PoqueResult_New(PGresult *res) {
    poque_Result *result;

    result = PyObject_New(poque_Result, &poque_ResultType);
    if (result == NULL) {
        return NULL;
    }
    result->result = res;
    result->vw_list = NULL;
    result->wr_list = NULL;
    return result;
}


static void
Result_dealloc(poque_Result *self) {
    PQclear(self->result);
    if (self->wr_list != NULL)
        PyObject_ClearWeakRefs((PyObject *) self);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject *
Result_clear(poque_Result *self, PyObject *unused) {
    if (self->vw_list) {
        Py_ssize_t i, len;

        len = PyList_GET_SIZE(self->vw_list);
        for (i = 0; i < len; i++) {
            PyObject *vw;
            vw = PyList_GET_ITEM(self->vw_list, i);
            PyObject_CallMethod(vw, "release", NULL);
        }
        Py_DECREF(self->vw_list);
        self->vw_list = NULL;
    }
    PQclear(self->result);
    self->result = NULL;
    Py_RETURN_NONE;
}


static int
parse_column_number(PyObject *args, PyObject *keywds, int *column)
{
    static char *kwlist[] = {"column_number", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, keywds, "i", kwlist, column))
        return -1;
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
    return PyLong_FromLong(fnumber);
}


static PyObject *
Result_column_oidproperty(
    poque_Result *self, PyObject *args, PyObject *keywds,
    Oid (*func)(const PGresult *, int))
{
    int column;

    if (parse_column_number(args, keywds, &column) == -1)
        return NULL;

    return PyLong_FromUnsignedLong(func(self->result, column));
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
    int column;

    if (parse_column_number(args, keywds, &column) == -1)
        return NULL;

    return PyLong_FromLong(func(self->result, column));
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


static PyObject *
Result_getvalue(poque_Result *self, PyObject *args, PyObject *kwds)
{
    int row, column, len;
    char *data;

    if (Result_colrow_args(args, kwds, &row, &column) == -1) {
        return NULL;
    }
    data = PQgetvalue(self->result, row, column);
    if (data == NULL) {
        Py_RETURN_NONE;
    }
    len = PQgetlength(self->result, row, column);
    if (PQfformat(self->result, column) == 1) {
        PyObject *vw, *vw_list;

        vw_list = self->vw_list;
        if (self->vw_list == NULL) {
            vw_list = PyList_New(0);
            if (vw_list == NULL) {
                return NULL;
            }
            self->vw_list = vw_list;
        }

        vw = PyMemoryView_FromMemory(data, len, PyBUF_READ);
        if (vw == NULL) {
            return NULL;
        }
        if (PyList_Append(vw_list, vw) == -1) {
            Py_DECREF(vw);
            return NULL;
        }
        return vw;
    }
    return PyUnicode_FromStringAndSize(data, len);
}

static PyObject *
Result_getlength(poque_Result *self, PyObject *args, PyObject *kwds) {
    int row, column;

    if (Result_colrow_args(args, kwds, &row, &column) == -1) {
        return NULL;
    }
    return PyLong_FromLong(PQgetlength(self->result, row, column));
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
        "clear", (PyCFunction)Result_clear, METH_NOARGS,
        PyDoc_STR("clear the result")
    }, {
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
