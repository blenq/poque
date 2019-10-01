#include "poque.h"

typedef struct PoqueCursor {
    PyObject_HEAD
    PyObject *wr_list;
    PoqueConn *conn;
    PoqueResult *result;
    int arraysize;
    int pos;
    int ntuples;
    int nfields;
} PoqueCursor;


static inline int
PoqueCursor_CheckClosed(PoqueCursor *self)
{
    int ret = 0;

    /* check state */
    if (self->conn == NULL) {
        PyErr_SetString(PoqueInterfaceError, "Cursor is closed");
        ret = -1;
    }
    return ret;
}


static PGresult *
_PoqueCursor_execute(PoqueCursor *self, PyObject *command, PyObject *parameters,
                     int format)
{
    PoqueConn *cn;
    PGresult *res;

    if (PoqueCursor_CheckClosed(self) == -1) {
        return NULL;
    }
    cn = self->conn;

    /* check if we should start a transaction */
    if (!cn->autocommit && PQtransactionStatus(cn->conn) == PQTRANS_IDLE) {
        PyObject *begin = PyUnicode_FromString("BEGIN");

        if (begin == NULL) {
            return NULL;
        }
        res = _Conn_execute(cn, begin, NULL, FORMAT_TEXT);
        Py_DECREF(begin);

        if (res == NULL) {
            return NULL;
        }
        PQclear(res);
    }

    /* execute statement */
    return _Conn_execute(cn, command, parameters, format);
}


static PyObject *
PoqueCursor_execute(PoqueCursor *self, PyObject *args, PyObject *kwds) {
    PyObject *command, *parameters = NULL;
    int format = FORMAT_AUTO;
    PoqueResult *result;
    PGresult *res;
    static char *kwlist[] = {"operation", "parameters", "result_format", NULL};

    /* args parsing */
    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "U|Oi", kwlist, &command, &parameters, &format)) {
        return NULL;
    }

    Py_CLEAR(self->result);

    res = _PoqueCursor_execute(self, command, parameters, format);
    if (res == NULL) {
        return NULL;
    }

    result = PoqueResult_New(res, self->conn);
    if (result == NULL) {
        PQclear(res);
        return NULL;
    }

    /* set PoqueResult on cursor */
    self->result = result;

    self->pos = 0;
    self->ntuples = PQntuples(res);
    self->nfields = Py_SIZE(result);

    /* and done */
    Py_RETURN_NONE;
}


static PyObject *
PoqueCursor_executemany(PoqueCursor *self, PyObject *args, PyObject *kwds) {
    PyObject *command, *seq_of_parameters = NULL, *parameters;
    int format = FORMAT_BINARY;
    Py_ssize_t i, seq_len;
    PGresult *res;
    static char *kwlist[] = {
        "operation", "seq_of_parameters", "result_format", NULL};

    /* args parsing */
    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "U|Oi", kwlist, &command, &seq_of_parameters, &format)) {
        return NULL;
    }
    seq_of_parameters = PySequence_Fast(
            seq_of_parameters, "seq_of_parameters should be a sequence");
    if (seq_of_parameters == NULL) {
        return NULL;
    }
    seq_len = PySequence_Fast_GET_SIZE(seq_of_parameters);
    for (i = 0; i < seq_len; i++) {

        parameters = PySequence_Fast_GET_ITEM(seq_of_parameters, i);
        res = _PoqueCursor_execute(self, command, parameters, format);
        if (res == NULL) {
            return NULL;
        }
        PQclear(res);
    }
    /* reset PoqueResult on cursor */
    Py_CLEAR(self->result);

    self->pos = 0;
    self->ntuples = 0;
    self->nfields = 0;

    /* and done */
    Py_RETURN_NONE;

}


static int
SetNoneIfNegative(PyObject *tup, Py_ssize_t idx, int n) {
    PyObject *item;

    if (n < 0) {
        Py_INCREF(Py_None);
        item = Py_None;
    }
    else {
        item = PyLong_FromLong(n);
        if (item == NULL) {
            return -1;
        }
    }
    PyTuple_SET_ITEM(tup, idx, item);
    return 0;
}


static PyObject *
PoqueCursor_description(PoqueCursor *self, void *val)
{
    PoqueResult *result;
    PGresult *res;
    int nfields, i;
    PyObject *desc;

    /* no result yet */
    result = self->result;
    if (result == NULL) {
        /* check connection */
        if (PoqueCursor_CheckClosed(self) == -1) {
            return NULL;
        }
        Py_RETURN_NONE;
    }

    /* result without result set */
    res = result->result;
    nfields = self->nfields;
    if (nfields == 0) {
        Py_RETURN_NONE;
    }

    /* create list for the fields */
    desc = PyList_New(nfields);
    if (desc == NULL) {
        return NULL;
    }

    /* get info per field */
    for (i = 0; i < nfields; i++) {
        Oid oid;
        int i_tmp, precision = -1, scale = -1;
        char *ch_tmp;
        PyObject *field_desc, *py_tmp;

        /* add field description tuple to description list */
        field_desc = PyTuple_New(7);
        if (field_desc == NULL) {
            goto error;
        }
        PyList_SET_ITEM(desc, i, field_desc);

        /* set name */
        ch_tmp = PQfname(res, i);
        if (ch_tmp == NULL) {
            Py_INCREF(Py_None);
            py_tmp = Py_None;
        }
        else {
            py_tmp = PyUnicode_FromString(ch_tmp);
            if (py_tmp == NULL) {
                goto error;
            }
        }
        PyTuple_SET_ITEM(field_desc, 0, py_tmp);

        /* set type */
        oid = PQftype(res, i);
        py_tmp = PyLong_FromUnsignedLong(oid);
        if (py_tmp == NULL) {
            goto error;
        }
        PyTuple_SET_ITEM(field_desc, 1, py_tmp);

        /* set display_size */
        Py_INCREF(Py_None);
        PyTuple_SET_ITEM(field_desc, 2, Py_None);

        /* set internal size */
        if (SetNoneIfNegative(field_desc, 3, PQfsize(res, i)) == -1) {
            goto error;
        }

        /* get precision and scale */
        if (oid == NUMERICOID) {
            i_tmp = PQfmod(res, i) - 4;
            if (i_tmp >= 0) {
                precision = i_tmp >> 16;
                scale = i_tmp & 0xffff;
            }
        }
        else if (oid == FLOAT8OID) {
            precision = 53;
        }
        else if (oid == FLOAT4OID) {
            precision = 24;
        }

        /* set precision */
        if (SetNoneIfNegative(field_desc, 4, precision) == -1) {
            goto error;
        }

        /* set scale */
        if (SetNoneIfNegative(field_desc, 5, scale) == -1) {
            goto error;
        }

        /* set null ok */
        Py_INCREF(Py_None);
        PyTuple_SET_ITEM(field_desc, 6, Py_None);
    }
    return desc;
error:
    Py_DECREF(desc);
    return NULL;
}


static PyObject *
PoqueCursor_rowcount(PoqueCursor *self, void *val) {
    PoqueResult *result;
    PGresult *res;
    char *cmd_tup;

    /* no result yet */
    result = self->result;
    if (result == NULL) {
        /* check state */
        if (PoqueCursor_CheckClosed(self) == -1) {
            return NULL;
        }
        return PyLong_FromLong(-1);
    }

    res = result->result;
    cmd_tup = PQcmdTuples(res);
    if (cmd_tup[0] != '\0') {
        return PyLong_FromString(cmd_tup, NULL, 10);
    }
    if (self->nfields > 0) {
        return PyLong_FromLong(self->ntuples);
    }
    return PyLong_FromLong(-1);
}


static inline int
PoqueCursor_CheckFetch(PoqueCursor *self) {

    if (self->nfields) {
        return 0;
    }

    if (self->result == NULL) {
        PyErr_SetString(PoqueInterfaceError, "No result set");
    }
    else if (PoqueCursor_CheckClosed(self) == 0) {
        PyErr_SetString(PoqueInterfaceError, "Invalid cursor state");
    }
    return -1;
}


static inline PyObject *
_PoqueCursor_FetchOne(PoqueCursor *self) {
    int i,
        nfields = self->nfields,
        pos = self->pos;
    PyObject *row;
    PoqueResult *result = self->result;

    row = PyTuple_New(nfields);
    if (row == NULL) {
        return NULL;
    }
    for (i = 0; i < nfields; i++) {
        PyObject *val = _Result_value(result, pos, i);
        if (val == NULL) {
            Py_DECREF(row);
            return NULL;
        }
        PyTuple_SET_ITEM(row, i, val);
    }
    self->pos++;
    return row;
}


static PyObject *
PoqueCursor_FetchOne(PoqueCursor *self, PyObject *unused) {

    if (PoqueCursor_CheckFetch(self) == -1) {
        return NULL;
    }
	if (self->pos == self->ntuples) {
	    Py_RETURN_NONE;
	}
    return _PoqueCursor_FetchOne(self);
}


static PyObject *
_PoqueCursor_FetchMany(PoqueCursor *self, int nrows)
{
    PyObject *rows;
    int i;

    rows = PyList_New(nrows);
    if (rows == NULL) {
        return NULL;
    }
    for (i = 0; i < nrows; i++) {
        PyObject *row = _PoqueCursor_FetchOne(self);
        if (row == NULL) {
            Py_DECREF(rows);
            return NULL;
        }
        PyList_SET_ITEM(rows, i, row);
    }
    return rows;
}


static PyObject *
PoqueCursor_FetchAll(PoqueCursor *self, PyObject *unused) {

    if (PoqueCursor_CheckFetch(self) == -1) {
        return NULL;
    }
	return _PoqueCursor_FetchMany(self, self->ntuples - self->pos);
}


static PyObject *
PoqueCursor_FetchMany(PoqueCursor *self, PyObject *args, PyObject *kwds) {
    int nrows, size = INT_MIN;
    static char *kwlist[] = {"size", NULL};

    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "|i", kwlist, &size))
        return NULL;

    if (PoqueCursor_CheckFetch(self) == -1) {
        return NULL;
    }

    if (size == INT_MIN) {
        size = self->arraysize;
    }
    nrows = self->ntuples - self->pos;
    if (nrows > size) {
        nrows = size;
    }
    return _PoqueCursor_FetchMany(self, nrows);
}


static PyObject *
PoqueCursor_scroll(PoqueCursor *self, PyObject *args, PyObject *kwds)
{
    char *mode = "relative";
    int value;
    int pos;
    static char *kwlist[] = {"value", "mode", NULL};

    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "i|s", kwlist, &value, &mode)) {
        return NULL;
    }

    if (PoqueCursor_CheckFetch(self) == -1) {
        return NULL;
    }
    if (strcmp(mode, "relative") == 0) {
        if (value > INT_MAX - self->pos) {
            /* overflow */
            PyErr_SetString(PoqueInterfaceIndexError, "Position out of range");
            return NULL;
        }
        pos = self->pos + value;
    }
    else if (strcmp(mode, "absolute") == 0) {
        pos = value;
    }
    else {
        PyErr_SetString(PoqueInterfaceError, "Invalid mode");
        return NULL;
    }
    if (pos < 0 || pos > self->ntuples) {
        PyErr_SetString(PoqueInterfaceIndexError, "Position out of range");
        return NULL;
    }
    self->pos = pos;
    Py_RETURN_NONE;
}


static PyObject *
PoqueCursor_iter(PyObject *self)
{
    Py_INCREF(self);
    return self;
}


static PyObject *
PoqueCursor_iternext(PoqueCursor *self)
{
    PyObject *row;

    row = PoqueCursor_FetchOne(self, NULL);
    if (row == Py_None) {
        Py_DECREF(Py_None);
        return NULL;
    }
    return row;
}


static PyObject *
PoqueCursor_close(PoqueCursor *self, PyObject *unused) {
    Py_CLEAR(self->conn);
    Py_CLEAR(self->result);
    self->nfields = 0;
    self->ntuples = 0;
    Py_RETURN_NONE;
}


static void
PoqueCursor_dealloc(PoqueCursor *self) {
    /* standard destructor, clear weakrefs, break ref chain and free */
    if (self->wr_list != NULL)
        PyObject_ClearWeakRefs((PyObject *) self);
    Py_CLEAR(self->result);
    Py_CLEAR(self->conn);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
Cursor_rownumber(PoqueCursor *self, void *val)
{
    if (self->nfields == 0) {
        Py_RETURN_NONE;
    }
    return PyLong_FromLong(self->pos);
}


static PyMemberDef Cursor_members[] = {
    {"connection", T_OBJECT, offsetof(PoqueCursor, conn), READONLY,
     "The connection"},
    {"arraysize", T_INT, offsetof(PoqueCursor, arraysize), 0,
     "Array size"},
    {NULL}
};

static PyObject *
Cursor_void(PyObject *self, PyObject *args, PyObject *kwds) {
    Py_RETURN_NONE;
}


static PyGetSetDef Cursor_getset[] = {{
        "rowcount",
        (getter)PoqueCursor_rowcount,
        NULL,
        PyDoc_STR("Row count"),
        NULL
    }, {
        "rownumber",
        (getter)Cursor_rownumber,
        NULL,
        PyDoc_STR("Row number"),
        NULL
    }, {
        "description",
        (getter)PoqueCursor_description,
        NULL,
        PyDoc_STR("Gets field descriptions"),
        NULL
    }, {
        NULL
}};


static PyMethodDef Cursor_methods[] = {{
        "setinputsizes", (PyCFunction)Cursor_void, METH_VARARGS | METH_KEYWORDS,
        PyDoc_STR("set input sizes")
    }, {
        "setoutputsize", (PyCFunction)Cursor_void, METH_VARARGS | METH_KEYWORDS,
        PyDoc_STR("set output size")
    }, {
        "close", (PyCFunction)PoqueCursor_close, METH_NOARGS,
        PyDoc_STR("closes cursor")
    }, {
        "execute", (PyCFunction)PoqueCursor_execute,
        METH_VARARGS | METH_KEYWORDS, PyDoc_STR("executes a statement")
    }, {
        "executemany", (PyCFunction)PoqueCursor_executemany,
        METH_VARARGS | METH_KEYWORDS, PyDoc_STR(
            "executes a statement multiple times")
    }, {
        "fetchone", (PyCFunction)PoqueCursor_FetchOne, METH_NOARGS,
        PyDoc_STR("fetch a row")
    }, {
        "fetchall", (PyCFunction)PoqueCursor_FetchAll, METH_NOARGS,
        PyDoc_STR("fetch remaining rows")
    }, {
        "fetchmany", (PyCFunction)PoqueCursor_FetchMany,
        METH_VARARGS | METH_KEYWORDS, PyDoc_STR("fetch multiple rows")
    }, {
        "scroll", (PyCFunction)PoqueCursor_scroll,
        METH_VARARGS | METH_KEYWORDS, PyDoc_STR("set cursor position")
    }, {
        NULL
}};


PyTypeObject PoqueCursorType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "poque.Cursor",                             /* tp_name */
    sizeof(PoqueCursor),                        /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)PoqueCursor_dealloc,            /* tp_dealloc */
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
    PyDoc_STR("poque cursor object"),           /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    offsetof(PoqueCursor, wr_list),             /* tp_weaklistoffset */
    PoqueCursor_iter,                           /* tp_iter */
    (iternextfunc)PoqueCursor_iternext,         /* tp_iternext */
    Cursor_methods,                             /* tp_methods */
    Cursor_members,                             /* tp_members */
    Cursor_getset,                              /* tp_getset */
    0
};


PoqueCursor *
PoqueCursor_New(PoqueConn *conn)
{
    /* PoqueCursor constructor */
    PoqueCursor *cursor;

    cursor = PyObject_New(PoqueCursor, &PoqueCursorType);
    if (cursor == NULL) {
        return NULL;
    }
    cursor->wr_list = NULL;
    Py_INCREF(conn);
    cursor->conn = conn;
    cursor->result = NULL;
    cursor->arraysize = 1;
    cursor->pos = 0;
    cursor->ntuples = 0;
    cursor->nfields = 0;
    return cursor;
}
