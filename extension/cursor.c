#include "poque.h"

typedef struct PoqueCursor {
    PyObject_HEAD
    PyObject *wr_list;
    PoqueConn *conn;
    PoqueResult *result;
    int arraysize;
    int pos;
} PoqueCursor;


static PGresult *
_PoqueCursor_execute(PoqueCursor *self, char *sql, PyObject *parameters,
                     int format)
{
    PoqueConn *cn;
    PGresult *res;

    cn = self->conn;

    /* check state */
    if (cn == NULL) {
        PyErr_SetString(PoqueInterfaceError, "Cursor is closed");
        return NULL;
    }

    /* check if we should start a transaction */
    if (!cn->autocommit && PQtransactionStatus(cn->conn) == PQTRANS_IDLE) {
        res = _Conn_execute(cn, "BEGIN", NULL, FORMAT_TEXT);
        if (res == NULL) {
            return NULL;
        }
        PQclear(res);
    }

    /* execute statement */
    res = _Conn_execute(cn, sql, parameters, format);
    if (res == NULL) {
        return NULL;
    }
    return res;
}


static PyObject *
PoqueCursor_execute(PoqueCursor *self, PyObject *args, PyObject *kwds) {
    char *sql;
    PyObject *parameters = NULL;
    int format = FORMAT_BINARY;
    PoqueResult *result, *tmp;
    PGresult *res;
    static char *kwlist[] = {"operation", "parameters", "result_format", NULL};

    /* args parsing */
    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "s|Oi", kwlist, &sql, &parameters, &format)) {
        return NULL;
    }

    res = _PoqueCursor_execute(self, sql, parameters, format);
    if (res == NULL) {
        return NULL;
    }

    result = PoqueResult_New(res, self->conn);
    if (result == NULL) {
        PQclear(res);
    }

    /* set PoqueResult on cursor */
    tmp = self->result;
    self->result = result;
    Py_XDECREF(tmp);
    self->pos = 0;

    /* and done */
    Py_RETURN_NONE;
}


static PyObject *
PoqueCursor_executemany(PoqueCursor *self, PyObject *args, PyObject *kwds) {
    char *sql;
    PyObject *seq_of_parameters = NULL, *parameters;
    int format = FORMAT_BINARY;
    Py_ssize_t i, seq_len;
    PGresult *res;
    PoqueResult *tmp;
    static char *kwlist[] = {
        "operation", "seq_of_parameters", "result_format", NULL};

    /* args parsing */
    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "s|Oi", kwlist, &sql, &seq_of_parameters, &format)) {
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
        res = _PoqueCursor_execute(self, sql, parameters, format);
        if (res == NULL) {
            return NULL;
        }
        PQclear(res);
    }
    /* reset PoqueResult on cursor */
    tmp = self->result;
    self->result = NULL;
    Py_XDECREF(tmp);

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

    /* check state */
    if (self->conn == NULL) {
        PyErr_SetString(PoqueInterfaceError, "Cursor is closed");
        return NULL;
    }

    /* no result yet */
    result = self->result;
    if (result == NULL) {
        Py_RETURN_NONE;
    }

    /* result without result set */
    res = result->result;
    nfields = PQnfields(res);
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

    /* check state */
    if (self->conn == NULL) {
        PyErr_SetString(PoqueInterfaceError, "Cursor is closed");
        return NULL;
    }

    /* no result yet */
    result = self->result;
    if (result == NULL) {
        return PyLong_FromLong(-1);
    }

    res = result->result;
    cmd_tup = PQcmdTuples(res);
    if (cmd_tup[0] != '\0') {
        return PyLong_FromString(cmd_tup, NULL, 10);
    }
    if (PQnfields(res) > 0) {
        return PyLong_FromLong(PQntuples(res));
    }
    return PyLong_FromLong(-1);
}


static int
PoqueCursor_CheckFetch(PoqueCursor *self) {
    PoqueResult *result;

    result = self->result;
    if (result == NULL) {
        PyErr_SetString(PoqueInterfaceError, "Invalid cursor state");
        return -1;
    }
    if (PQnfields(result->result) == 0) {
        PyErr_SetString(PoqueInterfaceError, "No result set");
        return -1;
    }
    return 0;
}


static PyObject *
_PoqueCursor_FetchOne(PoqueCursor *self) {
    PoqueResult *result;
    int pos;

    result = self->result;
    pos = self->pos;
    if (pos < PQntuples(result->result)) {
        int i, n;
        PyObject *row;

        n = PQnfields(result->result);
        row = PyTuple_New(n);
        if (row == NULL) {
            return NULL;
        }
        for (i = 0; i < n; i++) {
            PyObject *val;
            val = _Result_value(result, pos, i);
            if (val == NULL) {
                Py_DECREF(row);
                return NULL;
            }
            PyTuple_SET_ITEM(row, i, val);
        }
        self->pos++;
        return row;
    }
    Py_RETURN_NONE;
}


static PyObject *
PoqueCursor_FetchOne(PoqueCursor *self, PyObject *unused) {
    if (PoqueCursor_CheckFetch(self) == -1) {
        return NULL;
    }
    return _PoqueCursor_FetchOne(self);
}


static PyObject *
PoqueCursor_FetchAll(PoqueCursor *self, PyObject *unused) {
    int i, n;
    PyObject *rows;

    if (PoqueCursor_CheckFetch(self) == -1) {
        return NULL;
    }
    n = PQntuples(self->result->result) - self->pos;
    rows = PyList_New(n);
    if (rows == NULL) {
        return NULL;
    }
    for (i = 0; i < n; i++) {
        PyObject *row;
        row = _PoqueCursor_FetchOne(self);
        if (row == NULL) {
            Py_DECREF(rows);
            return row;
        }
        PyList_SET_ITEM(rows, i, row);
    }
    return rows;
}


static PyObject *
PoqueCursor_close(PoqueCursor *self, PyObject *unused) {
    if (self->conn) {
        PoqueConn *conn = self->conn;
        self->conn = NULL;
        Py_DECREF(conn);
    }
    if (self->result) {
        PoqueResult *result = self->result;
        self->result = NULL;
        Py_DECREF(result);
    }
    Py_RETURN_NONE;
}


static void
PoqueCursor_dealloc(PoqueCursor *self) {
    /* standard destructor, clear weakrefs, break ref chain and free */
    if (self->wr_list != NULL)
        PyObject_ClearWeakRefs((PyObject *) self);
    Py_XDECREF(self->result);
    Py_XDECREF(self->conn);
    Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyObject *
Cursor_rownumber(PoqueCursor *self, void *val)
{
    if (self->result == NULL || PQnfields(self->result->result) == 0) {
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
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
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
    return cursor;
}
