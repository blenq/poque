#include "poque.h"

PyObject *PoqueError;

PyObject *
Poque_info_options(PQconninfoOption *options) {
    PyObject *info, *values;
    size_t i;

    info = PyDict_New();
    if (info == NULL)
        return NULL;

    for (i = 0; options[i].keyword; i++) {
        values = Py_BuildValue("sssssi",
            options[i].envvar,
            options[i].compiled,
            options[i].val,
            options[i].label,
            options[i].dispchar,
            options[i].dispsize
        );
        if (values == NULL) {
            goto error;
        }
        if (PyDict_SetItemString(info, options[i].keyword, values) == -1) {
            goto error;
        }
        Py_DECREF(values);
    }
    return info;

error:
    Py_DECREF(info);
    Py_XDECREF(values);
    return NULL;
}

static PyObject *
Poque_conn_defaults(PyObject *self, PyObject *unused) {
    PQconninfoOption *options;
    PyObject *info;

    options = PQconndefaults();
    if (options == NULL) {
        return PyErr_NoMemory();
    }
    info = Poque_info_options(options);
    PQconninfoFree(options);
    return info;
}

static PyObject *
Poque_conninfo_parse(PyObject *self, PyObject *args) {
    char *conn_info, *err_msg;
    PQconninfoOption *options;
    PyObject *info;

    if (!PyArg_ParseTuple(args, "s", &conn_info)) {
        return NULL;
    }
    options = PQconninfoParse(conn_info, &err_msg);
    if (err_msg) {
        PyErr_SetString(PoqueError, err_msg);
        PQfreemem(err_msg);
        return NULL;
    }
    if (options == NULL) {
        return PyErr_NoMemory();
    }
    info = Poque_info_options(options);
    PQconninfoFree(options);
    return info;
}

static PyObject *
poque_libversion(PyObject *self, PyObject *unused) {
    return PyLong_FromLong(PQlibVersion());
}

static PyMethodDef PoqueMethods[] = {
    {"conn_defaults", Poque_conn_defaults, METH_NOARGS,
     PyDoc_STR("default connection options")},
    {"conninfo_parse", Poque_conninfo_parse,  METH_VARARGS,
     PyDoc_STR("parse a connection string")},
    {"libversion", poque_libversion, METH_NOARGS, PyDoc_STR("libpq version")},
    {NULL}
};

static struct PyModuleDef poque_module = {
    PyModuleDef_HEAD_INIT,
    "_poque",    /* name of module */
    NULL,        /* module documentation, may be NULL */
    -1,          /* size of per-interpreter state of the module,
                   or -1 if the module keeps state in global variables. */
    PoqueMethods
};

/* Everything besides module init func is hidden from the outside world */
#ifdef __GNUC__
#pragma GCC visibility pop
#endif

PyMODINIT_FUNC
PyInit__poque(void)
{
    PyObject *m;

    /* create the actual module */
    m = PyModule_Create(&poque_module);
    if (m == NULL)
        return NULL;

    /* add error to module */
    PoqueError = PyErr_NewException("poque.Error", NULL, NULL);
    Py_INCREF(PoqueError);
    PyModule_AddObject(m, "Error", PoqueError);

    /* boilerplate type initialization */
    poque_ConnType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&poque_ConnType) < 0)
        return NULL;

    /* Add connection type to the module */
    Py_INCREF(&poque_ConnType);
    if (PyModule_AddObject(
            m, "Conn", (PyObject *)&poque_ConnType) == -1) {
        return NULL;
    }

    /* Initialize result */
    poque_ResultType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&poque_ResultType) < 0)
        return NULL;

    /* Add result type to the module */
/*    Py_INCREF(&poque_ResultType);
    if (PyModule_AddObject(
            m, "Conn", (PyObject *)&poque_ResultType) == -1) {
        return NULL;
    } */

    if (PyModule_AddIntMacro(m, CONNECTION_OK) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CONNECTION_BAD) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CONNECTION_STARTED) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CONNECTION_MADE) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CONNECTION_AWAITING_RESPONSE) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CONNECTION_AUTH_OK) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CONNECTION_SSL_STARTUP) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CONNECTION_SETENV) == -1) return NULL;

    if (PyModule_AddIntMacro(m, BOOLOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, BYTEAOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CHAROID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, NAMEOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INT8OID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INT2OID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INT2VECTOROID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INT4OID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, REGPROCOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TEXTOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, OIDOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TIDOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, XIDOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CIDOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, OIDVECTOROID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, JSONOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, XMLOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, XMLARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, JSONARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, POINTOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, LSEGOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, PATHOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, BOXOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, POLYGONOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, ABSTIMEOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, RELTIMEOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TINTERVALOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, UNKNOWNOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, CIRCLEOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CIRCLEARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, CASHOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CASHARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, MACADDROID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, FLOAT4OID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, FLOAT8OID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, INT4ARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, UUIDOID) == -1) return NULL;

    if (PyModule_AddIntConstant(m, "TRANS_IDLE", PQTRANS_IDLE) == -1)
        return NULL;
    if (PyModule_AddIntConstant(m, "TRANS_ACTIVE", PQTRANS_ACTIVE) == -1)
        return NULL;
    if (PyModule_AddIntConstant(m, "TRANS_INTRANS", PQTRANS_INTRANS) == -1)
        return NULL;
    if (PyModule_AddIntConstant(m, "TRANS_INERROR", PQTRANS_INERROR) == -1)
        return NULL;
    if (PyModule_AddIntConstant(m, "TRANS_UNKNOWN", PQTRANS_UNKNOWN) == -1)
        return NULL;

    if (PyModule_AddIntConstant(
            m, "POLLING_READING", PGRES_POLLING_READING) == -1)
        return NULL;
    if (PyModule_AddIntConstant(
            m, "POLLING_WRITING", PGRES_POLLING_WRITING) == -1)
        return NULL;
    if (PyModule_AddIntConstant(m, "POLLING_OK", PGRES_POLLING_OK) == -1)
        return NULL;

    if (PyModule_AddObject(m, "INVALID_OID", PyLong_FromUnsignedLong(InvalidOid)) == -1)
        return NULL;

    if (PyModule_AddObject(m, "INVALID_OID", PyLong_FromUnsignedLong(InvalidOid)) == -1)
        return NULL;

    init_datetime();

    return m;
}
