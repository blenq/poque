#include "poque.h"
#include "poque_type.h"

PyObject *PoqueError;
PyObject *PoqueInterfaceError;
PyObject *PoqueInterfaceIndexError;
PyObject *PoqueWarning;

static PyTypeObject InfoOption;


static int
fill_info_option_char(PyObject *option, Py_ssize_t i, char *val) {
    PyObject *value;

    if (val == NULL) {
        value = Py_None;
        Py_INCREF(Py_None);
    }
    else {
        value = PyUnicode_FromString(val);
        if (value == NULL) {
            return -1;
        }
    }
    PyStructSequence_SET_ITEM(option, i, value);
    return 0;
}


PyObject *
Poque_info_options(PQconninfoOption *options) {
    PyObject *info, *option=NULL, *value;
    size_t i;

    info = PyDict_New();
    if (info == NULL)
        return NULL;

    for (i = 0; options[i].keyword; i++) {
        option = PyStructSequence_New(&InfoOption);
        if (option == NULL) {
            goto error;
        }
        if (fill_info_option_char(option, 0, options[i].envvar) < 0)
            goto error;
        if (fill_info_option_char(option, 1, options[i].compiled) < 0)
            goto error;
        if (fill_info_option_char(option, 2, options[i].val) < 0)
            goto error;
        if (fill_info_option_char(option, 3, options[i].label) < 0)
            goto error;
        if (fill_info_option_char(option, 4, options[i].dispchar) < 0)
            goto error;
        value = PyLong_FromLong(options[i].dispsize);
        if (value == NULL)
            goto error;
        PyStructSequence_SET_ITEM(option, 5, value);
        if (PyDict_SetItemString(info, options[i].keyword, option) == -1) {
            goto error;
        }
        Py_DECREF(option);



//
//
//        PyStructSequence_SetItem()
//        values = Py_BuildValue("sssssi",
//            options[i].envvar,
//            options[i].compiled,
//            options[i].val,
//            options[i].label,
//            options[i].dispchar,
//            options[i].dispsize
//        );
//        if (values == NULL) {
//            goto error;
//        }
//        if (PyDict_SetItemString(info, options[i].keyword, values) == -1) {
//            goto error;
//        }
//        Py_DECREF(values);
    }
    return info;

error:
    Py_DECREF(info);
    Py_XDECREF(option);
    return NULL;
}

static PyObject *
PoqueConn_defaults(PyObject *self, PyObject *unused) {
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
PoqueConninfo_parse(PyObject *self, PyObject *args) {
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


static PyObject *
poque_encrypt_password(PyObject *self, PyObject *args, PyObject *kwargs) {
    char *password, *user, *encrypted;
    PyObject *ret;
    static char *kwlist[] = {"password", "user", NULL};

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "ss", kwlist, &password, &user))
        return NULL;
    encrypted = PQencryptPassword(password, user);
    if (encrypted == NULL){
        return PyErr_NoMemory();
    }
    ret = PyUnicode_FromString(encrypted);
    PQfreemem(encrypted);
    return ret;
}


static PyMethodDef PoqueMethods[] = {
    {"conn_defaults", PoqueConn_defaults, METH_NOARGS,
     PyDoc_STR("default connection options")},
    {"conninfo_parse", PoqueConninfo_parse,  METH_VARARGS,
     PyDoc_STR("parse a connection string")},
    {"lib_version", poque_libversion, METH_NOARGS, PyDoc_STR("libpq version")},
    {"encrypt_password", (PyCFunction)poque_encrypt_password,
     METH_VARARGS | METH_KEYWORDS, PyDoc_STR("encrypts a password")
    },
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


static int
create_info_option(void) {
    PyStructSequence_Field fields[] = {
        {"envvar", NULL},
        {"compiled", NULL},
        {"val", NULL},
        {"label", NULL},
        {"dispchar", NULL},
        {"dispsize", NULL},
        {NULL}
    };
    PyStructSequence_Desc desc = {
        "poque.InfoOption",        /* name */
        NULL,                    /* doc */
        fields,                    /* fields */
        6
    };

    return PyStructSequence_InitType2(&InfoOption, &desc);
}


PyMODINIT_FUNC
PyInit__poque(void)
{
    PyObject *m;
    PyObject *tmp;

    /* create InfoOption type */
    if (create_info_option() < 0)
        return NULL;

    /* create the actual module */
    m = PyModule_Create(&poque_module);
    if (m == NULL)
        return NULL;

    /* add error to module */
    PoqueWarning = PyErr_NewException("poque.Warning", PyExc_UserWarning, NULL);
    Py_INCREF(PoqueWarning);
    PyModule_AddObject(m, "Warning", PoqueWarning);

    PoqueError = PyErr_NewException("poque.Error", NULL, NULL);
    Py_INCREF(PoqueError);
    PyModule_AddObject(m, "Error", PoqueError);

    /* add error to module */
    PoqueInterfaceError = PyErr_NewException("poque.InterfaceError", PoqueError, NULL);
    Py_INCREF(PoqueInterfaceError);
    PyModule_AddObject(m, "InterfaceError", PoqueInterfaceError);

    /* add error to module */
    tmp = Py_BuildValue("OO", PyExc_IndexError, PoqueInterfaceError);
    if (tmp == NULL) {
        return NULL;
    }
    PoqueInterfaceIndexError = PyErr_NewException("poque.InterfaceIndexError", tmp, NULL);
    Py_DECREF(tmp);
    Py_INCREF(PoqueInterfaceIndexError);
    PyModule_AddObject(m, "InterfaceIndexError", PoqueInterfaceIndexError);

    /* boilerplate type initialization */
    PoqueConnType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&PoqueConnType) < 0)
        return NULL;

    /* Add connection type to the module */
    Py_INCREF(&PoqueConnType);
    if (PyModule_AddObject(
            m, "Conn", (PyObject *)&PoqueConnType) == -1) {
        return NULL;
    }

    /* Initialize result */
    PoqueResultType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&PoqueResultType) < 0)
        return NULL;

//    PoqueValueType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&PoqueValueType) < 0)
        return NULL;

    if (PyType_Ready(&PoqueCursorType) < 0)
        return NULL;


    /* Add result type to the module */
/*    Py_INCREF(&PoqueResultType);
    if (PyModule_AddObject(
            m, "Conn", (PyObject *)&PoqueResultType) == -1) {
        return NULL;
    } */
    if (PyModule_AddIntMacro(m, FORMAT_BINARY) == -1) return NULL;
    if (PyModule_AddIntMacro(m, FORMAT_TEXT) == -1) return NULL;

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
    if (PyModule_AddIntMacro(m, BITOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, BITARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, VARBITOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, VARBITARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, JSONOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, JSONBOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, JSONBARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, XMLOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, XMLARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, JSONARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, POINTOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, LSEGOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, PATHOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, BOXOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, POLYGONOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, LINEOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, LINEARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, ABSTIMEOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, RELTIMEOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TINTERVALOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, DATEOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TIMEOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TIMETZOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TIMETZARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TIMESTAMPOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TIMESTAMPARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, DATEARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TIMEARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TIMESTAMPTZOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TIMESTAMPTZARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INTERVALOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INTERVALARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, UNKNOWNOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, CIRCLEOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CIRCLEARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, CASHOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CASHARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, MACADDROID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, MACADDR8OID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INETOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CIDROID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, BOOLARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, BYTEAARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CHARARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, NAMEARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INT2ARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INT2VECTORARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, REGPROCARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TEXTARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, OIDARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TIDARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, XIDARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CIDARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, OIDVECTORARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, BPCHARARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, VARCHARARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INT8ARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, POINTARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, LSEGARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, PATHARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, BOXARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, FLOAT4ARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, FLOAT8ARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, ABSTIMEARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, RELTIMEARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, TINTERVALARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, POLYGONARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, MACADDRARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, MACADDR8ARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, INETARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CIDRARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, CSTRINGOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, BPCHAROID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, VARCHAROID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, CSTRINGARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, FLOAT4OID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, FLOAT8OID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, INT4ARRAYOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, UUIDOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, UUIDARRAYOID) == -1) return NULL;

    if (PyModule_AddIntMacro(m, NUMERICOID) == -1) return NULL;
    if (PyModule_AddIntMacro(m, NUMERICARRAYOID) == -1) return NULL;

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
            m, "POLLING_FAILED", PGRES_POLLING_FAILED) == -1)
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

    if (init_type_map() < 0)
        return NULL;

    return m;
}
