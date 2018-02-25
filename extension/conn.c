#include "poque.h"
#include "poque_type.h"


static void Conn_set_error(PGconn *conn) {
    if (conn == NULL) {
        PyErr_SetString(PoqueInterfaceError, "Connection is closed");
    } else {
        PyErr_SetString(PoqueInterfaceError, PQerrorMessage(conn));
    }
}


#define CONN_MAX_KWDS 64

static int
Conn_init_kwds(PyObject *kwds, char **names, char **values,
               int *expand_dbname, int *blocking)
{
    /* Only keyword arguments, use the libpq parameter connect functions
     *
     * Parameters names and values are passed as is (except for blocking and
     * expand_dbname), so this stays compatible when libpq adds more
     * parameter options in future versions. Therefore it uses its own
     * argument parsing instead the provided Python API argument parser
     * funtions.
     *
     */
    char *name, *value;
    Py_ssize_t pos = 0;
    PyObject *key, *val;
    int params_size;

    if (PyMapping_Length(kwds) > CONN_MAX_KWDS - 2) {
        PyErr_SetString(PyExc_TypeError, "Too many arguments");
        return -1;
    }
    value = NULL;
    *expand_dbname = 0;
    params_size = 0;

    /* parse the parameters */
    while (PyDict_Next(kwds, &pos, &key, &val)) {
        name = PyUnicode_AsUTF8(key);
        if (name == NULL) {
            return -1;
        }
        if (strcmp(name, "blocking") == 0){
            /* special poque parameter, not passed to libpq, used to determine
             * the connect function
             */
            *blocking = PyObject_IsTrue(val);
            if (*blocking == -1) {
                return -1;
            }
        }
        else if (strcmp(name, "expand_dbname") == 0) {
            /* special libpq parameter. Boolean instead of name, value
             * pair
             */
            *expand_dbname = PyObject_IsTrue(val);
            if (*expand_dbname == -1) {
                return -1;
            }
        } else if (val != Py_None) {
            /* regular string value */
            if (!PyUnicode_Check(val)) {
                PyErr_SetString(PyExc_TypeError, "Argument must be a string");
                return -1;
            }
            value = PyUnicode_AsUTF8(val);
            if (value == NULL)
                return -1;
            /* Add parameter name and value to the argument arrays */
            names[params_size] = name;
            values[params_size] = value;
            params_size += 1;
        }
    }
    return params_size;
}


static void
Conn_notice_receiver(PoqueConn *self, const PGresult *res)
{
    char *sql_state;


    sql_state = PQresultErrorField(res, PG_DIAG_SQLSTATE);
    if (sql_state == NULL || sql_state[0] == '\0' ||
            strncmp(sql_state, "01", 2) == 0 ) {
        self->warning_msg = PQresultErrorMessage(res);
    }
}


static int
Conn_init(PoqueConn *self, PyObject *args, PyObject *kwds)
{
    /*
    * We allocate space for a possible 62 (= 64 minus a terminating
    * NULL and the added client_encoding) parameters.
    * At the moment of writing there are 27 recognized parameters names, so
    * this should be sufficient for a while.
    */
    Py_ssize_t args_len;
    char *names[CONN_MAX_KWDS], *values[CONN_MAX_KWDS];
    int blocking = 1, expand_dbname;
    int params_size;
    PGconn *conn;

    args_len = PyTuple_Size(args);
    if (args_len == 0) {
        params_size = Conn_init_kwds(kwds, names, values, &expand_dbname,
                                     &blocking);
        if (params_size < 0) {
            return -1;
        }
    } else {
        /* connection string version */
        static char *kwlist[] = {"conninfo", "blocking", NULL};
        if (!PyArg_ParseTupleAndKeywords(
                args, kwds, "s|i", kwlist, values, &blocking)) {
            return -1;
        }
        names[0] = "dbname";
        expand_dbname = 1;
        params_size = 1;
    }

    /* add or, if already present, override encoding */
    names[params_size] = "client_encoding";
    values[params_size] = "UTF8";
    names[params_size + 1] = NULL;  /* terminate array */

    /* and finally connect */
    if (blocking) {
        Py_BEGIN_ALLOW_THREADS
       conn = PQconnectdbParams(
           (const char * const *)names, (const char * const *)values,
           expand_dbname);
       Py_END_ALLOW_THREADS
    } else {
        conn = PQconnectStartParams(
            (const char * const *)names, (const char * const *)values,
            expand_dbname);
    }

    /* error checking */
    if (conn == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    if (PQstatus(conn) == CONNECTION_BAD) {
        Conn_set_error(conn);
        PQfinish(conn);
        return -1;
    }
    self->conn = conn;

    PQsetNoticeReceiver(conn, (PQnoticeReceiver)Conn_notice_receiver, self);

    return 0;
}


static PyObject *
Conn_finish(PoqueConn *self, PyObject *unused)
{
    PQfinish(self->conn);
    self->conn = NULL;
    Py_RETURN_NONE;
}


static PyObject *
Conn_fileno(PoqueConn *self, PyObject *unused)
{
    int ret;
    ret = PQsocket(self->conn);

    if (ret == -1) {
        PyErr_SetString(PyExc_ValueError, "Connection is closed");
        return NULL;
    }
    return PyLong_FromLong(ret);
}


static PGresult *
Conn_exec_params(PGconn *conn, char *sql, PyObject *parameters, Py_ssize_t num_params, int format)
{
    param_handler **param_handlers;
	Oid *param_types;
	char **param_values, **clean_up;
	int *param_lengths, *param_formats;
	PyObject *param;
	int i, clean_up_count = 0, handler_count = 0;
	PGresult *res = NULL;

	param_handlers = PyMem_Malloc(num_params * sizeof(param_handler *));
	param_types = PyMem_Calloc(num_params, sizeof(Oid));
	param_values = PyMem_Calloc(num_params, sizeof(char *));
	param_lengths = PyMem_Calloc(num_params, sizeof(int));
	param_formats = PyMem_Calloc(num_params, sizeof(int));
	clean_up = PyMem_Calloc(num_params, sizeof(char *));
	if (param_handlers == NULL || param_types == NULL || param_values == NULL ||
			param_lengths == NULL || param_formats == NULL ||
			clean_up == NULL) {
		PyErr_SetNone(PyExc_MemoryError);
		goto end;
	}

	for (i = 0; i < num_params; i++) {
		param = PySequence_ITEM(parameters, i);
		param_formats[i] = FORMAT_BINARY;
		if (param == Py_None) {
			/* Special case: NULL values */
			param_types[i] = TEXTOID;
			param_values[i] = NULL;
			param_lengths[i] = 0;
		}
		else {
			int size;
			param_handler *handler;

			/* get the parameter handler based on type */
			handler = get_param_handler_constructor(Py_TYPE(param))(1);
			if (handler == NULL) {
			    goto end;
			}
			if (PH_HasFree(handler)) {
			    param_handlers[handler_count++] = handler;
			}

			/* examine the value to calculate value size and determine Oid */
			size = PH_Examine(handler, param);
			if (size < 0) {
				goto end;
			}
			param_lengths[i] = size;
			param_types[i] = PH_Oid(handler);

			/* convert the parameter to pg char * format */
			if (PH_HasEncode(handler)) {
				/* If the handler has already access to the raw pointer (as
				 * for bytes and str objects), just use that without allocating
				 * and copying memory
				 */
				if (PH_EncodeValue(handler, param, &param_values[i]) < 0) {
					goto end;
				}
			}
			else {
			    char *param_value;

				/* Allocate memory for value */
				param_value = PyMem_Malloc(size);
				if (param_value == NULL) {
					PyErr_SetNone(PyExc_MemoryError);
					goto end;
				}

				/* set the value pointer and register for cleanup */
				param_values[i] = param_value;
				clean_up[clean_up_count++] = param_value;

				/* write char * value into pointer */
				if (PH_EncodeValueAt(handler, param, param_value) < 0) {
					goto end;
				}
			}
		}
	}

	/* everything set up, send the command with parameters */
	Py_BEGIN_ALLOW_THREADS
	res = PQexecParams(conn, sql, (int)num_params, param_types,
					   (const char * const*)param_values, param_lengths,
					   param_formats, format);
	Py_END_ALLOW_THREADS

end:

	/* clean up */
	for (i = 0; i < handler_count; i++) {
		PH_Free(param_handlers[i]);
	}
	for (i = 0; i < clean_up_count; i++) {
		PyMem_Free(clean_up[i]);
	}
	PyMem_Free(param_types);
	PyMem_Free(param_values);
	PyMem_Free(param_lengths);
	PyMem_Free(param_formats);
	PyMem_Free(param_handlers);
	PyMem_Free(clean_up);
	return res;
}


static PyObject *
Conn_execute(PoqueConn *self, PyObject *args, PyObject *kwds) {
    char *sql;
    PyObject *parameters = NULL;
    int format = FORMAT_BINARY;
    PGresult *res;

    static char *kwlist[] = {"command", "parameters", "result_format", NULL};
    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "s|Oi", kwlist, &sql, &parameters, &format))
        return NULL;
    res = _Conn_execute(self, sql, parameters, format);
    if (res == NULL) {
        return NULL;
    }
    return (PyObject *)PoqueResult_New(res, self);
}

PGresult *
_Conn_execute(PoqueConn *self, char *sql, PyObject *parameters, int format) {

    PGresult *res;
    ExecStatusType res_status;
    Py_ssize_t num_params = 0;

    if (format)
        format = FORMAT_BINARY;
    if (parameters != NULL) {
    	if (!PySequence_Check(parameters)) {
			PyErr_SetString(PoqueError, "parameters must be a sequence");
			return NULL;
		}
    	num_params = PySequence_Length(parameters);
    }

    if (num_params == 0) {
    	Py_BEGIN_ALLOW_THREADS
		if (format == FORMAT_BINARY)
			res = PQexecParams(self->conn, sql, 0, NULL, NULL, NULL, NULL, format);
		else
			res = PQexec(self->conn, sql);
    	Py_END_ALLOW_THREADS
    	if (res == NULL) {
            Conn_set_error(self->conn);
            return NULL;
        }
    } else {
    	res = Conn_exec_params(self->conn, sql, parameters, num_params, format);
    	if (res == NULL) {
            return NULL;
        }
    }

    res_status = PQresultStatus(res);
    if (res_status == PGRES_BAD_RESPONSE || res_status == PGRES_FATAL_ERROR) {
        PyErr_SetString(PoqueError, PQresultErrorMessage(res));
        PQclear(res);
        return NULL;
    }
    return res;
}


static PyObject *
Conn_parameter_status(PoqueConn *self, PyObject *args) {
    char *param_name;
    const char *ret;

    if (!PyArg_ParseTuple(args, "s", &param_name)) {
        return NULL;
    }
    ret = PQparameterStatus(self->conn, param_name);
    if (ret == NULL) {
        Py_RETURN_NONE;
    }
    return PyUnicode_FromString(ret);
}


static PyObject *
Conn_poll(PoqueConn *self, PostgresPollingStatusType (poll_func)(PGconn *)) {
    PostgresPollingStatusType status;

    status = poll_func(self->conn);
    if (status == PGRES_POLLING_FAILED) {
        Conn_set_error(self->conn);
        return NULL;
    }
    return PyLong_FromLong(status);
}


static PyObject *
Conn_connect_poll(PoqueConn *self, PyObject *unused)
{
    return Conn_poll(self, PQconnectPoll);
}


static PyObject *
Conn_info(PoqueConn *self, PyObject *unused)
{
    PyObject *options;
    PQconninfoOption *info;

    info = PQconninfo(self->conn);
    if (info == NULL) {
        if (self->conn == NULL) {
            Py_RETURN_NONE;
        }
        else {
            PyErr_SetString(PyExc_ValueError, "Connection is closed");
            return NULL;
        }
    }
    options = Poque_info_options(info);
    PQconninfoFree(info);
    return options;
}


static PyObject *
Conn_reset(PoqueConn *self, PyObject *unused)
{
    PQreset(self->conn);
    if (PQstatus(self->conn) == CONNECTION_BAD) {
        Conn_set_error(self->conn);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Conn_reset_start(PoqueConn *self, PyObject *unused)
{
    int ret;

    ret = PQresetStart(self->conn);
    if (ret == 0) {
        Conn_set_error(self->conn);
        return NULL;
    }
    Py_RETURN_NONE;
}


static PyObject *
Conn_reset_poll(PoqueConn *self, PyObject *unused)
{
    return Conn_poll(self, PQresetPoll);
}


static PoqueCursor *
Conn_cursor(PoqueConn *self, PyObject *unused)
{
    return PoqueCursor_New(self);
}


static PyObject *
Conn_escape_function(
        PoqueConn *self, PyObject *args, PyObject *kwds, char *kwlist[],
        char *(*func)(PGconn *, const char *, size_t)) {
    char *literal;
    Py_ssize_t lit_size;
    PyObject *ret;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwds, "s#", kwlist, &literal, &lit_size))
        return NULL;
    literal = func(self->conn, literal, lit_size);
    if (literal == NULL) {
        Conn_set_error(self->conn);
        return NULL;
    }
    ret = PyUnicode_FromString(literal);
    PQfreemem(literal);
    return ret;
}


static PyObject *
Conn_escape_literal(PoqueConn *self, PyObject *args, PyObject *kwds) {
    static char *kwlist[] = {"literal", NULL};
    return Conn_escape_function(self, args, kwds, kwlist, PQescapeLiteral);
}


static PyObject *
Conn_escape_identifier(PoqueConn *self, PyObject *args, PyObject *kwds) {
    static char *kwlist[] = {"identifier", NULL};
    return Conn_escape_function(self, args, kwds, kwlist, PQescapeIdentifier);
}


static void
Conn_dealloc(PoqueConn *self)
{
    PQfinish(self->conn);
    if (self->wr_list != NULL)
        PyObject_ClearWeakRefs((PyObject *) self);
    Py_TYPE(self)->tp_free((PyObject*)self);
}


static PyObject *
Conn_intprop(PoqueConn *self, int (*func)(PGconn *))
{
    /* Generic function to get int properties */
    return PyLong_FromLong(func(self->conn));
}


static PyObject *
Conn_charprop(PoqueConn *self, char *(*func)(PGconn *))
{
    char *ret;
    ret = func(self->conn);
    if (ret) {
        return PyUnicode_DecodeUTF8(ret, strlen(ret), "strict");
    }
    Py_RETURN_NONE;
}


static PyMemberDef Conn_members[] = {
    {"autocommit", T_BOOL, offsetof(PoqueConn, autocommit), 0,
     "Autocommit"},
    {NULL}
};


static PyGetSetDef Conn_getset[] = {{
        "status",
        (getter)Conn_intprop,
        NULL,
        PyDoc_STR("connection status"),
        PQstatus
    }, {
        "transaction_status",
        (getter)Conn_intprop,
        NULL,
        PyDoc_STR("transaction status"),
        PQtransactionStatus
    }, {
        "protocol_version",
        (getter)Conn_intprop,
        NULL,
        PyDoc_STR("protocol version"),
        PQprotocolVersion
    }, {
        "backend_pid",
        (getter)Conn_intprop,
        NULL,
        PyDoc_STR("backend process id"),
        PQbackendPID
    }, {
        "server_version",
        (getter)Conn_intprop,
        NULL,
        PyDoc_STR("server version"),
        PQserverVersion
    }, {
    	"client_encoding",
		(getter)Conn_intprop,
		NULL,
		PyDoc_STR("client encoding"),
		PQclientEncoding
    }, {
        "db",
        (getter)Conn_charprop,
        NULL,
        PyDoc_STR("database name"),
        PQdb
    }, {
        "user",
        (getter)Conn_charprop, NULL,
        PyDoc_STR("user"),
        PQuser
    }, {
        "password",
        (getter)Conn_charprop,
        NULL,
        PyDoc_STR("password"),
        PQpass
    }, {
        "port",
        (getter)Conn_charprop,
        NULL,
        PyDoc_STR("port"),
        PQport
    }, {
        "host",
        (getter)Conn_charprop,
        NULL,
        PyDoc_STR("host"),
        PQhost
    }, {
        "options",
        (getter)Conn_charprop,
        NULL,
        PyDoc_STR("command line options"),
        PQoptions
    }, {
        "error_message",
        (getter)Conn_charprop,
        NULL,
        PyDoc_STR("error message"),
        PQerrorMessage
    }, {
        NULL
}};


static PyMethodDef Conn_methods[] = {{
        "finish", (PyCFunction)Conn_finish, METH_NOARGS,
        PyDoc_STR("close the connection")
    }, {
        "close", (PyCFunction)Conn_finish, METH_NOARGS,
        PyDoc_STR("close the connection")
    }, {
        "fileno", (PyCFunction)Conn_fileno, METH_NOARGS,
        PyDoc_STR("gets the file descriptor of the connection")
    }, {
        "info", (PyCFunction)Conn_info, METH_NOARGS,
        PyDoc_STR("connection info")
    }, {
        "parameter_status", (PyCFunction)Conn_parameter_status,
        METH_VARARGS, PyDoc_STR("parameter status")
    }, {
        "reset", (PyCFunction)Conn_reset, METH_NOARGS, PyDoc_STR("reset")
    }, {
        "reset_start", (PyCFunction)Conn_reset_start, METH_NOARGS,
        PyDoc_STR("reset start")
    }, {
        "reset_poll", (PyCFunction)Conn_reset_poll, METH_NOARGS,
        PyDoc_STR("reset poll")
    }, {
        "connect_poll", (PyCFunction)Conn_connect_poll, METH_NOARGS,
        PyDoc_STR("poll connect")
    }, {
        "execute", (PyCFunction)Conn_execute, METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("execute a statement")
    }, {
        "escape_literal", (PyCFunction)Conn_escape_literal,
        METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("escape a literal")
    }, {
        "escape_identifier", (PyCFunction)Conn_escape_identifier,
        METH_VARARGS| METH_KEYWORDS,
        PyDoc_STR("escape an indentifier")
    }, {
        "cursor", (PyCFunction)Conn_cursor, METH_NOARGS,
        PyDoc_STR("create cursor")
    }, {
        NULL
}};


PyTypeObject PoqueConnType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "poque.Conn",                               /* tp_name */
    sizeof(PoqueConn),                         /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)Conn_dealloc,                   /* tp_dealloc */
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
    PyDoc_STR("poque connection object"),       /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    offsetof(PoqueConn, wr_list),              /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    Conn_methods,                               /* tp_methods */
    Conn_members,                               /* tp_members */
    Conn_getset,                                /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)Conn_init,                        /* tp_init */
    0                                           /* tp_alloc */
};
