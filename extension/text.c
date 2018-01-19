#include "poque.h"
#include "poque_type.h"


static int
bytes_examine(param_handler *handler, PyObject *param) {
	handler->total_size += PyBytes_GET_SIZE(param);
	return 0;
}

static int
bytes_encode(param_handler *handler, PyObject *param, char **loc) {
	*loc = PyBytes_AS_STRING(param);
	return 0;
}


static int
bytes_encode_at(param_handler *handler, PyObject *param, char *loc, size_t *size) {
	char *str;
	int _size;

	_size = PyBytes_GET_SIZE(param);
	str = PyBytes_AS_STRING(param);
	memcpy(loc, str, _size);

	if (size != NULL) {
		*size = _size;
	}
	return 0;
}

param_handler *
new_bytes_param_handler(int num_param) {
	static param_handler def_handler = {
		bytes_examine,
		bytes_encode,
		bytes_encode_at,
		(ph_free)PyMem_Free,
		BYTEAOID,
		BYTEAARRAYOID,
		0
	}; /* static initialized handler */

	return new_param_handler(&def_handler, sizeof(param_handler));
}


typedef struct _TextParamHandler {
	param_handler handler;
	int *sizes;
	char **strings;
	Py_ssize_t str_count;
	PyObject **refs;
	Py_ssize_t ref_count;
	int pos;
} TextParamHandler;


static int
text_examine(param_handler *handler, PyObject *param) {
	TextParamHandler *self;
	char *string;
	Py_ssize_t size;

	self = (TextParamHandler *)handler;
	if (!PyUnicode_Check(param)) {
		/* If the object is not a string, execute str(obj) and use the outcome.
		 * Keep a reference for cleanup
		 */
		param = PyObject_Str(param);
		if (param == NULL) {
			return -1;
		}
		self->refs[self->ref_count++] = param;
	}
	string = PyUnicode_AsUTF8AndSize(param, &size);
	if (string == NULL) {
		return -1;
	}
	self->sizes[self->str_count] = size;
	self->strings[self->str_count++] = string;
	handler->total_size += size;
	return 0;
}

static int
text_encode(param_handler *handler, PyObject *param, char **loc) {
	TextParamHandler *self;

	self = (TextParamHandler *)handler;
	*loc = self->strings[self->pos++];
	return 0;
}

static int
text_encode_at(param_handler *handler, PyObject *param, char *loc, size_t *size) {
	TextParamHandler *self;
	char *str;
	int _size;

	self = (TextParamHandler *)handler;
	_size = self->sizes[self->pos];
	str = self->strings[self->pos++];
	memcpy(loc, str, _size);
	if (size != NULL) {
		*size = _size;
	}
	return 0;
}

static void
text_handler_free(param_handler *handler) {
	TextParamHandler *self;
	int i;

	self = (TextParamHandler *)handler;
	PyMem_Free(self->strings);
	for (i = 0; i < self->ref_count; i++) {
		Py_DECREF(self->refs[i]);
	}
	PyMem_Free(self->refs);
	PyMem_Free(self->sizes);
	PyMem_Free(self);
}

param_handler *
new_text_param_handler(int num_param) {
	static TextParamHandler def_handler = {
		{
			text_examine,		/* examine */
			text_encode,		/* encode */
			text_encode_at,		/* encode_at */
			text_handler_free,	/* free */
			TEXTOID,			/* oid */
			TEXTARRAYOID,		/* array_oid */
			0					/* total_size */
		},
		0
	}; /* static initialized handler */
	TextParamHandler *handler;

	/* create new handler identical to static one */
	handler = (TextParamHandler *)new_param_handler(
		(param_handler *)&def_handler, sizeof(TextParamHandler));
	if (handler == NULL) {
		return NULL;
	}

	/* initialize TextParamHandler specifics */
	handler->strings = PyMem_Calloc(num_param, sizeof(char*));
	handler->refs = PyMem_Calloc(num_param, sizeof(PyObject *));
	handler->sizes = PyMem_Calloc(num_param, sizeof(int));
	if (handler->strings == NULL || handler->refs == NULL ||
			handler->sizes == NULL) {
		PyErr_SetNone(PyExc_MemoryError);
		text_handler_free((param_handler *)handler);
		return NULL;
	}

	return (param_handler *)handler;
}
