#ifndef _POQUE_TYPE_H_
#define _POQUE_TYPE_H_

typedef struct _param_handler param_handler;

typedef param_handler *(*ph_new)(int num_param);
typedef int (*ph_examine)(param_handler *self, PyObject *param);
typedef int (*ph_encode)(param_handler *self, PyObject *param, char **loc);
typedef int (*ph_encode_at)(param_handler *self, PyObject *param, char *loc, size_t *size);
typedef void (*ph_free)(param_handler *self);


typedef struct _param_handler {
	ph_examine examine;
	ph_encode encode;
	ph_encode_at encode_at;
	ph_free free;
	Oid oid;
	Oid array_oid;
	int total_size;
} param_handler;

#define PH_Examine(ph, param) (ph)->examine((ph), (param))
#define PH_Oid(ph) (ph)->oid
#define PH_TotalSize(ph) (ph)->total_size
#define PH_HasEncode(ph) ((ph)->encode != NULL)
#define PH_EncodeValue(ph, v, p) (ph)->encode(ph, v, p)
#define PH_EncodeValueAt(ph, v, p, s) (ph)->encode_at(ph, v, p, s)
#define PH_Free(ph) (ph)->free(ph)

param_handler *new_text_param_handler(int num_param);
param_handler *new_float_param_handler(int num_param);
param_handler *new_bytes_param_handler(int num_param);

ph_new get_param_handler_constructor(PyTypeObject *typ);
param_handler *new_param_handler(param_handler *def_handler, size_t handler_size);


#endif
