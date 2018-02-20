#ifndef _POQUE_TYPE_H_
#define _POQUE_TYPE_H_

#include "poque.h"
#include "val_crs.h"

typedef PyObject *(*pq_read)(ValueCursor *crs);

typedef struct _poqueTypeEntry {
    Oid oid;
    pq_read binval;
    pq_read strval;
    Oid el_oid;        /* type of subelement for array_binval converter */
    struct _poqueTypeEntry *next;
} PoqueTypeEntry;

PyObject *array_binval(ValueCursor *crs);
PyObject *Result_getview(poque_Result *self, char *data, int len);


typedef struct _param_handler param_handler;

typedef param_handler *(*ph_new)(int num_param);
typedef int (*ph_examine)(param_handler *self, PyObject *param);
typedef int (*ph_total_size)(param_handler *self);
typedef int (*ph_encode)(param_handler *self, PyObject *param, char **loc);
typedef int (*ph_encode_at)(param_handler *self, PyObject *param, char *loc);
typedef void (*ph_free)(param_handler *self);


typedef struct _param_handler {
	ph_examine examine;
	ph_total_size total_size;
	ph_encode encode;
	ph_encode_at encode_at;
	ph_free free;
	Oid oid;
	Oid array_oid;
} param_handler;

#define PH_Examine(ph, param) (ph)->examine((ph), (param))
#define PH_Oid(ph) (ph)->oid
#define PH_HasTotalSize(ph) ((ph)->total_size != NULL)
#define PH_TotalSize(ph) (ph)->total_size(ph)
#define PH_HasEncode(ph) ((ph)->encode != NULL)
#define PH_EncodeValue(ph, v, p) (ph)->encode(ph, v, p)
#define PH_EncodeValueAt(ph, v, p) (ph)->encode_at(ph, v, p)
#define PH_HasFree(ph) ((ph)->free != NULL)
#define PH_Free(ph) (ph)->free(ph)

#define current_param(h, p) &(h)->params[(*p)++]
#define current_examine_param(h) current_param((h), (&(h)->examine_pos))
#define current_encode_param(h) current_param((h), (&(h)->encode_pos))

ph_new get_param_handler_constructor(PyTypeObject *typ);
param_handler *new_param_handler(param_handler *def_handler, size_t handler_size);
param_handler *new_object_param_handler(int num_params);

void write_uint16(char **p, poque_uint16 val);
void write_uint32(char **p, PY_UINT32_T val);
void write_uint64(char **p, PY_UINT64_T val);

void register_value_handler_table(PoqueTypeEntry *table);
void register_parameter_handler(PyTypeObject *typ, ph_new constructor);
void register_compatible_param(PyTypeObject *typ1, PyTypeObject *typ2);

#endif
