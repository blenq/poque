#ifndef _POQUE_H_
#define _POQUE_H_

#include <Python.h>
#include <libpq-fe.h>
#include <structmember.h>


#ifdef __GNUC__
#pragma GCC visibility push(hidden)
#endif

typedef struct poque_Result poque_Result;

extern PyObject *PoqueError;
extern PyTypeObject poque_ConnType;
extern PyTypeObject poque_ResultType;

poque_Result *PoqueResult_New(PGresult *res);

PyObject *Poque_info_options(PQconninfoOption *options);
PyObject *Poque_value(Oid oid, int format, char *data, int len);

#define BOOLOID         16
#define BYTEAOID        17
#define CHAROID         18
#define NAMEOID         19
#define INT8OID         20
#define INT2OID         21
#define INT2VECTOROID   22
#define INT4OID         23
#define REGPROCOID      24
#define TEXTOID         25
#define OIDOID          26
#define TIDOID          27
#define XIDOID          28
#define CIDOID          29
#define OIDVECTOROID    30

#define JSONOID         114
#define XMLOID          142
#define XMLARRAYOID     143
#define JSONARRAYOID    199

#define POINTOID        600
#define LSEGOID         601
#define PATHOID         602
#define BOXOID          603
#define POLYGONOID      604

#define FLOAT4OID       700
#define FLOAT8OID       701
#define ABSTIMEOID      702
#define RELTIMEOID      703
#define TINTERVALOID    704
#define UNKNOWNOID      705

#define CIRCLEOID       718
#define CIRCLEARRAYOID  719

#define CASHOID         790
#define CASHARRAYOID    791

#define MACADDROID      829

#define INT4ARRAYOID    1007
#define UUIDOID         2950

void init_datetime(void);

#endif
