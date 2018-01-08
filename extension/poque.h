#ifndef _POQUE_H_
#define _POQUE_H_

#include <Python.h>
#include <libpq-fe.h>
#include <structmember.h>


#ifdef __GNUC__
#pragma GCC visibility push(hidden)
#endif

#if SIZEOF_SHORT == 2
typedef signed short poque_int16;
typedef unsigned short poque_uint16;
#endif

typedef struct poque_Result poque_Result;

extern PyObject *PoqueError;
extern PyObject *PoqueInterfaceError;
extern PyTypeObject poque_ConnType;
extern PyTypeObject poque_ResultType;

poque_Result *PoqueResult_New(PGresult *res);

PyObject *Poque_info_options(PQconninfoOption *options);
PyObject *Poque_value(Oid oid, int format, char *data, int len);


#define FORMAT_TEXT         0
#define FORMAT_BINARY       1

/* boolean */
#define BOOLOID             16
#define BOOLARRAYOID        1000

/* binary */
#define BYTEAOID            17
#define BYTEAARRAYOID       1001

/* integer types */
#define INT2OID             21
#define INT2VECTOROID       22
#define INT4OID             23
#define INT8OID             20
#define INT2ARRAYOID        1005
#define INT2VECTORARRAYOID  1006
#define INT4ARRAYOID        1007
#define INT8ARRAYOID        1016
#define BITOID              1560
#define BITARRAYOID         1561
#define VARBITOID           1562
#define VARBITARRAYOID      1563

/* string types */
#define CHAROID             18
#define NAMEOID             19
#define TEXTOID             25
#define BPCHAROID           1042
#define VARCHAROID          1043
#define CHARARRAYOID        1002
#define NAMEARRAYOID        1003
#define TEXTARRAYOID        1009
#define BPCHARARRAYOID      1014
#define VARCHARARRAYOID     1015
#define CSTRINGOID          2275
#define CSTRINGARRAYOID     1263

/* system identifiers */
#define REGPROCOID          24
#define OIDOID              26
#define TIDOID              27
#define XIDOID              28
#define CIDOID              29
#define OIDVECTOROID        30
#define REGPROCARRAYOID     1008
#define OIDARRAYOID         1028
#define TIDARRAYOID         1010
#define XIDARRAYOID         1011
#define CIDARRAYOID         1012
#define OIDVECTORARRAYOID   1013

/* complex types */
#define JSONOID             114
#define JSONBOID            3802
#define JSONBARRAYOID       3807
#define XMLOID              142
#define JSONARRAYOID        199
#define XMLARRAYOID         143

/* geometric types */
#define POINTOID            600
#define LSEGOID             601
#define PATHOID             602
#define BOXOID              603
#define POLYGONOID          604
#define LINEOID             628
#define LINEARRAYOID        629
#define POINTARRAYOID       1017
#define LSEGARRAYOID        1018
#define PATHARRAYOID        1019
#define BOXARRAYOID         1020
#define POLYGONARRAYOID     1027
#define CIRCLEOID           718
#define CIRCLEARRAYOID      719

/* floating point types */
#define FLOAT4OID           700
#define FLOAT8OID           701
#define FLOAT4ARRAYOID      1021
#define FLOAT8ARRAYOID      1022

/* date and time types */
#define ABSTIMEOID          702
#define RELTIMEOID          703
#define TINTERVALOID        704
#define ABSTIMEARRAYOID     1023
#define RELTIMEARRAYOID     1024
#define TINTERVALARRAYOID   1025
#define DATEOID             1082
#define TIMEOID             1083
#define TIMETZOID           1266
#define TIMESTAMPOID        1114
#define TIMESTAMPARRAYOID   1115
#define DATEARRAYOID        1182
#define TIMEARRAYOID        1183
#define TIMESTAMPTZOID      1184
#define TIMESTAMPTZARRAYOID 1185
#define INTERVALOID         1186
#define INTERVALARRAYOID    1187

/* misc */
#define UNKNOWNOID          705

/* money */
#define CASHOID             790
#define CASHARRAYOID        791

/* network types */
#define MACADDROID          829
#define MACADDR8OID         774
#define INETOID             869
#define CIDROID             650
#define MACADDRARRAYOID     1040
#define MACADDR8ARRAYOID    775
#define INETARRAYOID        1041
#define CIDRARRAYOID        651

/* uuid */
#define UUIDOID             2950
#define UUIDARRAYOID        2951

/* numeric */
#define NUMERICOID          1700
#define NUMERICARRAYOID     1231


int init_datetime(void);
int init_type_map(void);

#define NUMERIC_NAN         0xC000
#define NUMERIC_POS         0x0000
#define NUMERIC_NEG         0x4000

#endif
