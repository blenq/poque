CONNECTION_OK = 0
CONNECTION_BAD = 1
CONNECTION_STARTED = 2
CONNECTION_MADE = 3
CONNECTION_AWAITING_RESPONSE = 4
CONNECTION_AUTH_OK = 5
CONNECTION_SETENV = 6
CONNECTION_SSL_STARTUP = 7
CONNECTION_NEEDED = 8

TRANS_IDLE = 0
TRANS_ACTIVE = 1
TRANS_INTRANS = 2
TRANS_INERROR = 3
TRANS_UNKNOWN = 4

POLLING_FAILED = 0
POLLING_READING = 1
POLLING_WRITING = 2
POLLING_OK = 3
POLLING_ACTIVE = 4

EMPTY_QUERY = 0
COMMAND_OK = 1
TUPLES_OK = 2
COPY_OUT = 3
COPY_IN = 4
BAD_RESPONSE = 5
NONFATAL_ERROR = 6
FATAL_ERROR = 7
COPY_BOTH = 8
SINGLE_TUPLE = 9

INVALID_OID = 0

# boolean types
BOOLOID = 16
BOOLARRAYOID = 1000

# binary types
BYTEAOID = 17
BYTEAARRAYOID = 1001

# integer types
INT2OID = 21
INT2VECTOROID = 22
INT4OID = 23
INT8OID = 20
INT2ARRAYOID = 1005
INT2VECTORARRAYOID = 1006
INT4ARRAYOID = 1007
INT8ARRAYOID = 1016
BITOID = 1560
BITARRAYOID = 1561
VARBITOID = 1562

# string types
CHAROID = 18
NAMEOID = 19
TEXTOID = 25
BPCHAROID = 1042
VARCHAROID = 1043
CHARARRAYOID = 1002
NAMEARRAYOID = 1003
TEXTARRAYOID = 1009
BPCHARARRAYOID = 1014
VARCHARARRAYOID = 1015
CSTRINGOID = 2275
CSTRINGARRAYOID = 1263

# system identifiers
REGPROCOID = 24
OIDOID = 26
TIDOID = 27
XIDOID = 28
CIDOID = 29
OIDVECTOROID = 30
REGPROCARRAYOID = 1008
OIDARRAYOID = 1028
TIDARRAYOID = 1010
XIDARRAYOID = 1011
CIDARRAYOID = 1012
OIDVECTORARRAYOID = 1013

# complex types
JSONOID = 114
XMLOID = 142
JSONBOID = 3802
JSONARRAYOID = 199
JSONBARRAYOID = 3807
XMLARRAYOID = 143

# geometric types
POINTOID = 600
LSEGOID = 601
PATHOID = 602
BOXOID = 603
POLYGONOID = 604
POINTARRAYOID = 1017
LSEGARRAYOID = 1018
PATHARRAYOID = 1019
BOXARRAYOID = 1020
POLYGONARRAYOID = 1027
CIRCLEOID = 718
CIRCLEARRAYOID = 719
LINEOID = 628
LINEARRAYOID = 629

# floating point types
FLOAT4OID = 700
FLOAT8OID = 701
FLOAT4ARRAYOID = 1021
FLOAT8ARRAYOID = 1022

# date and time types
ABSTIMEOID = 702
RELTIMEOID = 703
TINTERVALOID = 704
ABSTIMEARRAYOID = 1023
RELTIMEARRAYOID = 1024
TINTERVALARRAYOID = 1025
DATEOID = 1082
TIMEOID = 1083
TIMESTAMPOID = 1114
TIMESTAMPARRAYOID = 1115
DATEARRAYOID = 1182
TIMEARRAYOID = 1183
TIMESTAMPTZOID = 1184
TIMESTAMPTZARRAYOID = 1185
INTERVALOID = 1186
INTERVALARRAYOID = 1187

# misc
UNKNOWNOID = 705

# money
CASHOID = 790
CASHARRAYOID = 791

# network types
MACADDROID = 829
INETOID = 869
CIDROID = 650
MACADDRARRAYOID = 1040
INETARRAYOID = 1041
CIDRARRAYOID = 651

# numeric types
NUMERICOID = 1700
NUMERICARRAYOID = 1231

# uuid
UUIDOID = 2950

# TODO: aclitem and _aclitem, timetz
