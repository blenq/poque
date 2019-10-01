#ifndef _POQUE_NETWORK_H_
#define _POQUE_NETWORK_H_

int init_network(void);

extern PoqueValueHandler mac_val_handler;
extern PoqueValueHandler mac8_val_handler;
extern PoqueValueHandler inet_val_handler;
extern PoqueValueHandler cidr_val_handler;

extern PoqueValueHandler macarray_val_handler;
extern PoqueValueHandler mac8array_val_handler;
extern PoqueValueHandler inetarray_val_handler;
extern PoqueValueHandler cidrarray_val_handler;

#endif
