#ifndef PROJECT_CONF_H_
#define PROJECT_CONF_H_

/* Buffer di rete IPv6 - Lo aumentiamo per contenere comodamente il pacchetto CoAP ed header */
#undef UIP_CONF_BUFFER_SIZE
#define UIP_CONF_BUFFER_SIZE 512

#define QUEUEBUF_CONF_NUM 8
#define UIP_CONF_ROUTER 1
#define LOG_CONF_LEVEL_COAP LOG_LEVEL_INFO

/* --- IMPOSTAZIONI COAP --- */
#undef REST_MAX_CHUNK_SIZE
#undef COAP_MAX_HEADER_SIZE

/* Manteniamo la struttura per avere un pacchetto CoAP massimo di 256 byte */
#define COAP_MAX_HEADER_SIZE 64
#define REST_MAX_CHUNK_SIZE  192

#endif /* PROJECT_CONF_H_ */