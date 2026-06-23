#ifndef PROJECT_CONF_H_
#define PROJECT_CONF_H_

/* Buffer di rete maggiorati per gestire le doppie connessioni */
#undef UIP_CONF_BUFFER_SIZE
#define UIP_CONF_BUFFER_SIZE 512
#define QUEUEBUF_CONF_NUM 16

/* --- IMPOSTAZIONI TCP / MQTT --- */
#define UIP_CONF_TCP 1
#define UIP_CONF_MAX_CONNECTIONS 4
#define UIP_CONF_TCP_MSS 128
#define MQTT_MAX_PACKET_SIZE 256

/* --- IMPOSTAZIONI COAP --- */
#undef REST_MAX_CHUNK_SIZE
#undef COAP_MAX_HEADER_SIZE
#define COAP_MAX_HEADER_SIZE 64
#define REST_MAX_CHUNK_SIZE  192
#define COAP_OBSERVE_CLIENT 1

#endif /* PROJECT_CONF_H_ */