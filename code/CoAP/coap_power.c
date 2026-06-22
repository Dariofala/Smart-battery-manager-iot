#include "contiki.h"
#include "coap-engine.h"
#include "coap-observe.h"
#include "sys/log.h"
#include <stdio.h>
#include <string.h>

/* Includiamo il dataset simulato generato dal vostro script Python */
#include "dataset_replay.h"

#define LOG_MODULE "Sensore CoAP"
#define LOG_LEVEL LOG_LEVEL_INFO

/* Intervallo di campionamento (es. ogni 10 secondi per la simulazione su Cooja) */
#define SAMPLING_INTERVAL (10 * CLOCK_SECOND)

/* Indice globale per scorrere l'array del dataset */
static int current_step = 0;

/* =========================================================================== */
/* 1. DEFINIZIONE DELLA RISORSA OSSERVABILE (CoAP Observe)                     */
/* =========================================================================== */

/* Prototipi degli handler */
static void res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset);
static void res_event_handler(void);

/* Usiamo EVENT_RESOURCE invece del normale RESOURCE per abilitare l'Observe */
EVENT_RESOURCE(res_potenza,
               "title=\"Potenza Generata AC\";obs",
               res_get_handler,
               NULL,
               NULL,
               NULL,
               res_event_handler);

/* Handler dell'evento: viene chiamato quando facciamo res_potenza.trigger() */
static void res_event_handler(void) {
  /* coap_notify_observers avverte l'engine CoAP che il dato è cambiato.
     L'engine chiamerà automaticamente il res_get_handler per costruire
     il nuovo pacchetto da inviare ai Client iscritti. */
  coap_notify_observers(&res_potenza);
}

/* Handler della GET: costruisce il payload formattato da inviare */
static void res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset) {
  
  /* Recuperiamo il valore simulato in base allo step corrente */
  float current_power = ac_power_array[current_step];

  /* Data Encoding: Formattiamo il payload in JSON (Vincolo d'esame) */
  snprintf((char *)buffer, preferred_size, 
           "{\"bn\":\"nRF52840-Inverter\", \"n\":\"ac_power\", \"v\":%.2f, \"u\":\"W\"}", 
           current_power);

  size_t len = strlen((char *)buffer);
  
  /* Impostiamo l'header CoAP per indicare che il contenuto è JSON */
  coap_set_header_content_format(response, APPLICATION_JSON);
  coap_set_payload(response, buffer, len);
}

/* =========================================================================== */
/* 2. PROCESSO PRINCIPALE DEL SENSORE                                          */
/* =========================================================================== */

PROCESS(coap_sensor_process, "CoAP Sensor Process");
AUTOSTART_PROCESSES(&coap_sensor_process);

PROCESS_THREAD(coap_sensor_process, ev, data)
{
  static struct etimer timer;

  PROCESS_BEGIN();

  LOG_INFO("Avvio Sensore CoAP (Potenza AC)...\n");

  /* Inizializzazione del motore CoAP di Contiki-NG */
  coap_engine_init();

  /* Colleghiamo la nostra risorsa all'URI "/telemetria/potenza" */
  coap_activate_resource(&res_potenza, "telemetria/potenza");

  /* Impostiamo il timer periodico */
  etimer_set(&timer, SAMPLING_INTERVAL);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

    /* Aggiorniamo l'indice per la simulazione dei dati cronologici */
    current_step++;
    if(current_step >= REPLAY_STEPS) {
      current_step = 0; /* Loop back se il dataset finisce */
    }

    LOG_INFO("Campionamento simulato: Step %d, Valore: %.2f W\n", current_step, ac_power_array[current_step]);

    /* Scateniamo l'evento CoAP. 
       Questa singola riga implementa tutto il pattern Observe verso l'Attuatore/Cloud */
    res_potenza.trigger();

    /* Riavvio del timer */
    etimer_reset(&timer);
  }

  PROCESS_END();
}