#include "contiki.h"
#include "coap-engine.h"
#include "coap-observe.h"
#include "sys/log.h"
#include <stdio.h>
#include <string.h>

/* Includiamo il dataset simulato */
#include "dataset_replay.h"

#define LOG_MODULE "Sensore CoAP (Irr)"
#define LOG_LEVEL LOG_LEVEL_INFO

/* Intervallo di campionamento simulato: 10 secondi */
#define SAMPLING_INTERVAL (10 * CLOCK_SECOND)

static int current_step = 0;

/* Prototipi degli handler per la risorsa */
static void res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset);
static void res_event_handler(void);

/* Definizione della risorsa Observe */
EVENT_RESOURCE(res_irraggiamento,
               "title=\"Irraggiamento Solare\";obs",
               res_get_handler,
               NULL,
               NULL,
               NULL,
               res_event_handler);

/* Handler per il trigger dell'evento (notifica gli iscritti) */
static void res_event_handler(void) {
  coap_notify_observers(&res_irraggiamento);
}

/* Handler per la GET / Observe (costruisce il payload JSON) */
static void res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset) {
  
  float current_irradiation = irradiation_array[current_step];

  /* Formattazione JSON. Usiamo %.4f perché l'irraggiamento ha 4 decimali nel dataset */
  snprintf((char *)buffer, preferred_size, 
           "{\"bn\":\"nRF52840-SensoreIrr\", \"n\":\"irradiation\", \"v\":%.4f, \"u\":\"W/m2\"}", 
           current_irradiation);

  size_t len = strlen((char *)buffer);
  
  coap_set_header_content_format(response, APPLICATION_JSON);
  coap_set_payload(response, buffer, len);
}

/* =========================================================================== */
/* PROCESSO PRINCIPALE DEL SENSORE                                             */
/* =========================================================================== */

PROCESS(coap_irradiation_process, "CoAP Irradiation Process");
AUTOSTART_PROCESSES(&coap_irradiation_process);

PROCESS_THREAD(coap_irradiation_process, ev, data)
{
  static struct etimer timer;

  PROCESS_BEGIN();

  LOG_INFO("Avvio Sensore CoAP (Irraggiamento)...\n");

  coap_engine_init();

  /* Colleghiamo la risorsa all'URI corretto */
  coap_activate_resource(&res_irraggiamento, "telemetria/irraggiamento");

  etimer_set(&timer, SAMPLING_INTERVAL);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

    current_step++;
    if(current_step >= REPLAY_STEPS) {
      current_step = 0; /* Riavvolge il dataset al termine dei 50 step */
    }

    LOG_INFO("Campionamento simulato: Step %d, Valore: %.4f W/m2\n", current_step, irradiation_array[current_step]);

    /* Scatena la notifica Observe verso il Cloud / Attuatore */
    res_irraggiamento.trigger();

    etimer_reset(&timer);
  }

  PROCESS_END();
}