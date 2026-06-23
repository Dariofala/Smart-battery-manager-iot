#include "contiki.h"
#include "coap-engine.h"
#include "coap-observe.h"
#include "sys/log.h"
#include <stdio.h>
#include <string.h>

#include "dataset_replay.h" // include the dataset

#define LOG_MODULE "Sensore CoAP (Irradiation)"
#define LOG_LEVEL LOG_LEVEL_INFO

// Time between each sample
#define SAMPLING_INTERVAL (10 * CLOCK_SECOND)

// Global index to iterate through the dataset array
static int current_step = 0;

// OBSERVABLE RESOURCE DEFINITION (CoAP Observe)

// Resource handler prototypes
static void res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset);
static void res_event_handler(void);

// Observe resource definition
EVENT_RESOURCE(res_irraggiamento, "title=\"Irraggiamento Solare\";obs", res_get_handler, NULL, NULL, NULL, res_event_handler);

// Event handler: called when we execute res_irraggiamento.trigger()
static void res_event_handler(void) {
  coap_notify_observers(&res_irraggiamento);
}

// GET handler: builds the JSON payload to be sent
static void res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset) {
  
  float current_irradiation = irradiation_array[current_step];

  //JSON formatting --> it uses %.4f because irradiation has 4 decimals in the dataset
  snprintf((char *)buffer, preferred_size, 
           "{\"bn\":\"nRF52840-SensorIrr\", \"n\":\"irradiation\", \"v\":%.4f, \"u\":\"W/m2\"}", 
           current_irradiation);

  size_t len = strlen((char *)buffer);
  
  // Set the CoAP header to indicate the content is JSON
  coap_set_header_content_format(response, APPLICATION_JSON);
  coap_set_payload(response, buffer, len);
}

// MAIN SENSOR PROCESS

PROCESS(coap_irradiation_process, "CoAP Irradiation Process");
AUTOSTART_PROCESSES(&coap_irradiation_process);

PROCESS_THREAD(coap_irradiation_process, ev, data)
{
  static struct etimer timer;

  PROCESS_BEGIN();

  LOG_INFO("Starting CoAP Sensor (Irradiation)...\n");

  coap_engine_init();

  // Bind the resource to the URI "/telemetria/irraggiamento"
  coap_activate_resource(&res_irraggiamento, "telemetria/irraggiamento");

  etimer_set(&timer, SAMPLING_INTERVAL);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

    current_step++;
    if(current_step >= REPLAY_STEPS) {
      current_step = 0; //  Rewind the dataset at the end of the 50 steps
    }

    LOG_INFO("Simulated sampling: Step %d, Value: %.4f W/m2\n", current_step, irradiation_array[current_step]);
    
    // Trigger the CoAP event. This single line implements the entire Observe pattern towards the Actuator/Cloud
    res_irraggiamento.trigger();

    etimer_reset(&timer);
  }

  PROCESS_END();
}