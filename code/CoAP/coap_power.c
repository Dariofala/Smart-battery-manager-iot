#include "contiki.h"
#include "coap-engine.h"
#include "coap-observe.h"
#include "sys/log.h"
#include <stdio.h>
#include <string.h>

#include "dataset_replay.h" // include the dataset

#define LOG_MODULE "Sensore CoAP (Power)"
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
EVENT_RESOURCE(res_potenza, "title=\"Potenza Generata AC\";obs", res_get_handler, NULL, NULL, NULL, res_event_handler);

// Event handler: called when we execute res_potenza.trigger()
static void res_event_handler(void) {
  coap_notify_observers(&res_potenza);
}

// GET handler: builds the JSON payload to be sent
static void res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset) {
  
  float current_power = ac_power_array[current_step];

  //JSON formatting --> it uses %.2f because power has 2 decimals in the dataset
  snprintf((char *)buffer, preferred_size, 
           "{\"bn\":\"nRF52840-SensorPower\", \"n\":\"ac_power\", \"v\":%.2f, \"u\":\"W\"}", 
           current_power);

  size_t len = strlen((char *)buffer);
  
  // Set the CoAP header to indicate the content is JSON
  coap_set_header_content_format(response, APPLICATION_JSON);
  coap_set_payload(response, buffer, len);
}

// MAIN SENSOR PROCESS

PROCESS(coap_sensor_process, "CoAP Power Process");
AUTOSTART_PROCESSES(&coap_sensor_process);

PROCESS_THREAD(coap_sensor_process, ev, data)
{
  static struct etimer timer;

  PROCESS_BEGIN();

  LOG_INFO("Starting CoAP Sensor (AC Power)...\n");

  coap_engine_init();

  // Bind the resource to the correct URI "/telemetria/potenza"
  coap_activate_resource(&res_potenza, "telemetria/potenza");

  etimer_set(&timer, SAMPLING_INTERVAL);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

    current_step++;
    if(current_step >= REPLAY_STEPS) {
      current_step = 0; //  Rewind the dataset at the end of the 50 steps
    }

    LOG_INFO("Simulated sampling: Step %d, Value: %.2f W\n", current_step, ac_power_array[current_step]);
    
    // Trigger the CoAP event. This single line implements the entire Observe pattern towards the Actuator/Cloud
    res_potenza.trigger();

    etimer_reset(&timer);
  }

  PROCESS_END();
}