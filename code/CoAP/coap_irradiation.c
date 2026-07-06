#include <stdio.h>
#include <string.h>
#include "contiki.h"
#include "coap-engine.h"
#include "coap-blocking-api.h" 
#include "net/ipv6/uip-ds6.h"
#include "net/ipv6/uiplib.h"
#include "sys/log.h"

#include "dataset_replay.h" // include the dataset

#define LOG_MODULE "CoAP Sensor (Irradiation)"
#define LOG_LEVEL LOG_LEVEL_INFO

// Time between each sample
#define SAMPLING_INTERVAL (10 * CLOCK_SECOND)


// REGISTRATION CONFIGURATION 
#define SERVER_EP "coap://[fd00::1]:5683"
#define NODE_ID "nRF52840-Irradiation"
#define NODE_TYPE "irradiation"

static coap_endpoint_t server_ep;
static coap_message_t request[1]; // The CoAP request packet 

// State Machine for Registration 
#define STATE_INIT 0
#define STATE_REGISTERING 1
#define STATE_REGISTERED 2

static uint8_t reg_state = STATE_INIT;


// Global index to iterate through the dataset array
static int current_step = 0;

// OBSERVABLE RESOURCE DEFINITION (CoAP Observe)

// Resource handler prototypes
static void res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset);
static void res_event_handler(void);

// Observe resource definition
EVENT_RESOURCE(res_irradiation, "title=\"Solar irradiation\";obs", res_get_handler, NULL, NULL, NULL, res_event_handler);

// Event handler: called when we execute res_irradiation.trigger()
static void res_event_handler(void) {
  coap_notify_observers(&res_irradiation);
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



/* Callback to handle the Cloud App's response to our POST request */
static void registration_callback(coap_message_t *response) {
  if(response == NULL) {
    LOG_ERR("[REGISTRATION] Request timed out. Cloud is unreachable.\n");
    reg_state = STATE_INIT; /* Will retry at the next cycle */
    return;
  }
  
  LOG_INFO("[REGISTRATION] Confirmed by the Cloud App!\n");
  reg_state = STATE_REGISTERED;
}

// Helper function to check if the node has joined the RPL network 
static bool have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL || uip_ds6_defrt_choose() == NULL) {
    return false;
  }
  return true;
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

  // Bind the resource to the URI "/telemetry/irradiation"
  coap_activate_resource(&res_irradiation, "telemetry/irradiation");

  etimer_set(&timer, SAMPLING_INTERVAL);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

   // Check if the IPv6 network is ready and the Border Router is reachable 
    if(have_connectivity()) {
      
      // PHASE 1: CLOUD REGISTRATION
      if(reg_state == STATE_INIT) {
        LOG_INFO("[REGISTRATION] Sending POST request to %s\n", SERVER_EP);
        
        coap_endpoint_parse(SERVER_EP, strlen(SERVER_EP), &server_ep);
        coap_init_message(request, COAP_TYPE_CON, COAP_POST, 0);
        coap_set_header_uri_path(request, "registration");
        
        // Create the JSON payload: {"id": "nRF52840-Irradiation", "type": "irradiation"} 
        static char reg_payload[64];
        snprintf(reg_payload, sizeof(reg_payload), "{\"id\":\"%s\", \"type\":\"%s\"}", NODE_ID, NODE_TYPE);
        coap_set_payload(request, (uint8_t *)reg_payload, strlen(reg_payload));
        
        reg_state = STATE_REGISTERING; /* Wait for the callback */
        /* Send the request to the Python server */
        COAP_BLOCKING_REQUEST(&server_ep, request, registration_callback);
      }
      
      // PHASE 2: TELEMETRY 
      else if(reg_state == STATE_REGISTERED) {
        
        current_step++;
        if(current_step >= REPLAY_STEPS) {
          current_step = 0; // Rewind the dataset at the end of the 150 steps
        }

        LOG_INFO("Simulated sampling: Step %d, Value: %.2f W/m2\n", current_step, irradiation_array[current_step]);
        
        // Trigger the CoAP event. This single line implements the entire Observe pattern towards the Actuator/Cloud
        res_irradiation.trigger();
      }
      
    } else {
        LOG_INFO("Waiting for RPL routing...\n");
        reg_state = STATE_INIT; // Reset registration state if we lose connectivity 
    }

    etimer_reset(&timer);
  }

  PROCESS_END();
}