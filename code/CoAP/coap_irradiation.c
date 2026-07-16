#include <stdio.h>
#include <string.h>
#include <math.h>
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
#define SAMPLING_INTERVAL (15 * CLOCK_SECOND)

// On congestion we double the period up to MAX_INTERVAL
// On normal we halve it back to BASE_INTERVAL
#define BASE_INTERVAL  (15 * CLOCK_SECOND)   // Stress Test set to 0.2s
#define MAX_INTERVAL   (60 * CLOCK_SECOND)   // slowest period under congestion

static clock_time_t current_interval = BASE_INTERVAL; // period in use now
static bool congested = false;                     

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

// The Cloud uses seq gaps to compute PDR
static uint32_t seq_num = 0;

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

// The Cloud publishes {"congested":1} or {"congested":0} on TOPIC_CONGESTION
static void res_congestion_post_handler(coap_message_t *request, coap_message_t *response,
                                        uint8_t *buffer, uint16_t preferred_size, int32_t *offset) {
  const uint8_t *payload = NULL;
  int len = coap_get_payload(request, &payload);

  if(payload != NULL && len > 0) {
    if(strstr((const char *)payload, "\"congested\":1") != NULL) {
      congested = true;
      current_interval *= 2;  // one step slower
      if(current_interval > MAX_INTERVAL) {
        current_interval = MAX_INTERVAL;
      }
      LOG_WARN("[ADAPT] Congestion signalled. New period: %lu s\n",
               (unsigned long)(current_interval / CLOCK_SECOND));
    } else {
      congested = false;
      current_interval /= 2;  // one step faster
      if(current_interval < BASE_INTERVAL) {
        current_interval = BASE_INTERVAL;
      }
      LOG_INFO("[ADAPT] Normal signalled. New period: %lu s\n",
               (unsigned long)(current_interval / CLOCK_SECOND));
    }
  }

  coap_set_status_code(response, CHANGED_2_04);
}

// POST-only resource exposed at control/congestion
RESOURCE(res_congestion, "title=\"Congestion control\"", NULL, res_congestion_post_handler, NULL, NULL);

// GET handler: builds the JSON payload to be sent
static void res_get_handler(coap_message_t *request, coap_message_t *response, uint8_t *buffer, uint16_t preferred_size, int32_t *offset) {
  
  float current_irradiation = irradiation_array[current_step];

  int irr_int = (int)current_irradiation;
  int irr_frac = (int)(fabs(current_irradiation - irr_int) * 10000 + 0.5);

  //JSON formatting --> it uses %d.%04d because irradiation has 4 decimals in the dataset
  snprintf((char *)buffer, preferred_size, 
           "{\"bn\":\"nRF52840-SensorIrr\", \"n\":\"irradiation\", \"v\":%d.%04d, \"u\":\"W/m2\", \"seq\":%lu}", 
           irr_int, irr_frac, (unsigned long)seq_num);

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

  coap_activate_resource(&res_congestion, "control/congestion");

  etimer_set(&timer, SAMPLING_INTERVAL);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

   // Check if the IPv6 network is ready and the Border Router is reachable 
    if(have_connectivity()) {
      
      // CLOUD REGISTRATION
      if(reg_state == STATE_INIT) {
        LOG_INFO("[REGISTRATION] Sending POST request to %s\n", SERVER_EP);
        
        coap_endpoint_parse(SERVER_EP, strlen(SERVER_EP), &server_ep);
        coap_init_message(request, COAP_TYPE_CON, COAP_POST, 0);
        coap_set_header_uri_path(request, "registration");
        
        // Create the JSON payload: {"id": "nRF52840-Irradiation", "type": "irradiation"} 
        static char reg_payload[64];
        snprintf(reg_payload, sizeof(reg_payload), "{\"id\":\"%s\", \"type\":\"%s\"}", NODE_ID, NODE_TYPE);
        coap_set_payload(request, (uint8_t *)reg_payload, strlen(reg_payload));
        
        reg_state = STATE_REGISTERING; // Wait for the callback
        // Send the request to the Python server
        COAP_BLOCKING_REQUEST(&server_ep, request, registration_callback);
      }
      
      // TELEMETRY 
      else if(reg_state == STATE_REGISTERED) {
        
        current_step++;
        if(current_step >= REPLAY_STEPS) {
          current_step = 0; // Rewind the dataset at the end of the 200 steps
        }

        float val = irradiation_array[current_step];
        int val_int = (int)val;
        int val_frac = (int)(fabs(val - val_int) * 10000 + 0.5);

        LOG_INFO("Simulated sampling: Step %d, Value: %d.%04d W/m2\n", current_step, val_int, val_frac);       
         
        seq_num++;

        // Trigger the CoAP event. This single line implements the entire Observe pattern towards the Actuator/Cloud
        res_irradiation.trigger();
      }
      
    } else {
        LOG_INFO("Waiting for RPL routing...\n");
        reg_state = STATE_INIT; // Reset registration state if we lose connectivity 
    }

    etimer_set(&timer, current_interval);
  }

  PROCESS_END();
}