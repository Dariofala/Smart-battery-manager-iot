#include "contiki.h"
#include "net/routing/routing.h"
#include "mqtt.h"
#include "net/ipv6/uip.h"
#include "sys/etimer.h"
#include "sys/log.h"
#include "dev/leds.h"

#include <stdlib.h>
#include <math.h> 
#include <string.h>
#include <stdio.h>

#define LOG_MODULE "Sensore MQTT (Battery)"
#define LOG_LEVEL LOG_LEVEL_INFO

//  MQTT CONFIGURATIONS
#define BROKER_IP "fd00::1" 
#define BROKER_PORT 1883

#define CLIENT_ID "nRF52840-Battery"
#define TOPIC_BATTERY "network/battery"             // Topic to publish battery state of charge
#define TOPIC_COMMANDS "network/commands/battery"   // Topic to receive commands from the actuator

#define APP_BUFFER_SIZE 128
static char app_buffer[APP_BUFFER_SIZE];
#define MAX_TCP_SEGMENT_SIZE 128

#define SAMPLING_INTERVAL (15 * CLOCK_SECOND)

// BATTERY LOGIC AND PHYSICAL STATE
static float current_soc = 60.0; 
static float last_published_soc = -100.0; 

// Battery Physical States 
#define BATT_STATE_CHARGING    0
#define BATT_STATE_DISCHARGING 1
#define BATT_STATE_CRITICAL    2

static uint8_t batt_state = BATT_STATE_CHARGING; // Initial state

// MQTT Connection Structure and State Machine
static struct mqtt_connection conn;
static mqtt_status_t status;

#define STATE_INIT         0
#define STATE_CONNECTING   1
#define STATE_CONNECTED    2
#define STATE_SUBSCRIBING  3
#define STATE_SUBSCRIBED   4
#define STATE_DISCONNECTED 5

static uint8_t state = STATE_INIT;

PROCESS(mqtt_battery_process, "MQTT Battery Process");
AUTOSTART_PROCESSES(&mqtt_battery_process);

// MQTT Callback
static void mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data) {
  switch(event) {
    case MQTT_EVENT_CONNECTED:
      LOG_INFO("TCP/MQTT connection established!\n");
      state = STATE_CONNECTED;
      break;
    
    case MQTT_EVENT_DISCONNECTED:
      LOG_WARN("Disconnected from the MQTT Broker.\n");
      state = STATE_DISCONNECTED;
      break;
    
    case MQTT_EVENT_SUBACK:
      LOG_INFO("Subscription confirmed to the commands topic.\n");
      state = STATE_SUBSCRIBED;
      break;
    
    case MQTT_EVENT_PUBACK:
      LOG_INFO("Message published successfully (PUBACK received).\n");
      break;

    // Received a message from the subscribed topic 
    case MQTT_EVENT_PUBLISH: {
      struct mqtt_message *msg = data;
      char payload[64];
      
      // Safely copy the payload to perform text parsing
      snprintf(payload, sizeof(payload), "%.*s", msg->payload_length, (char *)msg->payload_chunk);

      if(strstr(payload, "discharge") != NULL) {
        batt_state = BATT_STATE_DISCHARGING;
        LOG_INFO("COMMAND RECEIVED: START DISCHARGE\n");
      } else if(strstr(payload, "charge") != NULL) {
        batt_state = BATT_STATE_CHARGING;
        LOG_INFO("COMMAND RECEIVED: START CHARGE\n");
      } else if(strstr(payload, "critical") != NULL) {
        // Command received from the actuator to halt discharge
        batt_state = BATT_STATE_CRITICAL;
        LOG_INFO("COMMAND RECEIVED: CRITICAL STATE (HALT DISCHARGE)\n");
      }
      
      /* // TEMPORARILY COMMENTED OUT - CLI FORCED SOC OVERRIDE
      else if(strstr(payload, "soc_battery") != NULL) {
        // Handle CLI forced SOC override (Assuming format like "soc_battery: 45.5")
        char *val_ptr = strchr(payload, ':');
        if(val_ptr != NULL) {
            float forced_soc = atof(val_ptr + 1);
            if(forced_soc >= 0.0 && forced_soc <= 100.0) {
                current_soc = forced_soc;
                LOG_INFO("CLI COMMAND RECEIVED: SOC FORCED TO %.1f%%\n", current_soc);
                
                // Re-evaluate state if we were pushed back above critical while discharging
                if(batt_state == BATT_STATE_CRITICAL && current_soc > 20.0) {
                    batt_state = BATT_STATE_DISCHARGING; 
                }
            }
        }
      }
      */
      break;
    }
    
    default:
      break;
  }
}

static bool have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL || uip_ds6_defrt_choose() == NULL) { return false; }
  return true;
}

PROCESS_THREAD(mqtt_battery_process, ev, data)
{
  static struct etimer timer;
  static uip_ipaddr_t broker_address;

  PROCESS_BEGIN();

  LOG_INFO("Starting MQTT Sensor (Smart Battery 3-States)...\n");

  // Register the MQTT Client by assigning its name and callback 
  mqtt_register(&conn, &mqtt_battery_process, CLIENT_ID, mqtt_event, MAX_TCP_SEGMENT_SIZE);

  // Convert the IP string into a uip_ipaddr_t structure
  uiplib_ipaddrconv(BROKER_IP, &broker_address);

  etimer_set(&timer, SAMPLING_INTERVAL);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

    // MQTT STATE MACHINE

    // STATE_INIT or STATE_DISCONNECTED
    if(state == STATE_INIT || state == STATE_DISCONNECTED) {
      if(have_connectivity()) {
        state = STATE_CONNECTING;
      } else {
        LOG_INFO("Waiting for RPL routing towards the Border Router...\n");
      }
    }

    // STATE_CONNECTING
    if(state == STATE_CONNECTING) {
      LOG_INFO("Attempting connection to Broker [%s]:%d\n", BROKER_IP, BROKER_PORT);
      // Connect with calculated Keep Alive and Clean Session parameters
      status = mqtt_connect(&conn, BROKER_IP, BROKER_PORT,
                            (SAMPLING_INTERVAL * 3) / CLOCK_SECOND,
                            MQTT_CLEAN_SESSION_ON);
      if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
        LOG_ERR("Network queue is full. Retrying later...\n");
      }
    }

    // STATE_CONNECTED
    if(state == STATE_CONNECTED) {
      LOG_INFO("Sending Subscribe request to %s\n", TOPIC_COMMANDS);
      mqtt_subscribe(&conn, NULL, TOPIC_COMMANDS, MQTT_QOS_LEVEL_0);
      state = STATE_SUBSCRIBING; // Wait for the SUBACK from the callback
    }

    // STATE_SUBSCRIBED
    if(state == STATE_SUBSCRIBED) {
      
      // 1. MATH EXECUTION
      if(batt_state == BATT_STATE_CHARGING) {
        current_soc += 1.5;
        if(current_soc >= 100.0) {
            current_soc = 100.0;
        }
      } 
      else if(batt_state == BATT_STATE_DISCHARGING) {
        current_soc -= 1.5;
        // The actuator is responsible for monitoring the SOC and sending the "critical" command.
        // We just prevent it from going below 0% physically to avoid math errors.
        if(current_soc <= 0.0) {
            current_soc = 0.0;
        }
      }
      // If BATT_STATE_CRITICAL, current_soc does not change (simulating Grid Fallback)

      // 2. LED MANAGEMENT
      leds_off(LEDS_ALL); // Turn leds off
      
      if(batt_state == BATT_STATE_CHARGING) {
         leds_set(LEDS_NUM_TO_MASK(LEDS_GREEN));  // Charging -> Green
      } 
      else if(batt_state == BATT_STATE_DISCHARGING) {
         leds_set(LEDS_NUM_TO_MASK(LEDS_YELLOW)); // Discharging -> Yellow
      } 
      else if(batt_state == BATT_STATE_CRITICAL) {
         leds_set(LEDS_NUM_TO_MASK(LEDS_RED));    // Critical (Halted) -> Red
      }

      // 3. TELEMETRY PUBLICATION
      if(fabs(current_soc - last_published_soc) >= 1.0) {
        
        snprintf(app_buffer, APP_BUFFER_SIZE, 
                 "{\"bn\":\"%s\", \"n\":\"battery_soc\", \"v\":%.1f, \"u\":\"%%\"}", 
                 CLIENT_ID, current_soc);
        
        char* state_str = (batt_state == BATT_STATE_CHARGING) ? "Charge" : 
                          (batt_state == BATT_STATE_DISCHARGING) ? "Discharge" : "Critical/Halted";
                          
        LOG_INFO("State: %.1f%% (%s). Publishing to %s\n", 
                 current_soc, state_str, TOPIC_BATTERY);
        
        mqtt_publish(&conn, NULL, TOPIC_BATTERY, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
        
        last_published_soc = current_soc;
      }
    }

    etimer_reset(&timer);
  }

  PROCESS_END();
}