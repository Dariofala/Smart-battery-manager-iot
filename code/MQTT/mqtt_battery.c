#include "contiki.h"
#include "net/routing/routing.h"
#include "mqtt.h"
#include "net/ipv6/uip.h"
#include "net/ipv6/uiplib.h"
#include "net/ipv6/uip-ds6.h"
#include "sys/etimer.h"
#include "sys/log.h"
#include "dev/leds.h"
#include "dev/button-hal.h"

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
#define TOPIC_BATTERY "network/battery"           // Topic to publish battery state of charge
#define TOPIC_COMMANDS "network/commands/battery" // Topic to receive commands from the actuator

#define NODE_TYPE "battery"                       // Used in the registration payload
#define TOPIC_REGISTRATION "registration"         // Topic to register at startup
#define TOPIC_CONGESTION "network/congestion"     // Topic to receive congestion signals

#define APP_BUFFER_SIZE 128
static char app_buffer[APP_BUFFER_SIZE];
#define MAX_TCP_SEGMENT_SIZE 128

#define SAMPLING_INTERVAL (15 * CLOCK_SECOND)

// On congestion we double the period up to MAX_INTERVAL
// On normal we halve it back to BASE_INTERVAL
#define BASE_INTERVAL  (15 * CLOCK_SECOND)    // Stress Test set to 0.2s
#define MAX_INTERVAL   (60 * CLOCK_SECOND)

static clock_time_t current_interval = BASE_INTERVAL; // period in use now
static bool congested = false;                        

// The Cloud uses seq gaps to compute PDR
static uint32_t seq_num = 0;

static bool registered = false;

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

// A telemetry publish can fail with MQTT_STATUS_OUT_QUEUE_FULL when the TCP queue is full
// If it keeps failing we count consecutive failures and past a threshold,
// force a  reconnect so the node recovers instead of dying
#define MAX_PUBLISH_RETRIES        3   // immediate retries per publish
#define MAX_CONSECUTIVE_FAILURES   5   // failed publishes before forcing reconnect
static uint8_t consecutive_pub_failures = 0;

// Forward declaration: used in the process below
static void try_publish(const char *topic, const char *payload);

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

      // ANTI-CRASH PROTECTION
      if(msg == NULL || msg->payload_chunk == NULL || msg->payload_length <= 0) {
        break; 
      }

      char payload[128];
      snprintf(payload, sizeof(payload), "%.*s", msg->payload_length, (char *)msg->payload_chunk);

      // The Cloud publishes {"congested":1} or {"congested":0} on TOPIC_CONGESTION
      if(strstr(payload, "congested") != NULL) {
        if(strstr(payload, "\"congested\":1") != NULL) {
          congested = true;
          current_interval *= 2;                 // one step slower
          if(current_interval > MAX_INTERVAL) {
            current_interval = MAX_INTERVAL;
          }
          LOG_WARN("[ADAPT] Congestion signalled. New period: %lu s\n",
                   (unsigned long)(current_interval / CLOCK_SECOND));
        } else {
          congested = false;
          current_interval /= 2;                 // one step faster
          if(current_interval < BASE_INTERVAL) {
            current_interval = BASE_INTERVAL;
          }
          LOG_INFO("[ADAPT] Normal signalled. New period: %lu s\n",
                   (unsigned long)(current_interval / CLOCK_SECOND));
        }
        break; 
      }

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
      
      // CLI FORCED SOC OVERRIDE
      else if(strstr(payload, "battery_soc") != NULL) {
        // Find the start of the numeric value right after the "v": key
        char *val_ptr = strstr(payload, "\"v\":");
        if(val_ptr != NULL) {
            float forced_soc = atof(val_ptr + 4); // Extract the float value skipping the 4 characters of '"v":'
            current_soc = forced_soc;
            int soc_int = (int)current_soc;
            int soc_frac = (int)(fabs(current_soc - soc_int) * 10 + 0.5);
            LOG_INFO("CLI COMMAND RECEIVED: SOC FORCED TO %d.%d%%\n", soc_int, soc_frac);
        }
      }
      break;
    }
    default:
      break;
  }
}

// Function to check if the IPv6 network is ready to route packets
static bool have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL || uip_ds6_defrt_choose() == NULL) { return false; }
  return true;
}

// Tries to publish; on OUT_QUEUE_FULL it retries a few times immediately
static void try_publish(const char *topic, const char *payload) {
  mqtt_status_t st = MQTT_STATUS_OUT_QUEUE_FULL;
  int attempt;

  for(attempt = 0; attempt < MAX_PUBLISH_RETRIES; attempt++) {
    st = mqtt_publish(&conn, NULL, (char *)topic, (uint8_t *)payload,
                      strlen(payload), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
    if(st == MQTT_STATUS_OK) {
      break;
    }
  }

  if(st == MQTT_STATUS_OK) {
    consecutive_pub_failures = 0;
  } else {
    consecutive_pub_failures++;
    LOG_ERR("Publish failed on %s, status=%d (consecutive=%d)\n",
            topic, st, consecutive_pub_failures);
    if(consecutive_pub_failures >= MAX_CONSECUTIVE_FAILURES) {
      LOG_WARN("Too many publish failures. Forcing MQTT reconnect...\n");
      mqtt_disconnect(&conn);
      state = STATE_DISCONNECTED;
      consecutive_pub_failures = 0;
    }
  }
}

// Sends {"id":..., "type":..., "ip":...} on the registration topic
// Returns true only if the publish was accepted by the MQTT queue
static bool publish_registration(void) {
  static char reg_payload[128];
  static char ipbuf[48];
  uip_ds6_addr_t *lladdr = uip_ds6_get_global(ADDR_PREFERRED);
  if(lladdr != NULL) {
    uiplib_ipaddr_snprint(ipbuf, sizeof(ipbuf), &lladdr->ipaddr);
  } else {
    strcpy(ipbuf, "::");
  }
  // Compact JSON (no spaces) to match the Cloud/firmware string parsers.
  snprintf(reg_payload, sizeof(reg_payload),
           "{\"id\":\"%s\",\"type\":\"%s\",\"ip\":\"%s\"}", CLIENT_ID, NODE_TYPE, ipbuf);
  // RETAIN_ON so the broker keeps this message and delivers it when the Cloud subscribes to 'registration'
  mqtt_status_t st = mqtt_publish(&conn, NULL, TOPIC_REGISTRATION, (uint8_t *)reg_payload,
                                  strlen(reg_payload), MQTT_QOS_LEVEL_0, MQTT_RETAIN_ON);
  if(st == MQTT_STATUS_OK) {
    LOG_INFO("[REGISTRATION] Sent: %s\n", reg_payload);
    return true;
  }
  LOG_ERR("[REGISTRATION] Publish failed (status=%d). Will retry.\n", st);
  return false;
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
    PROCESS_WAIT_EVENT();

    // BUTTON
    if(ev == button_hal_periodic_event) {
        button_hal_button_t *btn = (button_hal_button_t *)data;
        
        if(btn->press_duration_seconds == 3) {
            LOG_INFO(">>> BUTTON PRESSED (3s): Simulating complete battery discharge! <<<\n");
            
            current_soc = 0.0; // Set battery to zero
            etimer_set(&timer, 0); // Force timer to zero
        }
    }
    
    // TIMER
    else if(ev == PROCESS_EVENT_TIMER && data == &timer) {

    // -- MQTT STATE MACHINE --

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
      LOG_INFO("Sending Subscribe request to %s\n", TOPIC_CONGESTION);
      mqtt_subscribe(&conn, NULL, TOPIC_CONGESTION, MQTT_QOS_LEVEL_0);

      state = STATE_SUBSCRIBING; // Wait for the SUBACK from the callback
    }

    // STATE_SUBSCRIBED
    if(state == STATE_SUBSCRIBED) {

      // Retries next cycle if it fails
      if(!registered) {
        if(publish_registration()) {
          registered = true;
        }
      }

      // MATH EXECUTION
      if(batt_state == BATT_STATE_CHARGING) {
        current_soc += 1.5;
        if(current_soc >= 100.0) {
            current_soc = 100.0;
        }
      } 
      else if(batt_state == BATT_STATE_DISCHARGING) {
        current_soc -= 1.5;
        // Prevent battery from going below 0%, to avoid math errors
        if(current_soc <= 0.0) {
            current_soc = 0.0;
        }
      }
      // If BATT_STATE_CRITICAL, current_soc does not change (simulating Grid Fallback)

      // LED MANAGEMENT
      leds_off(LEDS_ALL); // Turn leds off
      
      if(batt_state == BATT_STATE_CHARGING) {
         leds_off(LEDS_ALL);
         leds_on(LEDS_GREEN);         // Charging -> Green
      } 
      else if(batt_state == BATT_STATE_DISCHARGING) {
         leds_off(LEDS_ALL);
         leds_single_on(LEDS_YELLOW); // Discharging -> Yellow
      } 
      else if(batt_state == BATT_STATE_CRITICAL) {
         leds_off(LEDS_ALL);
         leds_on(LEDS_RED);           // Critical (Halted) -> Red
      }

      // TELEMETRY PUBLICATION
      // Check if a significant change occurred
      if(fabs(current_soc - last_published_soc) >= 1.0) {

        int soc_int = (int)current_soc;
        int soc_frac = (int)(fabs(current_soc - soc_int) * 10 + 0.5);
        
        seq_num++;
        snprintf(app_buffer, APP_BUFFER_SIZE, 
                 "{\"bn\":\"%s\", \"n\":\"battery_soc\", \"v\":%d.%d, \"u\":\"%%\", \"seq\":%lu}", 
                 CLIENT_ID, soc_int, soc_frac, (unsigned long)seq_num);
        
        char* state_str = (batt_state == BATT_STATE_CHARGING) ? "Charge" : 
                          (batt_state == BATT_STATE_DISCHARGING) ? "Discharge" : "Critical/Halted";
                          
        LOG_INFO("State: %d.%d%% (%s). Publishing to %s\n", 
                 soc_int, soc_frac, state_str, TOPIC_BATTERY);
        
        try_publish(TOPIC_BATTERY, app_buffer);
        
        last_published_soc = current_soc;
      }
    }

    etimer_set(&timer, current_interval);
    }
  }

  PROCESS_END();
}