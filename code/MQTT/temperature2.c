#include "contiki.h"
#include "net/routing/routing.h"
#include "mqtt.h"
#include "net/ipv6/uip.h"
// ===== [MQTT REGISTRATION - NEW] Needed to print own IPv6 as a string =====
#include "net/ipv6/uiplib.h"
#include "net/ipv6/uip-ds6.h"
// ===== [END MQTT REGISTRATION - NEW] =====
#include "sys/etimer.h"
#include "sys/log.h"

#include <math.h>
#include <string.h>
#include <stdio.h>

#include "dataset_replay.h" // include the dataset

#define LOG_MODULE "Sensore MQTT (Temperature)"
#define LOG_LEVEL LOG_LEVEL_INFO

//  MQTT CONFIGURATIONS
#define BROKER_IP "fd00::1" 
#define BROKER_PORT 1883

#define CLIENT_ID "nRF52840-Temperature"
#define TOPIC_TEMPERATURE "network/temperature"

// ===== [MQTT REGISTRATION / CONGESTION - NEW] Node type and extra topics =====
#define NODE_TYPE "temperature"                     // Used in the registration payload
#define TOPIC_REGISTRATION "registration"           // Topic to register at startup
#define TOPIC_CONGESTION "network/congestion"       // Topic to receive congestion signals
// ===== [END MQTT REGISTRATION / CONGESTION - NEW] =====

#define APP_BUFFER_SIZE 128
static char app_buffer[APP_BUFFER_SIZE];
#define MAX_TCP_SEGMENT_SIZE 128

// Time between each sample
#define SAMPLING_INTERVAL (15 * CLOCK_SECOND)

// EVENT-DRIVEN LOGIC 
// Data is published only if the temperature changes by at least 0.5 degrees
// ===== [ADAPTIVE MECHANISM 2 - MODIFIED] Threshold is now a variable =====
// Normally 0.5 C. Under congestion the Cloud makes us raise it to 2.0 C, so we
// publish fewer messages (filtering at the device level).
#define TEMP_THRESHOLD_NORMAL 0.5
#define TEMP_THRESHOLD_CONGESTED 2.0
static float temp_threshold = TEMP_THRESHOLD_NORMAL;
// ===== [END ADAPTIVE MECHANISM 2 - MODIFIED] =====
static float last_published_temp = -100.0; // Dummy initialization to force the 1st transmission
static int current_step = 0;

// Heartbeat counter 
static int cycles_without_publish = 0;
#define MAX_IDLE_CYCLES 3  // 3 cycles for a maximum of 45s, at the end of that temperature must be sent

// ===== [ADAPTIVE MECHANISM 1 - NEW] Adaptive sampling config and state =====
// On congestion we double the period (step by step) up to MAX_INTERVAL.
// On normal we halve it (step by step) back to BASE_INTERVAL.
#define BASE_INTERVAL  (15 * CLOCK_SECOND)   // normal sampling period
#define MAX_INTERVAL   (60 * CLOCK_SECOND)   // slowest period under congestion

static clock_time_t current_interval = BASE_INTERVAL; // period in use now
static bool congested = false;                        // last state from Cloud
// ===== [END ADAPTIVE MECHANISM 1 - NEW] =====

// ===== [INSTRUMENTATION - NEW] Sequence number for PDR =====
// Attached to every telemetry payload. The Cloud uses seq gaps to compute PDR.
static uint32_t seq_num = 0;
// ===== [END INSTRUMENTATION - NEW] =====

// ===== [MQTT REGISTRATION - NEW] Register only once =====
static bool registered = false;
// ===== [END MQTT REGISTRATION - NEW] =====

// MQTT Connection Structure and State Machine
static struct mqtt_connection conn;
static mqtt_status_t status;

#define STATE_INIT         0
#define STATE_CONNECTING   1
#define STATE_CONNECTED    2
// ===== [ADAPTIVE MECHANISM 1 - NEW] Extra states to subscribe to congestion =====
#define STATE_SUBSCRIBING  4
#define STATE_SUBSCRIBED   5
// ===== [END ADAPTIVE MECHANISM 1 - NEW] =====
#define STATE_DISCONNECTED 3

static uint8_t state = STATE_INIT;

PROCESS(mqtt_temperature_process, "MQTT Temperature Process");
AUTOSTART_PROCESSES(&mqtt_temperature_process);

// MQTT Callback
static void mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data) {
  switch(event) {
    case MQTT_EVENT_CONNECTED:
      LOG_INFO("TCP/MQTT connection established with the Broker!\n");
      state = STATE_CONNECTED;
      break;
    case MQTT_EVENT_DISCONNECTED:
      LOG_WARN("Disconnected from the MQTT Broker.\n");
      state = STATE_DISCONNECTED;
      break;
    case MQTT_EVENT_PUBACK:
      LOG_INFO("Message published successfully (PUBACK received).\n");
      break;
    // ===== [ADAPTIVE MECHANISM 1 - NEW] Subscription confirmed =====
    case MQTT_EVENT_SUBACK:
      LOG_INFO("Subscription confirmed to the congestion topic.\n");
      state = STATE_SUBSCRIBED;
      break;
    // ===== [END ADAPTIVE MECHANISM 1 - NEW] =====

    // ===== [ADAPTIVE MECHANISM - NEW] Handle congestion signal from the Cloud =====
    case MQTT_EVENT_PUBLISH: {
      struct mqtt_message *msg = data;

      // ANTI-CRASH PROTECTION
      if(msg == NULL || msg->payload_chunk == NULL || msg->payload_length <= 0) {
        break;
      }

      char payload[128];
      snprintf(payload, sizeof(payload), "%.*s", msg->payload_length, (char *)msg->payload_chunk);

      // The Cloud publishes {"congested":1} or {"congested":0} on TOPIC_CONGESTION.
      if(strstr(payload, "congested") != NULL) {
        if(strstr(payload, "\"congested\":1") != NULL) {
          congested = true;
          // Mechanism 1: slower sampling (one step, up to MAX_INTERVAL)
          current_interval *= 2;
          if(current_interval > MAX_INTERVAL) {
            current_interval = MAX_INTERVAL;
          }
          // Mechanism 2: higher threshold -> fewer messages
          temp_threshold = TEMP_THRESHOLD_CONGESTED;
          LOG_WARN("[ADAPT] Congestion: period=%lu s, threshold=2.0 C\n",
                   (unsigned long)(current_interval / CLOCK_SECOND));
        } else {
          congested = false;
          // Mechanism 1: faster sampling (one step, down to BASE_INTERVAL)
          current_interval /= 2;
          if(current_interval < BASE_INTERVAL) {
            current_interval = BASE_INTERVAL;
          }
          // Mechanism 2: back to the fine threshold
          temp_threshold = TEMP_THRESHOLD_NORMAL;
          LOG_INFO("[ADAPT] Normal: period=%lu s, threshold=0.5 C\n",
                   (unsigned long)(current_interval / CLOCK_SECOND));
        }
      }
      break;
    }
    // ===== [END ADAPTIVE MECHANISM - NEW] =====
    default:
      break;
  }
}

// Function to check if the IPv6 network is ready to route packets
static bool have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL || uip_ds6_defrt_choose() == NULL) {return false; }
  return true;
}

// ===== [MQTT REGISTRATION - NEW] Publish a registration message once =====
// Sends {"id":..., "type":..., "ip":...} on the registration topic so the
// Cloud can add this node to the DB directory and ping it for latency.
static void publish_registration(void) {
  char reg_payload[128];
  char ipbuf[48];
  uip_ds6_addr_t *lladdr = uip_ds6_get_global(ADDR_PREFERRED);
  if(lladdr != NULL) {
    uiplib_ipaddr_snprint(ipbuf, sizeof(ipbuf), &lladdr->ipaddr);
  } else {
    strcpy(ipbuf, "::");
  }
  // Compact JSON (no spaces) to match the Cloud/firmware string parsers.
  snprintf(reg_payload, sizeof(reg_payload),
           "{\"id\":\"%s\",\"type\":\"%s\",\"ip\":\"%s\"}", CLIENT_ID, NODE_TYPE, ipbuf);
  mqtt_publish(&conn, NULL, TOPIC_REGISTRATION, (uint8_t *)reg_payload,
               strlen(reg_payload), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
  LOG_INFO("[REGISTRATION] Sent: %s\n", reg_payload);
}
// ===== [END MQTT REGISTRATION - NEW] =====

PROCESS_THREAD(mqtt_temperature_process, ev, data)
{
  static struct etimer timer;
  static uip_ipaddr_t broker_address;

  PROCESS_BEGIN();

  LOG_INFO("Starting MQTT Sensor (Temperature)...\n");

  // Register the MQTT Client by assigning its name and callback 
  mqtt_register(&conn, &mqtt_temperature_process, CLIENT_ID, mqtt_event, MAX_TCP_SEGMENT_SIZE);

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
      } else {
        // We wait for the callback to switch the state to STATE_CONNECTED */
      }
    }

    // ===== [ADAPTIVE MECHANISM 1 / REGISTRATION - NEW] Subscribe and register =====
    // STATE_CONNECTED: subscribe to the congestion topic and register once.
    if(state == STATE_CONNECTED) {
      LOG_INFO("Sending Subscribe request to %s\n", TOPIC_CONGESTION);
      mqtt_subscribe(&conn, NULL, TOPIC_CONGESTION, MQTT_QOS_LEVEL_0);

      if(!registered) {
        publish_registration();
        registered = true;
      }

      state = STATE_SUBSCRIBING; // Wait for the SUBACK from the callback
    }
    // ===== [END ADAPTIVE MECHANISM 1 / REGISTRATION - NEW] =====

    // STATE_SUBSCRIBED
    // ===== [ADAPTIVE MECHANISM - MODIFIED] Publishing now happens in STATE_SUBSCRIBED =====
    if(state == STATE_SUBSCRIBED) {
      float current_temp = module_temp_array[current_step]; // Read temperature
      
      int temp_int = (int)current_temp;
      int temp_frac = (int)(fabs(current_temp - temp_int) * 100 + 0.5);

      // Check if a significant change occurred
      // ===== [ADAPTIVE MECHANISM 2 - MODIFIED] Use the adaptive threshold =====
      if(fabs(current_temp - last_published_temp) >= temp_threshold || cycles_without_publish >= MAX_IDLE_CYCLES) {
      // ===== [END ADAPTIVE MECHANISM 2 - MODIFIED] =====
        // ===== [INSTRUMENTATION - NEW] Advance seq and add "seq" field =====
        seq_num++;
        snprintf(app_buffer, APP_BUFFER_SIZE, 
                 "{\"bn\":\"%s\", \"n\":\"module_temp\", \"v\":%d.%02d, \"u\":\"Cel\", \"seq\":%lu}", 
                 CLIENT_ID, temp_int, temp_frac, (unsigned long)seq_num);
        // ===== [END INSTRUMENTATION - NEW] =====
        
        if(cycles_without_publish >= MAX_IDLE_CYCLES) {
            LOG_INFO("Security Heartbeat (45s), Publishing: %s\n", app_buffer);
        } else {
            LOG_INFO("Variation (>= threshold). Publishing: %s\n", app_buffer);
        }
        
        mqtt_publish(&conn, NULL, TOPIC_TEMPERATURE, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
        
        last_published_temp = current_temp;
        cycles_without_publish = 0; /* Reset the counter because we just published */
        
      } else {
        cycles_without_publish++; /* Increase the counter of silences */
        LOG_INFO("Step %d - Temp: %d.%02d C (Unchanged). Silence n.%d. Radio on sleep mode.\n", 
                 current_step, temp_int, temp_frac, cycles_without_publish);
      }

      current_step++;
      if(current_step >= REPLAY_STEPS) {
        current_step = 0;
      }
    }

    // ===== [ADAPTIVE MECHANISM 1 - MODIFIED] Reschedule with the adaptive period =====
    // Equals BASE_INTERVAL when not congested, larger under congestion.
    etimer_set(&timer, current_interval);
    // ===== [END ADAPTIVE MECHANISM 1 - MODIFIED] =====
  }

  PROCESS_END();
}