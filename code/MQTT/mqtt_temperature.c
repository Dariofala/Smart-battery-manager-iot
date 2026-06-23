#include "contiki.h"
#include "net/routing/routing.h"
#include "mqtt.h"
#include "net/ipv6/uip.h"
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

#define APP_BUFFER_SIZE 128
static char app_buffer[APP_BUFFER_SIZE];
#define MAX_TCP_SEGMENT_SIZE 128

// Time between each sample
#define SAMPLING_INTERVAL (15 * CLOCK_SECOND)

// EVENT-DRIVEN LOGIC 
// Data is published only if the temperature changes by at least 0.5 degrees
#define TEMP_THRESHOLD 0.5
static float last_published_temp = -100.0; // Dummy initialization to force the 1st transmission
static int current_step = 0;

// MQTT Connection Structure and State Machine
static struct mqtt_connection conn;
static mqtt_status_t status;

#define STATE_INIT         0
#define STATE_CONNECTING   1
#define STATE_CONNECTED    2
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
    default:
      break;
  }
}

// Function to check if the IPv6 network is ready to route packets
static bool have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL || uip_ds6_defrt_choose() == NULL) {return false; }
  return true;
}

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

    // STATE_CONNECTED
    if(state == STATE_CONNECTED) {
      float current_temp = module_temp_array[current_step]; // Read temperature
      
      // Check if a significant change occurred
      if(fabs(current_temp - last_published_temp) >= TEMP_THRESHOLD) { 
        
        /* JSON payload creation */
        snprintf(app_buffer, APP_BUFFER_SIZE, 
                 "{\"bn\":\"%s\", \"n\":\"module_temp\", \"v\":%.2f, \"u\":\"Cel\"}", 
                 CLIENT_ID, current_temp);
        
        LOG_INFO("Variation (> %.1f C). Publishing: %s\n", TEMP_THRESHOLD, app_buffer);
        
        // Asynchronous publication on the temperature topic with QoS 0
        mqtt_publish(&conn, NULL, TOPIC_TEMPERATURE, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
        
        last_published_temp = current_temp;
      } else {
        LOG_INFO("Step %d - Temp: %.2f C (Unchanged). Radio sleeping.\n", current_step, current_temp);
      }

      current_step++;
      if(current_step >= REPLAY_STEPS) {
        current_step = 0;
      }
    }

    etimer_reset(&timer);
  }

  PROCESS_END();
}