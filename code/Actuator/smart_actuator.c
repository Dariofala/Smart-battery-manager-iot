#include "contiki.h"
#include "net/routing/routing.h"
#include "mqtt.h"
#include "net/ipv6/uip.h"
#include "coap-engine.h"
#include "coap-observe-client.h"
#include "sys/etimer.h"
#include "sys/log.h"
#include "dev/leds.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Machine Learning Model */
#include "smart_battery_model.h"

#define LOG_MODULE "Smart Actuator"
#define LOG_LEVEL LOG_LEVEL_INFO

/* --- NETWORK CONFIGURATIONS --- */
#define BROKER_IP "fd00::1" 
#define BROKER_PORT 1883
#define CLIENT_ID "nRF52840-Actuator"

/* Addresses of CoAP Sensors */
#define COAP_POWER_IP "coap://[fd00::202:2:2:2]:5683"
#define COAP_IRR_IP   "coap://[fd00::203:3:3:3]:5683"

#define TOPIC_TEMPERATURE    "network/temperature"
#define TOPIC_COMMANDS  "network/commands/battery"
#define TOPIC_ALARMS  "network/alarms"

#define APP_BUFFER_SIZE 128
static char sensors_buffer[APP_BUFFER_SIZE];
static char alarms_buffer[APP_BUFFER_SIZE];
#define MAX_TCP_SEGMENT_SIZE 128

/* --- machine learning features --- */
static const float FEATURE_MEAN[5]  = { 276.5014f, 276.9828f, 0.2314f, 0.2313f, 32.0125f };
static const float FEATURE_SCALE[5] = { 380.8638f, 381.0449f, 0.3070f, 0.3065f, 11.8554f };

/* --- state variables --- */
static float current_power = 0.0, prev_power = 0.0;
static float current_irr = 0.0,   prev_irr = 0.0;
static float current_temp = 0.0;

static bool first_cycle_done = false;
static uint32_t last_temp_timestamp = 0;
static uint32_t last_power_timestamp = 0;
static uint32_t last_irr_timestamp = 0;
#define SAFE_MODE_TIMEOUT (60 * CLOCK_SECOND)

/* MQTT and CoAP Structures */
static struct mqtt_connection conn;
static mqtt_status_t status;
static coap_endpoint_t ep_power;
static coap_endpoint_t ep_irr;
static coap_observee_t *obs_power;
static coap_observee_t *obs_irr;

#define STATE_INIT         0
#define STATE_CONNECTING   1
#define STATE_CONNECTED    2
#define STATE_SUBSCRIBING  3
#define STATE_SUBSCRIBED   4
#define STATE_DISCONNECTED 5

static uint8_t mqtt_state = STATE_INIT;

PROCESS(smart_actuator_process, "Smart Actuator Process");
AUTOSTART_PROCESSES(&smart_actuator_process);

/* --- support function: extracts the value from JSON --- */
static float extract_json_value(const char* payload) {
  char *ptr = strstr(payload, "\"v\":");
  if(ptr != NULL) {
    return atof(ptr + 4);
  }
  return 0.0;
}

/* --- callback functions for CoAP observations --- */
static void notification_callback_power(coap_observee_t *obs, void *notification, coap_notification_flag_t flag) {
  if(notification) {
    const uint8_t *payload = NULL;
    coap_get_payload(notification, &payload);
    current_power = extract_json_value((const char *)payload);
    LOG_INFO("CoAP RX Power: %.2f W\n", current_power);
    last_power_timestamp = clock_time();
  }
}

static void notification_callback_irr(coap_observee_t *obs, void *notification, coap_notification_flag_t flag) {
  if(notification) {
    const uint8_t *payload = NULL;
    coap_get_payload(notification, &payload);
    current_irr = extract_json_value((const char *)payload);
    LOG_INFO("CoAP RX Irradiation: %.4f W/m2\n", current_irr);
    last_irr_timestamp = clock_time();
  }
}

/* --- callback function for MQTT events --- */
static void mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data) {
  switch(event) {
    case MQTT_EVENT_CONNECTED:
      LOG_INFO("MQTT Connected!\n");
      mqtt_state = STATE_CONNECTED;
      break;
    case MQTT_EVENT_DISCONNECTED:
      LOG_WARN("!!! Disconnected from MQTT Broker. Initiating Auto-Recovery... !!!\n");
      mqtt_state = STATE_DISCONNECTED;
      break;
    case MQTT_EVENT_SUBACK:
      LOG_INFO("Subscription to temperature topic completed.\n");
      mqtt_state = STATE_SUBSCRIBED;
      break;
    case MQTT_EVENT_PUBLISH: {
      struct mqtt_message *msg = data;
      char payload[64];
      snprintf(payload, sizeof(payload), "%.*s", msg->payload_length, (char *)msg->payload_chunk);
      
      /* If we receive the temperature, update the value and timestamp for Safe Mode */
      if(strstr(payload, "module_temp") != NULL) {
        current_temp = extract_json_value(payload);
        last_temp_timestamp = clock_time();
        LOG_INFO("MQTT RX Temperature: %.2f C\n", current_temp);
      }
      break;
    }
    default:
      break;
  }
}

static bool have_connectivity(void) {
  return !(uip_ds6_get_global(ADDR_PREFERRED) == NULL || uip_ds6_defrt_choose() == NULL);
}

PROCESS_THREAD(smart_actuator_process, ev, data)
{
  static struct etimer ai_timer;
  static uip_ipaddr_t broker_address;

  PROCESS_BEGIN();

  LOG_INFO("Starting Smart Actuator...\n");

  /* Initialization */
  coap_engine_init();
  mqtt_register(&conn, &smart_actuator_process, CLIENT_ID, mqtt_event, MAX_TCP_SEGMENT_SIZE);
  uiplib_ipaddrconv(BROKER_IP, &broker_address);

  /* Parsing Endpoints CoAP */
  coap_endpoint_parse(COAP_POWER_IP, strlen(COAP_POWER_IP), &ep_power);
  coap_endpoint_parse(COAP_IRR_IP, strlen(COAP_IRR_IP), &ep_irr);

  /* Actuator Timer */
  etimer_set(&ai_timer, 20 * CLOCK_SECOND);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&ai_timer));

    /* MQTT network management */
    if(have_connectivity()) {
      
      if(mqtt_state == STATE_INIT || mqtt_state == STATE_DISCONNECTED) {
        LOG_INFO("Connecting to MQTT Broker...\n");
        status = mqtt_connect(&conn, BROKER_IP, BROKER_PORT, 60, MQTT_CLEAN_SESSION_ON);
        if(status != MQTT_STATUS_OUT_QUEUE_FULL) {
          mqtt_state = STATE_CONNECTING;
        }
      }

      if(mqtt_state == STATE_CONNECTED) {
        mqtt_subscribe(&conn, NULL, TOPIC_TEMPERATURE, MQTT_QOS_LEVEL_0);
        mqtt_state = STATE_SUBSCRIBING;
        LOG_INFO("Subscribing to MQTT Temperature Topic...\n");
      }
    }

    /* CoAP observe management */
    if(obs_power == NULL) {
      LOG_INFO("Starting CoAP Observe for Power...\n");
      obs_power = coap_obs_request_registration(&ep_power, "telemetry/power", notification_callback_power, NULL);
    }
    
    if(obs_irr == NULL) {
      LOG_INFO("Starting CoAP Observe for Irradiation...\n");
      obs_irr = coap_obs_request_registration(&ep_irr, "telemetry/irradiation", notification_callback_irr, NULL);
    }

    /* Safe Mode Control */
    if(mqtt_state == STATE_SUBSCRIBED) {
      
      bool is_safe_mode = false;

      /* Controls on sensor timeouts */
      if ((clock_time() - last_temp_timestamp) > SAFE_MODE_TIMEOUT && last_temp_timestamp != 0) {
        LOG_WARN("!!! SAFE MODE ACTIVE !!! Timeout temperature sensor.\n");
        is_safe_mode = true;
      }
      if ((clock_time() - last_power_timestamp) > SAFE_MODE_TIMEOUT && last_power_timestamp != 0) {
        LOG_WARN("!!! SAFE MODE ACTIVE !!! Timeout power sensor.\n");
        is_safe_mode = true;
        if (obs_power != NULL) {  
          coap_obs_remove_observee(obs_power); 
          obs_power = NULL; /* to reconnect on next round */
        }
      }
      if ((clock_time() - last_irr_timestamp) > SAFE_MODE_TIMEOUT && last_irr_timestamp != 0) {
        LOG_WARN("!!! SAFE MODE ACTIVE !!! Timeout irradiation sensor.\n");
        is_safe_mode = true;
        if (obs_irr != NULL) {
          coap_obs_remove_observee(obs_irr); 
          obs_irr = NULL; /* to reconnect on next round */
        }
      }
      /* ML execution */
      if(first_cycle_done && !is_safe_mode) {
        
        float features[5];
        /* [0] AC_POWER, [1] AC_POWER_t-1, [2] IRRADIATION, [3] IRRADIATION_t-1, [4] MODULE_TEMPERATURE */
        features[0] = (current_power - FEATURE_MEAN[0]) / FEATURE_SCALE[0];
        features[1] = (prev_power    - FEATURE_MEAN[1]) / FEATURE_SCALE[1];
        features[2] = (current_irr   - FEATURE_MEAN[2]) / FEATURE_SCALE[2];
        features[3] = (prev_irr      - FEATURE_MEAN[3]) / FEATURE_SCALE[3];
        features[4] = (current_temp  - FEATURE_MEAN[4]) / FEATURE_SCALE[4];

        int prediction = smart_battery_predict(features, 5);
        LOG_INFO("AI Prediction Result: %d\n", prediction);

        if(prediction == 1) {
          LOG_INFO("Sending discharge command to Battery Sensor.\n");
          /* Collapse Predicted */
          leds_on(LEDS_RED);
          leds_off(LEDS_GREEN);
          snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"discharge\"}");
          mqtt_publish(&conn, NULL, TOPIC_COMMANDS, (uint8_t *)sensors_buffer, strlen(sensors_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
          /*
          snprintf(alarms_buffer, APP_BUFFER_SIZE, "{\"alarm\":\"Possible power collapse detected\"}");
          mqtt_publish(&conn, NULL, TOPIC_ALARMS, (uint8_t *)alarms_buffer, strlen(alarms_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
          */
        } else {
          /* System Stable */
          LOG_INFO("Sending charge command to Battery Sensor.\n");
          leds_on(LEDS_GREEN);
          leds_off(LEDS_RED);
          snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"charge\"}");
          mqtt_publish(&conn, NULL, TOPIC_COMMANDS, (uint8_t *)sensors_buffer, strlen(sensors_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
        }
      }

      /* Safe Mode Reaction */
      if(is_safe_mode) {
        LOG_INFO("SAFE MODE ON: Sending discharge command to Battery Sensor.\n");
        leds_on(LEDS_RED);
        leds_off(LEDS_GREEN);
        snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"discharge\"}");
        mqtt_publish(&conn, NULL, TOPIC_COMMANDS, (uint8_t *)sensors_buffer, strlen(sensors_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
      }

      /* Update variables for the next cycle */
      prev_power = current_power;
      prev_irr = current_irr;
      first_cycle_done = true;
    }

    etimer_reset(&ai_timer);
  }

  PROCESS_END();
}