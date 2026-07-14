#include "contiki.h"
#include "net/routing/routing.h"
#include "mqtt.h"
#include "net/ipv6/uip.h"
#include "coap-engine.h"
#include "coap-observe-client.h"
#include "coap-blocking-api.h"
#include "sys/etimer.h"
#include "sys/log.h"
#include "dev/leds.h"
#include "dev/button-hal.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

/* Machine Learning Model */
#include "smart_battery_model.h"

#define LOG_MODULE "Smart Actuator"
#define LOG_LEVEL LOG_LEVEL_INFO

/* --- NETWORK CONFIGURATIONS --- */
#define BROKER_IP "fd00::1" 
#define BROKER_PORT 1883
#define CLIENT_ID "nRF52840-Actuator"

#define SERVER_EP "coap://[fd00::1]:5683"
#define NODE_TYPE "actuator"

#define STATE_REG_INIT        0
#define STATE_REG_REGISTERING 1
#define STATE_REG_REGISTERED  2
static uint8_t reg_state = STATE_REG_INIT;
static coap_endpoint_t cloud_ep;
static coap_message_t reg_request[1];

/* Addresses of CoAP Sensors cooja simulation 
#define COAP_POWER_IP "coap://[fd00::202:2:2:2]:5683"
#define COAP_IRR_IP   "coap://[fd00::203:3:3:3]:5683" */

// dynamic addresses
static bool power_ep_known = false;
static bool irr_ep_known = false;

/* MQTT Topics */
#define TOPIC_SUBSCRIBE_ALL   "network/#" 
#define TOPIC_COMMANDS        "network/commands/battery"
#define TOPIC_ALARMS          "network/alarms"

#define APP_BUFFER_SIZE 128
static char sensors_buffer[APP_BUFFER_SIZE];
static char alarms_buffer[APP_BUFFER_SIZE];
#define MAX_TCP_SEGMENT_SIZE 128

/* --- Machine Learning Features --- */
static const float FEATURE_MEAN[5]  = { 277.4355f, 277.7913f, 0.2324f, 0.2329f, 32.0750f };
static const float FEATURE_SCALE[5] = { 380.5854f, 380.6051f, 0.3069f, 0.3076f, 11.8827f };

/* --- State Variables --- */
static float current_power = 0.0, prev_power = 0.0;
static float current_irr = 0.0,   prev_irr = 0.0;
static float current_temp = 0.0;
static float current_soc = 100.0; 

static bool first_cycle_done = false;
static uint32_t last_temp_timestamp = 0;
static uint32_t last_power_timestamp = 0;
static uint32_t last_irr_timestamp = 0;
// ===== [STRESS TEST - MODIFIED] Raised from 60s to 90s so the sampling
// back-off under congestion (up to 60s) does not trigger a false Safe Mode =====
#define SAFE_MODE_TIMEOUT (90 * CLOCK_SECOND)
// ===== [END STRESS TEST - MODIFIED] =====

/* --- FSM (Finite State Machine) Configurations --- */
#define ACT_STATE_PV    1
#define ACT_STATE_BATT  2
#define ACT_STATE_GRID  3

static uint8_t system_state = ACT_STATE_PV; 
static uint8_t recovery_counter = 0;
#define PV_THRESHOLD 140.0
#define CRITICAL_SOC 20.0

// ===== [SAFE MODE EXIT - NEW] Remember previous Safe Mode status =====
// Lets us detect the Safe Mode -> normal transition and publish a recovery
// state, so Grafana leaves the "Safe Mode" state instead of staying stuck.
static bool was_safe_mode = false;
// ===== [END SAFE MODE EXIT - NEW] =====

// ===== [SAFE MODE EXIT FIX - NEW] Make recovery reliable =====
// The recovery message can fail with OUT_QUEUE_FULL and be lost (it was sent
// only once). We keep a "pending" flag and re-publish the PV recovery every
// cycle until one publish actually succeeds. 'last_publish_ok' is set by
// try_publish so we know when it went through.
static bool recovery_pending = false;
static bool last_publish_ok = false;
// ===== [END SAFE MODE EXIT FIX - NEW] =====

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

// ===== [PUBLISH ROBUSTNESS - NEW] Retry + forced reconnect config/state =====
// A publish can fail with MQTT_STATUS_OUT_QUEUE_FULL when the TCP queue is full.
// We retry a few times immediately; if it keeps failing we count consecutive
// failures and, past a threshold, force a clean reconnect so the node recovers.
#define MAX_PUBLISH_RETRIES        3   // immediate retries per publish
#define MAX_CONSECUTIVE_FAILURES   5   // failed publishes before forcing reconnect
static uint8_t consecutive_pub_failures = 0;

// Forward declaration: try_publish is used inside mqtt_event (below) but
// defined later, so we declare its prototype here.
static void try_publish(const char *topic, const char *payload);
// ===== [END PUBLISH ROBUSTNESS - NEW] =====

PROCESS(smart_actuator_process, "Smart Actuator Process");
AUTOSTART_PROCESSES(&smart_actuator_process);

/* --- Support function: extracts the value from JSON --- */
static float extract_json_value(const char* payload) {
  if(payload == NULL) return 0.0;

  char *ptr = strstr(payload, "\"v\":");
  if(ptr != NULL) {
    return atof(ptr + 4);
  }
  return 0.0;
}

// Registration callback function 
static void registration_callback(coap_message_t *response) {
  if(response == NULL) {
    LOG_ERR("[REGISTRATION] Cloud unreachable. Will retry.\n");
    reg_state = STATE_REG_INIT;
    return;
  }
  LOG_INFO("[REGISTRATION] Actuator registered with Cloud App!\n");
  reg_state = STATE_REG_REGISTERED;
}

static void res_register_post_handler(coap_message_t *request, coap_message_t *response,
                                       uint8_t *buffer, uint16_t preferred_size, int32_t *offset);

RESOURCE(res_register_sensor, "title=\"Register Sensor Endpoint\"", NULL, res_register_post_handler, NULL, NULL);

static void res_register_post_handler(coap_message_t *request, coap_message_t *response,
                                       uint8_t *buffer, uint16_t preferred_size, int32_t *offset) {
  const uint8_t *payload = NULL;
  int len = coap_get_payload(request, &payload);
  if(len <= 0 || len >= 128) {
    coap_set_status_code(response, BAD_REQUEST_4_00);
    return;
  }

  char payload_str[128];
  memcpy(payload_str, payload, len);
  payload_str[len] = '\0';

  char *type_ptr = strstr(payload_str, "\"type\":\"");
  char *ep_ptr   = strstr(payload_str, "\"endpoint\":\"");
  if(!type_ptr || !ep_ptr) {
    coap_set_status_code(response, BAD_REQUEST_4_00);
    return;
  }

  char node_type[16];
  char endpoint[64];
  sscanf(type_ptr + 8, "%15[^\"]", node_type);
  sscanf(ep_ptr + 12, "%63[^\"]", endpoint);

  if(strcmp(node_type, "power") == 0) {
    coap_endpoint_parse(endpoint, strlen(endpoint), &ep_power);
    power_ep_known = true;
    LOG_INFO("Received Power endpoint: %s\n", endpoint);
  } else if(strcmp(node_type, "irradiation") == 0) {
    coap_endpoint_parse(endpoint, strlen(endpoint), &ep_irr);
    irr_ep_known = true;
    LOG_INFO("Received Irradiation endpoint: %s\n", endpoint);
  } else {
    coap_set_status_code(response, BAD_REQUEST_4_00);
    return;
  }

  coap_set_status_code(response, CHANGED_2_04);
}

/* --- Callback functions for CoAP observations --- */
static void notification_callback_power(coap_observee_t *obs, void *notification, coap_notification_flag_t flag) {
  if(notification) {
    const uint8_t *payload = NULL;
    coap_get_payload(notification, &payload);
    current_power = extract_json_value((const char *)payload);

    int p_int = (int)current_power;
    int p_frac = (int)(fabs(current_power - p_int) * 100 + 0.5);
    LOG_INFO("CoAP RX Power: %d.%02d W\n", p_int, p_frac);
    last_power_timestamp = clock_time();
  }
}

static void notification_callback_irr(coap_observee_t *obs, void *notification, coap_notification_flag_t flag) {
  if(notification) {
    const uint8_t *payload = NULL;
    coap_get_payload(notification, &payload);
    current_irr = extract_json_value((const char *)payload);

    int i_int = (int)current_irr;
    int i_frac = (int)(fabs(current_irr - i_int) * 10000 + 0.5);
    LOG_INFO("CoAP RX Irradiation: %d.%04d W/m2\n", i_int, i_frac);
    last_irr_timestamp = clock_time();
  }
}

/* --- Callback function for MQTT events --- */
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
      LOG_INFO("Subscription to MQTT topics completed.\n");
      mqtt_state = STATE_SUBSCRIBED;
      break;
    case MQTT_EVENT_PUBLISH: {
      struct mqtt_message *msg = data;

      // ANTI-CRASH PROTECTION
      if(msg == NULL || msg->payload_chunk == NULL || msg->payload_length <= 0) {
        break; 
      }

      char payload[128];
      snprintf(payload, sizeof(payload), "%.*s", msg->payload_length, (char *)msg->payload_chunk);

      /* Reaction to Alarm received by Application's CLI */
      if(strcmp(msg->topic, TOPIC_ALARMS) == 0) {
        if(strstr(payload, "Simulating Power collapse") != NULL) {
          LOG_WARN("[CLI ALARM] Received Alarm from Application's CLI. Forcing State 2.\n");
          system_state = ACT_STATE_BATT;
          recovery_counter = 0;
          leds_off(LEDS_ALL);
          leds_single_on(LEDS_YELLOW);
          
          snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"discharge\"}");
          try_publish(TOPIC_COMMANDS, sensors_buffer);
        }
        break;
      }

      /* 3. Parsing Telemetry (TEMPERATURE / BATTERY SOC) */
      if(strstr(payload, "module_temp") != NULL) {
        current_temp = extract_json_value(payload);
        last_temp_timestamp = clock_time();
        int t_int = (int)current_temp;
        int t_frac = (int)(fabs(current_temp - t_int) * 100 + 0.5);
        LOG_INFO("MQTT RX Temperature: %d.%02d C\n", t_int, t_frac);
      } 
      else if (strstr(payload, "battery_soc") != NULL) {
        current_soc = extract_json_value(payload);
        int s_int = (int)current_soc;
        int s_frac = (int)(fabs(current_soc - s_int) * 10 + 0.5);
        LOG_INFO("MQTT RX Battery SOC: %d.%d%%\n", s_int, s_frac);
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

// ===== [PUBLISH ROBUSTNESS - NEW] Centralized publish with retry + reconnect =====
// Tries to publish; on OUT_QUEUE_FULL it retries a few times immediately.
// On success it clears the failure counter. On repeated failure it increments
// consecutive_pub_failures and, past MAX_CONSECUTIVE_FAILURES, forces a clean
// MQTT reconnect (disconnect + go back to STATE_DISCONNECTED) so the FSM
// recovers instead of staying "mute" forever.
static void try_publish(const char *topic, const char *payload) {
  mqtt_status_t st = MQTT_STATUS_OUT_QUEUE_FULL;
  int attempt;

  for(attempt = 0; attempt < MAX_PUBLISH_RETRIES; attempt++) {
    st = mqtt_publish(&conn, NULL, (char *)topic, (uint8_t *)payload,
                      strlen(payload), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
    if(st == MQTT_STATUS_OK) {
      break;   // sent, stop retrying
    }
  }

  if(st == MQTT_STATUS_OK) {
    consecutive_pub_failures = 0;   // healthy again
    // ===== [SAFE MODE EXIT FIX - NEW] Report success to the caller =====
    last_publish_ok = true;
    // ===== [END SAFE MODE EXIT FIX - NEW] =====
  } else {
    consecutive_pub_failures++;
    // ===== [SAFE MODE EXIT FIX - NEW] Report failure to the caller =====
    last_publish_ok = false;
    // ===== [END SAFE MODE EXIT FIX - NEW] =====
    LOG_ERR("Publish failed on %s, status=%d (consecutive=%d)\n",
            topic, st, consecutive_pub_failures);

    // Too many failures in a row: the TCP link is likely stuck. Force a clean
    // reconnect so the next cycles rebuild the MQTT session from scratch.
    if(consecutive_pub_failures >= MAX_CONSECUTIVE_FAILURES) {
      LOG_WARN("Too many publish failures. Forcing MQTT reconnect...\n");
      mqtt_disconnect(&conn);
      mqtt_state = STATE_DISCONNECTED;
      consecutive_pub_failures = 0;
    }
  }
}
// ===== [END PUBLISH ROBUSTNESS - NEW] =====

PROCESS_THREAD(smart_actuator_process, ev, data)
{
  static struct etimer ai_timer;
  static struct etimer spacing_timer;
  static uip_ipaddr_t broker_address;

  PROCESS_BEGIN();

  LOG_INFO("Starting Smart Actuator (3-State FSM)...\n");

  /* Initialization */
  coap_engine_init();
  mqtt_register(&conn, &smart_actuator_process, CLIENT_ID, mqtt_event, MAX_TCP_SEGMENT_SIZE);
  uiplib_ipaddrconv(BROKER_IP, &broker_address);

  /* Parsing Endpoints CoAP (static for Cooja simulation) 
  coap_endpoint_parse(COAP_POWER_IP, strlen(COAP_POWER_IP), &ep_power);
  coap_endpoint_parse(COAP_IRR_IP, strlen(COAP_IRR_IP), &ep_irr); */

  coap_activate_resource(&res_register_sensor, "control/register_sensor");


  /* Actuator Timer */
  etimer_set(&ai_timer, 20 * CLOCK_SECOND);

  while(1) {

    PROCESS_WAIT_EVENT();

    // BUTTON
    if(ev == button_hal_periodic_event) {
        button_hal_button_t *btn = (button_hal_button_t *)data;
        
        if(btn->press_duration_seconds == 3) {
            LOG_INFO(">>> BUTTON PRESSED (3s): Manual Override. Forcing AI Prediction (Collapse)! <<<\n");
            
            // If currently in PV Regime, force the transition to Battery Regime
            if (system_state == ACT_STATE_PV) {
                system_state = ACT_STATE_BATT;
                recovery_counter = 0;
                
                LOG_INFO(">> STATE 2 ACTIVE: Battery Regime (Button Override).\n");
                leds_off(LEDS_ALL);
                leds_single_on(LEDS_YELLOW);
                
                /* Send command to the battery to unlock it from the critical state */
                snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"discharge\"}");
                try_publish(TOPIC_COMMANDS, sensors_buffer);

                /* Timer to clear the MQTT outbound queue before sending the alarm message */
                etimer_set(&spacing_timer, CLOCK_SECOND / 2);
                PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&spacing_timer));

                snprintf(alarms_buffer, APP_BUFFER_SIZE, "{\"alarm\":\"MANUAL OVERRIDE: Actuator button pressed. Entering Battery Mode.\"}");
                try_publish(TOPIC_ALARMS, alarms_buffer);
            }
        }
    }
    
    // TIMER
    else if(ev == PROCESS_EVENT_TIMER && data == &ai_timer) {

    /* MQTT network management */
    if(have_connectivity()) {
      //Registration of the actuator to the cloud application
      if(reg_state == STATE_REG_INIT) {
        LOG_INFO("[REGISTRATION] Sending POST request to %s\n", SERVER_EP);
        coap_endpoint_parse(SERVER_EP, strlen(SERVER_EP), &cloud_ep);
        coap_init_message(reg_request, COAP_TYPE_CON, COAP_POST, 0);
        coap_set_header_uri_path(reg_request, "registration");

        static char reg_payload[64];
        snprintf(reg_payload, sizeof(reg_payload), "{\"id\":\"%s\", \"type\":\"%s\"}", CLIENT_ID, NODE_TYPE);
        coap_set_payload(reg_request, (uint8_t *)reg_payload, strlen(reg_payload));

        reg_state = STATE_REG_REGISTERING;
        COAP_BLOCKING_REQUEST(&cloud_ep, reg_request, registration_callback);
      }

      if(mqtt_state == STATE_INIT || mqtt_state == STATE_DISCONNECTED) {
        LOG_INFO("Connecting to MQTT Broker...\n");
        status = mqtt_connect(&conn, BROKER_IP, BROKER_PORT, 60, MQTT_CLEAN_SESSION_ON);
        if(status != MQTT_STATUS_OUT_QUEUE_FULL) {
          mqtt_state = STATE_CONNECTING;
        }
      }

      if(mqtt_state == STATE_CONNECTED) {
        /* Subscribe using wildcard to get both temperature and battery updates */
        mqtt_subscribe(&conn, NULL, TOPIC_SUBSCRIBE_ALL, MQTT_QOS_LEVEL_0);
        mqtt_state = STATE_SUBSCRIBING;
        LOG_INFO("Subscribing to MQTT Telemetry Topics...\n");
      }
    }

    /* CoAP observe management */
    if(obs_power == NULL && power_ep_known) {
      LOG_INFO("Starting CoAP Observe for Power...\n");
      obs_power = coap_obs_request_registration(&ep_power, "telemetry/power", notification_callback_power, NULL);
    }

    if(obs_irr == NULL && irr_ep_known) {
      LOG_INFO("Starting CoAP Observe for Irradiation...\n");
      obs_irr = coap_obs_request_registration(&ep_irr, "telemetry/irradiation", notification_callback_irr, NULL);
    }

    /* Safe Mode Control */
    if(mqtt_state == STATE_SUBSCRIBED) {
      
      static bool is_safe_mode;
      is_safe_mode = false;

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

      /* === CORE LOGIC: 3-STATE FINITE STATE MACHINE === */
      if(first_cycle_done && !is_safe_mode) {
        
        /* STATE 1: PV REGIME */
        if (system_state == ACT_STATE_PV) {
            
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
                /* TRANSITION 1 -> 2: Collapse Predicted */
                system_state = ACT_STATE_BATT;
                recovery_counter = 0; // Reset recovery threshold
                
                LOG_INFO(">> STATE 2 ACTIVE: Battery Regime (Collapse Predicted).\n");
                leds_off(LEDS_ALL);
                leds_single_on(LEDS_YELLOW);
                
                snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"discharge\"}");
                try_publish(TOPIC_COMMANDS, sensors_buffer);            
                // Timer to clear the MQTT outbound queue before sending the alarm message
                etimer_set(&spacing_timer, CLOCK_SECOND / 2);
                PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&spacing_timer));

                snprintf(alarms_buffer, APP_BUFFER_SIZE, "{\"alarm\":\"Collapse predicted. Entering Battery Mode.\"}");
                try_publish(TOPIC_ALARMS, alarms_buffer);
            } else if (current_power < PV_THRESHOLD) {
              /* TRANSITION 1 -> 2: not enough power generation */
                system_state = ACT_STATE_BATT;
                recovery_counter = 0; // Reset recovery threshold
                
                LOG_INFO(">> STATE 2 ACTIVE: Battery Regime (Not enough power generation).\n");
                leds_off(LEDS_ALL);
                leds_single_on(LEDS_YELLOW);
                
                snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"discharge\"}");
                try_publish(TOPIC_COMMANDS, sensors_buffer);

                // Timer to clear the MQTT outbound queue before sending the alarm message
                etimer_set(&spacing_timer, CLOCK_SECOND / 2);
                PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&spacing_timer));

                snprintf(alarms_buffer, APP_BUFFER_SIZE, "{\"alarm\":\"Not enough power generation. Entering Battery Mode.\"}");
                try_publish(TOPIC_ALARMS, alarms_buffer); 
            } else {
                /* Maintain State 1 */
                leds_off(LEDS_ALL);
                leds_on(LEDS_GREEN);
                snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"charge\"}");
                LOG_INFO("Maintaining PV Regime. Sending charge command to battery.\n");
                try_publish(TOPIC_COMMANDS, sensors_buffer);
            }
        }
        
        /* STATE 2 & 3: BATTERY OR GRID REGIME (AI Gated Recovery) */
        else if (system_state == ACT_STATE_BATT || system_state == ACT_STATE_GRID) {
            
            /* Compute TinyML Features during isolation to double check recovery stability */
            float features[5];
            features[0] = (current_power - FEATURE_MEAN[0]) / FEATURE_SCALE[0];
            features[1] = (prev_power    - FEATURE_MEAN[1]) / FEATURE_SCALE[1];
            features[2] = (current_irr   - FEATURE_MEAN[2]) / FEATURE_SCALE[2];
            features[3] = (prev_irr      - FEATURE_MEAN[3]) / FEATURE_SCALE[3];
            features[4] = (current_temp  - FEATURE_MEAN[4]) / FEATURE_SCALE[4];

            int prediction = smart_battery_predict(features, 5);
            
            /* Check PV Recovery Condition */
            if (current_power >= PV_THRESHOLD) {
                recovery_counter++;
                LOG_INFO("PV Recovery evaluation: %d/3 consecutive cycles.\n", recovery_counter);
            } else {
                recovery_counter = 0;
                LOG_INFO("Current_power not enough for PV recovery evaluation. Restarting recovery_counter\n");
            }

            /* TRANSITION 2/3 -> 1: PV Restored (Gated by AI Validation) */
            if (recovery_counter >= 3) {
                
                /* If the physical threshold is met but AI predicts an imminent drop, abort recovery */
                if (prediction == 1) {
                    recovery_counter = 0; 
                    LOG_WARN("PV Recovery aborted! Physical threshold met, but AI predicts imminent drop on next step.\n");
                    
                    /* Re-evaluate current state configurations immediately */
                    if (system_state == ACT_STATE_BATT) {
                        leds_off(LEDS_ALL);
                        leds_single_on(LEDS_YELLOW);
                        snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"discharge\"}");
                        try_publish(TOPIC_COMMANDS, sensors_buffer);
                    }
                } 
                else {
                    /* Safe to switch back to PV Mode */
                    system_state = ACT_STATE_PV;
                    recovery_counter = 0;
                    
                    LOG_INFO(">> STATE 1 ACTIVE: PV Regime Restored. Resuming ML predictions.\n");
                    leds_off(LEDS_ALL);
                    leds_on(LEDS_GREEN);
                    
                    snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"charge\"}");
                    try_publish(TOPIC_COMMANDS, sensors_buffer);

                    // Timer to clear the MQTT outbound queue before sending the alarm message
                    etimer_set(&spacing_timer, CLOCK_SECOND / 2);
                    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&spacing_timer));

                    snprintf(alarms_buffer, APP_BUFFER_SIZE, "{\"info\":\"PV production sufficient. Entering PV Mode.\"}");
                    try_publish(TOPIC_ALARMS, alarms_buffer);
                }
            }
            else {
                /* Evaluate Grid vs Battery */
                if (system_state == ACT_STATE_BATT) {
                    if (current_soc <= CRITICAL_SOC) {
                        /* TRANSITION 2 -> 3: Battery depleted */
                        system_state = ACT_STATE_GRID;
                        
                        LOG_INFO(">> STATE 3 ACTIVE: Grid Regime. Battery < 20%%.\n");
                        leds_off(LEDS_ALL);
                        leds_on(LEDS_RED);
                        
                        snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"critical\"}");
                        try_publish(TOPIC_COMMANDS, sensors_buffer);

                        // Timer to clear the MQTT outbound queue before sending the alarm message
                        etimer_set(&spacing_timer, CLOCK_SECOND / 2);
                        PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&spacing_timer));

                        snprintf(alarms_buffer, APP_BUFFER_SIZE, "{\"alarm\":\"Battery critical (< 20%%). Switching to National Grid.\"}");
                        try_publish(TOPIC_ALARMS, alarms_buffer);
                    } else {
                        /* Maintain State 2 */
                        leds_off(LEDS_ALL);
                        leds_single_on(LEDS_YELLOW);
                        snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"discharge\"}");
                        try_publish(TOPIC_COMMANDS, sensors_buffer);
                    }
                } 
                else if (system_state == ACT_STATE_GRID) {

                    /* TRANSITION 3 -> 2: Battery magically recharged (CLI Override) */
                    if (current_soc > CRITICAL_SOC) {
                        system_state = ACT_STATE_BATT;

                        LOG_INFO(">> STATE 2 ACTIVE: Battery Regime Restored (SOC > 20%%).\n");
                        leds_off(LEDS_ALL);
                        leds_single_on(LEDS_YELLOW);

                        /* Send command to the battery to unlock it from the critical state */
                        snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"discharge\"}");
                        try_publish(TOPIC_COMMANDS, sensors_buffer);

                        /* Timer to clear the MQTT outbound queue before sending the alarm message */
                        etimer_set(&spacing_timer, CLOCK_SECOND / 2);
                        PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&spacing_timer));

                        snprintf(alarms_buffer, APP_BUFFER_SIZE, "{\"info\":\"Battery SOC restored. Resuming Battery Mode.\"}");
                        try_publish(TOPIC_ALARMS, alarms_buffer);
                    } 
                    else {
                        /* Maintain State 3 */ 
                        leds_off(LEDS_ALL);
                        leds_on(LEDS_RED);
                        snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"critical\"}");
                        try_publish(TOPIC_COMMANDS, sensors_buffer);
                    }
                }
            }
        }
      }

      /* Safe Mode Reaction (Overrides everything to protect the system) */
      if(is_safe_mode) {
        LOG_INFO("SAFE MODE ON: Forcing emergency state.\n");
        
        if (current_soc > CRITICAL_SOC) {
          snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"discharge\"}");
          leds_off(LEDS_ALL);
          leds_single_on(LEDS_YELLOW);
        } else {
          snprintf(sensors_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"critical\"}");
          leds_off(LEDS_ALL);
          leds_on(LEDS_RED);
        }
        try_publish(TOPIC_COMMANDS, sensors_buffer);

        etimer_set(&spacing_timer, CLOCK_SECOND / 2);
        PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&spacing_timer));

        snprintf(alarms_buffer, APP_BUFFER_SIZE, "{\"alarm\":\"SAFE MODE ON: Sensor timeout detected. Emergency regime active.\"}");
        try_publish(TOPIC_ALARMS, alarms_buffer);
      }

      // ===== [SAFE MODE EXIT - NEW] Detect recovery and publish restored state =====
      // If last cycle was Safe Mode and now it is not, we just recovered.
      // Reset the FSM to PV regime and start the recovery publish.
      if(was_safe_mode && !is_safe_mode) {
        system_state = ACT_STATE_PV;
        recovery_counter = 0;
        LOG_INFO(">> RECOVERY: Exiting Safe Mode, back to PV Regime.\n");
        leds_off(LEDS_ALL);
        leds_on(LEDS_GREEN);
        // ===== [SAFE MODE EXIT FIX - NEW] Arm the pending recovery =====
        recovery_pending = true;
        // ===== [END SAFE MODE EXIT FIX - NEW] =====
      }
      // Remember current status for the next cycle.
      was_safe_mode = is_safe_mode;
      // ===== [END SAFE MODE EXIT - NEW] =====

      // ===== [SAFE MODE EXIT FIX - NEW] Re-publish PV recovery until it succeeds =====
      // The single recovery publish can fail with OUT_QUEUE_FULL and be lost,
      // leaving Grafana stuck on Safe Mode. So we keep re-publishing every cycle
      // (only while not in Safe Mode) until one publish actually goes through.
      if(recovery_pending && !is_safe_mode) {
        // "PV Mode" keyword is what the Cloud maps to STATE 1.
        snprintf(alarms_buffer, APP_BUFFER_SIZE,
                 "{\"alarm\":\"Recovery complete. Back to PV Mode.\"}");
        try_publish(TOPIC_ALARMS, alarms_buffer);
        if(last_publish_ok) {
          recovery_pending = false;   // delivered, stop retrying
          LOG_INFO(">> RECOVERY message delivered to Cloud.\n");
        } else {
          LOG_WARN(">> RECOVERY message not sent yet, will retry next cycle.\n");
        }
      }
      // ===== [END SAFE MODE EXIT FIX - NEW] =====

      /* Update variables for the next cycle */
      prev_power = current_power;
      prev_irr = current_irr;
      first_cycle_done = true;
    }
      etimer_set(&ai_timer, 20 * CLOCK_SECOND);
    }
  }
  PROCESS_END();
}