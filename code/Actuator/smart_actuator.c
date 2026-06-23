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

/* Inclusione del Modello di Machine Learning */
#include "smart_battery_model.h"

#define LOG_MODULE "Attuatore Smart"
#define LOG_LEVEL LOG_LEVEL_INFO

/* --- CONFIGURAZIONI RETE --- */
#define BROKER_IP "fd00::1" 
#define BROKER_PORT 1883
#define CLIENT_ID "nRF52840-Actuator"

/* Indirizzi dei Sensori CoAP (Verificali su Cooja/Tunslip) */
#define COAP_POWER_IP "coap://[fd00::202:2:2:2]:5683"
#define COAP_IRR_IP   "coap://[fd00::203:3:3:3]:5683"

#define TOPIC_METEO    "network/temperature"
#define TOPIC_COMANDI  "network/commands/battery"
#define TOPIC_ALLARMI  "network/allarmi"

#define APP_BUFFER_SIZE 128
static char app_buffer[APP_BUFFER_SIZE];
#define MAX_TCP_SEGMENT_SIZE 128

/* --- COSTANTI MACHINE LEARNING (Da Python) --- */
static const float FEATURE_MEAN[5]  = { 276.5014f, 276.9828f, 0.2314f, 0.2313f, 32.0125f };
static const float FEATURE_SCALE[5] = { 380.8638f, 381.0449f, 0.3070f, 0.3065f, 11.8554f };

/* --- VARIABILI DI STATO --- */
static float current_power = 0.0, prev_power = 0.0;
static float current_irr = 0.0,   prev_irr = 0.0;
static float current_temp = 0.0;

static bool first_cycle_done = false;
static uint32_t last_temp_timestamp = 0;
#define SAFE_MODE_TIMEOUT (60 * CLOCK_SECOND)

/* Strutture MQTT e CoAP */
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

static uint8_t mqtt_state = STATE_INIT;

PROCESS(smart_actuator_process, "Smart Actuator Process");
AUTOSTART_PROCESSES(&smart_actuator_process);

/* --- FUNZIONE DI SUPPORTO: ESTRAE IL VALORE DAL JSON --- */
static float extract_json_value(const char* payload) {
  char *ptr = strstr(payload, "\"v\":");
  if(ptr != NULL) {
    return atof(ptr + 4);
  }
  return 0.0;
}

/* --- CALLBACKS COAP (OBSERVE) --- */
static void notification_callback_power(coap_observee_t *obs, void *notification, coap_notification_flag_t flag) {
  if(notification) {
    const uint8_t *payload = NULL;
    coap_get_payload(notification, &payload);
    current_power = extract_json_value((const char *)payload);
    LOG_INFO("CoAP RX Potenza: %.2f W\n", current_power);
  }
}

static void notification_callback_irr(coap_observee_t *obs, void *notification, coap_notification_flag_t flag) {
  if(notification) {
    const uint8_t *payload = NULL;
    coap_get_payload(notification, &payload);
    current_irr = extract_json_value((const char *)payload);
    LOG_INFO("CoAP RX Irraggiamento: %.4f W/m2\n", current_irr);
  }
}

/* --- CALLBACK MQTT --- */
static void mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data) {
  switch(event) {
    case MQTT_EVENT_CONNECTED:
      LOG_INFO("MQTT Connesso!\n");
      mqtt_state = STATE_CONNECTED;
      break;
    case MQTT_EVENT_SUBACK:
      LOG_INFO("Iscrizione a meteo completata.\n");
      mqtt_state = STATE_SUBSCRIBED;
      break;
    case MQTT_EVENT_PUBLISH: {
      struct mqtt_message *msg = data;
      char payload[64];
      snprintf(payload, sizeof(payload), "%.*s", msg->payload_length, (char *)msg->payload_chunk);
      
      /* Se riceviamo la temperatura, aggiorniamo il valore e il timestamp per il Safe Mode */
      if(strstr(payload, "module_temp") != NULL) {
        current_temp = extract_json_value(payload);
        last_temp_timestamp = clock_time();
        LOG_INFO("MQTT RX Temperatura: %.2f C\n", current_temp);
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

  LOG_INFO("Avvio Smart Actuator...\n");

  /* Inizializzazioni */
  coap_engine_init();
  mqtt_register(&conn, &smart_actuator_process, CLIENT_ID, mqtt_event, MAX_TCP_SEGMENT_SIZE);
  uiplib_ipaddrconv(BROKER_IP, &broker_address);

  /* Parsing Endpoints CoAP */
  coap_endpoint_parse(COAP_POWER_IP, strlen(COAP_POWER_IP), &ep_power);
  coap_endpoint_parse(COAP_IRR_IP, strlen(COAP_IRR_IP), &ep_irr);

  /* L'Attuatore valuta la situazione ogni 20 secondi */
  etimer_set(&ai_timer, 20 * CLOCK_SECOND);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&ai_timer));

    /* GESTIONE RETE MQTT E COAP OBSERVE */
    if(have_connectivity()) {
      
      if(mqtt_state == STATE_INIT) {
        LOG_INFO("Connessione MQTT e Avvio CoAP Observe...\n");
        status = mqtt_connect(&conn, BROKER_IP, BROKER_PORT, 60, MQTT_CLEAN_SESSION_ON);
        if(status != MQTT_STATUS_OUT_QUEUE_FULL) {
          mqtt_state = STATE_CONNECTING;
        }
        /* Avvio Observe CoAP */
        obs_power = coap_obs_request_registration(&ep_power, "telemetria/potenza", notification_callback_power, NULL);
        obs_irr = coap_obs_request_registration(&ep_irr, "telemetria/irraggiamento", notification_callback_irr, NULL);
      }

      if(mqtt_state == STATE_CONNECTED) {
        mqtt_subscribe(&conn, NULL, TOPIC_METEO, MQTT_QOS_LEVEL_0);
        mqtt_state = STATE_SUBSCRIBING;
      }
    }

    /* --- IL CUORE: LOGICA DI CONTROLLO AI E SAFE MODE --- */
    if(mqtt_state == STATE_SUBSCRIBED) {
      
      bool is_safe_mode = false;

      /* 1. Controllo Safe Mode (Mancanza di dati meteo) */
      if ((clock_time() - last_temp_timestamp) > SAFE_MODE_TIMEOUT && last_temp_timestamp != 0) {
        LOG_WARN("!!! SAFE MODE ATTIVATA !!! Timeout sensore meteo.\n");
        is_safe_mode = true;
      }

      /* 2. Esecuzione TinyML (Solo se abbiamo i dati storici dal primo ciclo) */
      if(first_cycle_done && !is_safe_mode) {
        
        float features[5];
        /* Ordine: AC_POWER, AC_POWER_t-1, IRRADIATION, IRRADIATION_t-1, MODULE_TEMPERATURE */
        features[0] = (current_power - FEATURE_MEAN[0]) / FEATURE_SCALE[0];
        features[1] = (prev_power    - FEATURE_MEAN[1]) / FEATURE_SCALE[1];
        features[2] = (current_irr   - FEATURE_MEAN[2]) / FEATURE_SCALE[2];
        features[3] = (prev_irr      - FEATURE_MEAN[3]) / FEATURE_SCALE[3];
        features[4] = (current_temp  - FEATURE_MEAN[4]) / FEATURE_SCALE[4];

        int prediction = smart_battery_predict(features, 5);
        LOG_INFO("AI Prediction Risultato: %d\n", prediction);

        if(prediction == 1) {
          LOG_INFO("Invio comandi di scarica al Sensore Batteria.\n");
          /* CROLLO IMMINENTE */
          leds_on(LEDS_RED);
          leds_off(LEDS_GREEN);
          snprintf(app_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"scarica\"}");
          mqtt_publish(&conn, NULL, TOPIC_COMANDI, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
          
          snprintf(app_buffer, APP_BUFFER_SIZE, "{\"allarme\":\"Possibile crollo potenza rilevato\"}");
          mqtt_publish(&conn, NULL, TOPIC_ALLARMI, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
        } else {
          /* SISTEMA STABILE */
          leds_on(LEDS_GREEN);
          leds_off(LEDS_RED);
          snprintf(app_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"carica\"}");
          mqtt_publish(&conn, NULL, TOPIC_COMANDI, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
        }
      }

      /* 3. Reazione al Safe Mode */
      if(is_safe_mode) {
        LOG_INFO("SAFE MODE ON:Invio comandi di scarica al Sensore Batteria.\n");
        leds_on(LEDS_RED);
        leds_off(LEDS_GREEN);
        snprintf(app_buffer, APP_BUFFER_SIZE, "{\"cmd\":\"scarica\"}");
        mqtt_publish(&conn, NULL, TOPIC_COMANDI, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
      }

      /* Aggiornamento variabili storiche per il ciclo successivo */
      prev_power = current_power;
      prev_irr = current_irr;
      first_cycle_done = true;
    }

    etimer_reset(&ai_timer);
  }

  PROCESS_END();
}