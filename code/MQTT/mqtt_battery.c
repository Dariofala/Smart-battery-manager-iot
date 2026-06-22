#include "contiki.h"
#include "net/routing/routing.h"
#include "mqtt.h"
#include "net/ipv6/uip.h"
#include "sys/etimer.h"
#include "sys/log.h"
#include "dev/leds.h" /* Nuova libreria per controllare i LED */

#include <math.h> 
#include <string.h>
#include <stdio.h>

#define LOG_MODULE "Sensore MQTT (Batteria)"
#define LOG_LEVEL LOG_LEVEL_INFO

/* --- CONFIGURAZIONI MQTT --- */
#define BROKER_IP "fd00::1" 
#define BROKER_PORT 1883

#define CLIENT_ID "nRF52840-Battery"
#define TOPIC_BATTERIA "rete/batteria"
#define TOPIC_COMANDI "rete/comandi/batteria" /* Il topic dove ascoltiamo l'Attuatore */

#define APP_BUFFER_SIZE 128
static char app_buffer[APP_BUFFER_SIZE];
#define MAX_TCP_SEGMENT_SIZE 128

#define SAMPLING_INTERVAL (15 * CLOCK_SECOND)

/* --- LOGICA BATTERIA E STATO FISICO --- */
#define SOC_THRESHOLD 1.0
static float current_soc = 100.0; /* Iniziamo col 100% di carica */
static float last_published_soc = -100.0; 
static bool is_charging = true;   /* Flag per determinare se carica o scarica */

/* Struttura MQTT */
static struct mqtt_connection conn;
static mqtt_status_t status;

/* Macchina a Stati ampliata per supportare la Subscribe */
#define STATE_INIT         0
#define STATE_CONNECTING   1
#define STATE_CONNECTED    2
#define STATE_SUBSCRIBING  3
#define STATE_SUBSCRIBED   4
#define STATE_DISCONNECTED 5

static uint8_t state = STATE_INIT;

PROCESS(mqtt_battery_process, "MQTT Battery Process");
AUTOSTART_PROCESSES(&mqtt_battery_process);

/* Callback MQTT */
static void mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data) {
  switch(event) {
    case MQTT_EVENT_CONNECTED:
      LOG_INFO("Connessione TCP/MQTT stabilita!\n");
      state = STATE_CONNECTED;
      break;
    
    case MQTT_EVENT_DISCONNECTED:
      LOG_WARN("Disconnesso dal Broker MQTT.\n");
      state = STATE_DISCONNECTED;
      break;
    
    case MQTT_EVENT_SUBACK:
      LOG_INFO("Iscrizione confermata al topic dei comandi.\n");
      state = STATE_SUBSCRIBED;
      break;
    
    case MQTT_EVENT_PUBACK:
      LOG_INFO("Messaggio pubblicato (PUBACK ricevuto).\n");
      break;

    /* EVENTO CRITICO: Ricezione di un messaggio dal topic sottoscritto */
    case MQTT_EVENT_PUBLISH: {
      struct mqtt_message *msg = data;
      char payload[32];
      
      /* Copiamo il payload in modo sicuro per fare il parsing testuale */
      snprintf(payload, sizeof(payload), "%.*s", msg->payload_length, (char *)msg->payload_chunk);
      
      if(strstr(payload, "scarica") != NULL) {
        is_charging = false;
        LOG_INFO("=== COMANDO RICEVUTO: INIZIO SCARICA ===\n");
      } else if(strstr(payload, "carica") != NULL) {
        is_charging = true;
        LOG_INFO("=== COMANDO RICEVUTO: INIZIO CARICA ===\n");
      }
      break;
    }
    
    default:
      break;
  }
}

static bool have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL || uip_ds6_defrt_choose() == NULL) {
    return false;
  }
  return true;
}

PROCESS_THREAD(mqtt_battery_process, ev, data)
{
  static struct etimer timer;
  static uip_ipaddr_t broker_address;

  PROCESS_BEGIN();

  LOG_INFO("Avvio Sensore MQTT (Batteria Interattiva)...\n");

  mqtt_register(&conn, &mqtt_battery_process, CLIENT_ID, mqtt_event, MAX_TCP_SEGMENT_SIZE);
  uiplib_ipaddrconv(BROKER_IP, &broker_address);

  etimer_set(&timer, SAMPLING_INTERVAL);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

    if(state == STATE_INIT || state == STATE_DISCONNECTED) {
      if(have_connectivity()) {
        state = STATE_CONNECTING;
      } else {
        LOG_INFO("Attesa di instradamento RPL...\n");
      }
    }

    if(state == STATE_CONNECTING) {
      LOG_INFO("Tentativo di connessione al Broker [%s]:%d\n", BROKER_IP, BROKER_PORT);
      status = mqtt_connect(&conn, BROKER_IP, BROKER_PORT,
                            (SAMPLING_INTERVAL * 3) / CLOCK_SECOND,
                            MQTT_CLEAN_SESSION_ON);
      if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
        LOG_ERR("Coda di rete piena. Riprovo più tardi...\n");
      }
    }

    /* Subito dopo la connessione, ci iscriviamo per ascoltare l'Attuatore */
    if(state == STATE_CONNECTED) {
      LOG_INFO("Invio richiesta di Subscribe a %s\n", TOPIC_COMANDI);
      mqtt_subscribe(&conn, NULL, TOPIC_COMANDI, MQTT_QOS_LEVEL_0);
      state = STATE_SUBSCRIBING; /* Aspettiamo il SUBACK dalla callback */
    }

    /* Flusso di lavoro a regime */
    if(state == STATE_SUBSCRIBED) {
      
      /* 1. ESECUZIONE DELLA MATEMATICA E CLIPPING FISICO */
      if(is_charging) {
        current_soc += 1.5;
        if(current_soc >= 100.0) {
            current_soc = 100.0;
        }
      } else {
        current_soc -= 1.5;
        if(current_soc <= 0.0) {
            current_soc = 0.0;
        }
      }

      /* 2. GESTIONE DEI LED (Feedback Visivo in Cooja) */
      leds_off(LEDS_ALL); /* Spegniamo i vecchi led */
      
      if(is_charging) {
        if(current_soc >= 100.0) {
          leds_set(LEDS_NUM_TO_MASK(LEDS_GREEN)); /* Carica al 100% -> Verde */
        } else {
          leds_on(LEDS_BLUE);  /* In ricarica -> Blu (o equivalente) */
        }
      } else {
        if(current_soc <= 20.0) {
          leds_on(LEDS_RED);   /* Sotto il 20% in scarica -> Emergenza Rosso */
        } else {
          leds_on(LEDS_YELLOW);/* In scarica normale -> Giallo */
        }
      }

      /* 3. PUBBLICAZIONE AL CLOUD/GRAFANA */
      if(fabs(current_soc - last_published_soc) >= SOC_THRESHOLD) {
        
        snprintf(app_buffer, APP_BUFFER_SIZE, 
                 "{\"bn\":\"%s\", \"n\":\"battery_soc\", \"v\":%.1f, \"u\":\"%%\"}", 
                 CLIENT_ID, current_soc);
        
        LOG_INFO("Stato: %.1f%% (%s). Pubblico su %s\n", 
                 current_soc, is_charging ? "Carica" : "Scarica", TOPIC_BATTERIA);
        
        mqtt_publish(&conn, NULL, TOPIC_BATTERIA, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
        
        last_published_soc = current_soc;
      }
    }

    etimer_reset(&timer);
  }

  PROCESS_END();
}