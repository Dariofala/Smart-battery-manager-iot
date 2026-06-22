#include "contiki.h"
#include "net/routing/routing.h"
#include "mqtt.h"
#include "net/ipv6/uip.h"
#include "sys/etimer.h"
#include "sys/log.h"

#include <math.h> /* Per la funzione fabs (valore assoluto) */
#include <string.h>
#include <stdio.h>

/* Includiamo l'array generato da Python */
#include "dataset_replay.h"

#define LOG_MODULE "Sensore MQTT (Meteo)"
#define LOG_LEVEL LOG_LEVEL_INFO

/* --- CONFIGURAZIONI MQTT --- */
/* L'IP del Broker è l'indirizzo del tuo PC Linux visto attraverso il tunnel tunslip6 */
#define BROKER_IP "fd00::1" 
#define BROKER_PORT 1883

#define CLIENT_ID "nRF52840-Meteo"
#define TOPIC_METEO "rete/meteo"

#define APP_BUFFER_SIZE 128
static char app_buffer[APP_BUFFER_SIZE];
#define MAX_TCP_SEGMENT_SIZE 128

/* Intervallo di campionamento */
#define SAMPLING_INTERVAL (15 * CLOCK_SECOND)

/* --- LOGICA EVENT-DRIVEN (ENERGY SAVING) --- */
/* Il dato viene pubblicato solo se la temperatura varia di almeno 0.5 gradi */
#define TEMP_THRESHOLD 0.5
static float last_published_temp = -100.0; /* Inizializzazione fittizia per forzare il 1° invio */
static int current_step = 0;

/* Struttura di connessione MQTT e Macchina a Stati */
static struct mqtt_connection conn;
static mqtt_status_t status;

#define STATE_INIT         0
#define STATE_CONNECTING   1
#define STATE_CONNECTED    2
#define STATE_DISCONNECTED 3

static uint8_t state = STATE_INIT;

PROCESS(mqtt_meteo_process, "MQTT Meteo Process");
AUTOSTART_PROCESSES(&mqtt_meteo_process);

/* Callback MQTT: Gestisce le risposte dal Broker (es. conferma di connessione) */
static void mqtt_event(struct mqtt_connection *m, mqtt_event_t event, void *data) {
  switch(event) {
    case MQTT_EVENT_CONNECTED:
      LOG_INFO("Connessione TCP/MQTT stabilita con il Broker!\n");
      state = STATE_CONNECTED;
      break;
    case MQTT_EVENT_DISCONNECTED:
      LOG_WARN("Disconnesso dal Broker MQTT.\n");
      state = STATE_DISCONNECTED;
      break;
    case MQTT_EVENT_PUBACK:
      LOG_INFO("Messaggio pubblicato (PUBACK ricevuto).\n");
      break;
    default:
      break;
  }
}

/* Funzione per verificare se la rete IPv6 è pronta per instradare pacchetti */
static bool have_connectivity(void) {
  if(uip_ds6_get_global(ADDR_PREFERRED) == NULL || uip_ds6_defrt_choose() == NULL) {
    return false;
  }
  return true;
}

PROCESS_THREAD(mqtt_meteo_process, ev, data)
{
  static struct etimer timer;
  static uip_ipaddr_t broker_address;

  PROCESS_BEGIN();

  LOG_INFO("Avvio Sensore MQTT (Meteo)...\n");

  /* Registriamo il Client MQTT assegnandogli il nome e la callback */
  mqtt_register(&conn, &mqtt_meteo_process, CLIENT_ID, mqtt_event, MAX_TCP_SEGMENT_SIZE);

  /* Convertiamo la stringa dell'IP in una struttura uip_ipaddr_t */
  uiplib_ipaddrconv(BROKER_IP, &broker_address);

  etimer_set(&timer, SAMPLING_INTERVAL);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&timer));

    /* --- MACCHINA A STATI MQTT --- */
    if(state == STATE_INIT || state == STATE_DISCONNECTED) {
      if(have_connectivity()) {
        state = STATE_CONNECTING;
      } else {
        LOG_INFO("Attesa di instradamento RPL verso il Border Router...\n");
      }
    }

    if(state == STATE_CONNECTING) {
      LOG_INFO("Tentativo di connessione al Broker [%s]:%d\n", BROKER_IP, BROKER_PORT);
      /* Modificato: aggiunti i parametri di Keep Alive calcolati e Clean Session */
      status = mqtt_connect(&conn, BROKER_IP, BROKER_PORT,
                            (SAMPLING_INTERVAL * 3) / CLOCK_SECOND,
                            MQTT_CLEAN_SESSION_ON);
      if(status == MQTT_STATUS_OUT_QUEUE_FULL) {
        LOG_ERR("Coda di rete piena. Riprovo più tardi...\n");
      } else {
        /* Restiamo in attesa che la callback ci porti in STATE_CONNECTED */
      }
    }

    if(state == STATE_CONNECTED) {
      /* Leggiamo la temperatura dal dataset fittizio */
      float current_temp = module_temp_array[current_step];
      
      /* ENERGY SAVING: Controlliamo se c'è stata una variazione significativa */
      if(fabs(current_temp - last_published_temp) >= TEMP_THRESHOLD) {
        
        /* Creazione del payload in formato JSON */
        snprintf(app_buffer, APP_BUFFER_SIZE, 
                 "{\"bn\":\"%s\", \"n\":\"module_temp\", \"v\":%.2f, \"u\":\"Cel\"}", 
                 CLIENT_ID, current_temp);
        
        LOG_INFO("Variazione (> %.1f C). Pubblico: %s\n", TEMP_THRESHOLD, app_buffer);
        
        /* Pubblicazione asincrona sul topic meteo con QoS 0 */
        mqtt_publish(&conn, NULL, TOPIC_METEO, (uint8_t *)app_buffer, strlen(app_buffer), MQTT_QOS_LEVEL_0, MQTT_RETAIN_OFF);
        
        last_published_temp = current_temp;
      } else {
        LOG_INFO("Step %d - Temp: %.2f C (Invariata). Radio a riposo.\n", current_step, current_temp);
      }

      /* Avanzamento della timeline del dataset */
      current_step++;
      if(current_step >= REPLAY_STEPS) {
        current_step = 0;
      }
    }

    etimer_reset(&timer);
  }

  PROCESS_END();
}