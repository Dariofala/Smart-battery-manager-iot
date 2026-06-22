CONTIKI_PROJECT = mqtt_meteo mqtt_battery
all: $(CONTIKI_PROJECT)

# Il percorso sale di 3 livelli (MQTT -> IoT-Project -> examples -> contiki-ng)
CONTIKI = ../../..

# Inclusione formale del modulo MQTT nativo (Sintassi d'esame e di laboratorio)
include $(CONTIKI)/Makefile.dir-variables
MODULES += $(CONTIKI_NG_APP_LAYER_DIR)/mqtt

# Link esplicito alla libreria matematica per la funzione fabs()
LDFLAGS += -lm

include $(CONTIKI)/Makefile.include