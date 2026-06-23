CONTIKI_PROJECT = mqtt_temperature mqtt_battery
all: $(CONTIKI_PROJECT)

# (MQTT -> IoT-Project -> examples -> contiki-ng)
CONTIKI = ../../..

include $(CONTIKI)/Makefile.dir-variables
MODULES += $(CONTIKI_NG_APP_LAYER_DIR)/mqtt

LDFLAGS += -lm # For fabs() function

include $(CONTIKI)/Makefile.include