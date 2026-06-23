# ==============================================================================
# CONFIGURAZIONE PERCORSO EMLEARN (DA MODIFICARE SU ALTRI PC)
# ==============================================================================
# Se usi un altro PC, esegui nel terminale: 
# python3 -c "import emlearn; print(emlearn.__path__[0])"
# e incolla il risultato qui sotto sostituendo questo percorso:
EMLEARN_PATH = /home/leona/.local/lib/python3.12/site-packages/emlearn
# ==============================================================================

CONTIKI_PROJECT = smart_actuator
all: $(CONTIKI_PROJECT)

# Risaliamo alla radice di contiki-ng (3 livelli)
CONTIKI = ../../..

# Includiamo i sottomoduli CoAP e MQTT usando le macro corrette
include $(CONTIKI)/Makefile.dir-variables
MODULES += $(CONTIKI_NG_APP_LAYER_DIR)/coap
MODULES += $(CONTIKI_NG_APP_LAYER_DIR)/mqtt

MODULES_REL += $(EMLEARN_PATH)
INC += $(EMLEARN_PATH)

# Libreria matematica obbligatoria per emlearn
TARGET_LIBFILES += -lm

CFLAGS += -Wno-unused-variable -Wno-unused-function

include $(CONTIKI)/Makefile.include