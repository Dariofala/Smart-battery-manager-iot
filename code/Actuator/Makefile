# ==============================================================================
# EMLEARN PATH CONFIGURATION (TO BE MODIFIED ON OTHER PCs)
# ==============================================================================
# To use another PC, run in the terminal: 
# python3 -c "import emlearn; print(emlearn.__path__[0])"
# and paste the result below replacing this path:
EMLEARN_PATH = /home/leona/.local/lib/python3.12/site-packages/emlearn
# EMLEARN_PATH = /opt/anaconda3/lib/python3.13/site-packages/emlearn
# ==============================================================================

CONTIKI_PROJECT = smart_actuator
all: $(CONTIKI_PROJECT)

# Path to Contiki-NG root (3 levels up)
CONTIKI = ../../..

# Include CoAP and MQTT submodules using the correct macros
include $(CONTIKI)/Makefile.dir-variables
MODULES += $(CONTIKI_NG_APP_LAYER_DIR)/coap
MODULES += $(CONTIKI_NG_APP_LAYER_DIR)/mqtt

MODULES_REL += $(EMLEARN_PATH)
INC += $(EMLEARN_PATH)

# Mandatory math library for emlearn
TARGET_LIBFILES += -lm

CFLAGS += -Wno-unused-variable -Wno-unused-function

# Disable the transformation of warnings into fatal errors by Contiki-NG
WERROR = 0

include $(CONTIKI)/Makefile.include