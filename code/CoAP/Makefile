# Dichiariamo ENTRAMBI i nomi dei file (senza l'estensione .c)
CONTIKI_PROJECT = coap_power coap_irradiation
all: $(CONTIKI_PROJECT)

# Il resto del Makefile rimane identico
MAKE_NET = MAKE_NET_IPV6
MODULES += os/net/app-layer/coap
CONTIKI = ../../..
include $(CONTIKI)/Makefile.include