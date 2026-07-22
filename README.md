# Internet-of-Things-Project
# Smart Battery Manager

This repository contains the source code and configuration files for an advanced IoT Smart Battery Management system, developed at the University of Pisa. 

The project simulates a smart microgrid environment running on a **Contiki-NG** 6LoWPAN network. An Edge AI-powered smart actuator dynamically manages the power supply of a load, autonomously switching between a Photovoltaic (PV) panel, a Battery, and the National Grid based on real-time sensor telemetry and Machine Learning predictions.

## System Architecture

The system relies on a heterogeneous IoT architecture, integrating both **CoAP** and **MQTT** protocols over an IPv6 RPL network.

### Edge Devices (Contiki-NG)
*   **Smart Actuator (The Brain):** Acts as the core decision-maker. It subscribes to MQTT telemetry, observes CoAP sensors, and runs a compiled `emlearn` TinyML model to predict power drops. It implements a robust 3-state Finite State Machine (FSM) to control the power regime.
*   **CoAP Sensors (UDP):** 
    *   `coap_power.c`: Simulates AC power generation.
    *   `coap_irradiation.c`: Simulates solar irradiation.
*   **MQTT Sensors (TCP):**
    *   `mqtt_temperature.c`: Transmits PV module temperature.
    *   `mqtt_battery.c`: Simulates a 3-state smart battery (Charging, Discharging, Critical) responding to actuator commands.

### Cloud Application & Dashboard
A centralized Python-based cloud application manages node registration, network monitoring, and data persistence. Telemetry, latency, and packet delivery ratio (PDR) metrics are stored in **InfluxDB** and visualized in real-time through a **Grafana** dashboard.

## Key Features

*   **Edge Machine Learning:** The actuator runs a lightweight Decision Tree model (via `emlearn`) to predict imminent solar power collapses, triggering preemptive battery utilization.
*   **Finite State Machine:** The system transitions between PV, Battery, and Grid regimes.
*   **Adaptive Congestion Control:** The Cloud continuously monitors the network. If congestion is detected, it signals the nodes to trigger an **exponential backoff** mechanism (halving/doubling sampling rates) and adaptive filtering thresholds to alleviate network stress.
*   **Fail-Safe Mechanisms:** The actuator incorporates a *Safe Mode* that automatically isolates the system and halts the discharge process if critical sensor timeouts or failures occur.
*   **Compact SenML-inspired Encoding:** All communications use a highly optimized, whitespace-free JSON format to minimize radio transmission payloads.

Group members:
- Falaschi Dario
- Lentini Leonardo
