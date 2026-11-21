# 🔋 Smart Battery Manager — IoT Microgrid with Edge AI

![Contiki-NG](https://img.shields.io/badge/Contiki--NG-6LoWPAN%20%2F%20RPL-0A7BBB)
![nRF52840](https://img.shields.io/badge/hardware-nRF52840-00A9CE)
![C](https://img.shields.io/badge/C-firmware-A8B9CC?logo=c&logoColor=black)
![Python](https://img.shields.io/badge/Python-asyncio-3776AB?logo=python&logoColor=white)
![MQTT](https://img.shields.io/badge/MQTT-Mosquitto-660066?logo=mqtt&logoColor=white)
![InfluxDB](https://img.shields.io/badge/InfluxDB-22ADF6?logo=influxdb&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?logo=grafana&logoColor=white)

An IoT system that manages the power supply of a load in a small **microgrid** with a photovoltaic panel, a battery
and the national grid. A network of **nRF52840** nodes running **Contiki-NG** collects real-time telemetry over
**CoAP and MQTT**, and a smart actuator runs an **on-device neural network** to predict solar power drops
and switch source *before* production collapses.

Group project for the **Internet of Things** course — MSc in Artificial Intelligence and Data Engineering,
University of Pisa, academic year 2025/26.

---

## ✨ Highlights

- **Real hardware**: four sensor nodes, one actuator and a border router on nRF52840 dongles, over a 6LoWPAN/RPL IPv6 network.
- **Two protocols, each where it fits best**:
  - **CoAP + Observe** (UDP) for fast-changing quantities: AC power and solar irradiation;
  - **MQTT** (TCP) for slow quantities with on-device filtering: battery state of charge and panel temperature.
- **TinyML on the edge**: a 5→32→16→2 neural network trained in Keras and compiled into the firmware with
  [emlearn](https://github.com/emlearn/emlearn); it predicts an imminent drop of PV production 15 minutes ahead.
- **Control logic**: a finite state machine (PV → Battery → Grid) with ML-gated recovery to avoid oscillations,
  plus a **Safe Mode** when sensor telemetry is lost.
- **Adaptive congestion control**: the cloud computes the Packet Delivery Ratio from sequence numbers and, when it
  drops below 0.7, nodes apply **exponential backoff** of the sampling rate and the temperature node widens its
  reporting threshold (0.5 °C → 2.0 °C).
- **Device directory**: CoAP and MQTT nodes self-register with the cloud, which forwards sensor endpoints to the actuator
  (no hard-coded addresses).
- **Observability**: every measurement, event and network metric (latency, PDR) is stored in **InfluxDB** and shown
  on a **Grafana** dashboard; a CLI allows live views and manual overrides for testing.

## 🏗️ Architecture

```
 CoAP sensors (power, irradiation) ─┐
                                    ├── 6LoWPAN / RPL ── Border router ── Cloud app (Python asyncio) ── InfluxDB ── Grafana
 MQTT sensors (battery, temperature)┤                                     │  Mosquitto broker
                                    │                                     │  device directory, PDR, latency
 Smart actuator (ML + FSM) ─────────┘◀──── congestion / control signals ──┘
```

| Node | Protocol | Role |
|------|----------|------|
| `coap_power` | CoAP server (Observe) | AC power produced by the PV side |
| `coap_irradiation` | CoAP server (Observe) | Solar irradiation |
| `mqtt_battery` | MQTT | Battery state of charge; executes charge / discharge / critical commands |
| `mqtt_temperature` | MQTT | Panel temperature, sent only on meaningful changes |
| `smart_actuator` | MQTT + CoAP client | Runs the ML model and the state machine, drives battery and LEDs |
| `border-router` | — | Connects the 6LoWPAN network to the IPv6 host |

Messages use a compact, SenML-inspired JSON format, e.g.
`{"bn":"nRF52840-Battery","n":"battery_soc","v":45.5,"u":"%","seq":10}`.
The sequence number lets the cloud detect lost packets and compute the PDR.

### Power regimes

| State | Source | Transition |
|-------|--------|------------|
| **1 — PV** | Solar panel (battery charging) | → 2 when the model predicts a drop or power < 140 W |
| **2 — Battery** | Battery (discharging) | → 3 when the state of charge reaches 20 % |
| **3 — Grid** | National grid | → 1 only after 3 cycles above threshold **and** no drop predicted by the model |
| **Safe Mode** | System isolated | Triggered by sensor timeouts or failures |

## 🧪 Experimental evaluation

Four scenarios were tested on the real network and monitored in Grafana:

1. **Normal operation**: regular telemetry, low latency, high PDR, correct regime transitions.
2. **Node failure**: the cloud distinguishes a dead node from congestion, and the actuator enters Safe Mode.
3. **Congestion with adaptation** (sampling interval forced to 0.2 s): the cloud detects the PDR drop, nodes back off
   and the network goes through a full *detect → adapt → recover* cycle.
4. **Congestion without adaptation**: transmit queues saturate and nodes disconnect under sustained load.

Details and dashboard screenshots are in [`Smart_Battery_Manager_Report.pdf`](Smart_Battery_Manager_Report.pdf).

## 🗂️ Repository structure

```
├── code/
│   ├── Actuator/          # smart actuator firmware + emlearn model (smart_battery_model.h)
│   ├── CoAP/              # power and irradiation sensors
│   ├── MQTT/              # battery and temperature sensors
│   ├── border-router/     # RPL border router
│   ├── Application/       # cloud application (Cloud_app.py) + requirements.txt
│   ├── ML_Notebook.ipynb  # training, evaluation and emlearn export of the model
│   └── Dashboard.json     # Grafana dashboard
├── datasets/              # solar plant generation and weather data
└── Smart_Battery_Manager_Report.pdf
```

## 🚀 Running it

**Requirements:** [Contiki-NG](https://github.com/contiki-ng/contiki-ng) with the ARM toolchain, nRF52840 dongles,
Mosquitto, InfluxDB 2, Grafana, Python 3.

**Firmware.** The Makefiles expect the repository inside the Contiki-NG `examples/` folder
(`CONTIKI = ../../..`). Build and flash each node, for example:

```bash
cd contiki-ng/examples/<this-repo>/code/Actuator
make TARGET=nrf52840 BOARD=dongle smart_actuator.dfu-upload PORT=/dev/tty.usbmodemXXXX
```

**Cloud application**

```bash
cd code/Application
pip install -r requirements.txt
export INFLUX_TOKEN="your-influxdb-token"   # optional: INFLUX_URL, INFLUX_ORG, INFLUX_BUCKET
python3 Cloud_app.py
```

Then import `code/Dashboard.json` into Grafana and connect it to the InfluxDB bucket.

## 📊 Dataset

The model is trained on the public [Solar Power Generation Data](https://www.kaggle.com/datasets/anikannal/solar-power-generation-data)
(plant generation and weather-sensor readings). A subset of the same data is embedded in the sensor firmware
(`dataset_replay.h`) to replay realistic photovoltaic conditions during the experiments.

## 👥 Authors

- **Dario Falaschi** — [@Dariofala](https://github.com/Dariofala)
- **Leonardo Lentini** — [@LeonardoLentini](https://github.com/LeonardoLentini)
