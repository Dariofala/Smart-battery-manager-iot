import asyncio
import json
import os
import paho.mqtt.client as mqtt
import aiocoap
import aiocoap.resource as resource

# ===== [METRICS/CONGESTION - NEW] Extra imports =====
import time                 # window timing
import threading            # protect counters shared between MQTT thread and asyncio
import subprocess           # run ping6 to measure latency
# ===== [END METRICS/CONGESTION - NEW] =====

# InfluxDB Imports
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# INFLUXDB CONFIGURATION
INFLUX_URL = "http://localhost:8086"
# INFLUX_TOKEN = "bSFIYhrvYaYcQxs1PuexIvAkiEaxIHYLxGXaWN6lM_A2cGE7Pib4fa4Y5qx9QeTrV7HQHQY--6L8CbYKc3ppEQ==" # Dario
INFLUX_TOKEN = "CibkW1KsPOOP7IHmaFHi4dteByZrsU1tgGBr3cpF9by58oulcTUjkWJ_2qGwBHyajJWq4_4BKajKis_KjTevuQ==" # Leo
INFLUX_ORG = "myorg"
INFLUX_BUCKET = "sensors"

client_db = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client_db.write_api(write_options=SYNCHRONOUS)

# --- GLOBAL STATE FOR CLI ---
display_mode = "menu" # Can be: "menu", "show", "table"

# Dictionary to hold the latest known values for the Table Mode
latest_data = {
    "systemState": "Unknown",
    "lastPrediction": "Unknown",
    "AC_Power": 0.0,
    "irradiation": 0.0,
    "temperature": 0.0,
    "battery": 0.0
}

# Global data structure to memorize registered nodes
registered_nodes = {
    "actuator": None,  # Address of actuator node
    "sensors": {}      # Sensors and their address: {"power": "...", "irradiation": "..."}
}

# Numeric codes for the system state, used to plot a clean step-chart in Grafana
STATE_CODES = {
    "STATE 1: PV Regime": 1,
    "STATE 2: Battery Regime": 2,
    "STATE 3: Grid Regime": 3,
    "EMERGENCY: Safe Mode Active": 0,
}

# ===== [METRICS/CONGESTION - NEW] Config and shared state =====
# The 4 sensors we track for the metrics (keys used everywhere below).
SENSOR_KEYS = ["power", "irradiation", "battery", "temperature"]

# Map the SenML base-name ("bn") / node_type to a sensor key.
# CoAP nodes are tracked by node_type, MQTT nodes by their "bn".
BN_TO_KEY = {
    "nRF52840-Battery": "battery",
    "nRF52840-Temperature": "temperature",
}

WINDOW_SECONDS = 60          # metrics window = normal sampling period
PDR_THRESHOLD = 0.7          # below this (and >=1 pkt per sensor) => congestion
# DA RIVEDERE (se togliere o no)
MAX_INTERVAL_S = 60          # only used for documentation/reference

# Congestion signalling topics/paths
CONGESTION_TOPIC = "network/congestion"     # MQTT nodes subscribe here
CONGESTION_COAP_PATH = "control/congestion" # CoAP nodes expose this resource

# Per-sensor sequence tracking, used to compute PDR from seq gaps.
# For each sensor: first/last seq seen and how many packets actually arrived
# inside the current window.
seq_state = {
    k: {"first": None, "last": None, "received": 0} for k in SENSOR_KEYS
}

# Total messages received in the current window (all sensors together).
msg_count_window = 0

# Current congestion status (as seen by the Cloud).
is_congested = False

# One lock protects all the counters above, because they are updated from the
# MQTT background thread (on_message) AND from the asyncio loop (observe_sensor).
metrics_lock = threading.Lock()

# MQTT nodes that registered their own IPv6, so latency (ping6) can reach them.
# Shape: {"battery": "fd00::...", "temperature": "fd00::..."}
mqtt_node_ips = {}
# ===== [END METRICS/CONGESTION - NEW] =====

# Helper function to print logs only if 'show' mode is active
def log_print(msg):
    if display_mode == "show":
        print(msg)

# INFLUXDB WRITE HELPERS

def write_measurement(device, measurement_type, value, extra_tags=None):
    # Safely write a numeric point into the 'iot_sensors' measurement.
    # Wrapped in try/except so a DB failure never breaks an MQTT/CoAP callback.
    try:
        point = (Point("iot_sensors")
                 .tag("device", device)
                 .tag("measurement_type", measurement_type)
                 .field("value", float(value)))
        if extra_tags:
            for k, v in extra_tags.items():
                point = point.tag(k, v)
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    except Exception as e:
        log_print(f"[INFLUX ERROR] write failed for {device}/{measurement_type}: {e}")


def write_event(category, message, state=None):
    # Write a discrete event (alarm, state transition, command) into a
    # separate 'iot_events' measurement, so dashboards can annotate the timeline
    # without mixing events into the continuous sensor series.
    try:
        point = (Point("iot_events")
                 .tag("category", category)          # "alarm" | "state" | "command"
                 .field("message", str(message)))
        if state is not None:
            point = point.tag("state", str(state))
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    except Exception as e:
        log_print(f"[INFLUX ERROR] event write failed: {e}")


def persist_registration(node_id, node_type, endpoint):
    # Persist the node directory into the 'registry' measurement, so the list of
    # registered sensors/actuators lives in the database (not only in RAM)
    try:
        point = (Point("registry")
                 .tag("node_id", node_id)
                 .tag("node_type", node_type)
                 .field("endpoint", str(endpoint)))
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    except Exception as e:
        log_print(f"[INFLUX ERROR] registry write failed: {e}")


def set_system_state(state_label):
    # Update the in-RAM state (for the CLI table) and persist it to InfluxDB
    # both as a numeric code (for step-charts) and as an event (for the timeline)
    latest_data["systemState"] = state_label
    code = STATE_CODES.get(state_label, -1)
    write_measurement("system", "system_state_code", code, extra_tags={"label": state_label})
    write_event("state", state_label, state=state_label)

# ===== [METRICS/CONGESTION - NEW] Record one received packet =====
# DA RIVEDERE 3^ riga dei commenti
def record_packet(sensor_key, seq):
    # Called on every telemetry message (MQTT or CoAP).
    # Updates the per-sensor seq window and the global message counter.
    # 'seq' may be None if the payload has no sequence number: in that case we
    # still count the message but cannot use it for PDR.
    global msg_count_window
    with metrics_lock:
        msg_count_window += 1
        if sensor_key not in seq_state:
            return
        if seq is None:
            seq_state[sensor_key]["received"] += 1
            return
        st = seq_state[sensor_key]
        if st["first"] is None:
            st["first"] = seq
        st["last"] = seq
        st["received"] += 1
# ===== [END METRICS/CONGESTION - NEW] =====

# MODULE 1: MQTT MANAGEMENT (Background Thread)
def on_connect(client, userdata, flags, rc, properties):
    log_print(f"[MQTT] Connected to local Mosquitto broker (Code: {rc})")
    # Subscribe to the topics defined in the C firmware
    client.subscribe("network/#")
    # ===== [MQTT REGISTRATION - NEW] Also listen to the registration topic =====
    # MQTT nodes publish here at startup so we can add them to the DB directory.
    client.subscribe("registration")
    # ===== [END MQTT REGISTRATION - NEW] =====

def on_message(client, userdata, msg):
    # ===== [BUGFIX - NEW] Never let a malformed payload crash the MQTT thread =====
    # Some messages (corrupted, binary, or stale retained) may not be valid
    # UTF-8/JSON. Decode inside try and drop the message instead of raising.
    try:
        payload = msg.payload.decode('utf-8')
    except (UnicodeDecodeError, AttributeError):
        log_print(f"[MQTT WARN] Dropped non-UTF8 payload on topic '{msg.topic}'")
        return
    # ===== [END BUGFIX - NEW] =====

    try:
        data = json.loads(payload)
        
        # ===== [MQTT REGISTRATION - NEW] Handle node registration on 'registration' topic =====
        # MQTT nodes publish {"id": "...", "type": "...", "ip": "..."} once at startup.
        if msg.topic == "registration":
            node_id = data.get("id", "Unknown")
            node_type = data.get("type", "Unknown")
            node_ip = data.get("ip")   # IPv6 string, used for latency ping
            log_print(f"\n[MQTT REGISTRATION] {node_id} (type: {node_type}, ip: {node_ip})")
            # Save the IP so measure_latency() can ping this MQTT node too.
            if node_ip and node_type in SENSOR_KEYS:
                mqtt_node_ips[node_type] = node_ip
            # Save it in the same 'registry' directory used for CoAP nodes.
            endpoint = f"mqtt://{node_id}" if not node_ip else f"mqtt://[{node_ip}]"
            persist_registration(node_id, node_type, endpoint)
            return
        # ===== [END MQTT REGISTRATION - NEW] =====

        # 1. Parsing SenML Telemetry Data
        if "bn" in data and "n" in data and "v" in data:
            device = data["bn"]
            measurement = data["n"]
            value = float(data["v"])
            unit = data.get("u", "")
            
            log_print(f"[MQTT TELEMETRY] {device} -> {measurement}: {value} {unit}")
            
            # Update the latest_data dictionary for the table
            if measurement == "battery_soc":
                latest_data["battery"] = value
            elif measurement == "module_temp":
                latest_data["temperature"] = value
            elif measurement == "prediction":
                latest_data["lastPrediction"] = value
            elif measurement == "system_state":
                # Only if the firmware publishes the state as SenML numeric value
                set_system_state(data.get("vs", "Unknown"))
            
            # ===== [METRICS/CONGESTION - NEW] Count MQTT telemetry for PDR/msg-count =====
            # Only real sensor telemetry counts (skip CLI overrides with bn="cli").
            sensor_key = BN_TO_KEY.get(device)
            if sensor_key is not None:
                record_packet(sensor_key, data.get("seq"))
            # ===== [END METRICS/CONGESTION - NEW] =====

            # DB Write
            write_measurement(device, measurement, value)
            
        # 2. Parsing Alarms & Info from the Smart Actuator
        elif "alarm" in data or "info" in data:
            msg_text = data.get("alarm", data.get("info", ""))
            log_print(f"\n[ACTUATOR MSG] {msg_text}\n")
            
            # Persist the raw actuator message as a discrete event
            write_event("alarm", msg_text)
            
            # Infer system state from alarm text and persist it (state is not sent explicitly)
            if "Battery Mode" in msg_text or "Collapse" in msg_text:
                set_system_state("STATE 2: Battery Regime")
            elif "National Grid" in msg_text:
                set_system_state("STATE 3: Grid Regime")
            elif "PV Mode" in msg_text:
                set_system_state("STATE 1: PV Regime")
            elif "SAFE MODE" in msg_text:
                set_system_state("EMERGENCY: Safe Mode Active")
                
        # 3. Parsing Commands
        elif "cmd" in data:
            log_print(f"[MQTT COMMAND] Action requested to the battery: {data['cmd']}")
            # Persist the command as an event, useful to correlate control actions in Grafana
            write_event("command", data["cmd"])
            
    except json.JSONDecodeError:
        log_print(f"[MQTT ERROR] Received non-JSON payload: {payload}")
    # ===== [BUGFIX - NEW] Catch-all so no handler error kills the MQTT thread =====
    except Exception as e:
        log_print(f"[MQTT ERROR] on_message failed on topic '{msg.topic}': {e}")
    # ===== [END BUGFIX - NEW] =====

# CoAP client Context reusable for all the output requests
coap_client_context = None

# MODULE 2: CoAP CLIENT (Observe Setup)
active_observes = {}

async def observe_sensor(client_ip, node_id, node_type):
    global coap_client_context
    protocol = coap_client_context
    resource_path = "telemetry/power" if node_type == "power" else "telemetry/irradiation"
    uri = f"coap://[{client_ip}]:5683/{resource_path}"
    
    request = aiocoap.Message(code=aiocoap.GET, uri=uri, observe=0)
    log_print(f"[CoAP OBSERVE] Starting to listen on {uri}...")
    
    try:
        requester = protocol.request(request)
        observation_iterator = requester.observation.__aiter__()
        
        while True:
            response = await asyncio.wait_for(observation_iterator.__anext__(), timeout=60.0)
            payload = response.payload.decode('utf-8')
            
            log_print(f"[CoAP TELEMETRY] {node_id} ({client_ip}): {payload}")

            try:
                data = json.loads(payload)
                val = float(data.get("v", 0.0))
                measurement_name = data.get("n", "unknown")
                
                # Update data for the table
                if node_type == "power":
                    latest_data["AC_Power"] = val
                elif node_type == "irradiation":
                    latest_data["irradiation"] = val
                
                # ===== [METRICS/CONGESTION - NEW] Count CoAP telemetry for PDR/msg-count =====
                record_packet(node_type, data.get("seq"))
                # ===== [END METRICS/CONGESTION - NEW] =====

                # DB Write
                write_measurement(node_id, measurement_name, val)
            except Exception as e:
                log_print(f"[CoAP INFLUX ERROR] Failed DB insert: {e}")
            
    except asyncio.TimeoutError:
        log_print(f"[CoAP TIMEOUT] Sensor {node_id} is not responding!")
    except Exception as e:
        log_print(f"[CoAP ERROR] Error on {node_id}: {e}")
    finally:
        if node_id in active_observes and active_observes[node_id]['ip'] == client_ip:
            del active_observes[node_id]


# MODULE 3: CoAP SERVER (Registration)
class RegistrationResource(resource.Resource):
    async def render_post(self, request):
        payload = request.payload.decode('utf-8')
        
        # Cross-platform IP extraction
        if hasattr(request.remote, 'sockaddr'):
            client_ip = request.remote.sockaddr[0]  # for Ubuntu/Linux
        elif hasattr(request.remote, 'address'):
            client_ip = request.remote.address[0]   # for macOS
        else:
            client_ip = "Unknown"
        
        try:
            data = json.loads(payload)
            # The C node must send a JSON like: {"id": "nRF52840-Power", "type": "power"}
            node_id = data.get("id", "Unknown")
            node_type = data.get("type", "Unknown")
            if not node_id or not node_type:
                return aiocoap.Message(code=aiocoap.BAD_REQUEST, payload=b"Missing node_id or type")
            
            log_print(f"\n[CoAP REGISTRATION] Received from {node_id} (IP: {client_ip})")
            
            if node_type == "actuator":
                registered_nodes["actuator"] = client_ip
                log_print(f"Actuator '{node_id}' successfully mapped to IP: {client_ip}")
                
                # Persist the actuator into the node directory in the database
                persist_registration(node_id, "actuator", f"coap://[{client_ip}]:5683")
                
                # If there are already sensors registered we send their IP to the actuator
                asyncio.create_task(self.flush_pending_sensors())

            elif node_type in ["power", "irradiation"]:
                if node_id in active_observes:
                    log_print(f"[CoAP RECOVERY] Node {node_id} IP may be different. Reconnecting...")
                    # Kill the old task that was listening to the dead IP
                    active_observes[node_id]['task'].cancel()
                    
                # Create the new task pointing to the NEW IP, but save it using the ID
                task = asyncio.create_task(observe_sensor(client_ip, node_id, node_type))
                # Save both the task and the IP for diagnostics
                active_observes[node_id] = {'task': task, 'ip': client_ip}

                sensor_endpoint = f"coap://[{client_ip}]:5683"
                registered_nodes["sensors"][node_type] = sensor_endpoint
                log_print(f"Sensor '{node_id}' successfully mapped, endpoint: {sensor_endpoint}")
                
                # Persist the sensor into the node directory in the database
                persist_registration(node_id, node_type, sensor_endpoint)
                
                if registered_nodes["actuator"]:
                    asyncio.create_task(self.notify_actuator(node_type, sensor_endpoint))
            else:
                return aiocoap.Message(code=aiocoap.BAD_REQUEST, payload=b"Unknown node type")
        except json.JSONDecodeError:
            log_print("[ERROR] Invalid registration payload.")
            return aiocoap.Message(code=aiocoap.BAD_REQUEST, payload=b"Invalid JSON payload")

        return aiocoap.Message(code=aiocoap.CREATED, payload=b"Registration OK")
    
    # comunicating coap IP address to the actuator
    async def notify_actuator(self, sensor_type, sensor_endpoint):
        actuator_ip = registered_nodes["actuator"]
        if not actuator_ip:
            return

        actuator_url = f"coap://[{actuator_ip}]:5683/control/register_sensor"
        payload_bytes = json.dumps({"type": sensor_type, "endpoint": sensor_endpoint},
                                   separators=(",", ":")).encode('utf-8')
        log_print(f"Pushing sensor endpoint to actuator at {actuator_url}...")
        
        try:
            request = aiocoap.Message(code=aiocoap.POST, uri=actuator_url, payload=payload_bytes)
            response = await asyncio.wait_for(coap_client_context.request(request).response, timeout=10.0)
            log_print(f"Actuator response to sensor mapping: {response.code}")
        except asyncio.TimeoutError:
            log_print(f"[TIMEOUT] Actuator at [{actuator_ip}] did not respond in time.")
        except Exception as e:
            log_print(f"Failed to notify actuator at [{actuator_ip}]: {str(e)}")

    async def flush_pending_sensors(self):
        tasks = [
            self.notify_actuator(sensor_type, sensor_endpoint)
            for sensor_type, sensor_endpoint in registered_nodes["sensors"].items()
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

# MODULE 4: CLI HELPERS
def print_menu():
    print("\n===========================================")
    print("      SMART BATTERY MANAGER - CLI          ")
    print("===========================================")
    print(" commands:")
    print("  show     -> Show real-time telemetry logs")
    print("  table    -> Show real-time data table")
    print("  alarm    -> Simulate power collapse")
    print("  soc NUM  -> Force battery SOC (e.g. soc 45)")
    print("  exit     -> Shutdown the application")
    print("===========================================")

async def table_renderer():
    """Async task that continuously redraws the table until cancelled."""
    try:
        while display_mode == "table":
            os.system('cls' if os.name == 'nt' else 'clear')
            print("======================================================")
            print("             REAL-TIME SYSTEM DASHBOARD               ")
            print("======================================================")
            print(f" System State    : {latest_data['systemState']}")
            print(f" Last Prediction : {latest_data['lastPrediction']}")
            print("------------------------------------------------------")
            print(f" AC Power        : {latest_data['AC_Power']:.2f} W")
            print(f" Irradiation     : {latest_data['irradiation']:.4f} W/m2")
            print(f" Temperature     : {latest_data['temperature']:.2f} °C")
            print(f" Battery SOC     : {latest_data['battery']:.1f} %")
            print("======================================================")
            print("\n>>> Press ENTER to return to the main menu... <<<")
            await asyncio.sleep(1) # Refresh every second
    except asyncio.CancelledError:
        pass # Clean exit when the task is cancelled


# ===== [METRICS/CONGESTION - NEW] Latency measurement via ping6 =====
async def measure_latency():
    # Ping every registered sensor and write the average RTT (ms) to InfluxDB.
    # Runs ping6 in a thread so it never blocks the asyncio loop.
    # CoAP sensors: IP taken from active_observes.
    # MQTT sensors: IP taken from mqtt_node_ips (sent at registration).
    targets = {}
    for node_id, info in dict(active_observes).items():
        if info.get("ip"):
            targets[node_id] = info["ip"]
    for node_type, ip in dict(mqtt_node_ips).items():
        if ip:
            targets[node_type] = ip

    for node_id, ip in targets.items():
        try:
            # -c 3: three echo requests, -W 2: 2s timeout each
            proc = await asyncio.to_thread(
                subprocess.run,
                ["ping6", "-c", "3", "-W", "2", ip],
                capture_output=True, text=True
            )
            rtt = _parse_avg_rtt(proc.stdout)
            if rtt is not None:
                write_measurement(node_id, "latency_ms", rtt)
                log_print(f"[LATENCY] {node_id} ({ip}): {rtt:.1f} ms")
        except Exception as e:
            log_print(f"[LATENCY ERROR] {node_id}: {e}")


def _parse_avg_rtt(ping_output):
    # Extract the average RTT from the ping summary line
    # ("rtt min/avg/max/mdev = 1.2/3.4/5.6/0.7 ms").
    try:
        for line in ping_output.splitlines():
            if "min/avg/max" in line:
                stats = line.split("=")[1].strip().split()[0]
                return float(stats.split("/")[1])
    except Exception:
        pass
    return None


def compute_pdr_locked():
    # Compute per-sensor PDR from the seq window and the aggregate (mean) PDR.
    # MUST be called while holding metrics_lock.
    # Returns (per_sensor_pdr_dict, aggregate_pdr, all_sensors_alive).
    per_sensor = {}
    all_alive = True
    for k in SENSOR_KEYS:
        st = seq_state[k]
        if st["received"] == 0:
            # No packet at all from this sensor -> possible node failure,
            # not congestion. We flag it so congestion logic can skip.
            all_alive = False
            per_sensor[k] = 0.0
            continue
        if st["first"] is None or st["last"] is None:
            # Packets arrived but without seq: assume delivered.
            per_sensor[k] = 1.0
            continue
        expected = (st["last"] - st["first"]) + 1
        if expected <= 0:
            expected = st["received"]
        pdr = st["received"] / expected
        if pdr > 1.0:
            pdr = 1.0
        per_sensor[k] = pdr

    aggregate = sum(per_sensor.values()) / len(SENSOR_KEYS)
    return per_sensor, aggregate, all_alive


def reset_window_locked():
    # Reset all per-window counters. MUST be called while holding metrics_lock.
    global msg_count_window
    msg_count_window = 0
    for k in SENSOR_KEYS:
        seq_state[k] = {"first": None, "last": None, "received": 0}


async def send_congestion_signal(mqtt_client, congested):
    # Notify all nodes of the current network condition.
    # MQTT nodes: publish on the congestion topic.
    # CoAP nodes: POST to their control/congestion resource.
    flag = 1 if congested else 0
    # ===== [METRICS/CONGESTION - NEW] Compact JSON so the C strstr parser matches =====
    # separators=(",", ":") produces {"congested":1} with no space, which the
    # firmware looks for with strstr. A plain json.dumps would add a space
    # ({"congested": 1}) and cause a 4.00 Bad Request on the node.
    payload = json.dumps({"congested": flag}, separators=(",", ":"))
    # ===== [END METRICS/CONGESTION - NEW] =====

    # --- MQTT side ---
    try:
        mqtt_client.publish(CONGESTION_TOPIC, payload)
    except Exception as e:
        log_print(f"[CONGESTION MQTT ERROR] {e}")

    # --- CoAP side ---
    payload_bytes = payload.encode('utf-8')
    for node_id, info in dict(active_observes).items():
        ip = info.get("ip")
        if not ip:
            continue
        uri = f"coap://[{ip}]:5683/{CONGESTION_COAP_PATH}"
        try:
            request = aiocoap.Message(code=aiocoap.POST, uri=uri, payload=payload_bytes)
            await asyncio.wait_for(coap_client_context.request(request).response, timeout=5.0)
        except Exception as e:
            log_print(f"[CONGESTION CoAP ERROR] {node_id}: {e}")


async def metrics_monitor(mqtt_client):
    # Main periodic loop: every WINDOW_SECONDS it computes the metrics,
    # writes them to InfluxDB, decides if the network is congested, and
    # signals all nodes. Also triggers a latency measurement each window.
    global is_congested
    while True:
        await asyncio.sleep(WINDOW_SECONDS)

        with metrics_lock:
            per_sensor, aggregate, all_alive = compute_pdr_locked()
            total_msgs = msg_count_window
            reset_window_locked()

        # Persist the metrics for Grafana
        write_measurement("metrics", "msg_count", total_msgs)
        write_measurement("metrics", "pdr", aggregate)
        for k, v in per_sensor.items():
            write_measurement(k, "pdr_node", v)

        # Congestion decision:
        # PDR under threshold AND every sensor sent at least one packet
        # (if a sensor is silent it's a node failure, handled elsewhere).
        congested_now = (aggregate < PDR_THRESHOLD) and all_alive

        # Send a signal every window (congestion OR normal), so nodes always
        # know the current state and can recover when it clears.
        is_congested = congested_now
        write_measurement("metrics", "congestion", 1 if congested_now else 0)
        await send_congestion_signal(mqtt_client, congested_now)

        if congested_now:
            log_print(f"[CONGESTION] PDR={aggregate:.2f} < {PDR_THRESHOLD} -> signalling nodes")
        else:
            log_print(f"[METRICS] PDR={aggregate:.2f}, msgs={total_msgs}, normal")

        # Measure latency once per window (non-blocking)
        asyncio.create_task(measure_latency())
# ===== [END METRICS/CONGESTION - NEW] =====


# MAIN LOOP: MULTI-PROTOCOL ORCHESTRATION
async def main():
    print("==================================================")
    print("  Starting Smart Battery Manager - Cloud App      ")
    print("==================================================\n")
    global display_mode

    set_system_state("STATE 1: PV Regime") #Starting State
    
    # 1. MQTT Client Initialization
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)   
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    try:
        mqtt_client.connect("127.0.0.1", 1883, 60)
        # loop_start() handles MQTT in a separate background thread without blocking asyncio
        mqtt_client.loop_start() 
    except ConnectionRefusedError:
        print("[ERROR] Local MQTT broker not found. Ensure Mosquitto is running.")
    
    # creating coap_client Context
    global coap_client_context
    coap_client_context = await aiocoap.Context.create_client_context()

    # 2. CoAP Server Initialization
    root = resource.Site()
    # Map the registration resource to the URI 'coap://localhost/registration'
    root.add_resource(['registration'], RegistrationResource())
    
    # Start the server context
    await aiocoap.Context.create_server_context(root, bind=("::", 5683))
    log_print("[CoAP] Server listening on standard port 5683...\n")

    # ===== [METRICS/CONGESTION - NEW] Start the periodic metrics/congestion monitor =====
    asyncio.create_task(metrics_monitor(mqtt_client))
    # ===== [END METRICS/CONGESTION - NEW] =====

    ALARM_TOPIC = "network/alarms" 
    COMMANDS_TOPIC = "network/commands/battery"

    # 3. CLI Interactive Loop
    try:
        while True:
            if display_mode == "menu":
                print_menu()
                # Wait for user input asynchronously (does not block CoAP operations)
                command = await asyncio.to_thread(input, "Enter command: ")
                command = command.strip().lower()

                if command == "show":
                    display_mode = "show"
                    print("\n[INFO] Real-time logs ENABLED.")
                    print(">>> Press ENTER to stop and return to menu... <<<\n")
                    # Wait for the user to press Enter to stop
                    await asyncio.to_thread(input, "")
                    display_mode = "menu"

                elif command == "table":
                    display_mode = "table"
                    # Start the background table renderer
                    table_task = asyncio.create_task(table_renderer())
                    # Wait for user input to stop it
                    await asyncio.to_thread(input, "")
                    display_mode = "menu"
                    table_task.cancel() # Stop the background printing

                elif command == "alarm":
                    payload = json.dumps({"alarm": "Simulating Power collapse"},
                                         separators=(",", ":"))
                    mqtt_client.publish(ALARM_TOPIC, payload)
                    print(f"\n> [CLI] Sent MQTT ALARM to Actuator.\n")

                elif command.startswith("soc "):
                    try:
                        num_str = command.split(" ")[1]
                        num = float(num_str)
                        if 0.0 <= num <= 100.0:
                            # Building a JSON SenML valid for C parsing
                            payload = json.dumps({"bn": "cli", "n": "battery_soc", "v": num},
                                                 separators=(",", ":"))
                            mqtt_client.publish(COMMANDS_TOPIC, payload)
                            print(f"\n> [CLI] Sent Battery SOC override: {num}%\n")
                        else:
                            print("\n> [CLI ERROR] Invalid range. SOC must be between 0 and 100.\n")
                    except (IndexError, ValueError):
                        print("\n> [CLI ERROR] Invalid format. Use: soc NUM (e.g. soc 45.5)\n")
                
                elif command == "exit":
                    print("\n> [SYSTEM] Shutdown requested by user...")
                    break

                elif command != "":
                    print("\n> [CLI] Command not recognized.\n")

    except asyncio.CancelledError:
        print("\n[SYSTEM] Shutting down application...")
    
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        write_api.flush()
        write_api.close()
        client_db.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Application terminated by the user.")