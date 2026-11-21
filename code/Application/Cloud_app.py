import asyncio
import json
import os
import paho.mqtt.client as mqtt
import aiocoap
import aiocoap.resource as resource

import time               
import threading  
import subprocess  

# InfluxDB Imports
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# INFLUXDB CONFIGURATION
INFLUX_URL = os.environ.get("INFLUX_URL", "http://localhost:8086")
# Credentials are read from environment variables (see README)
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN", "")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "myorg")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "sensors")

client_db = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client_db.write_api(write_options=SYNCHRONOUS)

# GLOBAL STATE FOR CLI
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

# The 4 sensors we track for the metrics 
SENSOR_KEYS = ["power", "irradiation", "battery", "temperature"]

# Map the SenML base-name ("bn") / node_type to a sensor key
# CoAP nodes are tracked by node_type, MQTT nodes by their "bn"
BN_TO_KEY = {
    "nRF52840-Battery": "battery",
    "nRF52840-Temperature": "temperature",
}

WINDOW_SECONDS = 60          # metrics window = normal sampling period
PDR_THRESHOLD = 0.7          # below this (and >=1 pkt per sensor) => congestion

# Congestion signalling topics/paths
CONGESTION_TOPIC = "network/congestion"     # MQTT nodes subscribe here
CONGESTION_COAP_PATH = "control/congestion" # CoAP nodes expose this resource

# Per-sensor sequence tracking, used to compute PDR from seq gaps
# For each sensor: first/last seq seen and how many packets actually arrived inside the current window
seq_state = {
    k: {"first": None, "last": None, "received": 0} for k in SENSOR_KEYS
}

# Total messages received in the current window 
msg_count_window = 0

# Current congestion status
is_congested = False

# One lock protects all the counters above, updated both from the MQTT thread and the asyncio loop
metrics_lock = threading.Lock()

# MQTT nodes that registered their own IPv6, so latency (ping6) can reach them
# Shape: {"battery": "fd00::...", "temperature": "fd00::..."}
mqtt_node_ips = {}

# Helper function to print logs only if 'show' mode is active
def log_print(msg):
    if display_mode == "show":
        print(msg)

# INFLUXDB WRITE HELPERS
def write_measurement(device, measurement_type, value, extra_tags=None):
    # Safely write a numeric point into the 'iot_sensors' measurement.
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
    # Write discrete events (alarm/state/command) to a separate 'iot_events' measurement
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
    # Persist the node directory into the 'registry' measurement of the database
    try:
        point = (Point("registry")
                 .tag("node_id", node_id)
                 .tag("node_type", node_type)
                 .field("endpoint", str(endpoint)))
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    except Exception as e:
        log_print(f"[INFLUX ERROR] registry write failed: {e}")

def set_system_state(state_label):
    # Update the state for the CLI table and persist it to InfluxDB
    latest_data["systemState"] = state_label
    code = STATE_CODES.get(state_label, -1)
    write_measurement("system", "system_state_code", code, extra_tags={"label": state_label})
    write_event("state", state_label, state=state_label)

def record_packet(sensor_key, seq):
    # Updates per-sensor seq window and global msg counter on every telemetry message
    global msg_count_window
    with metrics_lock:
        msg_count_window += 1
        if sensor_key not in seq_state:
            return
        if seq is None:
            # 'seq' may be None (payload without seq): message counted but unusable for PDR
            seq_state[sensor_key]["received"] += 1
            return
        st = seq_state[sensor_key]
        if st["first"] is None:
            st["first"] = seq
        st["last"] = seq
        st["received"] += 1

# MQTT MANAGEMENT
def on_connect(client, userdata, flags, rc, properties):
    log_print(f"[MQTT] Connected to local Mosquitto broker (Code: {rc})")
    # Subscribes to sensor topics and the registration topic on broker connect
    client.subscribe("network/#")
    client.subscribe("registration")

def on_message(client, userdata, msg):
    # Drops non-UTF8/corrupted or stale retained payloads
    try:
        payload = msg.payload.decode('utf-8')
    except (UnicodeDecodeError, AttributeError):
        log_print(f"[MQTT WARN] Dropped non-UTF8 payload on topic '{msg.topic}'")
        return

    # Parses payload as JSON (ignoring residual bytes) and handles registration messages
    try:
        # raw_decode reads the first valid JSON object, tolerating trailing bytes seen under stress conditions
        data, _ = json.JSONDecoder().raw_decode(payload)
        
        # MQTT nodes publish {"id": "...", "type": "...", "ip": "..."} at startup
        if msg.topic == "registration":
            node_id = data.get("id", "Unknown")
            node_type = data.get("type", "Unknown")
            node_ip = data.get("ip")   # IPv6 string, used for latency ping
            log_print(f"\n[MQTT REGISTRATION] {node_id} (type: {node_type}, ip: {node_ip})")
            if node_ip and node_type in SENSOR_KEYS:
                mqtt_node_ips[node_type] = node_ip
            endpoint = f"mqtt://{node_id}" if not node_ip else f"mqtt://[{node_ip}]"
            persist_registration(node_id, node_type, endpoint)
            return

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
                set_system_state(data.get("vs", "Unknown"))
            
            # Only real sensor telemetry counts (skip CLI overrides with bn="cli")
            sensor_key = BN_TO_KEY.get(device)
            if sensor_key is not None:
                record_packet(sensor_key, data.get("seq"))

            # DB Write
            write_measurement(device, measurement, value)
            
        # 2. Parsing Alarms & Info from the Smart Actuator
        elif "alarm" in data or "info" in data:
            msg_text = data.get("alarm", data.get("info", ""))
            log_print(f"\n[ACTUATOR MSG] {msg_text}\n")
            
            # Persist the actuator message as an event in the DB
            write_event("alarm", msg_text)
            
            # Infer system state from alarm text and persist it
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
            # Persist the command as an event in the DB
            write_event("command", data["cmd"])
            
    except json.JSONDecodeError:
        log_print(f"[MQTT ERROR] Received non-JSON payload: {payload}")
    except Exception as e:  
        log_print(f"[MQTT ERROR] on_message failed on topic '{msg.topic}': {e}")


# CoAP CLIENT 
# CoAP client Context reusable for all the output requests
coap_client_context = None
active_observes = {}  # Per-node observe tasks and their IP: {node_id: {'task': ..., 'ip': ...}} 

async def observe_sensor(client_ip, node_id, node_type):
    # Periodically takes telemetry data from a CoAP sensor
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
                
                # Count CoAP telemetry for PDR and message-count metrics 
                record_packet(node_type, data.get("seq"))

                # DB Write
                write_measurement(node_id, measurement_name, val)
            except Exception as e:
                log_print(f"[CoAP INFLUX ERROR] Failed DB insert: {e}")
            
    except asyncio.TimeoutError:  
        log_print(f"[CoAP TIMEOUT] Sensor {node_id} is not responding!")
    except Exception as e:
        log_print(f"[CoAP ERROR] Error on {node_id}: {e}")
    finally:
        # Safely unregister the observe task, comparing task identity so an old task cannot delete a recreated entry 
        current = asyncio.current_task()
        if (node_id in active_observes
                and active_observes[node_id].get('task') is current):
            del active_observes[node_id]


# CoAP SERVER (Registration)
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
            # The sensor send a JSON like: {"id": "nRF52840-Power", "type": "power"}
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
                # Cancel any previous observation task to avoid race conditions on reconnect
                old = active_observes.pop(node_id, None)
                if old is not None:
                    log_print(f"[CoAP RECOVERY] Node {node_id} reconnecting. Cancelling old observe...")
                    old['task'].cancel()
                    try:
                        await old['task']  # awaited so teardown completes before recreating the task 
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass

                # Start a new observation task
                task = asyncio.create_task(observe_sensor(client_ip, node_id, node_type))
                active_observes[node_id] = {'task': task, 'ip': client_ip}
                # Register sensor endpoint and update mapping
                sensor_endpoint = f"coap://[{client_ip}]:5683"
                registered_nodes["sensors"][node_type] = sensor_endpoint
                log_print(f"Sensor '{node_id}' successfully mapped, endpoint: {sensor_endpoint}")
                
                # Persist the sensor into the node directory in the DB
                persist_registration(node_id, node_type, sensor_endpoint)
                
                if registered_nodes["actuator"]:
                    asyncio.create_task(self.notify_actuator(node_type, sensor_endpoint))
            else:
                return aiocoap.Message(code=aiocoap.BAD_REQUEST, payload=b"Unknown node type")
        except json.JSONDecodeError:
            log_print("[ERROR] Invalid registration payload.")
            return aiocoap.Message(code=aiocoap.BAD_REQUEST, payload=b"Invalid JSON payload")

        return aiocoap.Message(code=aiocoap.CREATED, payload=b"Registration OK")
    
    async def notify_actuator(self, sensor_type, sensor_endpoint):
        # comunicates coap IP address to the actuator
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
        # Notify the actuator of all previously registered sensors in parallel
        tasks = [
            self.notify_actuator(sensor_type, sensor_endpoint)
            for sensor_type, sensor_endpoint in registered_nodes["sensors"].items()
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

# CLI INTERFACE
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
    # Async task that continuously redraws the real-time CLI table
    try:
        while display_mode == "table":
            os.system('cls' if os.name == 'nt' else 'clear')
            print("======================================================")
            print("             REAL-TIME SYSTEM DASHBOARD               ")
            print("======================================================")
            print(f" System State    : {latest_data['systemState']}")
            print("------------------------------------------------------")
            print(f" AC Power        : {latest_data['AC_Power']:.2f} W")
            print(f" Irradiation     : {latest_data['irradiation']:.4f} W/m2")
            print(f" Temperature     : {latest_data['temperature']:.2f} °C")
            print(f" Battery SOC     : {latest_data['battery']:.1f} %")
            print("======================================================")
            print("\n>>> Press ENTER to return to the main menu... <<<")
            await asyncio.sleep(1) # Refresh dashboard every second
    except asyncio.CancelledError:
        pass # Clean exit when the task is cancelled

async def measure_latency():
    # Measure average RTT latency (ms) for all registered sensors via ping6
    targets = {}
    for node_id, info in dict(active_observes).items():
        if info.get("ip"):
            targets[node_id] = info["ip"]
    for node_type, ip in dict(mqtt_node_ips).items():
        if ip:
            targets[node_type] = ip

    for node_id, ip in targets.items():
        try:
            proc = await asyncio.to_thread(
                subprocess.run,
                ["ping6", "-c", "3", "-W", "2", ip], # -c 3: three echo requests, -W 2: 2s timeout 
                capture_output=True, text=True
            )
            rtt = _parse_avg_rtt(proc.stdout)
            if rtt is not None:
                write_measurement(node_id, "latency_ms", rtt)
                log_print(f"[LATENCY] {node_id} ({ip}): {rtt:.1f} ms")
        except Exception as e:
            log_print(f"[LATENCY ERROR] {node_id}: {e}")


def _parse_avg_rtt(ping_output):
    # Extract the average RTT (ms) from the ping summary output
    try:
        for line in ping_output.splitlines():
            if "min/avg/max" in line:
                stats = line.split("=")[1].strip().split()[0]
                return float(stats.split("/")[1])
    except Exception:
        pass
    return None


def compute_pdr_locked():
    # Compute per-sensor PDR and mean aggregate PDR
    per_sensor = {}
    all_alive = True
    for k in SENSOR_KEYS:
        st = seq_state[k]
        if st["received"] == 0:
            # Silent sensor detected (node failure, not congestion)
            all_alive = False
            per_sensor[k] = 0.0
            continue
        if st["first"] is None or st["last"] is None:
            # Fallback for packets without sequence numbers
            per_sensor[k] = 1.0
            continue
        # Expected packets inferred from the seq gap
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
    # Reset all per-window counters
    global msg_count_window
    msg_count_window = 0
    for k in SENSOR_KEYS:
        seq_state[k] = {"first": None, "last": None, "received": 0}


async def send_congestion_signal(mqtt_client, congested):
    # Signal network congestion status to all registered MQTT and CoAP nodes
    flag = 1 if congested else 0

    payload = json.dumps({"congested": flag}, separators=(",", ":"))

    # MQTT
    try:
        mqtt_client.publish(CONGESTION_TOPIC, payload)
    except Exception as e:
        log_print(f"[CONGESTION MQTT ERROR] {e}")

    # CoAP
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
    # Periodic loop to compute network metrics, detect congestion, and signal nodes every WINDOW_SECONDS
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

        # Determine congestion (low PDR with all nodes actively sending)
        congested_now = (aggregate < PDR_THRESHOLD) and all_alive

        # Update congestion status and notify nodes every window, so they can also recover when it clears 
        is_congested = congested_now
        write_measurement("metrics", "congestion", 1 if congested_now else 0)
        # to disable adaptive mechanisms comment the next line
        await send_congestion_signal(mqtt_client, congested_now)

        if congested_now:
            log_print(f"[CONGESTION] PDR={aggregate:.2f} < {PDR_THRESHOLD} -> signalling nodes")
        else:
            log_print(f"[METRICS] PDR={aggregate:.2f}, msgs={total_msgs}, normal")

        # Measure latency once per window (non-blocking)
        asyncio.create_task(measure_latency())


# MAIN
async def main():
    print("==================================================")
    print("  Starting Smart Battery Manager - Cloud App      ")
    print("==================================================\n")
    global display_mode

    set_system_state("STATE 1: PV Regime") # Set initial State
    
    # 1. MQTT Client Initialization
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)   
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    
    try:
        mqtt_client.connect("127.0.0.1", 1883, 60)
        mqtt_client.loop_start() # Run MQTT network loop in a background thread
    except ConnectionRefusedError:
        print("[ERROR] Local MQTT broker not found. Ensure Mosquitto is running.")
    
    # 2. CoAP Server Initialization
    global coap_client_context
    coap_client_context = await aiocoap.Context.create_client_context()

    root = resource.Site()
    root.add_resource(['registration'], RegistrationResource())
    
    # Start the server context
    await aiocoap.Context.create_server_context(root, bind=("::", 5683))
    log_print("[CoAP] Server listening on standard port 5683...\n")

    # 3. Start the periodic metrics & congestion monitor
    asyncio.create_task(metrics_monitor(mqtt_client))

    # Topic definitions for CLI commands and alerts
    ALARM_TOPIC = "network/alarms" 
    COMMANDS_TOPIC = "network/commands/battery"

    # 4. CLI Interactive Loop
    try:
        while True:
            if display_mode == "menu":
                print_menu()
                # Non-blocking user input prompt
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
                    table_task = asyncio.create_task(table_renderer())
                    # Wait for the user to press Enter to stop
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
                            # Building a JSON SenML valid payload for battery SOC override
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
        # Clean up MQTT connection and flush InfluxDB buffer
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