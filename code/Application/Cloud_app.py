import asyncio
import json
import os
import paho.mqtt.client as mqtt
import aiocoap
import aiocoap.resource as resource

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

# Helper function to print logs only if 'show' mode is active
def log_print(msg):
    if display_mode == "show":
        print(msg)

# MODULE 1: MQTT MANAGEMENT (Background Thread)
def on_connect(client, userdata, flags, rc, properties):
    log_print(f"[MQTT] Connected to local Mosquitto broker (Code: {rc})")
    # Subscribe to the topics defined in the C firmware
    client.subscribe("network/#")

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    topic = msg.topic
    
    try:
        data = json.loads(payload)
        
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
            elif measurement == "system_state":
                latest_data["systemState"] = data.get("vs", "Unknown") # If actuator sends state as string
            elif measurement == "prediction":
                latest_data["lastPrediction"] = value
            
            # DB Write
            point = Point("iot_sensors").tag("device", device).tag("measurement_type", measurement).field("value", value)
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            
        # 2. Parsing Alarms & Info from the Smart Actuator
        elif "alarm" in data or "info" in data:
            msg_text = data.get("alarm", data.get("info", ""))
            log_print(f"\n[ACTUATOR MSG] {msg_text}\n")
            
            # Try to infer system state from alarm text for the table if not sent explicitly
            if "Battery Mode" in msg_text or "Collapse" in msg_text:
                latest_data["systemState"] = "STATE 2: Battery Regime"
            elif "National Grid" in msg_text:
                latest_data["systemState"] = "STATE 3: Grid Regime"
            elif "PV Mode" in msg_text:
                latest_data["systemState"] = "STATE 1: PV Regime"
            elif "SAFE MODE" in msg_text:
                inferred_state = "EMERGENCY: Safe Mode Active"
                
        # 3. Parsing Commands
        elif "cmd" in data:
            log_print(f"[MQTT COMMAND] Action requested to the battery: {data['cmd']}")
            
    except json.JSONDecodeError:
        log_print(f"[MQTT ERROR] Received non-JSON payload: {payload}")

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
                
                # DB Write
                point = Point("iot_sensors").tag("device", node_id).tag("measurement_type", measurement_name).field("value", val)
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
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
        payload_bytes = json.dumps({"type": sensor_type, "endpoint": sensor_endpoint}).encode('utf-8')

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
        for sensor_type, sensor_endpoint in registered_nodes["sensors"].items():
            await self.notify_actuator(sensor_type, sensor_endpoint)

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


# MAIN LOOP: MULTI-PROTOCOL ORCHESTRATION
async def main():
    print("==================================================")
    print("  Starting Smart Battery Manager - Cloud App      ")
    print("==================================================\n")
    global display_mode
    
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
                    payload = json.dumps({"alarm": "Simulating Power collapse"})
                    mqtt_client.publish(ALARM_TOPIC, payload)
                    print(f"\n> [CLI] Sent MQTT ALARM to Actuator.\n")

                elif command.startswith("soc "):
                    try:
                        num_str = command.split(" ")[1]
                        num = float(num_str)
                        if 0.0 <= num <= 100.0:
                            # Building a JSON SenML valid for C parsing
                            payload = json.dumps({"bn": "cli", "n": "battery_soc", "v": num})
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
        client_db.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Application terminated by the user.")