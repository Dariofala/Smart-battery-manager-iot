import asyncio
import json
import paho.mqtt.client as mqtt
import aiocoap
import aiocoap.resource as resource

# InfluxDB Imports
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# INFLUXDB CONFIGURATION (REMEMBER TO CHANGE the Token)
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "bSFIYhrvYaYcQxs1PuexIvAkiEaxIHYLxGXaWN6lM_A2cGE7Pib4fa4Y5qx9QeTrV7HQHQY--6L8CbYKc3ppEQ==" # Dario
# INFLUX_TOKEN = "CibkW1KsPOOP7IHmaFHi4dteByZrsU1tgGBr3cpF9by58oulcTUjkWJ_2qGwBHyajJWq4_4BKajKis_KjTevuQ==" # Leo
INFLUX_ORG = "myorg"
INFLUX_BUCKET = "sensors"

client_db = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client_db.write_api(write_options=SYNCHRONOUS)

# MODULE 1: MQTT MANAGEMENT (Background Thread)
def on_connect(client, userdata, flags, rc, properties):
    print(f"[MQTT] Connected to local Mosquitto broker (Code: {rc})")
    # Subscribe to the topics defined in the C firmware
    client.subscribe("network/#")

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    topic = msg.topic
    
    try:
        data = json.loads(payload)
        
        # Parsing SenML Telemetry Data (Battery, Weather)
        if "bn" in data and "v" in data:
            device = data["bn"]
            measurement = data["n"]
            value = float(data["v"])
            unit = data.get("u", "")
            
            print(f"[MQTT TELEMETRY] Topic: {topic} | {device} -> {measurement}: {value} {unit}")
            
            # INFLUXDB WRITE (Uncomment when ready)
            point = Point("iot_sensors") \
                .tag("device", device) \
                .tag("measurement_type", measurement) \
                .field("value", value)
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            
        # Parsing Alarms from the Smart Actuator
        elif "alarms" in data:
            alarm_msg = data.get("alarm", "Unknown Alarm")
            print(f"\n[!!! MQTT ALARM !!!] Detected by Actuator: {alarm_msg}\n")
            
        # Parsing Commands (Charge/Discharge)
        elif "cmd" in data:
            print(f"[MQTT COMMAND] Action requested to the battery: {data['cmd']}")
            
    except json.JSONDecodeError:
        print(f"[MQTT ERROR] Received non-JSON payload on {topic}: {payload}")


# MODULE 2: CoAP CLIENT (Observe Setup)
# Dictionary to track tasks: the key is now the NODE_ID, no longer the IP!
active_observes = {}

async def observe_sensor(client_ip, node_id, node_type):
    protocol = await aiocoap.Context.create_client_context()
    resource_path = "telemetry/power" if node_type == "power" else "telemetry/irradiation"
    uri = f"coap://[{client_ip}]:5683/{resource_path}"
    
    request = aiocoap.Message(code=aiocoap.GET, uri=uri, observe=0)
    print(f"[CoAP OBSERVE] Starting to listen on {uri}...")
    
    try:
        requester = protocol.request(request)
        observation_iterator = requester.observation.__aiter__()
        
        # Listening loop with timeout
        while True:
            response = await asyncio.wait_for(observation_iterator.__anext__(), timeout=60.0)
            payload = response.payload.decode('utf-8')
            
            print(f"[CoAP TELEMETRY] {node_id} ({client_ip}): {payload}")

            # INFLUXDB WRITE FOR CoAP
            try:
                data = json.loads(payload)
                val = float(data.get("v", 0.0))
                measurement_name = data.get("n", "unknown")
                
                point = Point("iot_sensors") \
                    .tag("device", node_id) \
                    .tag("measurement_type", measurement_name) \
                    .field("value", val)
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            except Exception as e:
                print(f"[CoAP INFLUX ERROR] Failed DB insert for {node_id}: {e}")
            
    except asyncio.TimeoutError:
        print(f"[CoAP TIMEOUT] Sensor {node_id} ({client_ip}) is not responding. It went down!")
    except Exception as e:
        print(f"[CoAP ERROR] Error on {node_id}: {e}")
    finally:
        # Memory cleanup when it dies
        if node_id in active_observes and active_observes[node_id]['ip'] == client_ip:
            del active_observes[node_id]


# MODULE 3: CoAP SERVER (Registration)
class RegistrationResource(resource.Resource):
    async def render_post(self, request):
        payload = request.payload.decode('utf-8')
        client_ip = request.remote.sockaddr[0]

        # remove line 113 and add:
        # if hasattr(request.remote, 'sockaddr'):
        #     client_ip = request.remote.sockaddr[0]  # Per Ubuntu/Linux
        # elif hasattr(request.remote, 'address'):
        #     client_ip = request.remote.address[0]   # Per macOS
        # else:
        #     client_ip = "Unknown"
        
        try:
            data = json.loads(payload)
            # The C node must send a JSON like: {"id": "nRF52840-Power", "type": "power"}
            node_id = data.get("id", "Unknown")
            node_type = data.get("type", "Unknown")
            
            print(f"\n[CoAP REGISTRATION] Received from {node_id} (IP: {client_ip})")
            
            if node_type in ["power", "irradiation"]:
                
                # THE MAGIC IS HERE: If we already know this ID, it means it changed IP or rebooted!
                if node_id in active_observes:
                    old_ip = active_observes[node_id]['ip']
                    print(f"[CoAP RECOVERY] Node {node_id} is back online. Old IP: {old_ip} -> New IP: {client_ip}")
                    print(f"[CoAP RECOVERY] Tearing down the old connection and establishing the new one.")
                    
                    # Kill the old task that was listening to the dead IP
                    active_observes[node_id]['task'].cancel()
                
                # Create the new task pointing to the NEW IP, but save it using the ID
                task = asyncio.create_task(observe_sensor(client_ip, node_id, node_type))
                
                # Save both the task and the IP for diagnostics
                active_observes[node_id] = {'task': task, 'ip': client_ip}
                
            else:
                print(f"[WARN] Node type ignored: {node_type}")
                
        except json.JSONDecodeError:
            print("[ERROR] Invalid payload.")

        return aiocoap.Message(code=aiocoap.CREATED, payload=b"Registration OK")


# MAIN LOOP: MULTI-PROTOCOL ORCHESTRATION
async def main():
    print("==================================================")
    print("  Starting Smart Battery Manager - Cloud App      ")
    print("==================================================\n")


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


    # 2. CoAP Server Initialization
    root = resource.Site()
    # Map the registration resource to the URI 'coap://localhost/registration'
    root.add_resource(['registration'], RegistrationResource())
    
    # Start the server context
    # await aiocoap.Context.create_server_context(root, bind=("127.0.0.1", 5683)) # PER MAC
    # await aiocoap.Context.create_server_context(root, bind=("fd00::1", 5683)) # PER MAC 2

    await aiocoap.Context.create_server_context(root, bind=("::", 5683))
    print("[CoAP] Server listening on standard port 5683...\n")


    # 3. CLI - Command Line Interface (User Logic)
    print("\n--- COMMAND LINE INTERFACE (CLI) ACTIVE ---")
    print("Type one of the following commands and press Enter:")
    print(" - 'alarm' : Simulates a power collapse")
    # print(" - 'stress' : Simulates a stress test")
    print(" - 'exit'  : Shuts down the system")
    print("-------------------------------------------\n")

    # Update this topic if your C actuator subscribes to a different one
    ALARM_TOPIC = "network/alarms" 

    try:
        while True:
            # Wait for user input asynchronously (does not block CoAP operations)
            command = await asyncio.to_thread(input, "")
            command = command.strip().lower()

            if command == "alarm":
                payload = json.dumps({"alarm": "Simulating Power collapse"})
                mqtt_client.publish(ALARM_TOPIC, payload)
                print(f"> [CLI] Sent MQTT ALARM: {payload} on topic {ALARM_TOPIC}\n")

           #  if command == "stress":
           #      ...
           #      print(f"> [CLI] Sent MQTT STRESS: {payload} on topic {ACTUATOR_TOPIC}\n")

            elif command == "exit":
                print("> [SYSTEM] Shutdown requested by user...")
                break # Exits the infinite loop and triggers the finally block

            elif command != "":
                print("> [CLI] Command not recognized. Use: alarm, exit.\n")

    except asyncio.CancelledError:
        print("\n[SYSTEM] Shutting down application...")
    
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        client_db.close()

if __name__ == "__main__":
    # Start the asyncio Event Loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Application terminated by the user.")