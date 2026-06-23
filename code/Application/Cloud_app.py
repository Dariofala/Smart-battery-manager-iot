import asyncio
import json
import paho.mqtt.client as mqtt
import aiocoap
import aiocoap.resource as resource

# InfluxDB Imports (Uncomment after installing influxdb-client)
# from influxdb_client import InfluxDBClient, Point
# from influxdb_client.client.write_api import SYNCHRONOUS

# INFLUXDB CONFIGURATION (To be filled)
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "INSERT_YOUR_TOKEN_HERE"
INFLUX_ORG = "unipi"
INFLUX_BUCKET = "smart_battery_db"

# client_db = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
# write_api = client_db.write_api(write_options=SYNCHRONOUS)

# MODULE 1: MQTT MANAGEMENT (Background Thread)
def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] Connected to local Mosquitto broker (Code: {rc})")
    # Subscribe to the topics defined in the C firmware
    client.subscribe("rete/#")
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
            # point = Point("iot_sensors") \
            #     .tag("device", device) \
            #     .tag("measurement_type", measurement) \
            #     .field("value", value)
            # write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            
        # Parsing Alarms from the Smart Actuator
        elif "allarme" in data or "alarm" in data:
            alarm_msg = data.get("alarm", data.get("allarme", "Unknown Alarm"))
            print(f"\n[!!! MQTT ALARM !!!] Detected by Actuator: {alarm_msg}\n")
            
        # Parsing Commands (Charge/Discharge)
        elif "cmd" in data:
            print(f"[MQTT COMMAND] Action requested to the battery: {data['cmd']}")
            
    except json.JSONDecodeError:
        print(f"[MQTT ERROR] Received non-JSON payload on {topic}: {payload}")


# MODULE 2: CoAP SERVER (Asynchronous Event Loop)
class RegistrationResource(resource.Resource):
    """
    CoAP Resource listening for POST requests to handle the 
    registration of peripheral nodes (Power and Irradiation).
    """
    async def render_post(self, request):
        payload = request.payload.decode('utf-8')
        client_ip = request.remote.sockaddr[0]
        
        print(f"[CoAP REGISTRATION] New node online at IPv6 address: {client_ip}")
        print(f"[CoAP REGISTRATION] Payload: {payload}")
        
        # Reply to the Contiki-NG node with a success code (2.01 Created)
        response_payload = b"Registration successfully completed"
        return aiocoap.Message(code=aiocoap.CREATED, payload=response_payload)



# MAIN LOOP: MULTI-PROTOCOL ORCHESTRATION
async def main():
    print("==================================================")
    print("  Starting Smart Battery Manager - Cloud App      ")
    print("==================================================\n")

    # 1. MQTT Client Initialization
    mqtt_client = mqtt.Client()
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
    await aiocoap.Context.create_server_context(root)
    print("[CoAP] Server listening on standard port 5683...\n")

    # 3. Application Keep-Alive
    # An asynchronous infinite loop that keeps the CoAP server active
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        print("\n[SYSTEM] Shutting down application...")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

if __name__ == "__main__":
    # Start the asyncio Event Loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[SYSTEM] Application terminated by the user.")