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


# Dizionario per tracciare i task: la chiave ora è il NODE_ID, non più l'IP!
active_observes = {}

async def observe_sensor(client_ip, node_id, node_type):
    # ... setup uri ...
    try:
        # ... ciclo di ascolto con timeout ...
        while True:
            response = await asyncio.wait_for(observation_iterator.__anext__(), timeout=30.0)
            payload = response.payload.decode('utf-8')
            
            print(f"[TELEMETRIA CoAP] {node_id} ({client_ip}): {payload}")
            
            # Quando sbloccherai INFLUXDB, il TAG deve essere il node_id, NON l'IP!
            # point = Point("iot_sensors").tag("device", node_id).field("value", float(json.loads(payload)["v"]))
            # write_api.write(...)
            
    except asyncio.TimeoutError:
        print(f"[CoAP TIMEOUT] Il sensore {node_id} ({client_ip}) non risponde. È caduto!")
    except Exception as e:
        print(f"[CoAP ERROR] Errore su {node_id}: {e}")
    finally:
        # Pulizia memoria quando muore
        if node_id in active_observes and active_observes[node_id]['ip'] == client_ip:
            del active_observes[node_id]

class RegistrationResource(resource.Resource):
    async def render_post(self, request):
        payload = request.payload.decode('utf-8')
        client_ip = request.remote.sockaddr[0]
        
        try:
            data = json.loads(payload)
            # Il nodo C deve inviare un JSON tipo: {"id": "nRF52840-Power", "type": "potenza"}
            node_id = data.get("id", "Sconosciuto")
            node_type = data.get("type", "Sconosciuto")
            
            print(f"\n[CoAP REGISTRAZIONE] Ricevuta da {node_id} (IP: {client_ip})")
            
            if node_type in ["potenza", "irraggiamento"]:
                
                # LA MAGIA È QUI: Se conosciamo già questo ID, significa che ha cambiato IP o si è riavviato!
                if node_id in active_observes:
                    vecchio_ip = active_observes[node_id]['ip']
                    print(f"[CoAP RECOVERY] Il nodo {node_id} è tornato online. Vecchio IP: {vecchio_ip} -> Nuovo IP: {client_ip}")
                    print(f"[CoAP RECOVERY] Abbatto la vecchia connessione e stabilisco la nuova.")
                    
                    # Killiamo il task vecchio che ascoltava l'IP morto
                    active_observes[node_id]['task'].cancel()
                
                # Creiamo il nuovo task puntato sul NUOVO IP, ma lo salviamo usando l'ID
                task = asyncio.create_task(observe_sensor(client_ip, node_id, node_type))
                
                # Salviamo sia il task che l'IP per diagnostica
                active_observes[node_id] = {'task': task, 'ip': client_ip}
                
            else:
                print(f"[WARN] Tipo di nodo ignorato: {node_type}")
                
        except json.JSONDecodeError:
            print("[ERROR] Payload non valido.")

        return aiocoap.Message(code=aiocoap.CREATED, payload=b"Registrazione OK")



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