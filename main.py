
import json

from mqtt_as import MQTTClient
from mqtt_local import config
import uasyncio as asyncio
import dht, machine
d = dht.DHT11(machine.Pin(15))

#pin interno
#led = machine.Pin(25)
led = machine.Pin("LED", machine.Pin.OUT)
pin_rele=machine.Pin(16,machine.Pin.OUT)


FILENAME = "CONFIG.json"

async def guardar():
    estado_rele = pin_rele.value()
    data = {"setpoint": setpoint, "periodo": periodo, "modo": modo,"rele":estado_rele}
    with open(FILENAME, "w") as f:
        json.dump(data, f)
    print('cambio guardado')

async def cargar():
    global setpoint, periodo, modo
    try:
        with open(FILENAME, "r") as f:
            contenido = f.read()
            if not contenido: # Si el archivo está vacío
                raise ValueError("Archivo vacío")
            data = json.loads(contenido)
            setpoint = data.get("setpoint", 20)
            periodo = data.get("periodo", 20)
            modo = data.get("modo", "manual")
            estado_guardado = data.get("rele", 1) 
            pin_rele.value(estado_guardado)
            print("Parámetros cargados")
    except (OSError, ValueError) as e:
        print(f"Error al cargar ({e}), usando valores por defecto...")
        setpoint=25
        periodo=20
        modo='manual'
        pin_rele.value(1)
        await guardar() # Crea un archivo válido con los valores actuales

async def wifi_han(state):
    print('Wifi is ', 'up' if state else 'down')
    await asyncio.sleep(1)

# If you connect with clean_session True, must re-subscribe (MQTT spec 3.1.2.4)
async def conn_han(client):
        await client.up.wait()
        client.up.clear()
        await client.subscribe('los_masones/setpoint', 1)
        await client.subscribe('los_masones/periodo', 1)
        await client.subscribe('los_masones/destello', 1)
        await client.subscribe('los_masones/modo', 1)
        await client.subscribe('los_masones/rele', 1)
        print("suscripto")

async def rele():
    while True:
        if modo.lower() == 'auto':
            temp = d.temperature()
            if setpoint <= temp:
                pin_rele.value(0)
            else:
                pin_rele.value(1)
        await asyncio.sleep_ms(300)

async def monitor(client):
    global setpoint, periodo, modo
    async for topic, msg, _ in client.queue:
        t = topic.decode()
        m = msg.decode()
        print(f"Mensaje en cola: {t}") 

        valor = m  
        try:
            dict_msg = json.loads(m)
            if isinstance(dict_msg, dict):
                valor = str(dict_msg.get("msg", m))
            else:
                valor = str(dict_msg)
        except Exception:
            pass 

        if t == 'los_masones/destello':
            if "on" in valor.lower():
                print("Destellando...")
                for x in range(20):
                    led.toggle()  
                    await asyncio.sleep_ms(200) 

        elif t == 'los_masones/setpoint':
            try:
                setpoint = abs(int(valor))
                print("Nuevo Setpoint:", setpoint)
                await guardar()
            except:
                print("ingrese un numero valido")
            
            await asyncio.sleep_ms(1)
        elif t == 'los_masones/rele' and modo.lower() == "manual":
            if "on" in valor.lower():
                pin_rele.value(0)
                print("Relé ACTIVADO")
            elif "off" in valor.lower():
                pin_rele.value(1)
                print("Relé DESACTIVADO")
            await guardar()
            await asyncio.sleep_ms(1)

        elif t == 'los_masones/modo':
            if valor.lower() in ["auto", "manual"]:
                modo = valor
                print(f"Modo cambiado a: {modo}")
            await guardar()
            await asyncio.sleep_ms(1)

        elif t == 'los_masones/periodo':
            try:
                periodo = abs(int(valor))
                print("Nuevo periodo:", periodo)
                await guardar()
            except:
                print("Periodo debe ser un número")
            
              

async def main(client):
    await cargar()


    await client.connect()
    asyncio.create_task(conn_han(client))
    await asyncio.sleep(2)  # Give broker time
    # LANZAR EL MONITOR 
    asyncio.create_task(monitor(client))
    asyncio.create_task(rele())

    while True:
        try:

            d.measure()
            try:
              #  inicio = utime.ticks_us() # Marca de tiempo inicial
                temperatura=d.temperature()
                humedad=d.humidity()
                datos = {
                    "temperatura": temperatura,
                    "humedad": humedad,
                    "Periodo": periodo,
                    "Setpoint": setpoint,
                    "Modo": modo
                }
                
                payload = json.dumps(datos)
                await client.publish('los_masones',payload, qos=1)
                
            except OSError as e:
                print("Existe un error")
        except OSError as e:
            print("sin sensor")
        await asyncio.sleep(periodo)  

# Define configuration
config['queue_len'] = 10
config['connect_coro'] = conn_han
config['wifi_coro'] = wifi_han
config['port'] = 8883                   
config['ssl'] = True                         


# Set up client
MQTTClient.DEBUG = True  # Optional
client = MQTTClient(config)
try:
    asyncio.run(main(client))
finally:
    client.close()
    asyncio.new_event_loop()
