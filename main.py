import asyncio
import json
import threading
import time
from typing import Any, Dict, List

import paho.mqtt.client as mqtt
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI(title="Smart Water Dispenser Dashboard")

# Mount folder static untuk logo dan aset lainnya
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Konfigurasi MQTT sesuai firmware ESP32
MQTT_BROKER = "192.168.1.66"
MQTT_PORT = 1883
MQTT_USER = "benderang"
MQTT_PASS = "benderangmqtt"
TOPIC_PUB = "/iotdispenser_input"
TOPIC_SUB = "/iotdispenser_output"

latest_state: Dict[str, Any] = {
    "suhuDingin": 0.0,
    "suhuPanas": 0.0,
    "autopump": 0,
    "ctrlPump": 0,
    "sw_heat_state": 0,
    "autocool": 0,
    "autohot": 0,
    "sw_door_state": 0,
    "float_sw_state": 0,
    "sw_cool_state": 0,
    "ctrlCompHot": 0,
    "ctrlCompCold": 0,
    "setpoint_cool": 5.0,
    "setpoint_heat": 80.0,
    "mqtt_connected": False,
    "last_update": None,
}

active_connections: List[WebSocket] = []
main_loop: asyncio.AbstractEventLoop = None


async def broadcast_state(data: Dict[str, Any]):
    disconnected = []
    for ws in active_connections:
        try:
            await ws.send_json(data)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        if ws in active_connections:
            active_connections.remove(ws)


mqtt_client = mqtt.Client(client_id="fastapi_dispenser_ui")
mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        latest_state["mqtt_connected"] = True
        client.subscribe(TOPIC_SUB)
        if main_loop:
            asyncio.run_coroutine_threadsafe(broadcast_state(latest_state), main_loop)
    else:
        latest_state["mqtt_connected"] = False


def on_disconnect(client, userdata, rc):
    latest_state["mqtt_connected"] = False
    if main_loop:
        asyncio.run_coroutine_threadsafe(broadcast_state(latest_state), main_loop)


def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str)
        latest_state.update(data)
        latest_state["mqtt_connected"] = True
        latest_state["last_update"] = time.strftime("%H:%M:%S")
        if main_loop:
            asyncio.run_coroutine_threadsafe(broadcast_state(latest_state), main_loop)
    except Exception as e:
        print(f"Error parsing MQTT payload: {e}")


mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message


def start_mqtt():
    while True:
        try:
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
            mqtt_client.loop_forever()
        except Exception as e:
            latest_state["mqtt_connected"] = False
            print(f"MQTT reconnecting in 5s... ({e})")
            time.sleep(5)


@app.on_event("startup")
async def startup_event():
    global main_loop
    main_loop = asyncio.get_running_loop()
    thread = threading.Thread(target=start_mqtt, daemon=True)
    thread.start()


@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    return templates.TemplateResponse(request, "index.html", {"request": request})


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    await websocket.send_json(latest_state)
    try:
        while True:
            raw_msg = await websocket.receive_text()
            cmd_data = json.loads(raw_msg)
            cmd = cmd_data.get("command")
            if cmd:
                mqtt_client.publish(TOPIC_PUB, cmd)
                if cmd.startswith("cold_set:"):
                    latest_state["setpoint_cool"] = float(cmd.split(":")[1])
                elif cmd.startswith("heat_set:"):
                    latest_state["setpoint_heat"] = float(cmd.split(":")[1])
                await broadcast_state(latest_state)
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)
