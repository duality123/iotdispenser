
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
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *

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

# ==========================================
# KONFIGURASI LARK BASE (Ubah sesuai milik Anda)
# ==========================================
LARK_APP_ID = "cli_aa49a51c9138de13"       # Masukkan App ID Lark Anda
LARK_APP_SECRET = "yBbkSeKrGp4vXmeFg69KeWZN4S51NjHX"     # Masukkan App Secret Lark Anda
LARK_APP_TOKEN = "BnzvbprAxa19nzsmn0ij1NnFpDg"      # Masukkan App Token dari URL Lark Base (app...)
LARK_TABLE_ID = "tblWYJzmrixJnVTa"    # Masukkan Table ID dari Lark Base (tbl...)


# Inisialisasi Lark Client
lark_client = lark.Client.builder() \
    .app_id(LARK_APP_ID) \
    .app_secret(LARK_APP_SECRET) \
    .log_level(lark.LogLevel.WARNING) \
    .build()

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


# ==========================================
# FUNGSI MANAJEMEN LARK BASE (INSERT & LIMIT 30)
# ==========================================
def sync_data_to_lark(data: Dict[str, Any], event_desc: str = "MQTT Update"):
    try:
        current_time_ms = int(time.time() * 1000) # Format timestamp milidetik untuk field tipe Datetime di Lark Base
        
        # 1. Buat record baru sesuai kolom pada gambar: Time, Event, suhu_dingin_set, suhu_dingin, suhu_panas_set, suhu_panas, dingin, panas, pump, float
        fields = {
            "Time": str(current_time_ms),
            "Event": str(event_desc),
            "suhu_dingin_set": str(data.get("setpoint_cool", 0.0)),
            "suhu_dingin": str(data.get("suhuDingin", 0.0)),
            "suhu_panas_set": str(data.get("setpoint_heat", 0.0)),
            "suhu_panas": str(data.get("suhuPanas", 0.0)),
            "dingin": str(data.get("sw_cool_state", 0)),
            "panas": str(data.get("sw_heat_state", 0)),
            "pump": str(data.get("ctrlPump", 0)),
            "float": str(data.get("float_sw_state", 0))
        }

        request_body = CreateAppTableRecordRequest.builder() \
            .app_token(LARK_APP_TOKEN) \
            .table_id(LARK_TABLE_ID) \
            .request_body(AppTableRecord.builder().fields(fields).build()) \
            .build()

        response = lark_client.bitable.v1.app_table_record.create(request_body)
        if not response.success():
            print(f"Gagal insert ke Lark Base: {response.msg}")
            return

        # 2. Cek total baris dan hapus jika melebihi 30 data (maksimal 30 data)
        list_req = ListAppTableRecordRequest.builder() \
            .app_token(LARK_APP_TOKEN) \
            .table_id(LARK_TABLE_ID) \
            .page_size(100) \
            .build()
        
        list_resp = lark_client.bitable.v1.app_table_record.list(list_req)
        if list_resp.success() and list_resp.data and list_resp.data.items:
            records = list_resp.data.items
            # Jika jumlah record sudah >= 30, ambil record terlama (di indeks awal) untuk dihapus
            if len(records) >= 30:
                # Urutkan berdasarkan waktu atau ambil langsung dari record yang masuk paling awal
                excess_count = len(records) - 29 # Menyisakan 29 agar ditambah 1 baru pas jadi 30, atau hapus kelebihan di atas 30
                record_ids_to_delete = [rec.record_id for rec in records[:len(records) - 30 + 1]]
                
                if record_ids_to_delete:
                    del_body = BatchDeleteAppTableRecordRequestor.builder() \
                        .records(record_ids_to_delete) \
                        .build()
                    
                    del_req = BatchDeleteAppTableRecordRequest.builder() \
                        .app_token(LARK_APP_TOKEN) \
                        .table_id(LARK_TABLE_ID) \
                        .request_body(del_body) \
                        .build()
                    
                    lark_client.bitable.v1.app_table_record.batch_delete(del_req)

    except Exception as e:
        print(f"Error sinkronisasi ke Lark Base: {e}")


mqtt_client = mqtt.Client(client_id="fastapi_dispenser_123")
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
        
        # Panggil fungsi sinkronisasi ke Lark Base secara asynchronous di thread terpisah agar MQTT callback tidak blocking
        threading.Thread(target=sync_data_to_lark, args=(latest_state, "MQTT Telemetry"), daemon=True).start()

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
                
                # Catat aksi perintah web juga ke Lark Base
                threading.Thread(target=sync_data_to_lark, args=(latest_state, f"Command: {cmd}"), daemon=True).start()
                
                await broadcast_state(latest_state)
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)