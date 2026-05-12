#!/usr/bin/env python3
"""
CardHub Relay Server
Servidor WebSocket que hace de puente entre todos los jugadores.
Cada sala es independiente. El host crea la sala con un código,
los clientes se unen con ese mismo código.
"""

import asyncio
import json
import os
import random
import string
import websockets
from websockets.server import WebSocketServerProtocol

# sala_code -> {"host": ws, "clients": [ws, ...], "all": [ws, ...]}
rooms: dict = {}
# ws -> sala_code
ws_to_room: dict = {}


def generate_code() -> str:
    """Genera un código de sala de 6 letras mayúsculas."""
    while True:
        code = "".join(random.choices(string.ascii_uppercase, k=6))
        if code not in rooms:
            return code


async def send_json(ws: WebSocketServerProtocol, data: dict) -> None:
    try:
        await ws.send(json.dumps(data))
    except Exception:
        pass


async def broadcast_room(code: str, data: dict, exclude=None) -> None:
    """Envía un mensaje a todos los miembros de una sala."""
    if code not in rooms:
        return
    for ws in list(rooms[code]["all"]):
        if ws is exclude:
            continue
        await send_json(ws, data)


async def handle_client(ws: WebSocketServerProtocol) -> None:
    print(f"[+] Conexión nueva: {ws.remote_address}")
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await send_json(ws, {"type": "error", "msg": "JSON inválido"})
                continue

            t = msg.get("type", "")

            # ── CREATE ──────────────────────────────────────────────
            if t == "create":
                if ws in ws_to_room:
                    await send_json(ws, {"type": "error", "msg": "Ya estás en una sala"})
                    continue
                code = generate_code()
                rooms[code] = {"host": ws, "clients": [], "all": [ws]}
                ws_to_room[ws] = code
                await send_json(ws, {"type": "created", "code": code})
                print(f"[Sala {code}] Creada por {ws.remote_address}")

            # ── JOIN ─────────────────────────────────────────────────
            elif t == "join":
                code = str(msg.get("code", "")).upper().strip()
                if code not in rooms:
                    await send_json(ws, {"type": "error", "msg": "Sala no encontrada"})
                    continue
                if ws in ws_to_room:
                    await send_json(ws, {"type": "error", "msg": "Ya estás en una sala"})
                    continue
                rooms[code]["clients"].append(ws)
                rooms[code]["all"].append(ws)
                ws_to_room[ws] = code
                # Asignar ID único dentro de la sala (2, 3, 4...)
                client_id = len(rooms[code]["all"])  # host=1, resto 2+
                await send_json(ws, {"type": "joined", "code": code, "id": client_id})
                # Avisar al host de que llegó alguien
                await send_json(rooms[code]["host"], {"type": "peer_joined", "id": client_id})
                print(f"[Sala {code}] Cliente {client_id} unido ({ws.remote_address})")

            # ── RELAY ────────────────────────────────────────────────
            # Todos los mensajes de juego se envían como {"type":"relay","to":"all"|id,"data":{...}}
            elif t == "relay":
                code = ws_to_room.get(ws)
                if not code:
                    continue
                to = msg.get("to", "all")
                data = msg.get("data", {})
                wrapped = {"type": "relay", "data": data}

                if to == "all":
                    await broadcast_room(code, wrapped, exclude=ws)
                elif to == "host":
                    await send_json(rooms[code]["host"], wrapped)
                else:
                    # to == peer id numérico
                    target_id = int(to)
                    all_ws = rooms[code]["all"]
                    if 0 < target_id <= len(all_ws):
                        await send_json(all_ws[target_id - 1], wrapped)

            else:
                await send_json(ws, {"type": "error", "msg": f"Tipo desconocido: {t}"})

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        # Limpiar sala si alguien se desconecta
        code = ws_to_room.pop(ws, None)
        if code and code in rooms:
            rooms[code]["all"].remove(ws)
            if ws in rooms[code]["clients"]:
                rooms[code]["clients"].remove(ws)

            if ws is rooms[code]["host"]:
                # Host se fue: cerrar sala y echar a todos
                print(f"[Sala {code}] Host desconectado, cerrando sala")
                for member in list(rooms[code]["all"]):
                    await send_json(member, {"type": "host_left"})
                    ws_to_room.pop(member, None)
                del rooms[code]
            elif not rooms[code]["all"]:
                del rooms[code]
            else:
                # Avisar al host de que alguien se fue
                idx = len(rooms[code]["all"]) + 1  # aproximado
                await send_json(rooms[code]["host"], {"type": "peer_left"})

        print(f"[-] Desconectado: {ws.remote_address}")


async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"[CardHub Relay] Escuchando en puerto {port}")
    async with websockets.serve(handle_client, "0.0.0.0", port):
        await asyncio.Future()  # correr para siempre


if __name__ == "__main__":
    asyncio.run(main())
