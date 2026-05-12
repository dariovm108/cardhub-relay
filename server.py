#!/usr/bin/env python3
"""
CardHub Relay v5
Protocolo:
  Cliente manda {"ctrl":"create"} -> recibe {"ctrl":"created","code":"ABCDEF"}
  Cliente manda {"ctrl":"join","code":"ABCDEF"} -> recibe {"ctrl":"joined","peer_id":N}
  Luego {"ctrl":"peer_connected","peer_id":N} al host cuando alguien entra
  Luego {"ctrl":"peer_disconnected","peer_id":N} al host cuando alguien sale
  {"ctrl":"host_disconnected"} a los clientes cuando el host se va

  Mensajes de juego (sin campo "ctrl"):
  {"method":"...","args":[...],"to":-1|1|N,"sender":ID}
  to=-1 -> broadcast a todos menos el remitente
  to=1  -> al host
  to=N  -> al peer N
  El relay añade "sender" antes de reenviar.
"""

import asyncio
import json
import os
import random
import string
import websockets

rooms = {}   # code -> {"host":ws, "clients":{peer_id:ws}, "next_id":int}
conns = {}   # ws -> {"code":str, "peer_id":int}


def make_code():
    while True:
        c = "".join(random.choices(string.ascii_uppercase, k=6))
        if c not in rooms:
            return c


async def tx(ws, data):
    try:
        await ws.send(json.dumps(data) if isinstance(data, dict) else data)
    except Exception:
        pass


async def handler(ws):
    print(f"[+] {ws.remote_address}")
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                await tx(ws, {"ctrl": "error", "msg": "JSON inválido"})
                continue

            ctrl = msg.get("ctrl", "")

            # ── Mensajes de control ─────────────────────────────────────────
            if ctrl == "create":
                if ws in conns:
                    await tx(ws, {"ctrl": "error", "msg": "Ya estás en una sala"})
                    continue
                code = make_code()
                rooms[code] = {"host": ws, "clients": {}, "next_id": 2}
                conns[ws] = {"code": code, "peer_id": 1}
                await tx(ws, {"ctrl": "created", "code": code})
                print(f"[{code}] Sala creada")

            elif ctrl == "join":
                code = str(msg.get("code", "")).upper().strip()
                if code not in rooms:
                    await tx(ws, {"ctrl": "error", "msg": f"Sala '{code}' no encontrada"})
                    continue
                if ws in conns:
                    await tx(ws, {"ctrl": "error", "msg": "Ya estás en una sala"})
                    continue
                room = rooms[code]
                pid = room["next_id"]
                room["next_id"] += 1
                room["clients"][pid] = ws
                conns[ws] = {"code": code, "peer_id": pid}
                await tx(ws, {"ctrl": "joined", "peer_id": pid, "code": code})
                await tx(room["host"], {"ctrl": "peer_connected", "peer_id": pid})
                print(f"[{code}] Cliente {pid} unido")

            # ── Mensajes de juego ───────────────────────────────────────────
            elif "method" in msg:
                info = conns.get(ws)
                if not info:
                    continue
                code = info["code"]
                room = rooms.get(code)
                if not room:
                    continue
                sender_id = info["peer_id"]
                to = msg.get("to", -1)
                msg["sender"] = sender_id  # añadir remitente

                if to == -1:
                    # Broadcast a todos menos el remitente
                    targets = []
                    if room["host"] is not ws:
                        targets.append(room["host"])
                    for cws in room["clients"].values():
                        if cws is not ws:
                            targets.append(cws)
                elif to == 1:
                    targets = [room["host"]] if room["host"] is not ws else []
                else:
                    cws = room["clients"].get(to)
                    targets = [cws] if cws and cws is not ws else []

                for t in targets:
                    await tx(t, msg)

            else:
                await tx(ws, {"ctrl": "error", "msg": f"Mensaje desconocido"})

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        info = conns.pop(ws, None)
        if not info:
            print(f"[-] {ws.remote_address} (sin sala)")
            return
        code = info["code"]
        pid = info["peer_id"]
        room = rooms.get(code)
        if not room:
            return
        if room["host"] is ws:
            print(f"[{code}] Host desconectado")
            for cws in list(room["clients"].values()):
                await tx(cws, {"ctrl": "host_disconnected"})
                conns.pop(cws, None)
            del rooms[code]
        else:
            room["clients"].pop(pid, None)
            await tx(room["host"], {"ctrl": "peer_disconnected", "peer_id": pid})
            print(f"[{code}] Cliente {pid} desconectado")
        print(f"[-] {ws.remote_address}")


async def main():
    port = int(os.environ.get("PORT", 8765))
    print(f"[CardHub Relay v5] Puerto {port}")
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
