"""
mock_browser_client.py — stands in for a real browser tab with
dashboard/index.html open, for testing the control channel (:8765).

Phase 2 update: the control channel is now token-authenticated over
WSS, same shared secret as the observability channel. This script
performs that handshake first, exactly like dashboard/app.js does,
before sending any of the real message shapes connectWS()/sendToQNX()/
triggerEmergency() send.

Usage:
    python mock_browser_client.py <token> [host] [port]
"""
import asyncio
import json
import ssl
import sys
import websockets

TOKEN = sys.argv[1] if len(sys.argv) > 1 else ""
HOST  = sys.argv[2] if len(sys.argv) > 2 else "localhost"
PORT  = int(sys.argv[3]) if len(sys.argv) > 3 else 8765


async def main():
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE   # self-signed cert, local testing

    uri = f"wss://{HOST}:{PORT}"
    async with websockets.connect(uri, ssl=ssl_ctx) as ws:
        await ws.send(json.dumps({"type": "auth", "token": TOKEN}))
        auth_result = json.loads(await ws.recv())
        print(f"[mock-browser] auth_result: {auth_result}")
        if not auth_result.get("ok"):
            print("[mock-browser] ✗ auth rejected — stopping")
            return

        init = await ws.recv()
        print(f"[mock-browser] <- init: {init}")

        await ws.send(json.dumps({"type": "set_mode", "mode": "rtos"}))
        print("[mock-browser] -> set_mode rtos")

        seq = {
            "type": "rtos_sequence",
            "sequence": [[0, 25, -35, 20, 0, 0], [90, 90, 90, 90, 90, 50]],
            "pose_duration_ms": 1000,
            "total_poses": 2,
        }
        await ws.send(json.dumps(seq))
        print("[mock-browser] -> rtos_sequence (2 poses)")
        ack = await ws.recv()
        print(f"[mock-browser] <- {ack}")

        await ws.send(json.dumps({"type": "stop_cycle"}))
        print("[mock-browser] -> stop_cycle")
        ack2 = await ws.recv()
        print(f"[mock-browser] <- {ack2}")

        await ws.send(json.dumps({"type": "emergency"}))
        print("[mock-browser] -> emergency")

        try:
            for _ in range(3):
                msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                print(f"[mock-browser] <- {msg}")
        except asyncio.TimeoutError:
            pass

        print("[mock-browser] done")


if __name__ == "__main__":
    asyncio.run(main())
