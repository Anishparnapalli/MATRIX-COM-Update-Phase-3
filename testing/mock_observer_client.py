"""
mock_observer_client.py — mimics comm-dashboard/app.js: connects to the
observability gateway over WSS, performs the auth handshake, and prints
every enveloped packet it receives. Used to verify the gateway for real
without needing a browser.

Usage:
    python mock_observer_client.py <token> [host] [port]
    python mock_observer_client.py --bad-token     # test rejection path
"""
import asyncio
import json
import ssl
import sys
import websockets

HOST = "localhost"
PORT = 8766


async def run(token, expect_reject=False):
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE   # self-signed cert for local testing

    uri = f"wss://{HOST}:{PORT}"
    async with websockets.connect(uri, ssl=ssl_ctx) as ws:
        await ws.send(json.dumps({"type": "auth", "token": token}))
        first = json.loads(await ws.recv())
        print(f"[observer] auth_result: {first}")

        if expect_reject:
            if first.get("ok") is False:
                print("[observer] ✔ correctly REJECTED bad token")
            else:
                print("[observer] ✗ FAILED: bad token was accepted!")
            return

        if not first.get("ok"):
            print("[observer] ✗ FAILED: valid token was rejected!")
            return

        print("[observer] ✔ authenticated — listening for real packets (10s)…")
        count = 0
        try:
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=10)
                env = json.loads(msg)
                if env.get("type") == "event_replay":
                    events = env.get("events", [])
                    print(f"[observer] << event_replay: {len(events)} backlog event(s) received on connect")
                    for e in events:
                        print(f"[observer]    (replay) #{e.get('packet_id')} {e.get('direction')}/{e.get('category')}")
                    continue
                if env.get("type") == "test_lab_status":
                    print(f"[observer] << test_lab_status: fault={env.get('fault')} ids={env.get('ids')}")
                    continue
                count += 1
                print(f"[observer] #{env.get('packet_id')} "
                      f"{env.get('direction')} / {env.get('category')} "
                      f"({env.get('size_bytes')}B) real={env.get('real')} "
                      f"payload_type={env.get('payload',{}).get('type')} "
                      f"phase={env.get('payload',{}).get('phase')}")
        except asyncio.TimeoutError:
            print(f"[observer] done — received {count} live packets total")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--bad-token":
        asyncio.run(run("not-a-real-token", expect_reject=True))
    else:
        token = sys.argv[1] if len(sys.argv) > 1 else ""
        asyncio.run(run(token))
