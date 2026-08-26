"""
security_test_clients.py — MATRIX Phase 2 Test Lab sandboxed clients
═══════════════════════════════════════════════════════════════════════
Generates REAL test traffic against the gateway's secured channels —
not simulated UI events. Two things it can do:

  1. `--attack`   Repeatedly connects with a deliberately wrong token to
                   both :8765 and :8766, until the IDS in security_monitor.py
                   blocks the source IP. This is genuine bad-auth traffic
                   hitting the real gateway code path.

  2. `--legit`    Connects once with the correct token to prove the
                   legitimate path still works (contrast case).

Usage:
    python security_test_clients.py --attack <bad_token_ignored> [host]
    python security_test_clients.py --legit <real_token> [host]
"""
import asyncio
import json
import ssl
import sys
import websockets

HOST = "localhost"
OBS_PORT = 8766
CTRL_PORT = 8765


def _ssl_ctx():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


async def _one_bad_attempt(port, n):
    uri = f"wss://{HOST}:{port}"
    try:
        async with websockets.connect(uri, ssl=_ssl_ctx()) as ws:
            await ws.send(json.dumps({"type": "auth", "token": f"not-a-real-token-{n}"}))
            try:
                reply = json.loads(await asyncio.wait_for(ws.recv(), timeout=3))
                print(f"[attack] :{port} attempt #{n} -> {reply}")
            except asyncio.TimeoutError:
                print(f"[attack] :{port} attempt #{n} -> no reply (timeout)")
    except (websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.InvalidStatusCode,
            ConnectionRefusedError, OSError) as e:
        print(f"[attack] :{port} attempt #{n} -> connection rejected: {e}")


async def run_attack(port):
    print(f"[attack] Sending repeated bad-token auth attempts to :{port} "
          f"until the IDS blocks this IP (watch bridge.py's console for [IDS] lines)…")
    for n in range(1, 8):
        await _one_bad_attempt(port, n)
        await asyncio.sleep(0.3)
    print("[attack] Done. Check the Communication Website's Test Lab -> "
          "Firewall/IDS Status panel — this IP should now show BLOCKED.")


async def run_legit(token, port):
    uri = f"wss://{HOST}:{port}"
    try:
        async with websockets.connect(uri, ssl=_ssl_ctx()) as ws:
            await ws.send(json.dumps({"type": "auth", "token": token}))
            reply = json.loads(await ws.recv())
            print(f"[legit] :{port} auth_result: {reply}")
            if reply.get("ok"):
                print("[legit] ✔ legitimate client authenticated normally")
            else:
                print("[legit] ✗ unexpected rejection with a supposedly valid token")
    except websockets.exceptions.ConnectionClosedError as e:
        print(f"[legit] connection closed by gateway: {e}")
        print("[legit] (expected if this IP is still under an active IDS block — "
              "block is IP-based and runs its full cooldown regardless of token validity)")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "--attack"
    token_or_ignored = sys.argv[2] if len(sys.argv) > 2 else ""
    host = sys.argv[3] if len(sys.argv) > 3 else HOST
    HOST = host

    if mode == "--attack":
        asyncio.run(run_attack(OBS_PORT))
    elif mode == "--legit":
        asyncio.run(run_legit(token_or_ignored, OBS_PORT))
    else:
        print(__doc__)
