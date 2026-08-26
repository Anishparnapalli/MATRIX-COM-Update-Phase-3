"""
bridge.py  —  M.A.T.R.I.X. Communication Bridge
═══════════════════════════════════════════════════════════════════════
Phase 2. Full RTOS / NON-RTOS simulation protocol, secured control
channel, observability gateway, fault injection Test Lab, and IDS.

ARCHITECTURE
────────────
  Browser (WSS :8765, token-authenticated)
       ↕
  bridge.py  ←— runs on Windows host
       ↕
  QNX Robotic_ARM  (TCP :12345, unchanged since Phase 1 — never touched
                     by Test Lab fault injection or auth; QNX has no
                     concept of the browser-side security layer at all)
       ↕
  Communication Website (WSS :8766, token-authenticated, observability
                          + Test Lab command channel in Phase 2)

WHAT THIS BRIDGE DOES
──────────────────────
  1. TCP server on port 12345  — QNX connects here (protocol unchanged
     from Phase 1: angle CSV, EMERGENCY_STOP, WATCHDOG_HIT, POSE_DONE,
     CYCLE_DONE, SEQ_COMPLETE)

  2. WSS server on port 8765  — Browser dashboard connects here.
     Phase 2: now requires the same shared token as the observability
     channel, presented as the client's first message, before any
     other traffic is accepted or the initial state snapshot is sent.
     After authentication, the message protocol is identical to Phase 1.

  3. WSS server on port 8766  — Communication Website connects here.
     Token-authenticated (Phase 1). Phase 2 adds: replay of recent
     events on connect/reconnect (so brief disconnects don't hide
     important events like emergencies), and a Test Lab command
     surface (fault injection config + IDS status) for already-
     authenticated clients only.

  4. Fault injection (fault_layer.py) can be armed via the Test Lab and
     is applied ONLY to the bridge→browser broadcast on :8765 — never
     to the QNX TCP link, and never to emergency messages (hard
     exclusion enforced inside fault_layer.py itself).

  5. IDS/firewall (security_monitor.py) tracks failed auth attempts per
     IP across both :8765 and :8766 and applies a temporary block.

Run first:
  python bridge.py

Ports:
  12345  TCP   ← QNX connects here (unchanged)
  8765   WSS   ← Browser connects here (token required, Phase 2)
  8766   WSS   ← Communication Website connects here (token required)
"""

import asyncio
import socket
import threading
import json
import time
import queue
import websockets

import hashlib
import hmac
import ssl
import itertools
import pathlib
import collections
from contextlib import AsyncExitStack

import fault_layer
import security_monitor

# ────────────────────────────────────────────────────────────────────
#  Configuration
# ────────────────────────────────────────────────────────────────────
TCP_HOST = "0.0.0.0"
TCP_PORT = 12345
WS_HOST  = "localhost"
WS_PORT  = 8765

HERE                = pathlib.Path(__file__).resolve().parent
GATEWAY_CONFIG_PATH = HERE / "gateway_config.json"

# ────────────────────────────────────────────────────────────────────
#  Shared state  (TCP thread ↔ WS coroutines)
# ────────────────────────────────────────────────────────────────────
state = {
    "angles":        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    "emergency":     False,
    "qnx_connected": False,
    "packets":       0,
    "mode":          "manual",
    "pose_idx":      0,
    # Phase 2 addition (Bug 3): tracks whether QNX is currently executing
    # an active RTOS motion sequence vs streaming idle/health telemetry.
    # Set True right when a sequence is dispatched to QNX, cleared on
    # SEQ_COMPLETE or EMERGENCY_STOP. Used only to tag telemetry
    # envelopes for the observability UI — never affects QNX control.
    "motion_active": False,
}
state_lock = threading.Lock()

# Queue for commands to send TO QNX (written by WS coroutines, read by TCP thread)
to_qnx_queue = queue.Queue()

# All connected, AUTHENTICATED WebSocket browser clients
ws_clients     = set()
ws_clients_lock = threading.Lock()

# asyncio loop (set when WS server starts)
ws_loop = None


def load_gateway_config():
    if not GATEWAY_CONFIG_PATH.exists():
        return None
    try:
        with open(GATEWAY_CONFIG_PATH) as f:
            return json.load(f)
    except Exception as e:
        print(f"[OBS]  Failed to read gateway_config.json: {e}")
        return None


_gateway_config = load_gateway_config()


def build_obs_ssl_context(cfg):
    """Build a TLS server context, shared by both the :8765 control
    channel and the :8766 observability channel (same shared-secret
    model, same self-signed localhost cert — see PROJECT_DESCRIPTION.md
    Section 5)."""
    if not cfg:
        return None
    cert_path = HERE / cfg.get("tls_cert_path", "certs/localhost.crt")
    key_path  = HERE / cfg.get("tls_key_path", "certs/localhost.key")
    if not cert_path.exists() or not key_path.exists():
        print(f"[OBS]  TLS cert/key not found ({cert_path} / {key_path}).")
        print("[OBS]  Run `python generate_token.py` once to generate them.")
        return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    return ctx


def _check_token(token: str) -> bool:
    if not token:
        return False
    presented_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    expected_hash  = (_gateway_config or {}).get("observability_token_hash", "")
    if not expected_hash:
        return False
    return hmac.compare_digest(presented_hash, expected_hash)


def _client_ip(websocket) -> str:
    try:
        return websocket.remote_address[0]
    except Exception:
        return "unknown"


# ════════════════════════════════════════════════════════════════════
#  Observability gateway: envelopes, recent-event buffer, broadcast
# ════════════════════════════════════════════════════════════════════

obs_clients      = set()
obs_clients_lock = threading.Lock()

_packet_id_counter = itertools.count(1)
_packet_id_lock     = threading.Lock()

# ── Bug 4 fix: recent-event ring buffer for reconnect replay ──────────
# Bounded history of the last N enveloped events, so an observer that
# connects (or reconnects after a brief drop) immediately gets caught up
# on anything important it missed — most notably an emergency stop that
# happened while the observability socket itself was cycling. This never
# touches the real QNX/bridge control path; it only affects what a
# reconnecting *observer* sees.
_RECENT_EVENTS_MAXLEN = 150
_recent_events = collections.deque(maxlen=_RECENT_EVENTS_MAXLEN)
_recent_events_lock = threading.Lock()


def wrap_envelope(direction: str, category: str, payload: dict, transport: str) -> dict:
    """
    direction : "qnx_to_bridge" | "bridge_to_qnx" |
                "browser_to_bridge" | "bridge_to_browser"
    category  : "telemetry" | "command" | "ack" | "sequence" |
                "emergency" | "system_status" | "error" | "security"
    payload   : JSON-serializable event data
    transport : "tcp" | "websocket"
    """
    with _packet_id_lock:
        pid = next(_packet_id_counter)
    size_bytes = len(json.dumps(payload).encode("utf-8"))
    return {
        "packet_id":    pid,
        "timestamp_ns": time.monotonic_ns(),
        "direction":    direction,
        "category":     category,
        "transport":    transport,
        "payload":      payload,
        "size_bytes":   size_bytes,
        "real":         True,
    }


async def _broadcast_obs(message: str):
    with obs_clients_lock:
        targets = set(obs_clients)
    if not targets:
        return
    await asyncio.gather(*[ws.send(message) for ws in targets], return_exceptions=True)


def broadcast_to_observers(envelope: dict):
    """Thread-safe. Also appends to the recent-event ring buffer so a
    reconnecting observer can be caught up (Bug 4 fix)."""
    with _recent_events_lock:
        _recent_events.append(envelope)
    if ws_loop and not ws_loop.is_closed():
        message = json.dumps(envelope)
        asyncio.run_coroutine_threadsafe(_broadcast_obs(message), ws_loop)


def _drain_security_alerts_to_observers():
    """Turn any queued IDS alerts into security-category envelopes."""
    for alert in security_monitor.drain_alerts():
        broadcast_to_observers(wrap_envelope("bridge_to_browser", "security", alert, "websocket"))


async def obs_ws_handler(websocket):
    """Handler for the observability WSS server (:8766).
    Phase 1: read-only, token-authenticated.
    Phase 2 additions:
      - IDS check before auth is even attempted (blocked IPs rejected
        immediately)
      - failed auth attempts are recorded with the IDS
      - on successful auth, recent event history is replayed
      - authenticated clients may now send Test Lab commands
        (fault injection config, kill switch, IDS status query) —
        still cannot send anything that reaches QNX or the control
        channel; this remains a strictly separate command surface.
    """
    client_addr = websocket.remote_address
    ip = _client_ip(websocket)

    blocked, remaining = security_monitor.is_blocked(ip)
    if blocked:
        print(f"[IDS]  ✗ Rejected {client_addr} — IP blocked for {remaining}s more")
        try:
            await websocket.close(code=4429, reason="temporarily blocked")
        except Exception:
            pass
        return

    try:
        raw = await asyncio.wait_for(websocket.recv(), timeout=10)
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        print(f"[OBS]  ✗ {client_addr} — no auth message within 10s, closing")
        security_monitor.record_failure(ip, "auth_timeout", "observability")
        try:
            await websocket.close(code=4001, reason="auth timeout")
        except Exception:
            pass
        return

    try:
        auth_data = json.loads(raw)
    except json.JSONDecodeError:
        print(f"[OBS]  ✗ {client_addr} — malformed auth payload, closing")
        security_monitor.record_failure(ip, "malformed_auth", "observability")
        await websocket.close(code=4002, reason="bad auth payload")
        return

    if auth_data.get("type") != "auth" or not auth_data.get("token"):
        print(f"[OBS]  ✗ {client_addr} — first message was not a valid auth request")
        security_monitor.record_failure(ip, "missing_token", "observability")
        await websocket.close(code=4003, reason="auth required")
        return

    if not _check_token(auth_data["token"]):
        just_blocked = security_monitor.record_failure(ip, "invalid_token", "observability")
        print(f"[OBS]  ✗ Auth REJECTED from {client_addr}"
              + ("  — IP now BLOCKED" if just_blocked else ""))
        try:
            await websocket.send(json.dumps({"type": "auth_result", "ok": False}))
            await websocket.close(code=4004, reason="invalid token")
        except Exception:
            pass
        if just_blocked:
            _drain_security_alerts_to_observers()
        return

    security_monitor.record_success(ip)

    with obs_clients_lock:
        obs_clients.add(websocket)
    print(f"[OBS]  ✔ Observer authenticated {client_addr}  (total: {len(obs_clients)})")

    await websocket.send(json.dumps({"type": "auth_result", "ok": True}))

    with state_lock:
        current = dict(state)
    await websocket.send(json.dumps(wrap_envelope(
        "bridge_to_browser", "system_status",
        {
            "qnx_connected": current["qnx_connected"],
            "mode":          current["mode"],
            "packets":       current["packets"],
            "emergency":     current["emergency"],
            "motion_active": current["motion_active"],
        },
        "websocket"
    )))

    # ── Bug 4 fix: replay recent event history on connect/reconnect ──
    with _recent_events_lock:
        backlog = list(_recent_events)
    if backlog:
        replay_msg = json.dumps({"type": "event_replay", "events": [
            {**env, "replayed": True} for env in backlog
        ]})
        try:
            await websocket.send(replay_msg)
        except Exception:
            pass

    # Send current fault-injection / IDS status so the Test Lab UI can
    # reflect reality immediately on connect (e.g. after a page reload
    # while a fault was still armed).
    try:
        await websocket.send(json.dumps({
            "type": "test_lab_status",
            "fault": fault_layer.status(),
            "ids":   security_monitor.snapshot(),
        }))
    except Exception:
        pass

    try:
        async for raw_msg in websocket:
            try:
                msg = json.loads(raw_msg)
            except json.JSONDecodeError:
                continue
            await _handle_obs_command(websocket, msg)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        with obs_clients_lock:
            obs_clients.discard(websocket)
        print(f"[OBS]  Observer disconnected {client_addr}  (total: {len(obs_clients)})")


async def _broadcast_test_lab_status():
    """Push a fresh test_lab_status to every connected+authenticated
    observer right now. Used so every open Test Lab panel updates
    immediately when a fault is armed/cleared/expires — not just the
    tab that triggered the change (fixes the close/reopen requirement)."""
    msg = json.dumps({
        "type": "test_lab_status",
        "fault": fault_layer.status(),
        "ids":   security_monitor.snapshot(),
    })
    with obs_clients_lock:
        targets = set(obs_clients)
    if targets:
        await asyncio.gather(*[ws.send(msg) for ws in targets], return_exceptions=True)


async def _handle_obs_command(websocket, msg: dict):
    """Test Lab command surface. Only reachable after successful auth
    (this coroutine is only invoked from inside obs_ws_handler's
    post-auth loop). Cannot reach QNX or the control channel — it can
    only configure fault_layer (which itself only touches the :8765
    browser broadcast) and query IDS state."""
    msg_type = msg.get("type")

    if msg_type == "test_lab_fault_config":
        ok, detail = fault_layer.configure(
            mode=msg.get("mode"),
            rate=msg.get("rate", 0.2),
            duration_s=msg.get("duration_s", 20),
        )
        print(f"[TESTLAB] fault_config mode={msg.get('mode')} -> ok={ok} ({detail})")
        broadcast_to_observers(wrap_envelope("bridge_to_browser", "security", {
            "type": "fault_config", "ok": ok, "detail": detail, **fault_layer.status()
        }, "websocket"))
        await websocket.send(json.dumps({"type": "test_lab_ack", "ok": ok, "detail": detail}))
        await _broadcast_test_lab_status()

    elif msg_type == "test_lab_kill":
        fault_layer.clear()
        print("[TESTLAB] KILL SWITCH — all fault injection cleared")
        broadcast_to_observers(wrap_envelope("bridge_to_browser", "security", {
            "type": "fault_config", "ok": True, "detail": "kill switch", **fault_layer.status()
        }, "websocket"))
        await websocket.send(json.dumps({"type": "test_lab_ack", "ok": True, "detail": "cleared"}))
        await _broadcast_test_lab_status()

    elif msg_type == "test_lab_status_query":
        await websocket.send(json.dumps({
            "type": "test_lab_status",
            "fault": fault_layer.status(),
            "ids":   security_monitor.snapshot(),
        }))
    # Anything else on this channel is silently ignored — no command
    # surface into QNX or the robotic control path exists here.


# ────────────────────────────────────────────────────────────────────
#  Broadcast helpers (browser control channel, :8765)
# ────────────────────────────────────────────────────────────────────
async def _broadcast(message: str):
    """Bridge → all authenticated browser dashboard clients. This is the
    ONLY place fault injection is applied (Test Lab scope, see
    fault_layer.py's module docstring for the full safety model)."""
    with ws_clients_lock:
        targets = set(ws_clients)
    if not targets:
        return

    try:
        decoded = json.loads(message)
    except json.JSONDecodeError:
        decoded = None

    if decoded is not None:
        action, arg = fault_layer.maybe_apply(decoded)
        if action == "drop":
            return
        if action == "delay":
            await asyncio.sleep(min(arg, 3.0))
            out_message = message
        elif action == "corrupt":
            out_message = json.dumps(arg)
        else:
            out_message = message
    else:
        out_message = message

    await asyncio.gather(*[ws.send(out_message) for ws in targets], return_exceptions=True)


def broadcast_from_thread(message: str):
    if ws_loop and not ws_loop.is_closed():
        asyncio.run_coroutine_threadsafe(_broadcast(message), ws_loop)


# ────────────────────────────────────────────────────────────────────
#  TCP Server  — runs in its own daemon thread (QNX link, UNCHANGED
#  protocol from Phase 1 — Test Lab fault injection never touches this)
# ────────────────────────────────────────────────────────────────────
def _categorize_qnx_command(cmd: str) -> str:
    """Best-effort category for a raw command block about to be sent to
    QNX, purely for observability envelope tagging (Card 2's pipeline)."""
    first_line = cmd.strip().split("\n", 1)[0]
    if first_line.startswith("SEQ_START"):
        return "sequence"
    if first_line in ("EMERGENCY_STOP", "RESET_EMERGENCY"):
        return "emergency"
    if first_line == "SEQ_STOP":
        return "command"
    return "command"


def tcp_server_thread():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((TCP_HOST, TCP_PORT))
    srv.listen(1)
    print(f"[TCP]  Listening on {TCP_HOST}:{TCP_PORT}  ← waiting for QNX")

    while True:
        conn, addr = srv.accept()
        conn.settimeout(0.05)
        print(f"[TCP]  QNX connected from {addr}")

        with state_lock:
            state["qnx_connected"] = True
            state["emergency"]     = False

        broadcast_from_thread(json.dumps({
            "type":          "status",
            "qnx_connected": True,
            "addr":          f"{addr[0]}:{addr[1]}"
        }))
        broadcast_to_observers(wrap_envelope("bridge_to_browser", "system_status", {
            "type":          "status",
            "qnx_connected": True,
            "addr":          f"{addr[0]}:{addr[1]}"
        }, "websocket"))

        buf = ""
        try:
            while True:
                # ── Drain outgoing queue (browser → QNX) ──────────────
                disconnected_on_send = False
                while not to_qnx_queue.empty():
                    try:
                        cmd = to_qnx_queue.get_nowait()
                        conn.sendall(cmd.encode("utf-8"))
                        broadcast_to_observers(wrap_envelope(
                            "bridge_to_qnx", _categorize_qnx_command(cmd),
                            {"type": "qnx_tx", "lines": cmd.strip().split("\n")},
                            "tcp"
                        ))
                    except (BrokenPipeError, ConnectionResetError, OSError) as e:
                        print(f"[TCP]  Send error (treating as disconnect): {e}")
                        disconnected_on_send = True
                        break
                    except Exception as e:
                        print(f"[TCP]  Send error: {e}")
                if disconnected_on_send:
                    break

                # ── Receive from QNX ───────────────────────────────────
                # Bug 2 fix: distinguish "no data yet" (socket.timeout,
                # expected every ~50ms while idle) from a genuine EOF
                # (conn.recv() returning b"" with NO exception, which
                # means the peer performed an orderly TCP close — i.e.
                # QNX terminated or the VM/network link dropped). The
                # previous version conflated both cases into the same
                # "raw == ''" check and therefore never detected a real
                # disconnect: it just kept spin-looping against a dead
                # socket, so qnx_connected stayed True forever.
                try:
                    raw_bytes = conn.recv(4096)
                except socket.timeout:
                    time.sleep(0.005)
                    continue
                except (ConnectionResetError, OSError) as e:
                    print(f"[TCP]  Recv error (treating as disconnect): {e}")
                    break

                if raw_bytes == b"":
                    print("[TCP]  QNX closed the connection (EOF)")
                    break

                raw = raw_bytes.decode("utf-8", errors="ignore")
                buf += raw

                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    _handle_qnx_line(line)

        except Exception as e:
            print(f"[TCP]  Connection error: {e}")
        finally:
            conn.close()
            with state_lock:
                state["qnx_connected"] = False
                state["motion_active"] = False
            print("[TCP]  QNX disconnected. Waiting for reconnect…")
            broadcast_from_thread(json.dumps({
                "type":          "status",
                "qnx_connected": False,
                "addr":          "—"
            }))
            broadcast_to_observers(wrap_envelope("bridge_to_browser", "system_status", {
                "type":          "status",
                "qnx_connected": False,
                "addr":          "—"
            }, "websocket"))


def _handle_qnx_line(line: str):
    """Parse one line received from QNX and broadcast to browser."""

    # ── EMERGENCY_STOP ───────────────────────────────────────────
    if line == "EMERGENCY_STOP":
        with state_lock:
            state["emergency"] = True
            state["motion_active"] = False
        print("[TCP]  ⚠ EMERGENCY_STOP received from QNX!")
        broadcast_from_thread(json.dumps({
            "type":   "emergency",
            "source": "qnx"
        }))
        broadcast_to_observers(wrap_envelope("qnx_to_bridge", "emergency", {
            "type":   "emergency",
            "source": "qnx"
        }, "tcp"))
        return

    # ── EMERGENCY_CLEARED (Item 6: QNX-confirmed recovery) ────────
    # QNX only ever sends this in direct response to a RESET_EMERGENCY
    # it actually processed. This is the ONLY place bridge.py clears
    # state["emergency"] — never implicitly, never from the browser side
    # alone. Only after this arrives does the dashboard get told it's
    # safe to allow motion commands again.
    if line == "EMERGENCY_CLEARED":
        with state_lock:
            state["emergency"] = False
        print("[TCP]  ✔ EMERGENCY_CLEARED — QNX confirmed recovery, motion re-armed")
        broadcast_from_thread(json.dumps({"type": "emergency_cleared"}))
        broadcast_to_observers(wrap_envelope("qnx_to_bridge", "emergency", {
            "type": "emergency_cleared"
        }, "tcp"))
        return

    # ── WATCHDOG_HIT:<ms_over> ───────────────────────────────────
    if line.startswith("WATCHDOG_HIT:"):
        try:
            ms_over = float(line.split(":", 1)[1])
        except Exception:
            ms_over = 0.0
        print(f"[TCP]  ⏱ Watchdog: deadline missed by {ms_over:.1f}ms")
        broadcast_from_thread(json.dumps({
            "type":      "watchdog",
            "missed_ms": ms_over
        }))
        broadcast_to_observers(wrap_envelope("qnx_to_bridge", "error", {
            "type":      "watchdog",
            "missed_ms": ms_over
        }, "tcp"))
        return

    # ── POSE_DONE:<pose_idx> ─────────────────────────────────────
    if line.startswith("POSE_DONE:"):
        try:
            idx = int(line.split(":", 1)[1])
        except Exception:
            idx = 0
        with state_lock:
            state["pose_idx"] = idx
        print(f"[TCP]  ✔ Pose #{idx+1} done")
        broadcast_from_thread(json.dumps({
            "type":     "pose_advance",
            "pose_idx": idx + 1
        }))
        broadcast_to_observers(wrap_envelope("qnx_to_bridge", "telemetry", {
            "type":     "pose_advance",
            "pose_idx": idx + 1,
        }, "tcp"))
        return

    # ── CYCLE_DONE:<n> ────────────────────────────────────────────
    if line.startswith("CYCLE_DONE:"):
        try:
            cycle_num = int(line.split(":", 1)[1])
        except Exception:
            cycle_num = 0
        print(f"[TCP]  🔁 Cycle #{cycle_num} complete")
        broadcast_from_thread(json.dumps({
            "type":      "cycle_done",
            "cycle_num": cycle_num
        }))
        broadcast_to_observers(wrap_envelope("qnx_to_bridge", "telemetry", {
            "type":      "cycle_done",
            "cycle_num": cycle_num,
        }, "tcp"))
        return

    # ── SEQ_COMPLETE ─────────────────────────────────────────────
    if line == "SEQ_COMPLETE":
        with state_lock:
            state["motion_active"] = False
        print("[TCP]  ✔ RTOS cycling stopped — SEQ_COMPLETE")
        broadcast_from_thread(json.dumps({"type": "seq_complete"}))
        broadcast_to_observers(wrap_envelope("qnx_to_bridge", "ack", {"type": "seq_complete"}, "tcp"))
        return

    # ── ANGLES: "j0,j1,j2,j3,j4" or "j0,j1,j2,j3,j4,j5" ────────
    # QNX now only ever sends these while actively executing a recorded
    # RTOS sequence (idle heartbeat telemetry was removed — see the QNX
    # source). So every angle packet reaching here is real motion data;
    # no idle/motion "phase" tag is needed anymore.
    parts = line.split(",")
    if len(parts) in (5, 6):
        try:
            angles = [float(p) for p in parts]
            if len(angles) == 5:
                angles.append(0.0)

            with state_lock:
                state["angles"]    = angles
                # NOTE: emergency is intentionally NOT cleared here.
                # It is cleared in exactly one place — on receiving
                # EMERGENCY_CLEARED from QNX, itself only sent after QNX
                # processes an explicit RESET_EMERGENCY request. Clearing
                # it implicitly just because telemetry resumed would let
                # the safety lock be bypassed without operator
                # confirmation, which defeats the point of Item 6.
                state["packets"]  += 1
                pkt   = state["packets"]
                pidx  = state["pose_idx"]

            angle_payload = {
                "type":     "angles",
                "angles":   angles,
                "packets":  pkt,
                "pose_idx": pidx
            }
            broadcast_from_thread(json.dumps(angle_payload))
            broadcast_to_observers(wrap_envelope("qnx_to_bridge", "telemetry", angle_payload, "tcp"))
        except ValueError:
            print(f"[TCP]  Bad packet: {line}")


# ────────────────────────────────────────────────────────────────────
#  WebSocket Server  — async, main thread (browser control channel)
# ────────────────────────────────────────────────────────────────────
async def ws_handler(websocket):
    """Phase 2: now requires the same shared token as the observability
    channel, presented as the client's FIRST message, before the initial
    state snapshot is sent or any other traffic is processed. After
    authentication, behavior is identical to Phase 1 — same message
    types, same semantics, same QNX routing."""
    client_addr = websocket.remote_address
    ip = _client_ip(websocket)

    blocked, remaining = security_monitor.is_blocked(ip)
    if blocked:
        print(f"[IDS]  ✗ Rejected browser {client_addr} — IP blocked for {remaining}s more")
        try:
            await websocket.close(code=4429, reason="temporarily blocked")
        except Exception:
            pass
        return

    try:
        raw = await asyncio.wait_for(websocket.recv(), timeout=10)
        auth_data = json.loads(raw)
        if auth_data.get("type") != "auth" or not _check_token(auth_data.get("token", "")):
            raise ValueError("bad token")
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        print(f"[WS]   ✗ {client_addr} — no auth within 10s")
        security_monitor.record_failure(ip, "auth_timeout", "control")
        try:
            await websocket.close(code=4001, reason="auth timeout")
        except Exception:
            pass
        return
    except (json.JSONDecodeError, ValueError):
        just_blocked = security_monitor.record_failure(ip, "invalid_token", "control")
        print(f"[WS]   ✗ Auth REJECTED from {client_addr}"
              + ("  — IP now BLOCKED" if just_blocked else ""))
        try:
            await websocket.send(json.dumps({"type": "auth_result", "ok": False}))
            await websocket.close(code=4004, reason="invalid token")
        except Exception:
            pass
        if just_blocked:
            _drain_security_alerts_to_observers()
        return

    security_monitor.record_success(ip)
    await websocket.send(json.dumps({"type": "auth_result", "ok": True}))

    with ws_clients_lock:
        ws_clients.add(websocket)
        browser_count = len(ws_clients)
    print(f"[WS]   Browser connected  {client_addr}  (total: {browser_count})")

    # Item 1: real connection-status event, independent of traffic —
    # observers use this (not "recent packet seen") to decide BR ONLINE.
    broadcast_to_observers(wrap_envelope("bridge_to_browser", "system_status", {
        "type": "browser_status", "browser_connected": True, "count": browser_count
    }, "websocket"))

    with state_lock:
        current = dict(state)
    await websocket.send(json.dumps({
        "type":          "init",
        "angles":        current["angles"],
        "emergency":     current["emergency"],
        "qnx_connected": current["qnx_connected"],
        "packets":       current["packets"],
    }))

    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type", "")

            # ── RTOS Sequence: browser → bridge → QNX ──────────────
            if msg_type == "rtos_sequence":
                with state_lock:
                    locked = state["emergency"]
                if locked:
                    print("[WS]   ✗ rtos_sequence rejected — EMERGENCY LOCK active")
                    await websocket.send(json.dumps({
                        "type": "seq_ack", "ok": False,
                        "msg":  "Rejected — system is EMERGENCY LOCKED. Send RESET_EMERGENCY first."
                    }))
                    continue

                seq     = data.get("sequence", [])
                dur_ms  = data.get("pose_duration_ms", 1000)
                n_poses = len(seq)
                if n_poses == 0:
                    continue

                print(f"[WS]   RTOS sequence: {n_poses} poses, {dur_ms}ms/pose → QNX")

                lines = [f"SEQ_START:{n_poses}:{dur_ms}"]
                for idx, pose in enumerate(seq):
                    csv = ",".join(f"{v:.2f}" for v in pose)
                    lines.append(f"POSE:{idx}:{csv}")
                lines.append("SEQ_END")
                block = "\n".join(lines) + "\n"

                with state_lock:
                    state["motion_active"] = True

                to_qnx_queue.put(block)
                broadcast_to_observers(wrap_envelope("browser_to_bridge", "sequence", {
                    "type":             "rtos_sequence",
                    "sequence":         seq,
                    "total_poses":      n_poses,
                    "pose_duration_ms": dur_ms
                }, "websocket"))

                await websocket.send(json.dumps({
                    "type": "seq_ack",
                    "msg":  f"RTOS sequence ({n_poses} poses @ {dur_ms}ms) sent to QNX"
                }))
                broadcast_to_observers(wrap_envelope("bridge_to_browser", "ack", {
                    "type": "seq_ack",
                    "msg":  f"RTOS sequence ({n_poses} poses @ {dur_ms}ms) sent to QNX"
                }, "websocket"))

            # ── Legacy sequence send ────────────────────────────────
            elif msg_type == "send_sequence":
                seq = data.get("sequence", [])
                n   = len(seq)
                if n == 0:
                    continue
                dur_ms = 1000
                lines  = [f"SEQ_START:{n}:{dur_ms}"]
                for idx, pose in enumerate(seq):
                    csv = ",".join(f"{v:.2f}" for v in pose)
                    lines.append(f"POSE:{idx}:{csv}")
                lines.append("SEQ_END")
                with state_lock:
                    state["motion_active"] = True
                to_qnx_queue.put("\n".join(lines) + "\n")
                broadcast_to_observers(wrap_envelope("browser_to_bridge", "sequence", {
                    "type":     "send_sequence",
                    "sequence": seq,
                    "total_poses": n
                }, "websocket"))
                await websocket.send(json.dumps({
                    "type": "seq_ack",
                    "msg":  f"Sequence ({n} poses) queued for QNX"
                }))

            # ── Emergency from browser → QNX ───────────────────────
            elif msg_type == "emergency":
                with state_lock:
                    state["emergency"] = True
                to_qnx_queue.put("EMERGENCY_STOP\n")
                print("[WS]   ⚠ Emergency from browser → QNX")
                broadcast_to_observers(wrap_envelope("browser_to_bridge", "emergency", {
                    "type": "emergency", "source": "browser"
                }, "websocket"))
                await _broadcast(json.dumps({"type": "emergency", "source": "browser"}))

            # ── Explicit, operator-confirmed recovery (Item 6) ──────
            # Bridge does NOT clear state["emergency"] here — only QNX's
            # EMERGENCY_CLEARED confirmation does that (see
            # _handle_qnx_line). This just forwards the request and lets
            # the real recovery happen on the QNX side.
            elif msg_type == "reset_emergency":
                to_qnx_queue.put("RESET_EMERGENCY\n")
                print("[WS]   🔓 RESET_EMERGENCY requested by browser → QNX")
                broadcast_to_observers(wrap_envelope("browser_to_bridge", "emergency", {
                    "type": "reset_emergency"
                }, "websocket"))
                await websocket.send(json.dumps({
                    "type": "reset_emergency_ack",
                    "msg":  "Reset request sent to QNX — waiting for confirmation"
                }))

            # ── Stop cycling: finish current pass then halt ─────────
            elif msg_type == "stop_cycle":
                to_qnx_queue.put("SEQ_STOP\n")
                print("[WS]   SEQ_STOP → QNX  (will stop after current pass)")
                broadcast_to_observers(wrap_envelope("browser_to_bridge", "command", {
                    "type": "stop_cycle"
                }, "websocket"))
                await websocket.send(json.dumps({
                    "type": "stop_cycle_ack",
                    "msg":  "Stop requested — arm will finish current cycle then halt"
                }))

            # ── Mode change (informational) ─────────────────────────
            elif msg_type == "set_mode":
                mode = data.get("mode", "manual")
                with state_lock:
                    state["mode"] = mode
                print(f"[WS]   Mode → {mode.upper()}")
                broadcast_to_observers(wrap_envelope("browser_to_bridge", "command", {
                    "type": "set_mode", "mode": mode
                }, "websocket"))

            else:
                print(f"[WS]   From browser: {data}")

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        with ws_clients_lock:
            ws_clients.discard(websocket)
            browser_count = len(ws_clients)
        print(f"[WS]   Browser disconnected {client_addr}  (total: {browser_count})")
        broadcast_to_observers(wrap_envelope("bridge_to_browser", "system_status", {
            "type": "browser_status", "browser_connected": browser_count > 0, "count": browser_count
        }, "websocket"))


async def _test_lab_status_ticker():
    """Runs for the lifetime of the bridge. Only pushes when a fault is
    actually active, so idle observers aren't spammed — but this is what
    makes the Test Lab's remaining-time countdown and natural expiry
    show up live instead of only updating on the next explicit action."""
    was_active = False
    while True:
        await asyncio.sleep(2)
        active_now = fault_layer.status()["active"]
        if active_now or was_active:
            await _broadcast_test_lab_status()
        was_active = active_now


# ────────────────────────────────────────────────────────────────────
#  Main
# ────────────────────────────────────────────────────────────────────
async def main():
    global ws_loop
    ws_loop = asyncio.get_running_loop()

    t = threading.Thread(target=tcp_server_thread, daemon=True, name="TCP-Server")
    t.start()

    asyncio.create_task(_test_lab_status_ticker())

    ssl_ctx = build_obs_ssl_context(_gateway_config)
    obs_port = (_gateway_config or {}).get("observability_port", 8766)

    secured = bool(_gateway_config and ssl_ctx)

    async with AsyncExitStack() as stack:
        if secured:
            await stack.enter_async_context(websockets.serve(ws_handler, WS_HOST, WS_PORT, ssl=ssl_ctx))
            await stack.enter_async_context(websockets.serve(obs_ws_handler, WS_HOST, obs_port, ssl=ssl_ctx))
        else:
            # No token/cert configured yet — bridge still runs, but both
            # secured channels are disabled until `generate_token.py` is
            # run once. (Nothing to connect to on 8765/8766 in this
            # state; QNX's TCP link on :12345 is unaffected either way.)
            print("[SEC]  No gateway_config.json / TLS cert found.")
            print("[SEC]  Run `python generate_token.py` once (in python_bridge/) "
                  "to enable the secured control (:8765) and observability (:8766) channels.")

        if secured:
            print(f"[WS]   Secured control channel on wss://{WS_HOST}:{WS_PORT}  (token required)")
            print(f"[OBS]  Observability WSS server on wss://{WS_HOST}:{obs_port}  (token required)")
        print()
        print("═" * 60)
        print("  M.A.T.R.I.X. Bridge  —  RUNNING  (Phase 2)")
        print()
        print("  1. Open dashboard/index.html with Live Server in VS Code")
        print("  2. Start QNX VM and run ./Robotic_ARM")
        print()
        print("  Ports:  TCP :12345  ← QNX")
        if secured:
            print("          WSS :8765   ← Browser (token required)")
            print(f"          WSS :{obs_port}   ← Communication Website (token required)")
        print("═" * 60)
        print()
        await asyncio.Future()   # run forever


if __name__ == "__main__":
    asyncio.run(main())
