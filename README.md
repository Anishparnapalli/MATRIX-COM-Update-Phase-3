# M.A.T.R.I.X. — Motion Articulation & Telemetric Real-time Interface eXecution
### Communication Layer + Security Test Lab (Phase 1 + Phase 2)

QNX 8.0 real-time robotic-arm controller + Python gateway + Three.js browser
dashboard + a secured, observable Communication Layer with a Network/Security
Test Lab. SRM Institute of Science & Technology, RTOS Programming, 2025–26.

---

## 1. Project structure

```
MATRIX/
├── QNX_RoboticARM/src/MATRIX_Robotic_ARM.c   ← QNX controller (UNTOUCHED — see §6)
├── python_bridge/
│   ├── bridge.py                 ← gateway: TCP↔WSS bridge, auth, IDS, fault injection
│   ├── fault_layer.py            ← Test Lab fault-injection middleware (Phase 2)
│   ├── security_monitor.py       ← IDS/firewall (Phase 2)
│   ├── generate_token.py         ← one-time setup: token + TLS cert
│   ├── gateway_config.json       ← generated (token HASH only, never plaintext)
│   └── certs/                    ← generated self-signed TLS cert/key
├── dashboard/                    ← robotic control website (split into 3 files)
│   ├── index.html
│   ├── style.css
│   └── app.js
├── comm-dashboard/                ← Communication Website + Test Lab (Phase 2)
│   ├── index.html
│   ├── style.css
│   └── app.js
└── testing/                      ← real (non-simulated) test clients
    ├── mock_qnx_client.py
    ├── mock_browser_client.py
    ├── mock_observer_client.py
    └── security_test_clients.py  ← Phase 2 IDS test clients
```

## 2. One-time setup

```bash
cd python_bridge
python generate_token.py
```

Prints a token **once**. Paste it into **both**:
- `comm-dashboard/app.js` → `OBSERVABILITY_TOKEN`
- `dashboard/app.js` → `CONTROL_TOKEN`

(Same shared secret, used by both channels — see §7 Security Model.)

## 3. Running everything

```
1. python_bridge/        →  python bridge.py
2. dashboard/index.html            → open with Live Server
3. comm-dashboard/index.html       → open with Live Server
4. QNX VM → run ./Robotic_ARM
```

First run only: visit `https://localhost:8765/` and `https://localhost:8766/`
directly once each and accept the self-signed cert warning, then reload both
dashboards — both channels are now TLS-secured (Phase 2), so both need the
cert accepted, not just the observability channel as in Phase 1.

---

## 4. Phase 2 — what changed and why

### 4.1 Bug fixes (found during Phase 1 testing)

**1. First RTOS send visual glitch.**
Root cause: the dashboard only applies incoming QNX telemetry to its local
`S.targets` while `S.rtosRunning` is true — idle sine-wave telemetry is
intentionally not visualized. That means the moment RTOS is first activated,
the dashboard's last-known position (default pose, or wherever sliders were
left) can be far from QNX's *actual* current position (which has been
wandering per the idle sine wave). The existing per-frame lerp then sweeps
visibly through that gap. On a second send shortly after, the gap is small
because the dashboard was actively tracking QNX during the first run — hence
"works the second time."
**Fix:** `S._rtosSyncPending` flag, set on every `SEND→QNX`. The very next
real telemetry packet hard-snaps `S.angles` (not just `S.targets`) straight
to QNX's true position — no sweep — then normal lerp resumes. See
`dashboard/app.js`, `sendToQNX()` and the `angles` handler in `connectWS()`.

**2. QNX online/offline status not detected on disconnect.**
Root cause: `bridge.py`'s TCP read loop treated `conn.recv()` returning
`b""` (a real, clean TCP close — i.e. QNX terminated) exactly the same as a
`socket.timeout` (no data yet, expected every ~50ms while idle) — both fell
into the same `if raw == "": continue` branch, so a genuine disconnect was
never detected; the loop just spun forever against a dead socket.
**Fix:** distinguish the two cases explicitly — `socket.timeout` → keep
polling; `recv()` returning `b""` with no exception → real EOF → break and
run the existing (already-correct) disconnect cleanup/broadcast path. See
`bridge.py`, `tcp_server_thread()`.

**3. Telemetry/observability redesign (idle heartbeat vs. active motion).**
QNX's idle sine-wave telemetry (~10 pkt/s) was interleaved with real RTOS
motion data in the packet monitor, burying meaningful events in noise.
**Fix:** `bridge.py` now tracks `state["motion_active"]` (true from the
moment a sequence is dispatched to QNX until `SEQ_COMPLETE`/emergency) and
tags every angle-telemetry envelope with `payload.phase = "idle"|"motion"`.
The Communication Website separates them architecturally: idle telemetry
updates a compact **QNX Health** heartbeat widget (rate + pulse indicator)
instead of streaming into the packet list; motion telemetry, commands, acks,
sequences, emergencies, and system events continue to stream normally. A
"Show idle heartbeat" checkbox reveals raw idle packets in the list on
demand — full debug detail is preserved, just not on by default.

**4. Emergency events missed across observer reconnects.**
**Fix:** `bridge.py` keeps a bounded ring buffer (`deque(maxlen=150)`) of
every enveloped event. On (re)connect, after the auth handshake, the
gateway replays this backlog to the observer, tagged `replayed: true`. The
Communication Website renders replayed events in the packet list (visually
marked, inspectable) but excludes them from live rate/latency stats to avoid
skewing "now" metrics with backfilled history. This never touches the real
QNX↔bridge safety path — only what a reconnecting *observer* sees.

**5. Pose duration now user-configurable.**
QNX and `bridge.py` already fully supported an arbitrary `dur_ms` in
`SEQ_START:<n>:<dur_ms>` — only the dashboard hardcoded `1000`. Added a
slider+number control (`dashboard/index.html`/`app.js`, RTOS mode only,
300–5000ms), persisted via `autoSave()`/`restoreProjectState()`.

**6. Network topology redesign.**
Rebuilt as SVG nodes with three explicit states (online/connecting/offline),
a pulsing ring on active nodes, and traveling dot animations along each hop
(QNX↔Gateway, Gateway↔Browser) that fire in real time as traffic actually
flows — not a generic dashed-line toggle. State labels under each node.

**7. Typography/readability pass.**
Both stylesheets now define a shared typographic scale (`--fs-h1` through
`--fs-secondary`, `comm-dashboard/style.css`) matching the requested ranges
(body 16–18px, H1 32–48px, H2 24–32px, H3 20–24px, buttons 14–16px,
secondary 12–14px), and every `font-size` declaration in both dashboards was
mapped onto that scale (single-pass substitution, verified against the
original value set to avoid compounding errors).

### 4.2 Phase 2 architecture

- **Secured control channel** (`dashboard/app.js` `connectWS()`, `bridge.py`
  `ws_handler()`): `:8765` is now `wss://` and requires the same shared
  token as the observability channel, presented as the client's first
  message, before the initial state snapshot or any other traffic is
  processed. After auth, the message protocol is byte-identical to Phase 1.
- **Test Lab** (`comm-dashboard`, red-bordered overlay, visually
  unmistakable from the read-only monitor): fault injection controls
  (packet loss / delay / corruption, each with an explicit duration and a
  kill switch) and live Firewall/IDS status.
- **Fault injection** (`fault_layer.py`): applied **only** to the
  bridge→browser broadcast on `:8765`. It **never** touches the QNX TCP
  link (`:12345`) and **never** applies to `emergency`-typed messages —
  both are hard exclusions in the module itself, not just caller
  discipline. Self-expiring (every fault has a `duration_s`, checked
  against wall-clock time on every use — no background timer to fail to
  fire) and has a synchronous kill switch.
- **Firewall/IDS** (`security_monitor.py`): tracks failed-auth attempts per
  IP across both `:8765` and `:8766`; after 4 failures in 30s, blocks that
  IP for 60s (rejected at the TCP/TLS handshake stage, before any auth
  message is even read). Alerts are broadcast to observers as
  `category:"security"` envelopes.
- **Real sandboxed test clients** (`testing/security_test_clients.py`):
  generate genuine bad-auth WebSocket traffic against the real gateway —
  not simulated UI events — to exercise the IDS for real.
- **QNX-side changes:** none. Every fix and every Phase 2 feature above was
  achievable entirely in `bridge.py` and the two dashboards; the QNX
  `.c` file did not need to change and was not touched.

---

## 5. Envelope format (unchanged from Phase 1, phase-tagging added)

```json
{
  "packet_id": 142, "timestamp_ns": 88210553041792,
  "direction": "qnx_to_bridge", "category": "telemetry", "transport": "tcp",
  "payload": {"type":"angles","angles":[90,90,90,90,90,0],"packets":142,"pose_idx":0,"phase":"idle"},
  "size_bytes": 94, "real": true, "replayed": false
}
```
`direction` ∈ `qnx_to_bridge | bridge_to_qnx | browser_to_bridge | bridge_to_browser`
`category`  ∈ `telemetry | command | ack | sequence | emergency | system_status | error | security`
`payload.phase` (telemetry/angles only) ∈ `idle | motion`

## 6. What was NOT changed

- `QNX_RoboticARM/src/MATRIX_Robotic_ARM.c` — byte-identical to the original.
  Every requested fix (RTOS send glitch, disconnect detection, pose
  duration) was resolvable on the bridge/dashboard side; nothing required
  touching QNX's wire protocol or real-time scheduling.
- QNX↔bridge TCP protocol (`:12345`) — completely unaffected by
  authentication or fault injection; those exist only on the browser-facing
  side.

## 7. Security model note (token)

Same model as Phase 1, now covering both `:8765` and `:8766`: the token is a
shared secret embedded in each dashboard's own JS source — not strong
secret storage, deliberately. Real protections: TLS in transit on both
channels, and a constant-time hash comparison (`hmac.compare_digest`) on the
gateway. The Phase 2 IDS adds a second real layer: repeated bad tokens from
one IP get that IP temporarily blocked, regardless of channel.


STARTUP COMMANDS:

Terminal 1 — Python Bridge
cd C:\Users\AnishP\Downloads\MATRIX_Phase3\python_bridge
python bridge.py

Terminal 2 — Communication Website
cd C:\Users\AnishP\Downloads\MATRIX_Phase3
python -m http.server 5500 --directory com_dashboard
Then open:
http://localhost:5500