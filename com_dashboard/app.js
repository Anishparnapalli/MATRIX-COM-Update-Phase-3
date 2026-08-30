"use strict";
/* ═══════════════════════════════════════════════════════════════════
 *  M.A.T.R.I.X. Communication Website — app.js  (Phase 2, three-card) HELLO THERE
 * ═══════════════════════════════════════════════════════════════════
 *
 *  OBSERVABILITY_TOKEN — same shared-secret model as the robotic
 *  dashboard's CONTROL_TOKEN. See PROJECT_DESCRIPTION.md Section 5.
 */
const OBSERVABILITY_TOKEN = "aEam6N5RQnkDF0Ew-IDpXPWFZAW3uNQzcUvIh7O1LYM";

const GATEWAY_HOST = "localhost";
const GATEWAY_PORT = 8766;

// ── State ──────────────────────────────────────────────────────────
const MAX_PACKETS = 300;
let packets = [];          // ALL envelopes, newest first (unfiltered — Card 2's "everything")
let selectedPacketId = null;
let paused = false;
let currentDetailCard = null;   // null | 'card1' | 'card2' | 'card3'
let c1ActiveFilter = 'all';     // ALL | COMMANDS | SEQUENCES | SAFETY | MODE
let c3ActiveFilter = 'all';     // ALL | TELEMETRY | POSE | CYCLE | SAFETY | SYSTEM

const stats = {
  total: 0,
  byCategory: {},
  sizeSum: 0,
  windowTimestamps: [],
};

let pendingCommand = null;
const timelineEntries = [];

// Card-specific running counters/latest-state
const card1 = { count: 0, mode: 'MANUAL', cmdCount: 0, seqCount: 0, poseCount: 0 };
const card2 = { total: 0, errors: 0 };
const card3 = { motionState: 'IDLE', poseIdx: null, totalPoses: null, lastAngles: null,
                cycleNum: null, safety: 'ARMED', lastTelemetryAt: null, telemetryTimestamps: [] };

// Topology: connection state is REAL (from explicit status events, never
// inferred from traffic — Item 1). Activity is a separate, short-lived
// "traffic seen recently on this hop" indicator.
const topo = {
  qnxOnline: null, brOnline: false, gwOnline: false, gwConnecting: false,
  lastQnxTrafficAt: 0, lastBrTrafficAt: 0, lastGwTrafficAt: 0,
};

let ws = null;
let authOk = false;
let retryTimer = null;

// ═══════════════════════════════════════════════════════════════════
//  CONNECTION + AUTH
// ═══════════════════════════════════════════════════════════════════
function connect(){
  setGwStatus("connecting");
  setAuthStatus("—", null);

  const url = `wss://${GATEWAY_HOST}:${GATEWAY_PORT}`;
  try{
    ws = new WebSocket(url);
  } catch(e){
    log("WS construction failed: " + e.message);
    scheduleRetry();
    return;
  }

  ws.onopen = () => {
    setGwStatus("on");
    setAuthStatus("AUTHENTICATING", "warn");
    ws.send(JSON.stringify({ type: "auth", token: OBSERVABILITY_TOKEN }));
  };

  ws.onmessage = (evt) => {
    let msg;
    try{ msg = JSON.parse(evt.data); } catch(e){ return; }

    if(msg.type === "auth_result"){
      if(msg.ok){
        authOk = true;
        setAuthStatus("AUTHENTICATED", "on");
        toast("Authenticated — receiving real MATRIX traffic");
        if(retryTimer){ clearTimeout(retryTimer); retryTimer=null; }
      } else {
        authOk = false;
        setAuthStatus("REJECTED", "off");
        toast("Auth REJECTED — check OBSERVABILITY_TOKEN in app.js");
      }
      return;
    }

    if(msg.type === "event_replay"){
      (msg.events || []).forEach(env => handleEnvelope(env, true));
      toast(`Caught up on ${((msg.events)||[]).length} recent event(s)`);
      return;
    }

    if(msg.type === "test_lab_status"){
      renderTestLabStatus(msg.fault, msg.ids);
      return;
    }
    if(msg.type === "test_lab_ack"){
      toast(msg.detail || (msg.ok ? "Test Lab command applied" : "Test Lab command rejected"));
      return;
    }

    handleEnvelope(msg, false);
  };

  ws.onclose = () => {
    setGwStatus("off");
    setAuthStatus(authOk ? "DISCONNECTED" : "—", "off");
    authOk = false;
    topo.gwOnline = false;
    topo.gwConnecting = false;
    topo.qnxOnline = null;
    topo.brOnline = false;
    renderTopology();
    scheduleRetry();
  };

  ws.onerror = () => { /* onclose follows */ };
}

function scheduleRetry(){
  if(retryTimer) return;
  topo.gwConnecting = true;
  renderTopology();
  retryTimer = setTimeout(()=>{ retryTimer=null; connect(); }, 3000);
}

function setGwStatus(state){
  const dot = document.getElementById('dot-gw');
  const lbl = document.getElementById('lbl-gw');
  topo.gwOnline = (state === "on");
  topo.gwConnecting = (state === "connecting");
  if(state === "on"){ dot.className='dot on'; lbl.textContent='ONLINE'; }
  else if(state === "connecting"){ dot.className='dot warn'; lbl.textContent='CONNECTING'; }
  else { dot.className='dot'; lbl.textContent='OFFLINE'; }
  renderTopology();
}

function setAuthStatus(text, cls){
  const dot = document.getElementById('dot-auth');
  const lbl = document.getElementById('lbl-auth');
  lbl.textContent = text;
  dot.className = 'dot' + (cls ? ' '+cls : '');
}

// Item 1: real connection status, driven ONLY by explicit status events
// (system_status.qnx_connected, system_status.browser_status), never by
// whether traffic happened to be seen recently.
function setQnxStatus(online){
  topo.qnxOnline = online;
  const dot = document.getElementById('dot-qnx');
  const lbl = document.getElementById('lbl-qnx');
  if(online === true){ dot.className='dot on'; lbl.textContent='ONLINE'; }
  else if(online === false){ dot.className='dot'; lbl.textContent='OFFLINE'; }
  else { dot.className='dot warn'; lbl.textContent='UNKNOWN'; }
  renderTopology();
}
function setBrStatus(online){
  topo.brOnline = online;
  const dot = document.getElementById('dot-br');
  const lbl = document.getElementById('lbl-br');
  dot.className = 'dot' + (online ? ' on' : '');
  lbl.textContent = online ? 'ONLINE' : 'OFFLINE';
  renderTopology();
}

// ═══════════════════════════════════════════════════════════════════
//  ENVELOPE HANDLING — routes into stats, the three cards, and lists
// ═══════════════════════════════════════════════════════════════════
function handleEnvelope(env, isReplay){
  if(!env || typeof env.packet_id === 'undefined') return;
  const replayed = !!(isReplay || env.replayed);
  const p = env.payload || {};

  // ── Real connection-status events (Item 1) ──
  if(env.category === 'system_status'){
    if(typeof p.qnx_connected !== 'undefined') setQnxStatus(!!p.qnx_connected);
    if(p.type === 'browser_status') setBrStatus(!!p.browser_connected);
  }

  // ── Traffic/activity tracking (separate from connection, Item 1) ──
  if(!replayed){
    const now = Date.now();
    if(env.direction === 'qnx_to_bridge' || env.direction === 'bridge_to_qnx'){
      topo.lastQnxTrafficAt = now; firePulse('qnx-gw');
    }
    if(env.direction === 'bridge_to_browser' || env.direction === 'browser_to_bridge'){
      topo.lastBrTrafficAt = now; firePulse('gw-br');
    }
    topo.lastGwTrafficAt = now;
  }

  if(!replayed){
    updateStats(env);
    updateLatency(env);
  }

  // ── Card-specific summaries ──
  if(!replayed) updateCard1(env);
  updateCard2(env, replayed);
  if(!replayed) updateCard3(env);

  // ── Packet store + whichever view is currently visible ──
  packets.unshift(env);
  if(packets.length > MAX_PACKETS) packets.pop();
  if(!paused && currentDetailCard) renderPacketList(!replayed);

  renderStats();
}

// ═══════════════════════════════════════════════════════════════════
//  CARD 1 — Robotic Dashboard → Bridge
// ═══════════════════════════════════════════════════════════════════
function updateCard1(env){
  if(env.direction !== 'browser_to_bridge') return;
  const p = env.payload || {};
  card1.count++;

  // Level-1/level-2 breakdown (redesign §3/§8): sequences vs plain
  // commands vs mode changes, all from real envelope categories/types.
  if(env.category === 'sequence' && (p.type === 'rtos_sequence' || p.type === 'send_sequence')){
    card1.seqCount++;
    card1.poseCount += (p.total_poses || (p.sequence ? p.sequence.length : 0)) || 0;
    renderCard1PosePreview(p);
  } else {
    card1.cmdCount++;
  }
  if(p.type === 'set_mode' && p.mode) card1.mode = p.mode.toUpperCase();

  document.getElementById('c1-latest').textContent = describe(env);
  document.getElementById('c1-latest-time').textContent = new Date().toLocaleTimeString('en-GB',{hour12:false});
  document.getElementById('c1-mode').textContent = card1.mode;
  document.getElementById('c1-count').textContent = card1.count;
  document.getElementById('c1-cmd-count').textContent = card1.cmdCount;
  document.getElementById('c1-seq-count').textContent = card1.seqCount;
  document.getElementById('c1-pose-count').textContent = card1.poseCount;
}

// Compact pose-preview strip (redesign §4 Level-2 / §5) — shows only the
// FIRST joint's angle per pose as a quick "shape" preview, never the
// full raw payload. Built only from the real sequence just transmitted.
function renderCard1PosePreview(p){
  const wrap = document.getElementById('c1-pose-preview');
  if(!wrap) return;
  const seq = p.sequence;
  if(!Array.isArray(seq) || seq.length === 0){ wrap.style.display = 'none'; return; }
  wrap.style.display = 'flex';
  wrap.innerHTML = seq.map((pose, i) => {
    const j0 = Array.isArray(pose) && pose.length ? pose[0] : null;
    return `<div class="pose-chip"><span class="pose-chip-lbl">P${i+1}</span><span class="pose-chip-val">${j0!=null ? j0.toFixed(0)+'°' : '—'}</span></div>`;
  }).join('');
}

// ═══════════════════════════════════════════════════════════════════
//  CARD 2 — Gateway internal processing pipeline
// ═══════════════════════════════════════════════════════════════════
function updateCard2(env, replayed){
  card2.total++;
  if(env.category === 'error') card2.errors++;
  document.getElementById('c2-total').textContent = card2.total;
  document.getElementById('c2-errors').textContent = card2.errors;

  if(replayed) return; // don't animate the pipeline for backlog replay

  const outbound = (env.direction === 'browser_to_bridge' || env.direction === 'bridge_to_qnx');
  const inbound  = (env.direction === 'qnx_to_bridge' || env.direction === 'bridge_to_browser');
  if(outbound) animatePipeline('out');
  else if(inbound) animatePipeline('in');
}

function animatePipeline(dir){
  const dotId = dir === 'out' ? 'pipe-out-dot' : 'pipe-in-dot';
  const groupId = dir === 'out' ? 'pipe-out' : 'pipe-in';
  const dot = document.getElementById(dotId);
  const group = document.getElementById(groupId);
  if(!dot || !group) return;

  const nodes = group.querySelectorAll('.pipe-node');
  const xs = dir === 'out' ? [20,72,124,176,228] : [228,176,124,72,20];
  const y = dir === 'out' ? 34 : 124;

  // Reset then animate the dot sweeping across, lighting each stage in turn
  dot.style.opacity = '1';
  nodes.forEach(n => n.classList.remove('lit'));
  xs.forEach((x, i) => {
    setTimeout(() => {
      dot.setAttribute('cx', x);
      if(nodes[i]) nodes[i].classList.add('lit');
    }, i * 70);
  });
  clearTimeout(group._fadeTimer);
  group._fadeTimer = setTimeout(() => {
    dot.style.opacity = '0';
    nodes.forEach(n => n.classList.remove('lit'));
  }, xs.length * 70 + 350);
}

// ═══════════════════════════════════════════════════════════════════
//  CARD 3 — QNX → Bridge (real-telemetry-driven stylized arm)
// ═══════════════════════════════════════════════════════════════════
function updateCard3(env){
  if(env.direction !== 'qnx_to_bridge') return;
  const p = env.payload || {};

  if(p.type === 'angles' && Array.isArray(p.angles)){
    card3.lastAngles = p.angles;
    driveArm(p.angles);
    _trackTelemetryRate();
    _updateLastTelemetryTime();
    // Continuous telemetry updates values IN PLACE but must never push
    // the "latest event" text (below) away from a real discrete event.
    return;
  }

  _updateLastTelemetryTime();

  // ── Discrete, meaningful events only — these DO update "latest event" ──
  document.getElementById('c3-latest').textContent = describe(env);

  if(p.type === 'pose_advance'){ card3.poseIdx = p.pose_idx; card3.motionState='CYCLING'; }
  if(p.type === 'cycle_done'){ card3.cycleNum = p.cycle_num; }
  if(p.type === 'emergency'){ card3.motionState = 'LOCKED'; card3.safety = 'TRIPPED'; }
  if(p.type === 'emergency_cleared'){ card3.motionState = 'IDLE'; card3.safety = 'ARMED'; }
  if(p.type === 'seq_complete'){ card3.motionState = 'IDLE'; card3.poseIdx = null; }

  renderCard3State();
}

function renderCard3State(){
  const motionEl = document.getElementById('c3-motion');
  motionEl.textContent = card3.motionState;
  motionEl.className = 'c3-state-val ' +
    (card3.motionState==='LOCKED' ? 'bad' : card3.motionState==='CYCLING' ? 'warn' : 'ok');

  document.getElementById('c3-pose').textContent =
    (card3.poseIdx!=null && card3.totalPoses) ? `${card3.poseIdx}/${card3.totalPoses}` : (card3.poseIdx!=null ? String(card3.poseIdx) : '—');

  document.getElementById('c3-cycle').textContent = card3.cycleNum!=null ? `#${card3.cycleNum}` : '—';

  const safetyEl = document.getElementById('c3-safety');
  safetyEl.textContent = card3.safety;
  safetyEl.className = 'c3-state-val ' + (card3.safety === 'TRIPPED' ? 'bad' : 'ok');

  // TCP link state mirrors the real qnx_connected system_status, not an
  // inference from telemetry timing.
  const tcpEl = document.getElementById('c3-tcp');
  if(tcpEl){
    tcpEl.textContent = topo.qnxOnline === true ? 'CONNECTED' : topo.qnxOnline === false ? 'DISCONNECTED' : '—';
    tcpEl.className = 'c3-state-val ' + (topo.qnxOnline === true ? 'ok' : topo.qnxOnline === false ? 'bad' : '');
  }
}

// A dispatched sequence (Card 1 direction, but Card 3 needs to know the
// total pose count too, and it's also how we know QNX is about to cycle).
function _observeSequenceForCard3(env){
  const p = env.payload || {};
  if(p.type === 'rtos_sequence' || p.type === 'send_sequence'){
    card3.totalPoses = p.total_poses || (p.sequence ? p.sequence.length : null);
    card3.motionState = 'CYCLING';
    renderCard3State();
  }
}

// Telemetry rate (redesign §26 "TELEMETRY RATE") — computed client-side
// from the real inter-arrival times of genuine qnx_to_bridge angle
// envelopes. Never a fabricated/fixed number; reads "—" until enough
// real samples exist.
function _trackTelemetryRate(){
  const now = performance.now();
  card3.telemetryTimestamps.push(now);
  const cutoff = now - 3000;
  while(card3.telemetryTimestamps.length && card3.telemetryTimestamps[0] < cutoff) card3.telemetryTimestamps.shift();
  const rateEl = document.getElementById('c3-rate');
  if(rateEl){
    if(card3.telemetryTimestamps.length >= 2){
      const hz = card3.telemetryTimestamps.length / 3;
      rateEl.textContent = hz.toFixed(1) + ' Hz';
    } else {
      rateEl.textContent = '—';
    }
  }
}
function _updateLastTelemetryTime(){
  card3.lastTelemetryAt = new Date();
  const el = document.getElementById('c3-latest-time');
  if(el) el.textContent = 'Last telemetry: ' + card3.lastTelemetryAt.toLocaleTimeString('en-GB',{hour12:false});
}

function driveArm(angles){
  const ids = ['arm-j0','arm-j1','arm-j2','arm-j3','arm-j4'];
  const prev = card3._prevAngles || [];
  for(let i=0;i<5;i++){
    const g = document.getElementById(ids[i]);
    if(g && angles[i]!=null){
      // Map 0-180 servo range to a readable -90..+90 visual swing so the
      // stylized arm doesn't just spin through a full half-circle for
      // every pose (purely a display mapping, not the real servo angle).
      const visualDeg = (angles[i] - 90);
      g.style.transform = `rotate(${visualDeg}deg)`;
    }
  }
  const labels = ['jr-j0','jr-j1','jr-j2','jr-j3','jr-j4'];
  const moveIds = ['jr-move-j0','jr-move-j1','jr-move-j2','jr-move-j3','jr-move-j4'];
  for(let i=0;i<5;i++){
    const el = document.getElementById(labels[i]);
    if(el && angles[i]!=null){ el.textContent = angles[i].toFixed(1)+'°'; el.classList.add('live'); }
    const moveEl = document.getElementById(moveIds[i]);
    if(moveEl && angles[i]!=null && prev[i]!=null){
      const d = angles[i] - prev[i];
      if(Math.abs(d) < 0.15){ moveEl.textContent = '●'; moveEl.className = 'jr-move'; }
      else if(d > 0){ moveEl.textContent = '↑'; moveEl.className = 'jr-move up'; }
      else { moveEl.textContent = '↓'; moveEl.className = 'jr-move down'; }
    }
  }
  const grip = document.getElementById('jr-grip');
  if(grip && angles[5]!=null){ grip.textContent = Math.round(angles[5])+'%'; grip.classList.add('live'); }
  const gripMove = document.getElementById('jr-move-j5');
  if(gripMove && angles[5]!=null && prev[5]!=null){
    const d = angles[5]-prev[5];
    if(Math.abs(d) < 0.5){ gripMove.textContent='●'; gripMove.className='jr-move'; }
    else if(d>0){ gripMove.textContent='↑'; gripMove.className='jr-move up'; }
    else { gripMove.textContent='↓'; gripMove.className='jr-move down'; }
  }
  card3._prevAngles = angles.slice();
}

// ═══════════════════════════════════════════════════════════════════
//  STATS
// ═══════════════════════════════════════════════════════════════════
function updateStats(env){
  stats.total++;
  stats.byCategory[env.category] = (stats.byCategory[env.category]||0) + 1;
  stats.sizeSum += (env.size_bytes||0);
  const now = Date.now();
  stats.windowTimestamps.push(now);
  const cutoff = now - 3000;
  while(stats.windowTimestamps.length && stats.windowTimestamps[0] < cutoff) stats.windowTimestamps.shift();

  // Feed Card 3's sequence/total-pose awareness here too (any direction
  // could carry it, but it's sent browser_to_bridge in practice).
  _observeSequenceForCard3(env);
}

function updateLatency(env){
  const isCommandish = (env.direction === 'browser_to_bridge') &&
    (env.category === 'command' || env.category === 'sequence' || env.category === 'emergency');
  const isResponse = (env.direction === 'qnx_to_bridge' || env.direction === 'bridge_to_browser') &&
    (env.category === 'telemetry' || env.category === 'ack' || env.category === 'emergency' || env.category === 'system_status');

  if(isCommandish){
    pendingCommand = { desc: describe(env), ts: env.timestamp_ns };
    return;
  }
  if(isResponse && pendingCommand){
    const latencyMs = (env.timestamp_ns - pendingCommand.ts) / 1e6;
    if(latencyMs >= 0 && latencyMs < 10000){
      timelineEntries.unshift({ cmdDesc: pendingCommand.desc, respDesc: describe(env), latencyMs, at: Date.now() });
      if(timelineEntries.length > 30) timelineEntries.pop();
      document.getElementById('stat-latency').textContent = latencyMs.toFixed(1);
      renderTimeline();
    }
    pendingCommand = null;
  }
}

// ═══════════════════════════════════════════════════════════════════
//  DECODING
// ═══════════════════════════════════════════════════════════════════
function describe(env){
  const p = env.payload || {};
  switch(p.type){
    case 'angles': {
      const a = p.angles || [];
      const j = a.map((v,i)=> i===5 ? `Gripper=${Math.round(v)}%` : `J${i}=${v.toFixed(1)}°`).join(' ');
      return `Joint telemetry — ${j} (packet #${p.packets})`;
    }
    case 'status':
      return `QNX ${p.qnx_connected ? 'connected' : 'disconnected'}${p.addr && p.addr!=='—' ? ' — '+p.addr : ''}`;
    case 'browser_status':
      return `Robotic dashboard ${p.browser_connected ? 'connected' : 'disconnected'} (${p.count} client${p.count===1?'':'s'})`;
    case 'emergency':
      return `EMERGENCY STOP — source: ${p.source || 'unknown'}`;
    case 'reset_emergency':
      return `Browser requested EMERGENCY RESET`;
    case 'emergency_cleared':
      return `QNX confirmed recovery — EMERGENCY CLEARED`;
    case 'watchdog':
      return `RTOS watchdog — deadline missed by ${Number(p.missed_ms).toFixed(1)}ms`;
    case 'pose_advance':
      return `QNX advanced to pose #${p.pose_idx}`;
    case 'cycle_done':
      return `RTOS cycle #${p.cycle_num} completed`;
    case 'seq_complete':
      return `RTOS cycling stopped cleanly (SEQ_COMPLETE)`;
    case 'rtos_sequence':
      return `Browser → QNX: ${p.total_poses} poses @ ${p.pose_duration_ms}ms/pose (RTOS)`;
    case 'send_sequence':
      return `Browser → QNX: ${p.total_poses} poses (legacy sequence)`;
    case 'stop_cycle':
      return `Browser requested graceful stop-after-cycle`;
    case 'set_mode':
      return `Browser switched operating mode → ${(p.mode||'').toUpperCase()}`;
    case 'seq_ack':
      return p.ok===false ? `Gateway REJECTED: ${p.msg}` : `Gateway acknowledged: ${p.msg}`;
    case 'fault_config':
      return `Test Lab: fault config ${p.ok ? 'applied' : 'rejected'} — ${p.detail||''}`;
    case 'qnx_tx':
      return `Bridge → QNX (wire): ${(p.lines||[]).join(' / ')}`;
    default:
      if(env.category === 'system_status')
        return `Gateway status snapshot`;
      if(env.category === 'security')
        return `Security alert — ${p.reason || ''} from ${p.ip || '?'}${p.blocked ? ' (IP BLOCKED)' : ''}`;
      return `${env.category} event`;
  }
}

// ═══════════════════════════════════════════════════════════════════
//  DETAIL VIEW (Item 3: click-to-expand per card)
// ═══════════════════════════════════════════════════════════════════
const CARD_TITLES = {
  card1: 'ROBOTIC DASHBOARD → BRIDGE — Transmission Inspector',
  card2: 'GATEWAY PROCESSING TRACE — every packet, both directions',
  card3: 'QNX → BRIDGE — Telemetry & Event Monitor',
};

// Card 1 filter tabs (redesign §5/§6): ALL / COMMANDS / SEQUENCES / SAFETY / MODE
const C1_FILTERS = [
  { id: 'all', label: 'ALL' },
  { id: 'commands', label: 'COMMANDS' },
  { id: 'sequences', label: 'SEQUENCES' },
  { id: 'safety', label: 'SAFETY' },
  { id: 'mode', label: 'MODE' },
];
// Card 3 filter tabs (redesign §30): ALL / TELEMETRY / POSE / CYCLE / SAFETY / SYSTEM
const C3_FILTERS = [
  { id: 'all', label: 'ALL' },
  { id: 'telemetry', label: 'TELEMETRY' },
  { id: 'pose', label: 'POSE' },
  { id: 'cycle', label: 'CYCLE' },
  { id: 'safety', label: 'SAFETY' },
  { id: 'system', label: 'SYSTEM' },
];

function openDetail(cardId){
  currentDetailCard = cardId;
  document.getElementById('cards-view').style.display = 'none';
  document.getElementById('detail-view').style.display = 'flex';
  document.getElementById('detail-title').textContent = CARD_TITLES[cardId];
  renderDetailSubfilters();
  renderPacketList(false);
}
function closeDetail(){
  currentDetailCard = null;
  document.getElementById('detail-view').style.display = 'none';
  document.getElementById('cards-view').style.display = 'grid';
}

function renderDetailSubfilters(){
  const wrap = document.getElementById('detail-subfilters');
  if(!wrap) return;
  if(currentDetailCard === 'card1'){
    wrap.style.display = 'block';
    wrap.innerHTML = `<div class="c1-filter-row">${C1_FILTERS.map(f =>
      `<button class="c1-filter-btn${f.id===c1ActiveFilter?' active':''}" onclick="setC1Filter('${f.id}')">${f.label}</button>`
    ).join('')}</div>`;
  } else if(currentDetailCard === 'card3'){
    wrap.style.display = 'block';
    wrap.innerHTML = `<div class="c3-filter-row">${C3_FILTERS.map(f =>
      `<button class="c1-filter-btn${f.id===c3ActiveFilter?' active':''}" onclick="setC3Filter('${f.id}')">${f.label}</button>`
    ).join('')}</div>`;
  } else {
    wrap.style.display = 'none';
    wrap.innerHTML = '';
  }
}
function setC1Filter(id){ c1ActiveFilter = id; renderDetailSubfilters(); renderPacketList(false); }
function setC3Filter(id){ c3ActiveFilter = id; renderDetailSubfilters(); renderPacketList(false); }

function _matchesC1Filter(env){
  if(c1ActiveFilter === 'all') return true;
  const p = env.payload || {};
  if(c1ActiveFilter === 'sequences') return env.category === 'sequence';
  if(c1ActiveFilter === 'safety') return env.category === 'emergency';
  if(c1ActiveFilter === 'mode') return p.type === 'set_mode';
  if(c1ActiveFilter === 'commands') return env.category === 'command';
  return true;
}
function _matchesC3Filter(env){
  if(c3ActiveFilter === 'all') return true;
  const p = env.payload || {};
  if(c3ActiveFilter === 'telemetry') return p.type === 'angles';
  if(c3ActiveFilter === 'pose') return p.type === 'pose_advance';
  if(c3ActiveFilter === 'cycle') return p.type === 'cycle_done' || p.type === 'seq_complete';
  if(c3ActiveFilter === 'safety') return env.category === 'emergency';
  if(c3ActiveFilter === 'system') return env.category === 'system_status' || p.type === 'watchdog';
  return true;
}

function _packetsForCurrentDetail(){
  if(currentDetailCard === 'card1') return packets.filter(p=>p.direction==='browser_to_bridge').filter(_matchesC1Filter);
  if(currentDetailCard === 'card3') return packets.filter(p=>p.direction==='qnx_to_bridge').filter(_matchesC3Filter);
  return packets; // card2 = everything
}

function renderPacketList(flashNewest){
  if(!currentDetailCard) return;
  const list = document.getElementById('packet-list');
  const visible = _packetsForCurrentDetail();
  list.innerHTML = '';
  visible.slice(0, 200).forEach((env, idx) => {
    const row = document.createElement('div');
    const replayed = !!env.replayed;
    row.className = 'pkt-row'
      + (env.packet_id===selectedPacketId ? ' selected':'')
      + (flashNewest && idx===0 ? ' new':'')
      + (replayed ? ' replayed':'');
    const t = new Date();
    row.innerHTML = `
      <div class="pkt-id">#${env.packet_id}</div>
      <div class="pkt-cat cat-${env.category}">${env.category}</div>
      <div class="pkt-desc">${escapeHtml(describe(env))}${replayed ? '<span class="pkt-replay-tag">REPLAYED</span>' : ''}</div>
      <div class="pkt-size">${env.size_bytes}B</div>
      <div class="pkt-time">${t.toLocaleTimeString('en-GB',{hour12:false})}</div>
    `;
    row.onclick = () => { selectedPacketId = env.packet_id; renderPacketList(false); renderInspector(env); };
    list.appendChild(row);
  });
}

function renderInspector(env){
  const box = document.getElementById('inspector');
  box.innerHTML = `
    <div class="insp-decoded">${escapeHtml(describe(env))}</div>
    <div class="insp-field"><div class="insp-label">PACKET ID</div><div class="insp-val">#${env.packet_id}</div></div>
    <div class="insp-field"><div class="insp-label">DIRECTION</div><div class="insp-val">${env.direction}</div></div>
    <div class="insp-field"><div class="insp-label">CATEGORY</div><div class="insp-val">${env.category}</div></div>
    <div class="insp-field"><div class="insp-label">TRANSPORT</div><div class="insp-val">${env.transport}</div></div>
    <div class="insp-field"><div class="insp-label">SIZE</div><div class="insp-val">${env.size_bytes} bytes</div></div>
    <div class="insp-field"><div class="insp-label">REAL / SIMULATED</div><div class="insp-val" style="color:${env.real?'var(--green)':'var(--amber)'}">${env.real ? 'REAL' : 'SIMULATED'}</div></div>
    <div class="insp-field"><div class="insp-label">REPLAYED</div><div class="insp-val">${env.replayed ? 'Yes — backfilled after reconnect' : 'No — received live'}</div></div>
    <div class="insp-field"><div class="insp-label">GATEWAY TIMESTAMP (monotonic ns)</div><div class="insp-val">${env.timestamp_ns}</div></div>
    <div class="insp-field"><div class="insp-label">RAW ENVELOPE</div><div class="insp-raw">${escapeHtml(JSON.stringify(env, null, 2))}</div></div>
  `;
}

// ═══════════════════════════════════════════════════════════════════
//  RENDERING — Stats
// ═══════════════════════════════════════════════════════════════════
const CATEGORY_COLORS = {
  telemetry:'#00d4ff', command:'#00ff88', ack:'#00ff88', sequence:'#8844ff',
  emergency:'#ff2244', system_status:'#ffaa00', error:'#ff2244', security:'#ff66cc'
};

function renderStats(){
  document.getElementById('stat-total').textContent = stats.total;
  document.getElementById('stat-rate').textContent = Math.round(stats.windowTimestamps.length / 3);
  document.getElementById('stat-size').textContent = stats.total ? Math.round(stats.sizeSum/stats.total) : 0;

  const wrap = document.getElementById('cat-breakdown');
  wrap.innerHTML = '';
  Object.keys(stats.byCategory).sort((a,b)=>stats.byCategory[b]-stats.byCategory[a]).forEach(cat=>{
    const row = document.createElement('div');
    row.className = 'cat-row';
    row.innerHTML = `<span class="cat-swatch" style="background:${CATEGORY_COLORS[cat]||'#888'}"></span>
      <span class="cat-name">${cat}</span><span class="cat-count">${stats.byCategory[cat]}</span>`;
    wrap.appendChild(row);
  });
}

// ═══════════════════════════════════════════════════════════════════
//  RENDERING — Topology (Item 1: connection vs activity, separated)
// ═══════════════════════════════════════════════════════════════════
function connState(kind){
  if(kind === 'gw') return topo.gwOnline ? 'on' : (topo.gwConnecting ? 'warn' : 'off');
  if(kind === 'qnx') return topo.qnxOnline===true ? 'on' : topo.qnxOnline===false ? 'off' : 'warn';
  if(kind === 'br') return topo.brOnline ? 'on' : 'off';
  return 'off';
}
const STATE_LABELS = { on: 'ONLINE', warn: 'CONNECTING', off: 'OFFLINE' };

function renderTopology(){
  const nQnx = document.getElementById('node-qnx');
  const nGw  = document.getElementById('node-gw');
  const nBr  = document.getElementById('node-br');

  const sQnx = connState('qnx'), sGw = connState('gw'), sBr = connState('br');
  const now = Date.now();
  const qnxActive = sQnx==='on' && (now - topo.lastQnxTrafficAt) < 2000;
  const gwActive  = sGw==='on'  && (now - topo.lastGwTrafficAt)  < 2000;
  const brActive  = sBr==='on'  && (now - topo.lastBrTrafficAt)  < 2000;

  nQnx.setAttribute('class', 'topo-node ' + sQnx + (qnxActive ? ' active-traffic' : ''));
  nGw.setAttribute('class',  'topo-node ' + sGw  + (gwActive  ? ' active-traffic' : ''));
  nBr.setAttribute('class',  'topo-node ' + sBr  + (brActive  ? ' active-traffic' : ''));

  document.getElementById('topo-state-qnx').textContent = STATE_LABELS[sQnx];
  document.getElementById('topo-state-gw').textContent  = STATE_LABELS[sGw];
  document.getElementById('topo-state-br').textContent  = STATE_LABELS[sBr];
  document.getElementById('topo-activity-qnx').textContent = qnxActive ? 'ACTIVE' : (sQnx==='on' ? 'idle' : '');
  document.getElementById('topo-activity-gw').textContent  = gwActive  ? 'ACTIVE' : (sGw==='on'  ? 'idle' : '');
  document.getElementById('topo-activity-br').textContent  = brActive  ? 'ACTIVE' : (sBr==='on'  ? 'idle' : '');

  document.getElementById('link-qnx-gw').classList.toggle('live', sQnx==='on' && sGw==='on');
  document.getElementById('link-gw-br').classList.toggle('live', sGw==='on' && sBr==='on');

  // Card 3's TCP-link readout mirrors the same real connection state.
  renderCard3State();
}
setInterval(renderTopology, 800);

function firePulse(hop){
  const el = document.getElementById('pulse-' + hop);
  if(!el) return;
  el.classList.add('active');
  clearTimeout(el._fadeTimer);
  el._fadeTimer = setTimeout(()=>el.classList.remove('active'), 700);
}

// ═══════════════════════════════════════════════════════════════════
//  RENDERING — Timeline
// ═══════════════════════════════════════════════════════════════════
function renderTimeline(){
  const wrap = document.getElementById('timeline-list');
  wrap.innerHTML = '';
  if(timelineEntries.length===0){
    wrap.innerHTML = '<div class="inspector-empty">No paired command→response events yet. Trigger RTOS send, emergency stop, or mode change on the robotic dashboard.</div>';
    return;
  }
  timelineEntries.forEach(e=>{
    const item = document.createElement('div');
    item.className = 'tl-item';
    item.innerHTML = `
      <div class="tl-pair"><span>→ ${escapeHtml(e.cmdDesc)}</span></div>
      <div class="tl-pair"><span>← ${escapeHtml(e.respDesc)}</span><span class="tl-lat">${e.latencyMs.toFixed(1)}ms</span></div>
    `;
    wrap.appendChild(item);
  });
}

// ═══════════════════════════════════════════════════════════════════
//  TEST LAB (Other-Problems §2/§3: redesigned Fault Injection & Experiment
//  Console + compact main-dashboard status widget)
// ═══════════════════════════════════════════════════════════════════

// Client-tracked state for what the *last known real* fault status was,
// used only to (a) detect start/stop transitions for the history list
// and (b) know when a fault genuinely started so we can compute elapsed
// time for the progress bar. All of this derives from real
// `test_lab_status` messages — nothing here is fabricated traffic.
const testLab = {
  lastActive: false,
  lastMode: null,
  lastRate: null,
  lastDurationS: null,
  activeStartedAt: null,   // Date, set when active transitions false->true
  lastKnownRemaining: null,
  history: [],             // bounded list of {mode, rate, durationS, startedAt, endedAt, status}
};
const TESTLAB_HISTORY_MAX = 12;

// Per fault type, the backend's "rate" field means something different
// (see fault_layer usage in bridge.py: packet_loss/corrupt = probability
// 0..1 shown here as %, delay = seconds). Only the three modes actually
// wired in bridge.py's Test Lab command handler are offered.
const FAULT_TYPE_META = {
  packet_loss: { label: 'DROP RATE (%)', toBackendRate: v => Math.min(1, Math.max(0, v/100)), defaultUi: 25 },
  delay:       { label: 'DELAY (ms)',    toBackendRate: v => Math.min(3000, Math.max(0, v))/1000, defaultUi: 400 },
  corrupt:     { label: 'CORRUPT RATE (%)', toBackendRate: v => Math.min(1, Math.max(0, v/100)), defaultUi: 30 },
};

function tlOnFaultTypeChange(){
  const type = document.getElementById('tl-fault-type').value;
  const meta = FAULT_TYPE_META[type];
  const rateLbl = document.getElementById('tl-rate-label');
  const rateInp = document.getElementById('tl-fault-rate');
  if(rateLbl) rateLbl.textContent = meta.label;
  if(rateInp) rateInp.value = meta.defaultUi;
}

function tlArmFault(){
  const type = document.getElementById('tl-fault-type').value;
  const meta = FAULT_TYPE_META[type];
  const uiRate = parseFloat(document.getElementById('tl-fault-rate').value) || meta.defaultUi;
  const duration = Math.max(1, parseInt(document.getElementById('tl-fault-duration').value, 10) || 30);
  const backendRate = meta.toBackendRate(uiRate);
  testLabFault(type, backendRate, duration);
}

function testLabFault(mode, rate, duration_s){
  if(!ws || ws.readyState !== 1 || !authOk){ toast('Not connected/authenticated'); return; }
  ws.send(JSON.stringify({ type: 'test_lab_fault_config', mode, rate, duration_s }));
  toast(`Requested: ${mode} @ rate ${rate} for ${duration_s}s`);
}
function testLabKill(){
  if(!ws || ws.readyState !== 1 || !authOk){ toast('Not connected/authenticated'); return; }
  ws.send(JSON.stringify({ type: 'test_lab_kill' }));
  toast('Kill switch sent — clearing all fault injection');
}

function renderTestLabStatus(fault, ids){
  fault = fault || { active: false };
  const active = !!fault.active;

  // ── Detect real start/stop transitions for the history log ──────
  if(active && !testLab.lastActive){
    testLab.activeStartedAt = new Date();
    testLab.lastKnownRemaining = fault.remaining_s;
  } else if(!active && testLab.lastActive){
    // Just ended — log it. "COMPLETED" if the last known remaining time
    // was at/near zero, otherwise treat as an early stop (kill switch or
    // a fresh reconfig cutting it short) — this is inferred from the
    // real last-seen remaining_s, not fabricated.
    const wasNearZero = testLab.lastKnownRemaining != null && testLab.lastKnownRemaining <= 1.5;
    testLab.history.unshift({
      mode: testLab.lastMode, rate: testLab.lastRate, durationS: testLab.lastDurationS,
      startedAt: testLab.activeStartedAt, endedAt: new Date(),
      status: wasNearZero ? 'completed' : 'stopped'
    });
    if(testLab.history.length > TESTLAB_HISTORY_MAX) testLab.history.pop();
    testLab.activeStartedAt = null;
  }
  if(active){
    testLab.lastMode = fault.mode;
    testLab.lastRate = fault.rate;
    testLab.lastDurationS = fault.duration_s != null ? fault.duration_s : testLab.lastDurationS;
    testLab.lastKnownRemaining = fault.remaining_s;
  }
  testLab.lastActive = active;

  // ── Main-dashboard compact widget ──
  const widget = document.getElementById('tl-dash-widget');
  const statusText = document.getElementById('tl-dash-status-text');
  const detailEl = document.getElementById('tl-dash-detail');
  if(widget && statusText && detailEl){
    widget.classList.toggle('active', active);
    if(active){
      statusText.textContent = 'FAULT ACTIVE';
      detailEl.innerHTML = `<b>${escapeHtml((fault.mode||'').toUpperCase())}</b> · rate ${escapeHtml(String(fault.rate))} · ${fault.remaining_s!=null ? fault.remaining_s.toFixed(1)+'s remaining' : ''}`;
    } else {
      statusText.textContent = 'INACTIVE';
      detailEl.textContent = 'No fault currently active.';
    }
  }

  // ── Overlay: Current Experiment panel ──
  const curPanel = document.getElementById('tl-current-panel');
  if(curPanel){
    if(active){
      const pct = (fault.duration_s && fault.remaining_s!=null)
        ? Math.max(0, Math.min(100, 100 * (1 - (fault.remaining_s / fault.duration_s))))
        : null;
      curPanel.innerHTML = `
        <div class="tl-current-type">🔴 FAULT ACTIVE — ${escapeHtml((fault.mode||'').toUpperCase())}</div>
        <div class="tl-current-path">Applied to: Bridge → Browser broadcast (:8765)</div>
        <div class="tl-progress-track"><div class="tl-progress-fill" style="width:${pct!=null?pct:50}%"></div></div>
        <div class="tl-progress-lbl">${fault.remaining_s!=null ? fault.remaining_s.toFixed(1)+'s remaining' : 'remaining time not reported'}</div>
        <button class="tl-kill-btn" onclick="testLabKill()">⛔ KILL / STOP FAULT</button>
      `;
    } else {
      curPanel.innerHTML = `<div class="tl-current-empty">● TEST LAB INACTIVE<br>No fault currently active.</div>`;
    }
  }

  // ── Overlay: Live Effect (only real fields) ──
  const liveMode = document.getElementById('tl-live-mode');
  const liveRate = document.getElementById('tl-live-rate');
  const liveRemaining = document.getElementById('tl-live-remaining');
  if(liveMode) liveMode.textContent = active ? (fault.mode||'—').toUpperCase() : '—';
  if(liveRate) liveRate.textContent = active ? String(fault.rate) : '—';
  if(liveRemaining) liveRemaining.textContent = active && fault.remaining_s!=null ? fault.remaining_s.toFixed(1) : '—';

  renderTestLabHistory();

  // ── IDS status (unchanged) ──
  const idsEl = document.getElementById('ids-status');
  const entries = Object.entries(ids || {});
  if(entries.length === 0){
    idsEl.innerHTML = 'No failed-auth activity recorded yet.';
    return;
  }
  idsEl.innerHTML = entries.map(([ip, rec]) => {
    const cls = rec.blocked ? 'ids-row-blocked' : 'ids-row-ok';
    const status = rec.blocked ? `BLOCKED (${rec.blocked_remaining_s}s left)` : `${rec.failures_in_window} failure(s) in window`;
    return `<div class="${cls}">${escapeHtml(ip)} — ${status}</div>`;
  }).join('');
}

function renderTestLabHistory(){
  const wrap = document.getElementById('tl-history-list');
  if(!wrap) return;
  if(testLab.history.length === 0){
    wrap.innerHTML = '<div class="tl-history-empty">No experiments run yet this session.</div>';
    return;
  }
  wrap.innerHTML = testLab.history.map(h => {
    const t = h.startedAt ? h.startedAt.toLocaleTimeString('en-GB',{hour12:false}) : '—';
    return `<div class="tl-history-item">
      <div>
        <div class="tl-history-main">${t} — ${escapeHtml((h.mode||'').toUpperCase())}</div>
        <div class="tl-history-sub">rate ${escapeHtml(String(h.rate))}${h.durationS?(' · '+h.durationS+'s'):''}</div>
      </div>
      <div class="tl-history-status ${h.status}">${h.status === 'completed' ? '✓ COMPLETED' : '✕ STOPPED'}</div>
    </div>`;
  }).join('');
}

document.getElementById('btn-testlab-toggle').addEventListener('click', () => {
  document.getElementById('testlab-overlay').classList.remove('testlab-hidden');
  if(ws && ws.readyState===1 && authOk){
    ws.send(JSON.stringify({ type: 'test_lab_status_query' }));
  }
});
document.getElementById('btn-testlab-close').addEventListener('click', () => {
  document.getElementById('testlab-overlay').classList.add('testlab-hidden');
});

// ═══════════════════════════════════════════════════════════════════
//  UTIL
// ═══════════════════════════════════════════════════════════════════
function escapeHtml(s){
  return String(s).replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

let toastTimer=null;
function toast(msg){
  const el = document.getElementById('toast');
  el.textContent = msg; el.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(()=>el.classList.remove('show'), 3200);
}

function log(msg){ console.log('[COMM-DASH] ' + msg); }

// ═══════════════════════════════════════════════════════════════════
//  UI WIRING
// ═══════════════════════════════════════════════════════════════════
document.getElementById('btn-clear').addEventListener('click', ()=>{
  packets = []; selectedPacketId = null;
  renderPacketList(false);
  document.getElementById('inspector').innerHTML = '<div class="inspector-empty">Select a packet to inspect its envelope.</div>';
});
document.getElementById('btn-pause').addEventListener('click', (e)=>{
  paused = !paused;
  e.target.textContent = paused ? '▶ RESUME' : '⏸ PAUSE';
  e.target.classList.toggle('active', paused);
  if(!paused) renderPacketList(false);
});

// ═══════════════════════════════════════════════════════════════════
//  BOOT
// ═══════════════════════════════════════════════════════════════════
renderTopology();
renderStats();
renderTimeline();
connect();