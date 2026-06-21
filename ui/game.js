/**
 * Ring of the Middle Earth — Game Client (Vanilla JS + SSE)
 * No framework dependencies. Uses Server-Sent Events for real-time updates.
 */

'use strict';

// ── State ──────────────────────────────────────────────────────────────────────
const state = {
  side:         null,         // 'light' | 'dark'
  playerId:     null,
  turn:         1,
  units:        {},           // unitId → UnitPublic
  regions:      {},           // regionId → RegionState
  paths:        {},           // pathId → PathState
  selectedUnit: null,
  rbRegion:     null,         // Light Side only
  lastDetected: null,         // Dark Side only
  eventSource:  null,
  timerInterval:   null,
  pollingInterval: null,      // periodic /game/state poll for game-over detection
  timerSeconds:  60,
  gameOver:      false,       // set when game ends
};

// ── Map Layout ─────────────────────────────────────────────────────────────────
// SVG viewBox koordinatları (1400x1000, map-content transform="translate(0,50)")
// Node pozisyonları SVG'den alındı
const REGION_COORDS = {
  'the-shire':    { x: 100,  y: 300 },  // SVG: translate(100,250) + offset 50
  'bree':         { x: 250,  y: 300 },
  'tharbad':      { x: 250,  y: 450 },
  'weathertop':   { x: 400,  y: 200 },
  'rivendell':    { x: 550,  y: 200 },
  'fangorn':      { x: 500,  y: 500 },
  'fords-of-isen':{ x: 350,  y: 600 },
  'rohan-plains': { x: 650,  y: 550 },
  'moria':        { x: 600,  y: 350 },
  'helms-deep':   { x: 500,  y: 750 },
  'isengard':     { x: 350,  y: 700 },
  'edoras':       { x: 650,  y: 700 },
  'lothlorien':   { x: 700,  y: 350 },
  'dead-marshes': { x: 1000, y: 300 },
  'emyn-muil':    { x: 850,  y: 400 },
  'minas-tirith': { x: 850,  y: 700 },
  'ithilien':     { x: 1000, y: 500 },
  'osgiliath':    { x: 1000, y: 700 },
  'minas-morgul': { x: 1150, y: 700 },
  'cirith-ungol': { x: 1150, y: 500 },
  'mordor':       { x: 1150, y: 300 },
  'mount-doom':   { x: 1300, y: 400 },
};

// cost=1 → solid, cost=2 → dashed (map.conf'tan)
const PATH_EDGES = [
  { from:'the-shire',    to:'bree',          cost:1, id:'shire-to-bree' },
  { from:'bree',         to:'weathertop',    cost:1, id:'bree-to-weathertop' },
  { from:'bree',         to:'rivendell',     cost:2, id:'bree-to-rivendell' },
  { from:'bree',         to:'tharbad',       cost:1, id:'bree-to-tharbad' },
  { from:'the-shire',    to:'tharbad',       cost:2, id:'shire-to-tharbad' },
  { from:'weathertop',   to:'rivendell',     cost:1, id:'weathertop-to-rivendell' },
  { from:'rivendell',    to:'moria',         cost:2, id:'rivendell-to-moria' },
  { from:'rivendell',    to:'lothlorien',    cost:2, id:'rivendell-to-lothlorien' },
  { from:'moria',        to:'lothlorien',    cost:1, id:'moria-to-lothlorien' },
  { from:'lothlorien',   to:'emyn-muil',     cost:1, id:'lothlorien-to-emyn-muil' },
  { from:'lothlorien',   to:'rohan-plains',  cost:1, id:'lothlorien-to-rohan-plains' },
  { from:'rohan-plains', to:'fangorn',       cost:1, id:'rohan-plains-to-fangorn' },
  { from:'rohan-plains', to:'edoras',        cost:1, id:'rohan-plains-to-edoras' },
  { from:'rohan-plains', to:'minas-tirith',  cost:2, id:'rohan-plains-to-minas-tirith' },
  { from:'fangorn',      to:'isengard',      cost:1, id:'fangorn-to-isengard' },
  { from:'isengard',     to:'rohan-plains',  cost:1, id:'isengard-to-rohan-plains' },
  { from:'tharbad',      to:'fords-of-isen', cost:2, id:'tharbad-to-fords-of-isen' },
  { from:'fords-of-isen',to:'isengard',      cost:1, id:'fords-of-isen-to-isengard' },
  { from:'fords-of-isen',to:'helms-deep',    cost:1, id:'fords-of-isen-to-helms-deep' },
  { from:'fords-of-isen',to:'edoras',        cost:1, id:'fords-of-isen-to-edoras' },
  { from:'edoras',       to:'helms-deep',    cost:1, id:'edoras-to-helms-deep' },
  { from:'helms-deep',   to:'isengard',      cost:1, id:'helms-deep-to-isengard' },
  { from:'edoras',       to:'minas-tirith',  cost:2, id:'edoras-to-minas-tirith' },
  { from:'emyn-muil',    to:'dead-marshes',  cost:1, id:'emyn-muil-to-dead-marshes' },
  { from:'emyn-muil',    to:'ithilien',      cost:2, id:'emyn-muil-to-ithilien' },
  { from:'dead-marshes', to:'ithilien',      cost:1, id:'dead-marshes-to-ithilien' },
  { from:'dead-marshes', to:'mordor',        cost:2, id:'dead-marshes-to-mordor' },
  { from:'ithilien',     to:'minas-tirith',  cost:1, id:'ithilien-to-minas-tirith' },
  { from:'ithilien',     to:'osgiliath',     cost:1, id:'ithilien-to-osgiliath' },
  { from:'ithilien',     to:'cirith-ungol',  cost:2, id:'ithilien-to-cirith-ungol' },
  { from:'minas-tirith', to:'osgiliath',     cost:1, id:'minas-tirith-to-osgiliath' },
  { from:'osgiliath',    to:'minas-morgul',  cost:1, id:'osgiliath-to-minas-morgul' },
  { from:'minas-morgul', to:'cirith-ungol',  cost:1, id:'minas-morgul-to-cirith-ungol' },
  { from:'minas-morgul', to:'mordor',        cost:1, id:'minas-morgul-to-mordor' },
  { from:'cirith-ungol', to:'mordor',        cost:1, id:'cirith-ungol-to-mordor' },
  { from:'cirith-ungol', to:'mount-doom',    cost:2, id:'cirith-ungol-to-mount-doom' },
  { from:'mordor',       to:'mount-doom',    cost:1, id:'mordor-to-mount-doom' },
];

// Path id → edge objesi lookup
const PATH_BY_ID = {};
PATH_EDGES.forEach(e => { PATH_BY_ID[e.id] = e; });

const TERRAIN_COLORS = {
  PLAINS:   '#4a6741', MOUNTAINS: '#7a6a5a', FOREST: '#2d6b3a',
  FORTRESS: '#5a4a3a', VOLCANIC:  '#6b2020', SWAMP:  '#3d5a3a',
};

const SIDE_COLORS = {
  FREE_PEOPLES: '#4a9eff',
  SHADOW:       '#c0392b',
  NEUTRAL:      '#5a5a7a',
};

// SVG ViewBox boyutları
const SVG_W = 1400, SVG_H = 1000;

// ── Entry Point ────────────────────────────────────────────────────────────────
function chooseSide(side, playerId = null) {
  state.side     = side;
  state.playerId = playerId || `player-${side}-${Date.now()}`;

  document.getElementById('side-select-screen').classList.add('hidden');
  document.getElementById('game-ui').classList.remove('hidden');

  const badge = document.getElementById('side-badge');
  badge.textContent = side === 'light' ? '⚔ Free Peoples' : '🔥 The Shadow';
  badge.className = `side-badge ${side}`;

  initCanvas();
  connectSSE();
  fetchGameState();
  startTurnTimer();
  startGameStatePolling(); // Polls /game/state every 5s to catch game-over
                           // regardless of which backend instance serves SSE.

  log(`Connected as ${side === 'light' ? 'Light Side' : 'Dark Side'}`, 'important');
}

// ── SSE Connection ─────────────────────────────────────────────────────────────
function connectSSE() {
  const url = `/events?playerId=${state.playerId}&side=${state.side}`;
  const es  = new EventSource(url);
  state.eventSource = es;

  es.addEventListener('connected', () => {
    document.getElementById('connection-status').className = 'connection-dot connected';
    log('SSE stream connected', 'success');
  });

  es.addEventListener('game.broadcast', e => {
    const snap = JSON.parse(e.data);
    applySnapshot(snap);
  });

  es.addEventListener('game.ring.position', e => {
    if (state.side !== 'light') return;
    const { trueRegion } = JSON.parse(e.data);
    state.rbRegion = trueRegion;
    log(`Ring Bearer moved to: ${trueRegion}`, 'important');
    redrawMap();
  });

  es.addEventListener('game.ring.detection', e => {
    if (state.side !== 'dark') return;
    const d = JSON.parse(e.data);
    if (d.regionId) {
      state.lastDetected = d.regionId;
      log(`⚠ Ring Bearer DETECTED at ${d.regionId}!`, 'danger');
    }
    if (d.pathId && d.kind === 'SPOTTED') {
      log(`Ring Bearer spotted on path ${d.pathId}`, 'warning');
    }
    redrawMap();
  });

  es.addEventListener('game.events.path', e => {
    const { pathId, newStatus, surveillanceLevel } = JSON.parse(e.data);
    if (state.paths[pathId]) {
      state.paths[pathId].status           = newStatus;
      state.paths[pathId].surveillanceLevel = surveillanceLevel;
    }
    redrawMap();
  });

  // Game-over via game.session SSE event
  es.addEventListener('game.session', e => {
    try {
      const sess = JSON.parse(e.data);
      if (sess.gameOver) {
        showGameOver(sess.gameOverWinner, sess.gameOverCause, sess.gameOverTurn);
      }
      // Also update turn display if present
      if (sess.turn) {
        state.turn = sess.turn;
        document.getElementById('turn-number').textContent = sess.turn;
      }
    } catch (_) {}
  });

  es.onerror = () => {
    document.getElementById('connection-status').className = 'connection-dot disconnected';
    log('SSE connection lost — reconnecting…', 'warning');
    setTimeout(connectSSE, 3000);
  };
}

// ── Game State Fetch ───────────────────────────────────────────────────────────
async function fetchGameState() {
  try {
    const r = await fetch(`/game/state?side=${state.side}&playerId=${state.playerId}`);
    const data = await r.json();
    applySnapshot(data);
  } catch (e) {
    log('Failed to fetch game state', 'warning');
  }
}

// Poll /game/state every 5 seconds to detect game-over.
// This is necessary because SSE connections are load-balanced across Go
// instances but Kafka events are consumed per-instance — game.session may
// arrive on an instance that is NOT serving this browser's SSE stream.
function startGameStatePolling() {
  if (state.pollingInterval) clearInterval(state.pollingInterval);
  state.pollingInterval = setInterval(async () => {
    if (state.gameOver) {
      clearInterval(state.pollingInterval);
      state.pollingInterval = null;
      return;
    }
    try {
      const r = await fetch(`/game/state?side=${state.side}&playerId=${state.playerId}`);
      const data = await r.json();
      // Only apply game-over — avoid overwriting map state with stale data
      if (data.gameOver) {
        showGameOver(data.gameOverWinner, data.gameOverCause, data.gameOverTurn);
      }
    } catch (_) {}
  }, 5000);
}

function applySnapshot(snap) {
  if (snap.turn) {
    state.turn = snap.turn;
    document.getElementById('turn-number').textContent = snap.turn;
  }
  if (snap.ringBearerTrueRegion && state.side === 'light') {
    state.rbRegion = snap.ringBearerTrueRegion;
  }
  if (snap.units) {
    snap.units.forEach(u => { state.units[u.id] = u; });
  }
  if (snap.regions) {
    snap.regions.forEach(r => { state.regions[r.id] = r; });
  }
  if (snap.paths) {
    snap.paths.forEach(p => {
      state.paths[p.id] = {
        status: p.newStatus,
        surveillanceLevel: p.surveillanceLevel,
        tempOpenTurns: p.tempOpenTurns,
      };
    });
  }
  // Handle game-over coming from /game/state initial fetch
  if (snap.gameOver) {
    showGameOver(snap.gameOverWinner, snap.gameOverCause, snap.gameOverTurn);
  }
  renderUnitsList();
  redrawMap();
}

// ── Game Over ──────────────────────────────────────────────────────────────────
function showGameOver(winner, cause, turn) {
  if (state.gameOver) return; // prevent double-show
  state.gameOver = true;

  // Stop the turn timer
  clearInterval(state.timerInterval);
  state.timerInterval = null;
  document.getElementById('turn-timer').textContent = '—';
  document.getElementById('turn-timer').classList.remove('urgent');

  // Determine if this side won or lost
  const mySideWon = (state.side === 'light' && winner === 'FREE_PEOPLES') ||
                    (state.side === 'dark'  && winner === 'SHADOW');
  const isDraw    = winner === 'DRAW';

  let titleText, causeText;
  if (isDraw) {
    titleText = '⚖ Draw — Time Has Run Out';
    causeText = `Maximum turns reached (Turn ${turn})`;
  } else if (mySideWon) {
    titleText = state.side === 'light' ? '🌟 Victory! The Ring is Destroyed!' : '🔥 Victory! The Ring Bearer is Caught!';
    causeText = cause === 'RING_DESTROYED'
      ? `Frodo destroyed the Ring at Mount Doom on Turn ${turn}`
      : `The Nazgûl captured the Ring Bearer on Turn ${turn}`;
  } else {
    titleText = state.side === 'light' ? '💀 Defeat — The Shadow Prevails' : '💀 Defeat — The Ring Reaches Mount Doom';
    causeText = cause === 'RING_DESTROYED'
      ? `The Ring was destroyed at Mount Doom on Turn ${turn}`
      : `The Nazgûl captured the Ring Bearer on Turn ${turn}`;
  }

  document.getElementById('gameover-title').textContent = titleText;
  document.getElementById('gameover-cause').textContent  = causeText;
  document.getElementById('gameover-overlay').className  =
    `gameover-overlay ${mySideWon ? 'victory' : isDraw ? 'draw' : 'defeat'}`;

  log(`🏁 GAME OVER — ${winner} wins (${cause}) at turn ${turn}`, 'important');
}

// ── SVG Map ────────────────────────────────────────────────────────────────────
let mapSvgOverlay = null;

function initCanvas() {
  const panel = document.getElementById('map-panel');

  // Statik SVG haritası + dinamik overlay SVG'si
  panel.innerHTML = `
    <div id="map-svg-wrap" style="position:relative;width:100%;height:100%;">
      <svg id="map-static" xmlns="http://www.w3.org/2000/svg"
        viewBox="0 0 ${SVG_W} ${SVG_H}" preserveAspectRatio="xMidYMid meet"
        style="position:absolute;inset:0;width:100%;height:100%;">
        ${getStaticMapSVG()}
      </svg>
      <svg id="map-overlay" xmlns="http://www.w3.org/2000/svg"
        viewBox="0 0 ${SVG_W} ${SVG_H}" preserveAspectRatio="xMidYMid meet"
        style="position:absolute;inset:0;width:100%;height:100%;pointer-events:none;">
        <defs>
          <filter id="ov-glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="6" result="blur"/>
            <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
          </filter>
          <filter id="ov-glow-sm" x="-30%" y="-30%" width="160%" height="160%">
            <feGaussianBlur stdDeviation="3" result="blur"/>
            <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
          </filter>
        </defs>
        <g id="overlay-paths"></g>
        <g id="overlay-nodes"></g>
        <style>
          @keyframes svgPulse {
            0%,100% { opacity:1; r:24; }
            50%      { opacity:0.5; r:30; }
          }
          @keyframes svgBlink {
            0%,100% { opacity:1; }
            50%      { opacity:0.2; }
          }
          .rb-pulse   { animation: svgPulse 1.8s ease-in-out infinite; }
          .det-blink  { animation: svgBlink 0.7s step-end infinite; }
          .path-anim  { animation: svgBlink 1s ease-in-out infinite; }
        </style>
      </svg>
      <div id="tooltip" class="map-tooltip hidden"></div>
    </div>`;

  mapSvgOverlay = document.getElementById('map-overlay');

  // Tıklama event'ini statik SVG'ye bağla
  document.getElementById('map-static').addEventListener('click', onMapClick);
  document.getElementById('map-static').addEventListener('mousemove', onMapHover);
  document.getElementById('map-static').style.pointerEvents = 'all';
  document.getElementById('map-static').style.cursor = 'pointer';

  redrawMap();
}

function resizeCanvas() {
  // SVG viewBox tabanlı, resize gerekmiyor — preserveAspectRatio halleder
}

// PATH durumuna göre renk
function pathColor(pathId) {
  const ps = state.paths[pathId];
  if (!ps) return null; // statik SVG rengini koru
  switch (ps.status) {
    case 'BLOCKED':          return '#e74c3c'; // kırmızı
    case 'TEMPORARILY_OPEN': return '#00d2d3'; // cyan
    case 'THREATENED':       return '#f39c12'; // turuncu
    default:                 return null;       // OPEN → statik rengi göster
  }
}

function redrawMap() {
  if (!mapSvgOverlay) return;

  const oPaths = document.getElementById('overlay-paths');
  const oNodes = document.getElementById('overlay-nodes');
  oPaths.innerHTML = '';
  oNodes.innerHTML = '';

  // 1. Yolları çiz — sadece özel durumda (BLOCKED/THREATENED/TEMP_OPEN) renkli overlay
  PATH_EDGES.forEach(edge => {
    const pa = REGION_COORDS[edge.from], pb = REGION_COORDS[edge.to];
    if (!pa || !pb) return;
    const color = pathColor(edge.id);
    if (!color) return; // OPEN → statik SVG yeterli

    const isBlocked = state.paths[edge.id]?.status === 'BLOCKED';
    const isTempOpen = state.paths[edge.id]?.status === 'TEMPORARILY_OPEN';
    const dash = edge.cost === 2 ? '12,10' : (isBlocked ? '8,6' : 'none');

    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    line.setAttribute('x1', pa.x); line.setAttribute('y1', pa.y);
    line.setAttribute('x2', pb.x); line.setAttribute('y2', pb.y);
    line.setAttribute('stroke', color);
    line.setAttribute('stroke-width', isBlocked ? '6' : '5');
    line.setAttribute('stroke-linecap', 'round');
    if (dash !== 'none') line.setAttribute('stroke-dasharray', dash);
    if (isTempOpen) line.classList.add('path-anim');
    line.setAttribute('filter', 'url(#ov-glow-sm)');
    line.setAttribute('opacity', '0.9');
    oPaths.appendChild(line);

    // Surveillance badge (gözetleme seviyesi)
    const sl = state.paths[edge.id]?.surveillanceLevel;
    if (sl && sl > 0) {
      const mx = (pa.x + pb.x) / 2, my = (pa.y + pb.y) / 2;
      const bg = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      bg.setAttribute('cx', mx); bg.setAttribute('cy', my); bg.setAttribute('r', 11);
      bg.setAttribute('fill', '#8b0000'); bg.setAttribute('opacity', '0.9');
      oPaths.appendChild(bg);
      const txt = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      txt.setAttribute('x', mx); txt.setAttribute('y', my + 5);
      txt.setAttribute('text-anchor', 'middle');
      txt.setAttribute('font-size', '12');
      txt.setAttribute('font-weight', 'bold');
      txt.setAttribute('fill', '#fff');
      txt.textContent = `S${sl}`;
      oPaths.appendChild(txt);
    }
  });

  // 2. Node overlay'leri — birimler, ring bearer, detection
  Object.entries(REGION_COORDS).forEach(([id, pos]) => {
    const region = state.regions[id] || {};
    const ctrl   = region.controlledBy || 'NEUTRAL';
    const x = pos.x, y = pos.y;

    // Kontrol rengi çemberi (overlay)
    const ctrlColor = SIDE_COLORS[ctrl];
    const ctrlRing = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    ctrlRing.setAttribute('cx', x); ctrlRing.setAttribute('cy', y); ctrlRing.setAttribute('r', 22);
    ctrlRing.setAttribute('fill', 'none');
    ctrlRing.setAttribute('stroke', ctrlColor);
    ctrlRing.setAttribute('stroke-width', ctrl === 'NEUTRAL' ? '0' : '3');
    ctrlRing.setAttribute('opacity', '0.7');
    oNodes.appendChild(ctrlRing);

    // Fortification halkası
    if (region.fortified) {
      const fort = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      fort.setAttribute('cx', x); fort.setAttribute('cy', y); fort.setAttribute('r', 26);
      fort.setAttribute('fill', 'none');
      fort.setAttribute('stroke', '#f0d080');
      fort.setAttribute('stroke-width', '3');
      fort.setAttribute('stroke-dasharray', '5,3');
      oNodes.appendChild(fort);
    }

    // Ring Bearer marker (Light Side)
    if (state.side === 'light' && state.rbRegion === id) {
      const rb = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      rb.setAttribute('cx', x); rb.setAttribute('cy', y); rb.setAttribute('r', 24);
      rb.setAttribute('fill', 'none');
      rb.setAttribute('stroke', '#f0d080');
      rb.setAttribute('stroke-width', '3');
      rb.classList.add('rb-pulse');
      rb.setAttribute('filter', 'url(#ov-glow)');
      oNodes.appendChild(rb);

      // 💍 sembol
      const ring = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      ring.setAttribute('x', x); ring.setAttribute('y', y - 28);
      ring.setAttribute('text-anchor', 'middle');
      ring.setAttribute('font-size', '16'); ring.setAttribute('fill', '#f0d080');
      ring.textContent = '💍';
      oNodes.appendChild(ring);
    }

    // Dark Side detection marker
    if (state.side === 'dark' && state.lastDetected === id) {
      const det = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      det.setAttribute('cx', x); det.setAttribute('cy', y); det.setAttribute('r', 28);
      det.setAttribute('fill', 'none');
      det.setAttribute('stroke', '#e74c3c');
      det.setAttribute('stroke-width', '3');
      det.classList.add('det-blink');
      oNodes.appendChild(det);

      const eye = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      eye.setAttribute('x', x); eye.setAttribute('y', y - 32);
      eye.setAttribute('text-anchor', 'middle');
      eye.setAttribute('font-size', '14'); eye.setAttribute('fill', '#e74c3c');
      eye.textContent = '👁';
      oNodes.appendChild(eye);
    }

    // Birim noktaları (orbiting dots)
    const unitsHere = Object.values(state.units).filter(u =>
      u.currentRegion === id && u.status === 'ACTIVE'
    );
    unitsHere.forEach((u, i) => {
      const angle = (i / Math.max(unitsHere.length, 1)) * Math.PI * 2 - Math.PI / 2;
      const dx = Math.cos(angle) * 32, dy = Math.sin(angle) * 32;
      const dot = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      dot.setAttribute('cx', x + dx); dot.setAttribute('cy', y + dy); dot.setAttribute('r', 7);
      dot.setAttribute('fill', u.side === 'FREE_PEOPLES' ? '#4a9eff' : '#c0392b');
      dot.setAttribute('stroke', '#fff'); dot.setAttribute('stroke-width', '1.5');
      dot.setAttribute('filter', 'url(#ov-glow-sm)');
      oNodes.appendChild(dot);

      // Birim baş harfi
      const lbl = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      lbl.setAttribute('x', x + dx); lbl.setAttribute('y', y + dy + 4);
      lbl.setAttribute('text-anchor', 'middle');
      lbl.setAttribute('font-size', '8'); lbl.setAttribute('font-weight', 'bold');
      lbl.setAttribute('fill', '#fff'); lbl.setAttribute('pointer-events', 'none');
      lbl.textContent = u.id.charAt(0).toUpperCase();
      oNodes.appendChild(lbl);
    });
  });
}

// SVG koordinatı → region id
function getRegionAt(svgX, svgY) {
  for (const [id, pos] of Object.entries(REGION_COORDS)) {
    const dx = svgX - pos.x, dy = svgY - pos.y;
    if (dx*dx + dy*dy <= 900) return id; // 30px radius — geniş hit area
  }
  return null;
}

// Mouse event koordinatını SVG viewBox koordinatına çevir
// getScreenCTM() kullanarak preserveAspectRatio letterboxing'i ve
// tüm transform'ları doğru hesaplar.
function clientToSvg(svgEl, clientX, clientY) {
  const pt = svgEl.createSVGPoint();
  pt.x = clientX;
  pt.y = clientY;
  return pt.matrixTransform(svgEl.getScreenCTM().inverse());
}

function onMapClick(e) {
  const svgEl = document.getElementById('map-static');
  const { x, y } = clientToSvg(svgEl, e.clientX, e.clientY);
  const regionId = getRegionAt(x, y);
  if (!regionId) return;

  // Select unit in that region
  const unitsHere = Object.values(state.units).filter(u =>
    u.currentRegion === regionId && u.side === (state.side === 'light' ? 'FREE_PEOPLES' : 'SHADOW')
  );
  if (unitsHere.length > 0) selectUnit(unitsHere[0].id);
}

function onMapHover(e) {
  const svgEl = document.getElementById('map-static');
  const { x, y } = clientToSvg(svgEl, e.clientX, e.clientY);
  const regionId = getRegionAt(x, y);
  const tooltip  = document.getElementById('tooltip');
  if (regionId) {
    const r  = state.regions[regionId] || {};
    const ps = Object.entries(state.paths)
      .filter(([id, p]) => PATH_BY_ID[id] && (PATH_BY_ID[id].from === regionId || PATH_BY_ID[id].to === regionId))
      .map(([id, p]) => `${id.replace(/-/g,' ')}: <b style="color:${pathStatusColor(p.status)}">${p.status}</b>`)
      .join('<br>');
    tooltip.innerHTML = `
      <strong style="color:#f0d080">${regionId.replace(/-/g,' ').replace(/\b\w/g,c=>c.toUpperCase())}</strong><br>
      Terrain: ${r.terrain || '?'} &nbsp;|&nbsp; Control: ${r.controlledBy || '?'}<br>
      Threat: ${r.threatLevel ?? '?'}${ps ? '<br><small style="opacity:0.7">Paths:<br>'+ps+'</small>' : ''}`;
    const rect = svgEl.getBoundingClientRect();
    tooltip.style.left = (e.clientX - rect.left + 14) + 'px';
    tooltip.style.top  = (e.clientY - rect.top  + 14) + 'px';
    tooltip.classList.remove('hidden');
  } else {
    tooltip.classList.add('hidden');
  }
}

function pathStatusColor(s) {
  switch (s) {
    case 'BLOCKED':          return '#e74c3c';
    case 'TEMPORARILY_OPEN': return '#00d2d3';
    case 'THREATENED':       return '#f39c12';
    default:                 return '#27ae60';
  }
}

// Statik SVG haritası içeriği — MiddleEarthMap (1).svg'den inline
function getStaticMapSVG() {
  return `
  <defs>
    <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
      <feDropShadow dx="2" dy="4" stdDeviation="3" flood-opacity="0.25" />
    </filter>
    <filter id="box-shadow" x="-20%" y="-20%" width="140%" height="140%">
      <feDropShadow dx="0" dy="2" stdDeviation="2" flood-opacity="0.15" />
    </filter>
    <g id="icon-plains">
      <path d="M-8 4 L8 4 M-5 4 L-7 -2 M0 4 L0 -4 M5 4 L7 -2" fill="none" stroke="#fff" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"/>
    </g>
    <g id="icon-swamp">
      <path d="M-10 1 Q-5 -3 0 1 T10 1 M-8 6 Q-4 2 1 6 T9 6" fill="none" stroke="#fff" stroke-width="2.5" stroke-linecap="round"/>
    </g>
    <g id="icon-mountains">
      <path d="M-12 6 L-4 -8 L2 2 M-2 6 L6 -10 L14 6" fill="none" stroke="#fff" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
    </g>
    <g id="icon-forest">
      <path d="M0 -10 L-7 2 L-3 2 L-9 12 L9 12 L3 2 L7 2 Z" fill="#fff"/>
    </g>
    <g id="icon-fortress">
      <path d="M-9 -6 L-9 8 L9 8 L9 -6 L5 -6 L5 -2 L-5 -2 L-5 -6 Z" fill="#fff"/>
    </g>
    <g id="icon-volcanic">
      <path d="M-11 8 L-4 -4 L4 -4 L11 8 Z" fill="none" stroke="#fff" stroke-width="2.5" stroke-linejoin="round"/>
      <path d="M-3 -8 Q0 -13 3 -8" fill="none" stroke="#ffdd59" stroke-width="2.5" stroke-linecap="round"/>
    </g>
  </defs>
  <rect width="100%" height="100%" fill="#f8f5eb" />
  <text x="700" y="55" font-size="38" font-weight="900" text-anchor="middle" fill="#2c3e50" letter-spacing="2">RING OF THE MIDDLE EARTH</text>
  <text x="700" y="90" font-size="18" font-weight="600" text-anchor="middle" fill="#576574">22 NODES (REGIONS) • 37 PATHS (EDGES) — STRATEGY GRAPH</text>
  <g id="map-content" transform="translate(0, 50)">
    <g stroke-linecap="round" stroke-linejoin="round">
      <line x1="100" y1="250" x2="250" y2="250" stroke="#888" stroke-width="3" />
      <line x1="250" y1="250" x2="400" y2="150" stroke="#888" stroke-width="3" />
      <line x1="250" y1="250" x2="550" y2="150" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
      <line x1="400" y1="150" x2="550" y2="150" stroke="#888" stroke-width="3" />
      <line x1="550" y1="150" x2="600" y2="300" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
      <line x1="550" y1="150" x2="700" y2="300" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
      <line x1="600" y1="300" x2="700" y2="300" stroke="#888" stroke-width="3" />
      <line x1="700" y1="300" x2="850" y2="350" stroke="#888" stroke-width="3" />
      <line x1="700" y1="300" x2="650" y2="500" stroke="#888" stroke-width="3" />
      <line x1="650" y1="500" x2="500" y2="450" stroke="#888" stroke-width="3" />
      <line x1="650" y1="500" x2="650" y2="650" stroke="#888" stroke-width="3" />
      <line x1="650" y1="500" x2="850" y2="650" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
      <line x1="500" y1="450" x2="350" y2="650" stroke="#888" stroke-width="3" />
      <line x1="350" y1="650" x2="650" y2="500" stroke="#888" stroke-width="3" />
      <line x1="650" y1="650" x2="500" y2="700" stroke="#888" stroke-width="3" />
      <line x1="500" y1="700" x2="350" y2="650" stroke="#888" stroke-width="3" />
      <line x1="850" y1="350" x2="1000" y2="250" stroke="#888" stroke-width="3" />
      <line x1="850" y1="350" x2="1000" y2="450" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
      <line x1="1000" y1="250" x2="1000" y2="450" stroke="#888" stroke-width="3" />
      <line x1="1000" y1="250" x2="1150" y2="250" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
      <line x1="1000" y1="450" x2="850" y2="650" stroke="#888" stroke-width="3" />
      <line x1="1000" y1="450" x2="1000" y2="650" stroke="#888" stroke-width="3" />
      <line x1="1000" y1="450" x2="1150" y2="450" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
      <line x1="850" y1="650" x2="1000" y2="650" stroke="#888" stroke-width="3" />
      <line x1="1000" y1="650" x2="1150" y2="650" stroke="#888" stroke-width="3" />
      <line x1="1150" y1="650" x2="1150" y2="450" stroke="#888" stroke-width="3" />
      <line x1="1150" y1="650" x2="1150" y2="250" stroke="#888" stroke-width="3" />
      <line x1="1150" y1="450" x2="1150" y2="250" stroke="#888" stroke-width="3" />
      <line x1="1150" y1="450" x2="1300" y2="350" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
      <line x1="1150" y1="250" x2="1300" y2="350" stroke="#888" stroke-width="3" />
      <line x1="100" y1="250" x2="250" y2="400" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
      <line x1="250" y1="250" x2="250" y2="400" stroke="#888" stroke-width="3" />
      <line x1="250" y1="400" x2="350" y2="550" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
      <line x1="350" y1="550" x2="350" y2="650" stroke="#888" stroke-width="3" />
      <line x1="350" y1="550" x2="500" y2="700" stroke="#888" stroke-width="3" />
      <line x1="350" y1="550" x2="650" y2="650" stroke="#888" stroke-width="3" />
      <line x1="650" y1="650" x2="850" y2="650" stroke="#555" stroke-width="4" stroke-dasharray="8,8" />
    </g>
    <g id="nodes">
      <g transform="translate(100,250)"><rect x="-45" y="-50" width="90" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="-31" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">The Shire</text><circle r="20" fill="#4b7bec" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-plains" /></g>
      <g transform="translate(250,250)"><rect x="-35" y="-50" width="70" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="-31" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Bree</text><circle r="20" fill="#a5b1c2" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-plains" /></g>
      <g transform="translate(250,400)"><rect x="-45" y="24" width="90" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="43" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Tharbad</text><circle r="20" fill="#a5b1c2" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-swamp" /></g>
      <g transform="translate(400,150)"><rect x="-55" y="-50" width="110" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="-31" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Weathertop</text><circle r="20" fill="#a5b1c2" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-mountains" /></g>
      <g transform="translate(550,150)"><rect x="-45" y="-50" width="90" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="-31" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Rivendell</text><circle r="20" fill="#4b7bec" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-mountains" /></g>
      <g transform="translate(500,450)"><rect x="-45" y="-50" width="90" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="-31" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Fangorn</text><circle r="20" fill="#4b7bec" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-forest" /></g>
      <g transform="translate(350,550)"><rect x="-130" y="-14" width="105" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="-77.5" y="5" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Fords of Isen</text><circle r="20" fill="#a5b1c2" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-plains" /></g>
      <g transform="translate(650,500)"><rect x="-55" y="-50" width="110" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="-31" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Rohan Plains</text><circle r="20" fill="#4b7bec" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-plains" /></g>
      <g transform="translate(600,300)"><rect x="-35" y="24" width="70" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="43" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Moria</text><circle r="20" fill="#a5b1c2" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-mountains" /></g>
      <g transform="translate(500,700)"><rect x="-50" y="24" width="100" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="43" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Helm's Deep</text><circle r="20" fill="#4b7bec" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-fortress" /></g>
      <g transform="translate(350,650)"><rect x="-45" y="24" width="90" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="43" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Isengard</text><circle r="20" fill="#eb3b5a" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-fortress" /></g>
      <g transform="translate(650,650)"><rect x="-40" y="24" width="80" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="43" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Edoras</text><circle r="20" fill="#4b7bec" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-plains" /></g>
      <g transform="translate(700,300)"><rect x="-45" y="-50" width="90" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="-31" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Lothlórien</text><circle r="20" fill="#4b7bec" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-forest" /></g>
      <g transform="translate(1000,250)"><rect x="-60" y="-50" width="120" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="-31" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Dead Marshes</text><circle r="20" fill="#a5b1c2" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-swamp" /></g>
      <g transform="translate(850,350)"><rect x="-45" y="24" width="90" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="43" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Emyn Muil</text><circle r="20" fill="#a5b1c2" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-mountains" /></g>
      <g transform="translate(850,650)"><rect x="-50" y="24" width="100" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="43" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Minas Tirith</text><circle r="20" fill="#4b7bec" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-fortress" /></g>
      <g transform="translate(1000,450)"><rect x="15" y="-45" width="75" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="52.5" y="-26" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Ithilien</text><circle r="20" fill="#a5b1c2" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-forest" /></g>
      <g transform="translate(1000,650)"><rect x="-45" y="24" width="90" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="43" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Osgiliath</text><circle r="20" fill="#a5b1c2" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-plains" /></g>
      <g transform="translate(1150,650)"><rect x="-55" y="24" width="110" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="43" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Minas Morgul</text><circle r="20" fill="#eb3b5a" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-fortress" /></g>
      <g transform="translate(1150,450)"><rect x="15" y="15" width="105" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="67.5" y="34" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Cirith Ungol</text><circle r="20" fill="#eb3b5a" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-mountains" /></g>
      <g transform="translate(1150,250)"><rect x="-40" y="-50" width="80" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="-31" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Mordor</text><circle r="20" fill="#eb3b5a" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-volcanic" /></g>
      <g transform="translate(1300,350)"><rect x="-55" y="-50" width="110" height="28" rx="6" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/><text x="0" y="-31" font-size="14" font-weight="bold" text-anchor="middle" fill="#2c3e50">Mount Doom</text><circle r="20" fill="#eb3b5a" stroke="#ffffff" stroke-width="3" filter="url(#shadow)" /><use href="#icon-volcanic" /></g>
    </g>
  </g>
  <g id="legends" transform="translate(0, 830)">
    <g transform="translate(100, 0)">
      <rect x="0" y="0" width="220" height="140" rx="8" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/>
      <text x="15" y="30" font-size="15" font-weight="bold" fill="#2c3e50">Start Control</text>
      <circle cx="25" cy="60" r="8" fill="#4b7bec" stroke="#fff" stroke-width="1.5" /><text x="45" y="65" font-size="14" font-weight="600" fill="#576574">Free Peoples</text>
      <circle cx="25" cy="90" r="8" fill="#eb3b5a" stroke="#fff" stroke-width="1.5" /><text x="45" y="95" font-size="14" font-weight="600" fill="#576574">Shadow</text>
      <circle cx="25" cy="120" r="8" fill="#a5b1c2" stroke="#fff" stroke-width="1.5" /><text x="45" y="125" font-size="14" font-weight="600" fill="#576574">Neutral</text>
    </g>
    <g transform="translate(350, 0)">
      <rect x="0" y="0" width="240" height="140" rx="8" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/>
      <text x="15" y="30" font-size="15" font-weight="bold" fill="#2c3e50">Path Costs</text>
      <line x1="15" y1="70" x2="60" y2="70" stroke="#888" stroke-width="3" /><text x="70" y="75" font-size="13" font-weight="600" fill="#576574">Cost 1 (Solid)</text>
      <line x1="15" y1="110" x2="60" y2="110" stroke="#555" stroke-width="4" stroke-dasharray="6,4" /><text x="70" y="115" font-size="13" font-weight="600" fill="#576574">Cost 2 (Dashed)</text>
    </g>
    <g transform="translate(620, 0)">
      <rect x="0" y="0" width="260" height="140" rx="8" fill="#ffffff" stroke="#dcdde1" stroke-width="2" filter="url(#box-shadow)"/>
      <text x="15" y="30" font-size="15" font-weight="bold" fill="#2c3e50">Game Overlay</text>
      <line x1="15" y1="65" x2="55" y2="65" stroke="#e74c3c" stroke-width="4" stroke-dasharray="6,4"/><text x="65" y="70" font-size="13" font-weight="600" fill="#576574">Blocked</text>
      <line x1="15" y1="90" x2="55" y2="90" stroke="#00d2d3" stroke-width="4"/><text x="65" y="95" font-size="13" font-weight="600" fill="#576574">Temp. Open</text>
      <line x1="15" y1="115" x2="55" y2="115" stroke="#f39c12" stroke-width="4"/><text x="65" y="120" font-size="13" font-weight="600" fill="#576574">Threatened</text>
    </g>
  </g>
  `;
}

// ── Units List ─────────────────────────────────────────────────────────────────
function renderUnitsList() {
  const list = document.getElementById('units-list');
  list.innerHTML = '';
  const mySide = state.side === 'light' ? 'FREE_PEOPLES' : 'SHADOW';
  const myUnits = Object.values(state.units).filter(u => u.side === mySide);

  myUnits.forEach(u => {
    const row = document.createElement('div');
    row.className = `unit-row ${u.status.toLowerCase()} ${state.selectedUnit === u.id ? 'selected' : ''}`;
    row.id = `unit-row-${u.id}`;
    row.onclick = () => selectUnit(u.id);

    const region = u.id === 'ring-bearer' && state.side === 'light'
      ? (state.rbRegion || '?').replace(/-/g, ' ')
      : (u.currentRegion || '?').replace(/-/g, ' ');

    row.innerHTML = `
      <div class="unit-side-bar ${state.side}"></div>
      <div class="unit-row-name">${u.id.replace(/-/g, ' ')}</div>
      <div class="unit-row-region">${region}</div>
      <div class="unit-row-str">${u.strength}</div>
    `;
    list.appendChild(row);
  });
}

function selectUnit(unitId) {
  state.selectedUnit = unitId;
  document.querySelectorAll('.unit-row').forEach(r => r.classList.remove('selected'));
  const row = document.getElementById(`unit-row-${unitId}`);
  if (row) row.classList.add('selected');

  document.getElementById('no-selection').classList.add('hidden');
  document.getElementById('selected-unit-info').classList.remove('hidden');
  document.getElementById('selected-unit-name').textContent = unitId.replace(/-/g, ' ');

  fetchAvailableOrders(unitId);
}

async function fetchAvailableOrders(unitId) {
  try {
    const r = await fetch(`/orders/available?unitId=${unitId}&playerId=${state.playerId}`);
    const data = await r.json();
    renderOrders(unitId, data.orders || []);
  } catch (e) {
    log('Failed to fetch orders', 'warning');
  }
}

function renderOrders(unitId, orders) {
  const container = document.getElementById('available-orders');
  container.innerHTML = '';
  orders.forEach(orderType => {
    const btn = document.createElement('button');
    btn.className = 'order-btn';
    btn.textContent = orderType.replace(/_/g, ' ');
    btn.onclick = () => showOrderModal(unitId, orderType);
    container.appendChild(btn);
  });
}

// ── Order Modal ────────────────────────────────────────────────────────────────
function showOrderModal(unitId, orderType) {
  document.getElementById('modal-title').textContent = `${orderType.replace(/_/g,' ')} — ${unitId.replace(/-/g,' ')}`;

  const body = document.getElementById('modal-body');
  body.innerHTML = buildOrderForm(unitId, orderType);

  document.getElementById('modal-confirm').onclick = () => submitOrder(unitId, orderType);
  document.getElementById('order-modal').classList.remove('hidden');
}

function buildOrderForm(unitId, orderType) {
  switch (orderType) {
    case 'ASSIGN_ROUTE':
    case 'REDIRECT_UNIT':
      return `<p style="font-size:0.8rem;color:var(--text-muted)">Select path IDs (comma-separated):</p>
              <input id="form-paths" style="width:100%;margin-top:0.5rem;padding:0.4rem;
                background:var(--bg-card2);border:1px solid var(--border);
                color:var(--text-main);border-radius:6px;font-size:0.8rem"
                placeholder="shire-to-bree, bree-to-weathertop, ...">`;

    case 'BLOCK_PATH':
    case 'SEARCH_PATH':
    case 'MAIA_ABILITY': {
      // Birimin bulunduğu bölgeye komşu path'leri bul ve dropdown olarak göster
      const unit = state.units[unitId];
      const unitRegion = unit ? unit.currentRegion : null;
      const nearbyPaths = unitRegion
        ? PATH_EDGES.filter(e => e.from === unitRegion || e.to === unitRegion)
        : PATH_EDGES;

      const pathStatus = (id) => {
        const ps = state.paths[id];
        if (!ps) return 'OPEN';
        return ps.status || 'OPEN';
      };

      const options = nearbyPaths.map(e =>
        `<option value="${e.id}">${e.id.replace(/-/g,' ')} [${pathStatus(e.id)}]</option>`
      ).join('');

      const allOptions = PATH_EDGES.map(e =>
        `<option value="${e.id}">${e.id.replace(/-/g,' ')} [${pathStatus(e.id)}]</option>`
      ).join('');

      return `
        ${unitRegion ? `<p style="font-size:0.75rem;color:var(--text-muted)">📍 Unit at: <strong style="color:var(--gold)">${unitRegion.replace(/-/g,' ')}</strong></p>` : ''}
        <p style="font-size:0.8rem;color:var(--text-muted);margin-top:0.5rem">Adjacent paths (recommended):</p>
        <select id="form-path" style="width:100%;padding:0.4rem;
          background:var(--bg-card2);border:1px solid var(--border);
          color:var(--text-main);border-radius:6px;font-size:0.8rem">
          ${nearbyPaths.length > 0 ? options : allOptions}
        </select>
        <p style="font-size:0.75rem;color:var(--text-muted);margin-top:0.5rem">Or type manually:</p>
        <input id="form-path-manual" style="width:100%;padding:0.4rem;
          background:var(--bg-card2);border:1px solid var(--border);
          color:var(--text-main);border-radius:6px;font-size:0.8rem"
          placeholder="Leave empty to use dropdown above">`;
    }

    case 'ATTACK_REGION':
    case 'REINFORCE_REGION':
    case 'DEPLOY_NAZGUL':
      return `<p style="font-size:0.8rem;color:var(--text-muted)">Target region ID:</p>
              <select id="form-region" style="width:100%;padding:0.4rem;
                background:var(--bg-card2);border:1px solid var(--border);
                color:var(--text-main);border-radius:6px;font-size:0.8rem">
                ${Object.keys(REGION_COORDS).map(r =>
                  `<option value="${r}">${r.replace(/-/g,' ')}</option>`
                ).join('')}
              </select>`;
    case 'DESTROY_RING':
      return `<p style="font-size:0.85rem;color:var(--gold)">⚡ Destroy the Ring at Mount Doom?</p>`;
    case 'FORTIFY_REGION':
      return `<p style="font-size:0.85rem">Fortify current region for 2 turns (+2 defense)?</p>`;
    default:
      return `<p style="font-size:0.8rem;color:var(--text-muted)">Confirm order: ${orderType}</p>`;
  }
}

async function submitOrder(unitId, orderType) {
  const order = {
    orderType,
    playerId: state.playerId,
    unitId,
    turn: state.turn,
  };

  const pathsInput   = document.getElementById('form-paths');
  const pathDropdown = document.getElementById('form-path');        // dropdown (BLOCK/MAIA)
  const pathManual   = document.getElementById('form-path-manual'); // manual override
  const regionInput  = document.getElementById('form-region');

  if (pathsInput) {
    const paths = pathsInput.value.split(',').map(s => s.trim()).filter(Boolean);
    if (orderType === 'REDIRECT_UNIT') {
      order.newPathIds = paths;
    } else {
      order.pathIds = paths;
    }
  }

  // BLOCK_PATH / SEARCH_PATH / MAIA_ABILITY: manual input öncelikli, yoksa dropdown
  if (pathDropdown) {
    const manual = pathManual ? pathManual.value.trim() : '';
    order.targetPathId = manual || pathDropdown.value;
  }

  if (regionInput) order.targetRegion = regionInput.value;

  try {
    const r = await fetch(`/order?playerId=${state.playerId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Side': state.side },
      body: JSON.stringify(order),
    });
    if (r.status === 202) {
      log(`✅ Order submitted: ${orderType} for ${unitId} (path: ${order.targetPathId || order.targetRegion || ''})`, 'success');
    } else {
      const err = await r.json();
      log(`❌ Order rejected: ${err.error || r.status}`, 'danger');
    }
  } catch (e) {
    log('Network error submitting order', 'warning');
  }

  closeModal();
}

function closeModal() {
  document.getElementById('order-modal').classList.add('hidden');
}

// ── Analysis Panel ─────────────────────────────────────────────────────────────
function toggleAnalysisPanel() {
  const panel = document.getElementById('analysis-panel');
  const isOpen = panel.classList.toggle('open');
  panel.classList.toggle('hidden', false);
  if (isOpen) loadAnalysis();
}

async function loadAnalysis() {
  const content = document.getElementById('analysis-content');
  content.innerHTML = '<p class="loading">Computing analysis...</p>';

  const endpoint = state.side === 'light' ? '/analysis/routes' : '/analysis/intercept';
  try {
    const r = await fetch(`${endpoint}?playerId=${state.playerId}&side=${state.side}`);
    const data = await r.json();
    renderAnalysis(data);
  } catch (e) {
    content.innerHTML = '<p class="loading">Analysis unavailable</p>';
  }
}

function renderAnalysis(data) {
  const content = document.getElementById('analysis-content');
  if (state.side === 'light' && data.routes) {
    content.innerHTML = data.routes.map(route => {
      const level = route.riskScore < 15 ? 'score-low' : route.riskScore < 30 ? 'score-medium' : 'score-high';
      const rec = route.name === data.recommended ? ' recommended' : '';
      return `<div class="route-card${rec}">
        <div class="route-name">${route.name}${rec ? ' ⭐' : ''}</div>
        <div class="risk-score">Risk: <span class="score-val ${level}">${route.riskScore}</span></div>
        ${route.blockedPaths?.length ? `<div style="font-size:0.7rem;color:#e74c3c">🚫 Blocked: ${route.blockedPaths.join(', ')}</div>` : ''}
      </div>`;
    }).join('');
  } else if (state.side === 'dark' && data.byUnit) {
    content.innerHTML = data.byUnit.map(entry => `
      <div class="intercept-card">
        <div class="route-name">${entry.unitId.replace(/-/g,' ')}</div>
        <div style="font-size:0.75rem;color:var(--text-muted)">
          Target: <strong style="color:var(--gold)">${entry.targetRegion?.replace(/-/g,' ')}</strong><br>
          Score: ${(entry.score * 100).toFixed(0)}%<br>
          Route: ${entry.routeCandidate}
        </div>
      </div>
    `).join('');
  }
}

// ── Turn Timer ─────────────────────────────────────────────────────────────────
// Tur süresi units.conf'tan okunur (backend SSE'den gelir).
// Varsayılan 8s — backend yapılandırmasıyla eşleşmeli.
const TURN_DURATION_SECONDS = 8;

function startTurnTimer() {
  if (state.gameOver) return; // don't start timer after game ends
  state.timerSeconds = TURN_DURATION_SECONDS;
  clearInterval(state.timerInterval);
  state.timerInterval = setInterval(() => {
    if (state.gameOver) {
      clearInterval(state.timerInterval);
      state.timerInterval = null;
      return;
    }
    state.timerSeconds--;
    const el = document.getElementById('turn-timer');
    el.textContent = `${state.timerSeconds}s`;
    // Son 3 saniyede urgent rengi
    if (state.timerSeconds <= 3) el.classList.add('urgent');
    else                         el.classList.remove('urgent');
    if (state.timerSeconds <= 0) {
      state.timerSeconds = TURN_DURATION_SECONDS;
      log(`Turn ${state.turn} ended — next turn starting…`, 'important');
    }
  }, 1000);
}

// ── Utility ────────────────────────────────────────────────────────────────────
function log(msg, type = '') {
  const logEl = document.getElementById('event-log');
  const entry = document.createElement('div');
  entry.className = `log-entry ${type}`;
  const now = new Date();
  const time = `${now.getHours().toString().padStart(2,'0')}:${now.getMinutes().toString().padStart(2,'0')}`;
  entry.innerHTML = `<span class="log-time">${time}</span>${msg}`;
  logEl.insertBefore(entry, logEl.firstChild);
  if (logEl.children.length > 100) logEl.lastChild.remove();
}

async function startGame() {
  try {
    const r = await fetch('/game/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode: 'HVH' }),
    });
    if (r.ok) {
      log('Game started — HvH mode', 'success');
      fetchGameState();
    }
  } catch (e) {
    log('Failed to start game', 'warning');
  }
}

window.addEventListener('DOMContentLoaded', () => {
  const params = new URLSearchParams(window.location.search);
  const side = params.get('side');
  if (side === 'light' || side === 'dark') {
    chooseSide(side, params.get('playerId'));
  }
});
