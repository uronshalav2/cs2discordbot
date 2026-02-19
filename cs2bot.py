import os
import re
import json
import pytz
import a2s
import asyncio
import discord
import requests
import io
from datetime import datetime, timedelta
from discord.ext import commands, tasks
from discord import app_commands
from typing import Literal, Optional
from mcrcon import MCRcon
from collections import defaultdict

# HTTP server for receiving CS2 logs
from aiohttp import web

# Discord UI components
from discord.ui import Button, View

# Track kill events from HTTP logs
pending_kill_events = []

# In-memory ring buffer — last 500 lines for the browser debug viewer
from collections import deque
_recent_log_lines: deque = deque(maxlen=500)

# ========== LIVE MATCH STATE (for HLTV-style page) ==========
_match_state = {
    "active":    False,
    "map":       "—",
    "server":    "—",
    "round":     0,
    "score_t":   0,
    "score_ct":  0,
    "started_at": None,
    "warmup":    False,
}
_kill_feed: deque = deque(maxlen=20)   # last 20 kills
_sse_clients: list = []                # connected browser SSE clients

# Bot keywords defined early so log handler can use them without forward-ref
_BOT_KEYWORDS = ["CSTV", "BOT", "GOTV", "SourceTV"]

def _is_bot_name(name: str) -> bool:
    u = name.upper()
    return any(kw in u for kw in _BOT_KEYWORDS)

# Regex to strip the "L MM/DD/YYYY - HH:MM:SS: " prefix MatchZy adds to every line
_LOG_PREFIX_RE = re.compile(r'^L \d{2}/\d{2}/\d{4} - \d{2}:\d{2}:\d{2}: ?')

def _strip_prefix(line: str) -> str:
    return _LOG_PREFIX_RE.sub('', line)

def _parse_matchzy_block(raw_lines: list) -> dict | None:
    """
    MatchZy sends round stats wrapped in log lines like:
        L date - time: JSON_BEGIN{
        L date - time: "name": "round_stats",
        ...
        L date - time: }}JSON_END
    Strip the log prefix from each line, reassemble, fix the wrapper so it
    becomes valid JSON, then parse it.
    """
    in_block = False
    block_lines = []
    for line in raw_lines:
        stripped = _strip_prefix(line.strip())
        if stripped.startswith('JSON_BEGIN{'):
            in_block = True
            # Replace JSON_BEGIN{ with just {
            block_lines = ['{']
            continue
        if stripped.startswith('}}JSON_END'):
            in_block = False
            block_lines.append('}')
            break
        if in_block:
            block_lines.append(stripped)
    if not block_lines:
        return None
    try:
        return json.loads('\n'.join(block_lines))
    except json.JSONDecodeError:
        return None

# ========== SSE BROADCAST ==========

def _sse_broadcast(event_type: str, data: dict):
    """Push a JSON event to all connected SSE browser clients."""
    payload = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    dead = []
    for q in _sse_clients:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.append(q)
    for q in dead:
        try:
            _sse_clients.remove(q)
        except ValueError:
            pass

# ========== HTTP LOG ENDPOINT ==========

async def handle_log_post(request):
    """
    Receives logs from CS2 / MatchZy. Parses events, updates match state,
    and broadcasts to all connected SSE clients (live page).
    """
    global pending_kill_events

    try:
        remote       = request.remote
        content_type = request.headers.get("Content-Type", "unknown")
        text         = await request.text()

        # Ring buffer for /logs debug viewer
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        _recent_log_lines.append(f"── {ts}  {remote}  ({len(text)} bytes) ──")
        for ln in text.splitlines():
            if ln.strip():
                _recent_log_lines.append(f"  {ln}")

        raw_lines = text.splitlines()

        # ── MatchZy round_stats block ────────────────────────────────────────
        block = _parse_matchzy_block(raw_lines)
        if block:
            rnd      = int(block.get("round_number", 0) or 0)
            score_t  = int(block.get("score_t",  0) or 0)
            score_ct = int(block.get("score_ct", 0) or 0)
            map_name = block.get("map",    _match_state["map"])
            server   = block.get("server", _match_state["server"])
            players_raw = block.get("players", {})
            fields      = [f.strip() for f in block.get("fields", "").split(",")]

            # Parse player rows
            players = []
            for pval in players_raw.values():
                vals = [v.strip() for v in pval.split(",")]
                if len(vals) == len(fields):
                    row = dict(zip(fields, vals))
                    if row.get("accountid","0").strip() != "0":
                        players.append(row)

            _match_state.update({
                "active":   True,
                "warmup":   False,
                "map":      map_name,
                "server":   server,
                "round":    rnd,
                "score_t":  score_t,
                "score_ct": score_ct,
            })
            if not _match_state["started_at"]:
                _match_state["started_at"] = datetime.utcnow().isoformat()

            print(f"[LOG] 📊 round={rnd}  map={map_name}  T:{score_t} CT:{score_ct}  players={len(players)}")
            _sse_broadcast("round_stats", {
                "round":    rnd,
                "score_t":  score_t,
                "score_ct": score_ct,
                "map":      map_name,
                "server":   server,
                "players":  players,
            })

        # ── Plain log lines ──────────────────────────────────────────────────
        kill_re = re.compile(
            r'"([^"<]+)<\d+><[^>]*><([^>]*)>" killed '
            r'"([^"<]+)<\d+><[^>]*><[^>]*>" with "([^"]+)"'
        )
        for line in raw_lines:
            c = _strip_prefix(line).strip()
            if not c:
                continue

            # Kill
            km = kill_re.search(c)
            if km and " killed " in c:
                killer = km.group(1).strip()
                team   = km.group(2).strip()   # CT or TERRORIST
                victim = km.group(3).strip()
                weapon = km.group(4).strip()
                if not _is_bot_name(killer) and not _is_bot_name(victim):
                    side = "CT" if "CT" in team.upper() else "T"
                    entry = {"killer": killer, "victim": victim, "weapon": weapon, "side": side,
                             "ts": datetime.utcnow().strftime("%H:%M:%S")}
                    _kill_feed.appendleft(entry)
                    pending_kill_events.append((killer, victim))
                    print(f"[LOG] 💀 {killer} ({side}) → {victim} [{weapon}]")
                    _sse_broadcast("kill", entry)
                continue

            # Match start
            if "Match_Start" in c:
                map_m = re.search(r'on "([^"]+)"', c)
                mn    = map_m.group(1) if map_m else _match_state["map"]
                _match_state.update({"active": True, "warmup": False, "map": mn,
                                     "score_t": 0, "score_ct": 0, "round": 0,
                                     "started_at": datetime.utcnow().isoformat()})
                _kill_feed.clear()
                print(f"[LOG] 🏁 Match start on {mn}")
                _sse_broadcast("match_start", {"map": mn})

            # Match over
            elif "Match_Over" in c or "Game_Over" in c:
                _match_state["active"] = False
                print(f"[LOG] 🏆 Match over")
                _sse_broadcast("match_end", {
                    "score_t":  _match_state["score_t"],
                    "score_ct": _match_state["score_ct"],
                    "map":      _match_state["map"],
                })

            # Warmup
            elif "Warmup_Start" in c:
                _match_state["warmup"] = True
                print(f"[LOG] 🔥 Warmup")
                _sse_broadcast("warmup", {})

            # Round end
            elif "round_end" in c.lower():
                print(f"[LOG] 🔄 Round end")

        return web.Response(text="OK")

    except Exception as e:
        print(f"[LOG] ❌ Error: {e}")
        import traceback; traceback.print_exc()
        return web.Response(text="Error", status=500)


async def handle_health_check(request):
    return web.Response(text='Bot is running')

async def handle_log_get(request):
    """
    GET /logs — live log viewer. All times UTC.
    """
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    def fmt_line(line):
        escaped = line.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        css = "hdr" if not line.startswith("  ") else "ln"
        return f"<div class='{css}'>{escaped}</div>"

    lines_html = "\n".join(fmt_line(l) for l in _recent_log_lines) or (
        "<div class='empty'>No log data received yet. "
        "Make sure the MatchZy remote log URL is set to POST /logs on this endpoint.</div>"
    )

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>CS2 Remote Log Viewer</title>
  <style>
    body  {{ background:#1a1a2e; color:#e0e0e0; margin:0; padding:16px 20px; font-family:monospace; font-size:13px; }}
    h1    {{ color:#64b5f6; margin:0 0 4px; font-size:18px; }}
    .meta {{ color:#888; margin:0 0 10px; font-size:12px; }}
    .meta b {{ color:#aaa; }}
    #log  {{ background:#0d0d1a; padding:12px 14px; border-radius:6px; overflow:auto; max-height:88vh; border:1px solid #2a2a4a; }}
    .hdr  {{ color:#a5d6a7; white-space:pre; padding-top:6px; }}
    .ln   {{ color:#b0bec5; white-space:pre; }}
    .empty{{ color:#555; }}
    #clock{{ float:right; color:#546e7a; font-size:11px; }}
  </style>
</head>
<body>
  <h1>📡 CS2 Remote Log Viewer <span id="clock"></span></h1>
  <p class="meta">
    Endpoint: <b>POST /logs</b> &nbsp;|&nbsp;
    Lines buffered: <b>{len(_recent_log_lines)}/500</b> &nbsp;|&nbsp;
    Page loaded: <b>{now_utc}</b> &nbsp;|&nbsp;
  </p>
  <div id="log">{lines_html}</div>
  <script>
    var log = document.getElementById('log');
    log.scrollTop = log.scrollHeight;

    function utcClock() {{
      var d = new Date();
      var p = n => String(n).padStart(2,'0');
      document.getElementById('clock').textContent =
        d.getUTCFullYear()+'-'+p(d.getUTCMonth()+1)+'-'+p(d.getUTCDate())+' '+
        p(d.getUTCHours())+':'+p(d.getUTCMinutes())+':'+p(d.getUTCSeconds())+' UTC';
    }}
    utcClock();
    setInterval(utcClock, 1000);
  </script>
</body>
</html>"""
    return web.Response(text=html, content_type='text/html')


async def handle_sse(request):
    """
    GET /events — Server-Sent Events stream.
    Browsers connect here and receive live match updates without polling.
    """
    q = asyncio.Queue(maxsize=50)
    _sse_clients.append(q)
    # Send current state immediately on connect
    q.put_nowait(f"event: state\ndata: {json.dumps(_match_state_snapshot())}\n\n")

    resp = web.StreamResponse(headers={
        "Content-Type":  "text/event-stream",
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",
        "Access-Control-Allow-Origin": "*",
    })
    await resp.prepare(request)

    try:
        while True:
            try:
                msg = await asyncio.wait_for(q.get(), timeout=25)
                await resp.write(msg.encode())
            except asyncio.TimeoutError:
                # Heartbeat so the connection stays alive
                await resp.write(b": heartbeat\n\n")
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        try:
            _sse_clients.remove(q)
        except ValueError:
            pass
    return resp


def _match_state_snapshot() -> dict:
    """Return current match state + kill feed for initial page load."""
    elapsed = ""
    if _match_state.get("started_at"):
        try:
            start = datetime.fromisoformat(_match_state["started_at"])
            secs  = int((datetime.utcnow() - start).total_seconds())
            elapsed = f"{secs // 60}:{secs % 60:02d}"
        except Exception:
            pass
    return {
        **_match_state,
        "elapsed":   elapsed,
        "kill_feed": list(_kill_feed),
    }


async def handle_live_page(request):
    """GET / — HLTV-style live match tracker page."""
    html = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CS2 Live</title>
<style>
  :root {
    --bg:     #0f1117;
    --panel:  #181c24;
    --border: #252a35;
    --accent: #e8a020;
    --ct:     #5b9bd5;
    --t:      #d4863a;
    --green:  #4caf50;
    --red:    #e53935;
    --text:   #d0d6e0;
    --muted:  #5a6072;
    --kill-bg:#1a1f2b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif;
         font-size: 14px; min-height: 100vh; }

  /* ── Header ── */
  header { background: var(--panel); border-bottom: 1px solid var(--border);
            padding: 10px 20px; display: flex; align-items: center; gap: 16px; }
  .logo  { font-size: 18px; font-weight: 700; letter-spacing: 1px; color: var(--accent); }
  #conn-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--muted);
               transition: background .3s; margin-left: auto; }
  #conn-dot.live { background: var(--green); box-shadow: 0 0 6px var(--green); }
  #conn-label { font-size: 11px; color: var(--muted); }

  /* ── Scoreboard ── */
  #scoreboard { background: var(--panel); border: 1px solid var(--border); border-radius: 6px;
                 margin: 16px; padding: 20px; text-align: center; }
  #map-name   { font-size: 11px; color: var(--muted); text-transform: uppercase;
                 letter-spacing: 2px; margin-bottom: 8px; }
  #server-name{ font-size: 12px; color: var(--muted); margin-bottom: 16px; }
  .score-row  { display: flex; align-items: center; justify-content: center; gap: 0; }
  .team-block { flex: 1; }
  .team-name  { font-size: 11px; text-transform: uppercase; letter-spacing: 1px;
                 font-weight: 600; margin-bottom: 4px; }
  .team-score { font-size: 64px; font-weight: 800; line-height: 1; }
  .ct .team-name  { color: var(--ct); }
  .ct .team-score { color: var(--ct); }
  .t  .team-name  { color: var(--t); }
  .t  .team-score { color: var(--t); }
  .divider    { font-size: 32px; color: var(--muted); padding: 0 24px; font-weight: 300; }
  #round-info { margin-top: 14px; font-size: 12px; color: var(--muted); }
  #round-info span { color: var(--text); font-weight: 600; }
  #status-badge { display: inline-block; padding: 3px 10px; border-radius: 20px;
                   font-size: 11px; font-weight: 700; letter-spacing: 1px;
                   text-transform: uppercase; margin-top: 10px; }
  .badge-live    { background: #1b3a1b; color: var(--green); border: 1px solid var(--green); }
  .badge-warmup  { background: #3a2e0a; color: var(--accent); border: 1px solid var(--accent); }
  .badge-waiting { background: #1e1e1e; color: var(--muted); border: 1px solid var(--muted); }
  .badge-ended   { background: #2a1a1a; color: var(--red); border: 1px solid var(--red); }

  /* ── Player tables ── */
  .tables-row { display: flex; gap: 12px; margin: 0 16px 16px; }
  .team-table { flex: 1; background: var(--panel); border: 1px solid var(--border);
                 border-radius: 6px; overflow: hidden; }
  .team-table h3 { padding: 10px 14px; font-size: 11px; font-weight: 700;
                    text-transform: uppercase; letter-spacing: 1px; }
  .team-table.ct-table h3 { color: var(--ct); border-bottom: 1px solid var(--border); }
  .team-table.t-table  h3 { color: var(--t);  border-bottom: 1px solid var(--border); }
  table { width: 100%; border-collapse: collapse; }
  th { padding: 6px 10px; text-align: right; font-size: 10px; color: var(--muted);
        text-transform: uppercase; letter-spacing: .5px; font-weight: 600; }
  th:first-child { text-align: left; }
  td { padding: 7px 10px; text-align: right; border-top: 1px solid var(--border); font-size: 13px; }
  td:first-child { text-align: left; }
  tr:hover td { background: rgba(255,255,255,.03); }
  .kd-good { color: var(--green); }
  .kd-bad  { color: var(--red); }

  /* ── Kill feed ── */
  #killfeed-wrap { margin: 0 16px 16px; background: var(--panel);
                    border: 1px solid var(--border); border-radius: 6px; }
  #killfeed-wrap h3 { padding: 10px 14px; font-size: 11px; font-weight: 700;
                       text-transform: uppercase; letter-spacing: 1px; color: var(--muted);
                       border-bottom: 1px solid var(--border); }
  #killfeed { padding: 6px 0; }
  .kf-row  { display: flex; align-items: center; gap: 8px; padding: 5px 14px;
               animation: fadein .3s ease; font-size: 13px; }
  .kf-row:hover { background: var(--kill-bg); }
  @keyframes fadein { from { opacity:0; transform: translateY(-4px); } to { opacity:1; } }
  .kf-killer { font-weight: 600; }
  .kf-killer.ct { color: var(--ct); }
  .kf-killer.t  { color: var(--t); }
  .kf-arrow  { color: var(--muted); font-size: 10px; }
  .kf-victim { color: var(--text); }
  .kf-weapon { margin-left: auto; font-size: 11px; color: var(--muted);
                background: var(--border); padding: 1px 7px; border-radius: 10px; }
  .kf-time   { font-size: 10px; color: var(--muted); width: 48px; }
  .kf-empty  { padding: 16px 14px; color: var(--muted); font-size: 12px; }

  /* ── Responsive ── */
  @media (max-width: 640px) {
    .tables-row { flex-direction: column; }
    .team-score { font-size: 44px; }
  }
</style>
</head>
<body>

<header>
  <div class="logo">⚡ CS2 LIVE</div>
  <div id="conn-dot"></div>
  <div id="conn-label">Connecting…</div>
</header>

<div id="scoreboard">
  <div id="map-name">—</div>
  <div id="server-name">—</div>
  <div class="score-row">
    <div class="team-block ct">
      <div class="team-name">CT</div>
      <div class="team-score" id="score-ct">0</div>
    </div>
    <div class="divider">:</div>
    <div class="team-block t">
      <div class="team-score" id="score-t">0</div>
      <div class="team-name">T</div>
    </div>
  </div>
  <div id="round-info">Round <span id="round-num">—</span> &nbsp;·&nbsp; <span id="elapsed">—</span></div>
  <div id="status-badge" class="badge-waiting">Waiting</div>
</div>

<div class="tables-row">
  <div class="team-table ct-table">
    <h3>🔵 Counter-Terrorists</h3>
    <table><thead><tr>
      <th>Player</th><th>K</th><th>D</th><th>ADR</th><th>K/D</th>
    </tr></thead><tbody id="ct-body"><tr><td colspan="5" style="color:var(--muted);text-align:center;padding:16px">—</td></tr></tbody></table>
  </div>
  <div class="team-table t-table">
    <h3>🔴 Terrorists</h3>
    <table><thead><tr>
      <th>Player</th><th>K</th><th>D</th><th>ADR</th><th>K/D</th>
    </tr></thead><tbody id="t-body"><tr><td colspan="5" style="color:var(--muted);text-align:center;padding:16px">—</td></tr></tbody></table>
  </div>
</div>

<div id="killfeed-wrap">
  <h3>💀 Kill Feed</h3>
  <div id="killfeed"><div class="kf-empty">No kills yet this round</div></div>
</div>

<script>
const $ = id => document.getElementById(id);
let elapsedBase = null, elapsedOffset = 0, elapsedTimer = null;

function setStatus(label, cls) {
  const b = $('status-badge');
  b.textContent = label;
  b.className   = 'badge-' + cls;
}

function fmtKD(k, d) {
  const v = d > 0 ? (k / d).toFixed(2) : k.toFixed(2);
  const cls = parseFloat(v) >= 1 ? 'kd-good' : 'kd-bad';
  return `<span class="${cls}">${v}</span>`;
}

function renderPlayers(players) {
  const ct = players.filter(p => p.team === '3');
  const t  = players.filter(p => p.team === '2');
  const sort = arr => arr.sort((a,b) => (parseInt(b.kills)||0) - (parseInt(a.kills)||0));

  function rows(arr) {
    if (!arr.length) return '<tr><td colspan="5" style="color:var(--muted);text-align:center;padding:14px">—</td></tr>';
    return sort(arr).map(p => {
      const k   = parseInt(p.kills  || 0);
      const d   = parseInt(p.deaths || 0);
      const adr = parseFloat(p.adr  || 0).toFixed(0);
      const id  = p.accountid || '?';
      return `<tr>
        <td title="AccountID: ${id}">${id}</td>
        <td>${k}</td><td>${d}</td><td>${adr}</td>
        <td>${fmtKD(k,d)}</td>
      </tr>`;
    }).join('');
  }
  $('ct-body').innerHTML = rows(ct);
  $('t-body').innerHTML  = rows(t);
}

function renderKillFeed(kills) {
  if (!kills || !kills.length) {
    $('killfeed').innerHTML = '<div class="kf-empty">No kills yet this round</div>';
    return;
  }
  $('killfeed').innerHTML = kills.map(k =>
    `<div class="kf-row">
      <span class="kf-time">${k.ts || ''}</span>
      <span class="kf-killer ${(k.side||'').toLowerCase()}">${k.killer}</span>
      <span class="kf-arrow">▶</span>
      <span class="kf-victim">${k.victim}</span>
      <span class="kf-weapon">${k.weapon}</span>
    </div>`
  ).join('');
}

function addKill(k) {
  const empty = $('killfeed').querySelector('.kf-empty');
  if (empty) $('killfeed').innerHTML = '';
  const row = document.createElement('div');
  row.className = 'kf-row';
  row.innerHTML = `
    <span class="kf-time">${k.ts || ''}</span>
    <span class="kf-killer ${(k.side||'').toLowerCase()}">${k.killer}</span>
    <span class="kf-arrow">▶</span>
    <span class="kf-victim">${k.victim}</span>
    <span class="kf-weapon">${k.weapon}</span>`;
  $('killfeed').insertBefore(row, $('killfeed').firstChild);
  // Keep max 20 rows
  while ($('killfeed').children.length > 20)
    $('killfeed').removeChild($('killfeed').lastChild);
}

function startElapsed(isoStart) {
  if (elapsedTimer) clearInterval(elapsedTimer);
  if (!isoStart) return;
  const base = new Date(isoStart).getTime();
  elapsedTimer = setInterval(() => {
    const s = Math.floor((Date.now() - base) / 1000);
    $('elapsed').textContent = Math.floor(s/60) + ':' + String(s%60).padStart(2,'0');
  }, 1000);
}

function applyState(s) {
  $('map-name').textContent    = (s.map    || '—').toUpperCase();
  $('server-name').textContent = s.server  || '—';
  $('score-ct').textContent    = s.score_ct ?? 0;
  $('score-t').textContent     = s.score_t  ?? 0;
  $('round-num').textContent   = s.round   || '—';
  if (s.warmup)        setStatus('Warmup',  'warmup');
  else if (s.active)   setStatus('LIVE',    'live');
  else                 setStatus('Waiting', 'waiting');
  if (s.started_at) startElapsed(s.started_at);
  if (s.kill_feed)  renderKillFeed(s.kill_feed);
}

// ── SSE connection ──────────────────────────────────────────────────────────
function connect() {
  const es = new EventSource('/events');

  es.onopen = () => {
    $('conn-dot').className   = 'live';
    $('conn-label').textContent = 'Live';
  };
  es.onerror = () => {
    $('conn-dot').className   = '';
    $('conn-label').textContent = 'Reconnecting…';
    setTimeout(connect, 3000);
    es.close();
  };

  es.addEventListener('state', e => {
    applyState(JSON.parse(e.data));
  });
  es.addEventListener('round_stats', e => {
    const d = JSON.parse(e.data);
    $('score-ct').textContent = d.score_ct;
    $('score-t').textContent  = d.score_t;
    $('round-num').textContent = d.round;
    $('map-name').textContent = (d.map || '').toUpperCase();
    $('server-name').textContent = d.server || '';
    setStatus('LIVE', 'live');
    renderPlayers(d.players || []);
    $('killfeed').innerHTML = '<div class="kf-empty">No kills yet this round</div>';
  });
  es.addEventListener('kill', e => {
    addKill(JSON.parse(e.data));
  });
  es.addEventListener('match_start', e => {
    const d = JSON.parse(e.data);
    $('map-name').textContent = (d.map || '').toUpperCase();
    $('score-ct').textContent = 0;
    $('score-t').textContent  = 0;
    $('round-num').textContent = 0;
    setStatus('LIVE', 'live');
    $('killfeed').innerHTML = '<div class="kf-empty">Match started!</div>';
    startElapsed(new Date().toISOString());
  });
  es.addEventListener('match_end', e => {
    setStatus('Ended', 'ended');
    if (elapsedTimer) clearInterval(elapsedTimer);
  });
  es.addEventListener('warmup', () => setStatus('Warmup', 'warmup'));
}

connect();
</script>
</body>
</html>"""
    return web.Response(text=html, content_type='text/html')


async def start_http_server():
    app = web.Application()
    app.router.add_post('/logs',   handle_log_post)
    app.router.add_get('/logs',    handle_log_get)
    app.router.add_get('/events',  handle_sse)
    app.router.add_get('/',        handle_live_page)
    app.router.add_get('/health',  handle_health_check)
    
    port = int(os.getenv('PORT', 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    print(f"✓ HTTP log endpoint started on port {port}")
    return runner

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP", "127.0.0.1")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))
RCON_IP = os.getenv("RCON_IP", SERVER_IP)
RCON_PORT = int(os.getenv("RCON_PORT", 27015))
RCON_PASSWORD = os.getenv("RCON_PASSWORD", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
SERVER_DEMOS_CHANNEL_ID = int(os.getenv("SERVER_DEMOS_CHANNEL_ID", 0))
DEMOS_JSON_URL = os.getenv("DEMOS_JSON_URL")
GUILD_ID = int(os.getenv("GUILD_ID", "0") or "0")
FACEIT_API_KEY = os.getenv("FACEIT_API_KEY")
OWNER_ID = int(os.getenv("OWNER_ID", 0))
STATUS_CHANNEL_ID = int(os.getenv("STATUS_CHANNEL_ID", 0))
NOTIFICATIONS_CHANNEL_ID = int(os.getenv("NOTIFICATIONS_CHANNEL_ID", 0))
SERVER_LOG_PATH = os.getenv("SERVER_LOG_PATH", "")
FACEIT_GAME_ID = "cs2"
MAP_WHITELIST = [
    "de_inferno", "de_mirage", "de_dust2", "de_overpass",
    "de_nuke", "de_ancient", "de_vertigo", "de_anubis"
]

BOT_FILTER = ["CSTV", "BOT", "GOTV", "SourceTV"]
log_file_position = 0

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
bot = commands.Bot(command_prefix="!", intents=intents, owner_id=OWNER_ID)

# ========== DATABASE SETUP (Railway MySQL via mysql-connector-python) ==========
# Railway MySQL env vars:
#   MYSQL_URL  (mysql://user:pass@host:port/dbname)
# OR individual vars:
#   MYSQLHOST, MYSQLPORT, MYSQLUSER, MYSQLPASSWORD, MYSQLDATABASE
import mysql.connector
from mysql.connector import pooling

def _mysql_cfg() -> dict:
    """Build MySQL connection kwargs from Railway env vars."""
    url = os.getenv("MYSQL_URL") or os.getenv("DATABASE_URL", "")
    if url.startswith("mysql://") or url.startswith("mysql+pymysql://"):
        # Parse URL: mysql://user:pass@host:port/dbname
        url = url.replace("mysql+pymysql://", "mysql://").replace("mysql://", "")
        userpass, rest = url.split("@", 1)
        user, password = userpass.split(":", 1)
        hostport, database = rest.split("/", 1)
        host, port = (hostport.split(":", 1) if ":" in hostport else (hostport, "3306"))
        return dict(host=host, port=int(port), user=user, password=password,
                    database=database, autocommit=False)
    # Individual env vars (Railway default naming)
    return dict(
        host=os.getenv("MYSQLHOST", "localhost"),
        port=int(os.getenv("MYSQLPORT", 3306)),
        user=os.getenv("MYSQLUSER", "root"),
        password=os.getenv("MYSQLPASSWORD", ""),
        database=os.getenv("MYSQLDATABASE", "railway"),
        autocommit=False,
    )

_DB_CFG = _mysql_cfg()

def get_db():
    """Return a new MySQL connection."""
    return mysql.connector.connect(**_DB_CFG)

def init_database():
    conn = get_db()
    c = conn.cursor()

    # Bot-managed tables (session tracking, snapshots)
    c.execute('''CREATE TABLE IF NOT EXISTS player_sessions (
                 id INT AUTO_INCREMENT PRIMARY KEY,
                 player_name VARCHAR(255) NOT NULL,
                 steam_id VARCHAR(64),
                 join_time DATETIME,
                 leave_time DATETIME,
                 duration_minutes INT,
                 map_name VARCHAR(128))''')

    c.execute('''CREATE TABLE IF NOT EXISTS server_snapshots (
                 id INT AUTO_INCREMENT PRIMARY KEY,
                 timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                 player_count INT,
                 map_name VARCHAR(128))''')

    # map_stats and player_stats are intentionally NOT created here.
    # MatchZy writes matchzy_stats_maps, matchzy_stats_players, and
    # matchzy_stats_matches to MySQL automatically when matches finish.
    # The bot reads from those tables — it does not duplicate them.

    conn.commit()
    c.close()
    conn.close()
    print("✓ Database initialized (Railway MySQL)")

try:
    init_database()
except Exception as e:
    print(f"⚠️ Database init error: {e}")

# ========== MATCHZY INTEGRATION ==========
# MatchZy writes to these tables automatically when matches finish.
# Schema reference: https://github.com/shobhit-pathak/MatchZy
#
#   matchzy_stats_maps   – per-map summary (match_id, map_number, team1_score, team2_score …)
#   matchzy_stats_players – per-player per-map row
#       (match_id, map_number, steam_id, player_name,
#        team, kills, deaths, assists, adr, rating …)
#
# We JOIN across these to build aggregated career stats.

# MatchZy actual table/column names (confirmed from Railway DB screenshots):
#
# matchzy_stats_players:
#   matchid, mapnumber, steamid64, team, name,
#   kills, deaths, assists, damage,
#   enemies5k, enemies4k, enemies3k, enemies2k,
#   utility_count, utility_damage, utility_successes, utility_enemies,
#   flash_count, flash_successes,
#   health_points_removed_total, health_points_dealt_total,
#   shots_fired_total, shots_on_target_total,
#   v1_count, v1_wins, v2_count, v2_wins,
#   entry_count, entry_wins, equipment_value, money_saved,
#   kill_reward, live_time, head_shot_kills, cash_earned, enemies_flashed
#
# matchzy_stats_maps:
#   matchid, mapnumber, start_time, end_time, winner, mapname, team1_score, team2_score
#
# matchzy_stats_matches:
#   matchid, start_time, end_time, winner, series_type,
#   team1_name, team1_score, team2_name, team2_score, server_ip

MATCHZY_TABLES = {
    "matches": "matchzy_stats_matches",
    "maps":    "matchzy_stats_maps",
    "players": "matchzy_stats_players",
}

def matchzy_tables_exist(conn) -> bool:
    """Return True if MatchZy tables are present in the database."""
    c = conn.cursor()
    c.execute("SHOW TABLES LIKE 'matchzy_stats_players'")
    result = c.fetchone()
    c.close()
    return result is not None

def get_matchzy_player_stats(steamid64: str = None, player_name: str = None) -> dict | None:
    """
    Pull aggregated career stats for a player from MatchZy tables.
    Lookup by steamid64 (preferred) or name.
    Returns None if MatchZy tables don't exist or player not found.
    """
    conn = get_db()
    try:
        if not matchzy_tables_exist(conn):
            return None

        c = conn.cursor(dictionary=True)
        table = MATCHZY_TABLES["players"]

        if steamid64:
            where = "steamid64 = %s"
            param = steamid64
        elif player_name:
            where = "name = %s"
            param = player_name
        else:
            return None

        c.execute(f'''
            SELECT
                name,
                steamid64,
                COUNT(DISTINCT matchid)                      AS matches_played,
                SUM(kills)                                   AS kills,
                SUM(deaths)                                  AS deaths,
                SUM(assists)                                 AS assists,
                SUM(head_shot_kills)                         AS headshots,
                SUM(damage)                                  AS total_damage,
                SUM(enemies5k)                               AS aces,
                SUM(v1_wins)                                 AS clutch_wins,
                SUM(entry_wins)                              AS entry_wins,
                ROUND(
                    SUM(kills) / NULLIF(SUM(deaths), 0), 2
                )                                            AS kd_ratio,
                ROUND(
                    SUM(head_shot_kills) / NULLIF(SUM(kills), 0) * 100, 1
                )                                            AS hs_pct
            FROM {table}
            WHERE {where}
            GROUP BY steamid64, name
        ''', (param,))

        row = c.fetchone()
        c.close()
        return row
    except Exception as e:
        print(f"[MatchZy] Error fetching player stats: {e}")
        return None
    finally:
        conn.close()

def get_matchzy_leaderboard(limit: int = 10) -> list[dict]:
    """
    Return top players by total kills from MatchZy tables.
    Falls back to bot's own player_stats table if MatchZy unavailable.
    """
    conn = get_db()
    try:
        if not matchzy_tables_exist(conn):
            return _fallback_leaderboard(conn, limit)

        c = conn.cursor(dictionary=True)
        placeholders = ",".join(["%s"] * len(BOT_FILTER))
        table = MATCHZY_TABLES["players"]
        c.execute(f'''
            SELECT
                name                                         AS player_name,
                steamid64,
                COUNT(DISTINCT matchid)                      AS matches_played,
                SUM(kills)                                   AS kills,
                SUM(deaths)                                  AS deaths,
                ROUND(
                    SUM(kills) / NULLIF(SUM(deaths), 0), 2
                )                                            AS kd_ratio,
                SUM(damage)                                  AS total_damage,
                ROUND(
                    SUM(head_shot_kills) / NULLIF(SUM(kills), 0) * 100, 1
                )                                            AS hs_pct
            FROM {table}
            WHERE name NOT IN ({placeholders})
            GROUP BY steamid64, name
            ORDER BY kills DESC
            LIMIT %s
        ''', (*BOT_FILTER, limit))

        rows = c.fetchall()
        c.close()
        return rows
    except Exception as e:
        print(f"[MatchZy] Leaderboard error: {e}")
        return _fallback_leaderboard(conn, limit)
    finally:
        conn.close()

def _fallback_leaderboard(conn, limit: int) -> list[dict]:
    """Use bot's own player_stats table when MatchZy is absent."""
    c = conn.cursor(dictionary=True)
    c.execute('''
        SELECT
            ps.player_name,
            NULL AS steamid64,
            0    AS matches_played,
            COALESCE(pst.kills, 0)  AS kills,
            COALESCE(pst.deaths, 0) AS deaths,
            ROUND(
                COALESCE(pst.kills, 0) / NULLIF(COALESCE(pst.deaths, 0), 0), 2
            )                       AS kd_ratio,
            NULL AS total_damage,
            NULL AS hs_pct
        FROM player_sessions ps
        LEFT JOIN player_stats pst ON ps.player_name = pst.player_name
        GROUP BY ps.player_name
        ORDER BY kills DESC
        LIMIT %s
    ''', (limit,))
    rows = c.fetchall()
    c.close()
    return rows

def get_matchzy_recent_matches(limit: int = 5) -> list[dict]:
    """
    Return recent matches. Joins matchzy_stats_matches (team names) with
    matchzy_stats_maps (per-map results).
    """
    conn = get_db()
    try:
        if not matchzy_tables_exist(conn):
            return []

        c = conn.cursor(dictionary=True)
        # Use matchzy_stats_matches for team names + matchzy_stats_maps for map detail
        c.execute(f'''
            SELECT
                m.matchid,
                m.start_time,
                m.end_time,
                m.winner,
                m.series_type,
                m.team1_name,
                m.team2_name,
                mp.mapname,
                mp.team1_score,
                mp.team2_score,
                mp.mapnumber
            FROM {MATCHZY_TABLES["matches"]} m
            LEFT JOIN {MATCHZY_TABLES["maps"]} mp
                ON m.matchid = mp.matchid
            ORDER BY m.end_time DESC
            LIMIT %s
        ''', (limit,))
        rows = c.fetchall()
        c.close()
        return rows
    except Exception as e:
        print(f"[MatchZy] Recent matches error: {e}")
        return []
    finally:
        conn.close()

def get_matchzy_match_mvp(matchid: str, mapnumber: int = None) -> dict | None:
    """Return the top-kill player for a given match (by kills, since no rating col)."""
    conn = get_db()
    try:
        if not matchzy_tables_exist(conn):
            return None

        c = conn.cursor(dictionary=True)
        table = MATCHZY_TABLES["players"]
        extra = "AND mapnumber = %s" if mapnumber is not None else ""
        params = [matchid]
        if mapnumber is not None:
            params.append(mapnumber)
        params.append(1)

        c.execute(f'''
            SELECT name, steamid64, kills, deaths, assists, head_shot_kills, damage
            FROM {table}
            WHERE matchid = %s {extra}
            ORDER BY kills DESC
            LIMIT %s
        ''', params)
        row = c.fetchone()
        c.close()
        return row
    except Exception as e:
        print(f"[MatchZy] MVP lookup error: {e}")
        return None
    finally:
        conn.close()

# ========== PAGINATION VIEW FOR DEMOS ==========
class DemosView(View):
    def __init__(self, offset=0):
        super().__init__(timeout=300)
        self.offset = offset
        self.update_buttons()
    
    def update_buttons(self):
        self.clear_items()
        if self.offset > 0:
            prev_btn = Button(label="◀ Previous", style=discord.ButtonStyle.secondary, custom_id="prev")
            prev_btn.callback = self.previous_page
            self.add_item(prev_btn)
        next_btn = Button(label="Next ▶", style=discord.ButtonStyle.primary, custom_id="next")
        next_btn.callback = self.next_page
        self.add_item(next_btn)
        refresh_btn = Button(label="🔄 Refresh", style=discord.ButtonStyle.success, custom_id="refresh")
        refresh_btn.callback = self.refresh_page
        self.add_item(refresh_btn)
    
    async def previous_page(self, interaction: discord.Interaction):
        self.offset = max(0, self.offset - 5)
        await self.update_message(interaction)
    
    async def next_page(self, interaction: discord.Interaction):
        self.offset += 5
        await self.update_message(interaction)
    
    async def refresh_page(self, interaction: discord.Interaction):
        await self.update_message(interaction)
    
    async def update_message(self, interaction: discord.Interaction):
        await interaction.response.defer()
        result = fetch_demos(self.offset, 5)
        embed = discord.Embed(
            title="🎥 Server Demos",
            description="\n\n".join(result["demos"]),
            color=0x9B59B6
        )
        if result.get("total"):
            embed.set_footer(text=f"Showing {result['showing']} of {result['total']} demos")
        self.update_buttons()
        if not result.get("has_more", False):
            for item in self.children:
                if item.custom_id == "next":
                    item.disabled = True
        await interaction.followup.edit_message(
            message_id=interaction.message.id, embed=embed, view=self
        )

current_players = {}

def owner_only():
    async def predicate(interaction: discord.Interaction):
        return interaction.user.id == OWNER_ID
    return app_commands.check(predicate)

def is_bot_player(player_name: str) -> bool:
    player_upper = player_name.upper()
    return any(kw in player_upper for kw in BOT_FILTER)

def send_rcon(command: str) -> str:
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            resp = rcon.command(command)
            if not resp or resp.strip() == "":
                return "✅ Command executed successfully"
            if any(i in resp.lower() for i in ["success", "completed", "done"]):
                return f"✅ {resp[:1000]}"
            response_text = resp[:2000] if len(resp) > 2000 else resp
            if any(e in resp.lower() for e in ["error", "failed", "invalid", "unknown"]):
                return f"⚠️ {response_text}"
            return response_text
    except Exception as e:
        return f"❌ RCON Connection Error: {e}"

def send_rcon_silent(command: str):
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            rcon.command(command)
    except:
        pass

def fetch_demos(offset=0, limit=5):
    if not DEMOS_JSON_URL:
        return {"demos": ["DEMOS_JSON_URL not configured"], "has_more": False}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://fshost.me/'
    }
    try:
        response = requests.get(DEMOS_JSON_URL, headers=headers, timeout=15)
        if response.status_code == 403:
            return {"demos": ["Access Denied (403). URL may have expired."], "has_more": False}
        response.raise_for_status()
        data = response.json()
        demos = data.get("demos", [])
        if not demos:
            return {"demos": ["No demos available"], "has_more": False}
        demos_sorted = sorted(demos, key=lambda x: x.get("modified_at", ""), reverse=True)
        start_idx = offset
        end_idx = offset + limit
        page_demos = demos_sorted[start_idx:end_idx]
        has_more = end_idx < len(demos_sorted)
        formatted_demos = []
        for demo in page_demos:
            name = demo.get("name", "Unknown")
            url = demo.get("download_url", "#")
            size = demo.get("size_formatted", "N/A")
            date_str = demo.get("modified_at", "")
            try:
                date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                date_display = date_obj.strftime("%b %d, %Y %H:%M")
            except:
                date_display = "Unknown date"
            formatted_demos.append(
                f"🎬 [{name}](<{url}>)\n    📅 {date_display} • 💾 {size}"
            )
        return {
            "demos": formatted_demos,
            "has_more": has_more,
            "total": len(demos_sorted),
            "showing": f"{start_idx + 1}-{min(end_idx, len(demos_sorted))}"
        }
    except Exception as e:
        return {"demos": [f"Error: {str(e)}"], "has_more": False}

STATUS_NAME_RE = re.compile(r'^#\s*\d+\s+"(?P<n>.*?)"\s+')
CSS_LIST_RE = re.compile(r'^\s*•\s*\[#\d+\]\s*"(?P<n>[^"]*)"')

def sanitize(s: str) -> str:
    if not s:
        return "-"
    for ch in ['*', '_', '`', '~', '|', '>', '@']:
        s = s.replace(ch, f"\\{ch}")
    return s.replace("\x00", "").strip()

def rcon_list_players():
    txt = send_rcon("css_players")
    if "Unknown command" in txt or "Error" in txt:
        txt = send_rcon("status")
    players = []
    for line in txt.splitlines():
        line = line.strip()
        css = CSS_LIST_RE.match(line)
        if css:
            players.append({"name": sanitize(css.group("name")), "ping": "-", "time": "-"})
            continue
        m = STATUS_NAME_RE.match(line)
        if m:
            name = sanitize(m.group("name"))
            time_match = re.search(r'\b(\d{1,2}:\d{2})\b', line)
            ping_match = re.search(r'(\d+)\s*$', line.split('"')[-1])
            players.append({
                "name": name,
                "time": time_match.group(1) if time_match else "-",
                "ping": ping_match.group(1) if ping_match else "-",
            })
    uniq = []
    seen = set()
    for p in players:
        if p["name"] not in seen:
            uniq.append(p)
            seen.add(p["name"])
    return uniq

def flag(cc):
    if not cc or len(cc) != 2:
        return "🏳️"
    return "".join(chr(ord(c.upper()) + 127397) for c in cc)

async def fetch_faceit_stats_cs2(nickname: str) -> dict:
    if not FACEIT_API_KEY:
        raise ValueError("FACEIT_API_KEY missing")
    headers = {"Authorization": f"Bearer {FACEIT_API_KEY}"}
    info_url = f"https://open.faceit.com/data/v4/players?nickname={nickname}"
    r = requests.get(info_url, headers=headers)
    if r.status_code == 404:
        raise ValueError("Player not found on FACEIT.")
    r.raise_for_status()
    pdata = r.json()
    pid = pdata.get("player_id")
    stats_url = f"https://open.faceit.com/data/v4/players/{pid}/stats/{FACEIT_GAME_ID}"
    s = requests.get(stats_url, headers=headers)
    if s.status_code == 404:
        raise ValueError("Player has not played CS2 on FACEIT.")
    s.raise_for_status()
    stats = s.json().get("lifetime", {})
    game_info = pdata.get("games", {}).get(FACEIT_GAME_ID, {})
    return {
        "nickname": pdata.get("nickname"),
        "player_id": pid,
        "country_flag": flag(pdata.get("country")),
        "avatar": pdata.get("avatar"),
        "level": game_info.get("skill_level"),
        "elo": game_info.get("faceit_elo"),
        "matches": stats.get("Matches"),
        "win_rate": stats.get("Win Rate %"),
        "kd_ratio": stats.get("Average K/D Ratio"),
    }

async def get_enhanced_status_embed():
    addr = (SERVER_IP, SERVER_PORT)
    try:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, a2s.info, addr)
        a2s_players = await asyncio.wait_for(
            loop.run_in_executor(None, a2s.players, addr), 5
        )
        if not a2s_players or all(not getattr(p, "name", "") for p in a2s_players):
            players = rcon_list_players()
        else:
            players = a2s_players
        
        player_count = info.player_count
        if player_count == 0:
            color = 0x95A5A6
        elif player_count < info.max_players / 3:
            color = 0xE74C3C
        elif player_count < info.max_players * 2/3:
            color = 0xF39C12
        else:
            color = 0x2ECC71
        
        embed = discord.Embed(
            title="🎮 CS2 Server Status",
            description=f"**{info.server_name}**",
            color=color,
            timestamp=datetime.now()
        )
        embed.add_field(name="🗺️ Current Map", value=f"`{info.map_name}`", inline=True)
        embed.add_field(name="👥 Players", value=f"`{player_count}/{info.max_players}`", inline=True)
        embed.add_field(name="🌐 Connect", value=f"`connect {SERVER_IP}:{SERVER_PORT}`", inline=False)
        
        if isinstance(players, list) and players and isinstance(players[0], dict):
            listing = "\n".join(f"`{i}.` **{p['name']}**" for i, p in enumerate(players, 1))
        elif players:
            listing = "\n".join(
                f"`{i}.` **{sanitize(p.name)}** • `{p.score}` pts"
                for i, p in enumerate(players, 1)
            )
        else:
            listing = "*No players online*"
        
        embed.add_field(
            name=f"🎯 Players Online ({player_count})",
            value=listing if len(listing) < 1024 else listing[:1020] + "...",
            inline=False
        )
        embed.set_footer(
            text="Last updated",
            icon_url="https://cdn.cloudflare.steamstatic.com/steamcommunity/public/images/apps/730/69f7ebe2735c366c65c0b33dae00e12dc40edbe4.jpg"
        )
        return embed, info
    except:
        embed = discord.Embed(
            title="❌ Server Offline",
            description="The server appears to be offline or unreachable.",
            color=0xFF0000,
            timestamp=datetime.now()
        )
        embed.set_footer(text="Status check failed")
        return embed, None

def generate_player_graph():
    if not HAS_MATPLOTLIB:
        return None
    conn = get_db()
    c = conn.cursor()
    # MySQL uses NOW() / INTERVAL syntax
    c.execute('''SELECT timestamp, player_count
                 FROM server_snapshots
                 WHERE timestamp > DATE_SUB(NOW(), INTERVAL 24 HOUR)
                 ORDER BY timestamp''')
    data = c.fetchall()
    c.close()
    conn.close()
    if len(data) < 2:
        return None
    timestamps = [row[0] if isinstance(row[0], datetime) else datetime.fromisoformat(str(row[0])) for row in data]
    counts = [row[1] for row in data]
    plt.figure(figsize=(12, 6))
    plt.plot(timestamps, counts, linewidth=2, color='#5865F2', marker='o')
    plt.fill_between(timestamps, counts, alpha=0.3, color='#5865F2')
    plt.title('Server Player Count (Last 24 Hours)', fontsize=16, fontweight='bold')
    plt.xlabel('Time', fontsize=12)
    plt.ylabel('Players', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.gcf().autofmt_xdate()
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=2))
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    return buf

def record_snapshot(player_count, map_name):
    conn = get_db()
    c = conn.cursor()
    c.execute('INSERT INTO server_snapshots (player_count, map_name) VALUES (%s, %s)',
              (player_count, map_name))
    conn.commit()
    c.close()
    conn.close()

def update_player_tracking(current_player_names, map_name):
    global current_players
    now = datetime.now()
    notifications = []
    conn = get_db()
    c = conn.cursor()
    for player_name in list(current_players.keys()):
        if player_name not in current_player_names:
            join_time = current_players[player_name]
            duration = int((now - join_time).total_seconds() / 60)
            hours = duration // 60
            mins = duration % 60
            time_str = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"
            c.execute('''INSERT INTO player_sessions
                         (player_name, join_time, leave_time, duration_minutes, map_name)
                         VALUES (%s, %s, %s, %s, %s)''',
                      (player_name, join_time, now, duration, map_name))
            notifications.append({'type': 'leave', 'player': player_name, 'duration': time_str})
            del current_players[player_name]
    for player_name in current_player_names:
        if player_name not in current_players:
            current_players[player_name] = now
            notifications.append({'type': 'join', 'player': player_name, 'map': map_name})
    conn.commit()
    c.close()
    conn.close()
    return notifications

def get_player_stats(player_name):
    """
    Combines session data (bot-tracked) with MatchZy K/D/stats.
    Falls back to bot's own player_stats table if MatchZy unavailable.
    """
    conn = get_db()
    c = conn.cursor()
    
    c.execute('''SELECT SUM(duration_minutes), COUNT(*)
                 FROM player_sessions WHERE player_name = %s''', (player_name,))
    result = c.fetchone()
    total_minutes = result[0] or 0
    total_sessions = result[1] or 0
    
    c.execute('''SELECT map_name, SUM(duration_minutes) as total
                 FROM player_sessions WHERE player_name = %s
                 GROUP BY map_name ORDER BY total DESC LIMIT 1''', (player_name,))
    fav_map = c.fetchone()
    favorite_map = fav_map[0] if fav_map else "N/A"
    
    c.execute('''SELECT leave_time FROM player_sessions
                 WHERE player_name = %s ORDER BY leave_time DESC LIMIT 1''', (player_name,))
    last_seen = c.fetchone()
    last_seen_time = last_seen[0] if last_seen else None
    c.close()
    conn.close()

    # Try MatchZy first for K/D
    mz = get_matchzy_player_stats(player_name=player_name)
    if mz:
        kills        = int(mz.get("kills") or 0)
        deaths       = int(mz.get("deaths") or 0)
        assists      = int(mz.get("assists") or 0)
        hs           = int(mz.get("headshots") or 0)
        total_damage = int(mz.get("total_damage") or 0)
        hs_pct       = mz.get("hs_pct")
        aces         = int(mz.get("aces") or 0)
        clutch_wins  = int(mz.get("clutch_wins") or 0)
        entry_wins   = int(mz.get("entry_wins") or 0)
        matches      = int(mz.get("matches_played") or 0)
        kd_ratio     = kills / deaths if deaths > 0 else float(kills)
    else:
        # Fallback: bot's own table
        conn2 = get_db()
        c2 = conn2.cursor()
        c2.execute('SELECT kills, deaths FROM player_stats WHERE player_name = %s', (player_name,))
        kd_result = c2.fetchone()
        c2.close()
        conn2.close()
        if kd_result:
            kills, deaths = kd_result
            kd_ratio = kills / deaths if deaths > 0 else float(kills)
        else:
            kills, deaths, kd_ratio = 0, 0, 0.0
        assists = hs = total_damage = hs_pct = aces = clutch_wins = entry_wins = matches = None
    
    return {
        'total_minutes': total_minutes,
        'total_sessions': total_sessions,
        'favorite_map': favorite_map,
        'last_seen': last_seen_time,
        'kills': kills,
        'deaths': deaths,
        'assists': assists,
        'headshots': hs,
        'kd_ratio': kd_ratio,
        'total_damage': total_damage if mz else None,
        'hs_pct': hs_pct,
        'aces': aces if mz else None,
        'clutch_wins': clutch_wins if mz else None,
        'entry_wins': entry_wins if mz else None,
        'matches_played': matches,
        'source': 'matchzy' if mz else 'bot',
    }

def update_kd_stats(events):
    """Update kill/death stats in fallback bot table."""
    if not events:
        return
    conn = get_db()
    c = conn.cursor()
    for killer, victim in events:
        c.execute('''INSERT INTO player_stats (player_name, kills, deaths)
                     VALUES (%s, 1, 0)
                     ON DUPLICATE KEY UPDATE kills = kills + 1''', (killer,))
        c.execute('''INSERT INTO player_stats (player_name, kills, deaths)
                     VALUES (%s, 0, 1)
                     ON DUPLICATE KEY UPDATE deaths = deaths + 1''', (victim,))
    conn.commit()
    c.close()
    conn.close()
    print(f"Updated K/D stats: {len(events)} kill events processed")

# ========== BACKGROUND TASKS ==========
@tasks.loop(minutes=1)
async def update_server_stats():
    try:
        global pending_kill_events
        if pending_kill_events:
            events_to_process = pending_kill_events.copy()
            pending_kill_events = []
            update_kd_stats(events_to_process)
            print(f"K/D: Processed {len(events_to_process)} kills from HTTP logs")
        
        addr = (SERVER_IP, SERVER_PORT)
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, a2s.info, addr)
        
        try:
            a2s_players = await asyncio.wait_for(
                loop.run_in_executor(None, a2s.players, addr), 5
            )
            if not a2s_players:
                player_list = rcon_list_players()
            else:
                player_list = a2s_players
        except:
            player_list = []
        
        if player_list and isinstance(player_list[0], dict):
            player_names = [p['name'] for p in player_list]
        elif player_list:
            player_names = [sanitize(p.name) for p in player_list]
        else:
            player_names = []
        
        real_player_names = [name for name in player_names if not is_bot_player(name)]
        record_snapshot(len(real_player_names), info.map_name)
        notifications = update_player_tracking(real_player_names, info.map_name)
        
        if notifications and NOTIFICATIONS_CHANNEL_ID:
            channel = bot.get_channel(NOTIFICATIONS_CHANNEL_ID)
            if channel:
                for notif in notifications:
                    if notif['type'] == 'join':
                        embed = discord.Embed(
                            description=f"👋 **{notif['player']}** joined the server",
                            color=0x2ECC71
                        )
                        embed.add_field(name="Current Map", value=f"`{notif['map']}`", inline=True)
                        embed.set_footer(text=f"Players online: {len(real_player_names)}")
                        await channel.send(embed=embed)
                    elif notif['type'] == 'leave':
                        embed = discord.Embed(
                            description=f"👋 **{notif['player']}** left the server",
                            color=0x95A5A6
                        )
                        embed.add_field(name="Session Duration", value=f"`{notif['duration']}`", inline=True)
                        embed.set_footer(text=f"Players online: {len(real_player_names)}")
                        await channel.send(embed=embed)
    except Exception as e:
        print(f"Error in update_server_stats: {e}")


@tasks.loop(minutes=10)
async def update_status_message():
    if not STATUS_CHANNEL_ID:
        return
    try:
        channel = bot.get_channel(STATUS_CHANNEL_ID)
        if not channel:
            return
        embed, info = await get_enhanced_status_embed()
        async for message in channel.history(limit=20):
            if message.author == bot.user and message.embeds:
                if "CS2 Server Status" in message.embeds[0].title:
                    await message.edit(embed=embed)
                    return
        await channel.send(embed=embed)
    except Exception as e:
        print(f"Error updating status message: {e}")

@bot.event
async def on_ready():
    print(f"Bot online as {bot.user.name}")
    print(f"Bot ID: {bot.user.id}")
    print(f"Owner ID from env: {OWNER_ID}")
    try:
        await start_http_server()
    except Exception as e:
        print(f"⚠️ Failed to start HTTP server: {e}")
    try:
        send_rcon_silent("mp_logdetail 3")
        send_rcon_silent("log on")
        print("✓ Server kill logging enabled (mp_logdetail 3)")
    except Exception as e:
        print(f"⚠️ Could not enable kill logging: {e}")
    print("Syncing slash commands...")
    try:
        synced = await bot.tree.sync()
        print(f"✓ Synced {len(synced)} commands globally")
    except Exception as e:
        print(f"✗ Failed to sync commands: {e}")
    
    # Log MatchZy status on startup
    try:
        conn = get_db()
        has_mz = matchzy_tables_exist(conn)
        conn.close()
        print(f"✓ MatchZy tables {'found — using MatchZy stats' if has_mz else 'NOT found — using fallback stats'}")
    except Exception as e:
        print(f"⚠️ Could not check MatchZy tables: {e}")
    
    update_server_stats.start()
    update_status_message.start()

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    if message.content.startswith('!') and message.author.id == OWNER_ID:
        print(f"Owner command detected: {message.content}")
    await bot.process_commands(message)

# ========== COMMANDS ==========
@bot.command()
@commands.guild_only()
@commands.is_owner()
async def sync(ctx, guilds: commands.Greedy[discord.Object] = None,
               spec: Optional[Literal["~", "*", "^"]] = None):
    await ctx.send("⏳ Syncing commands...", delete_after=5)
    if not guilds:
        if spec == "~":
            synced = await bot.tree.sync(guild=ctx.guild)
        elif spec == "*":
            bot.tree.copy_global_to(guild=ctx.guild)
            synced = await bot.tree.sync(guild=ctx.guild)
        elif spec == "^":
            bot.tree.clear_commands(guild=ctx.guild)
            await bot.tree.sync(guild=ctx.guild)
            await ctx.send("✅ Cleared all commands from this server.")
            return
        else:
            synced = await bot.tree.sync()
        await ctx.send(f"✅ Synced {len(synced)} commands.")
        return
    count = sum(1 for guild in guilds if (await bot.tree.sync(guild=guild)) is not None)
    await ctx.send(f"✅ Synced to {count}/{len(guilds)} guilds.")

@bot.command()
async def ping(ctx):
    await ctx.send(f"🏓 Pong! Latency: {round(bot.latency * 1000)}ms")

@bot.tree.command(name="status", description="View server status")
async def status_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    embed, _ = await get_enhanced_status_embed()
    await inter.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="graph", description="View player count graph (24h)")
async def graph_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    if not HAS_MATPLOTLIB:
        return await inter.followup.send("❌ Graph feature requires matplotlib.", ephemeral=True)
    buf = generate_player_graph()
    if buf is None:
        return await inter.followup.send("❌ Not enough data yet (need 2+ snapshots).", ephemeral=True)
    file = discord.File(buf, filename="player_graph.png")
    embed = discord.Embed(title="📊 Player Count Analytics", color=0x5865F2)
    embed.set_image(url="attachment://player_graph.png")
    await inter.followup.send(embed=embed, file=file, ephemeral=True)

@bot.tree.command(name="profile", description="View player statistics (pulls from MatchZy if available)")
async def profile_cmd(inter: discord.Interaction, player_name: str):
    await inter.response.defer(ephemeral=True)
    stats = get_player_stats(player_name)
    if stats['total_sessions'] == 0 and stats['kills'] == 0:
        return await inter.followup.send(f"❌ No stats found for **{player_name}**", ephemeral=True)
    
    hours = stats['total_minutes'] / 60
    avg_session = stats['total_minutes'] / stats['total_sessions'] if stats['total_sessions'] else 0
    
    source_label = "📊 MatchZy" if stats['source'] == 'matchzy' else "📋 Bot-tracked"
    embed = discord.Embed(
        title=f"👤 {player_name}",
        description=f"Stats source: {source_label}",
        color=0x2ECC71
    )
    embed.add_field(name="⏱️ Playtime", value=f"**{hours:.1f}h**", inline=True)
    embed.add_field(name="🎮 Sessions", value=f"**{stats['total_sessions']}**", inline=True)
    embed.add_field(name="⏰ Avg Session", value=f"**{avg_session:.0f}m**", inline=True)
    embed.add_field(name="💀 Kills", value=f"**{stats['kills']}**", inline=True)
    embed.add_field(name="☠️ Deaths", value=f"**{stats['deaths']}**", inline=True)
    embed.add_field(name="📊 K/D", value=f"**{stats['kd_ratio']:.2f}**", inline=True)
    
    if stats.get('assists') is not None:
        embed.add_field(name="🤝 Assists", value=f"**{stats['assists']}**", inline=True)
    if stats.get('headshots') is not None and stats.get('kills', 0) > 0:
        embed.add_field(name="🎯 Headshots", value=f"**{stats['headshots']}** ({stats.get('hs_pct') or 0:.1f}%)", inline=True)
    if stats.get('total_damage') is not None:
        embed.add_field(name="💥 Total Damage", value=f"**{stats['total_damage']:,}**", inline=True)
    if stats.get('aces'):
        embed.add_field(name="⭐ Aces (5K)", value=f"**{stats['aces']}**", inline=True)
    if stats.get('clutch_wins'):
        embed.add_field(name="🔥 1vX Wins", value=f"**{stats['clutch_wins']}**", inline=True)
    if stats.get('entry_wins'):
        embed.add_field(name="🚪 Entry Wins", value=f"**{stats['entry_wins']}**", inline=True)
    if stats.get('matches_played'):
        embed.add_field(name="🏆 Matches", value=f"**{stats['matches_played']}**", inline=True)
    
    embed.add_field(name="🗺️ Fav Map", value=f"`{stats['favorite_map']}`", inline=True)
    
    if stats['last_seen']:
        last_seen_dt = stats['last_seen'] if isinstance(stats['last_seen'], datetime) \
                       else datetime.fromisoformat(str(stats['last_seen']))
        time_ago = datetime.now() - last_seen_dt
        days = time_ago.days
        hours_ago = time_ago.seconds // 3600
        last_seen_str = f"{days}d {hours_ago}h ago" if days > 0 else f"{hours_ago}h ago"
        embed.add_field(name="👁️ Last Seen", value=last_seen_str, inline=True)
    
    await inter.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="leaderboard", description="Top players (MatchZy kills leaderboard)")
async def leaderboard_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    leaderboard = get_matchzy_leaderboard(10)
    if not leaderboard:
        return await inter.followup.send("❌ No player data available yet.", ephemeral=True)
    
    embed = discord.Embed(
        title="🏆 Top Players",
        description="*Sorted by kills • Bots excluded*",
        color=0xF1C40F
    )
    medals = ["🥇", "🥈", "🥉"]
    for i, row in enumerate(leaderboard, 1):
        name     = row.get("player_name", "Unknown")
        kills    = int(row.get("kills") or 0)
        deaths   = int(row.get("deaths") or 0)
        kd       = row.get("kd_ratio")
        damage   = row.get("total_damage")
        hs_pct   = row.get("hs_pct")
        matches  = row.get("matches_played")
        medal    = medals[i-1] if i <= 3 else f"`{i}.`"
        kd_str   = f"K/D: {kd:.2f}" if kd else f"K/D: {kills}/{deaths}"
        extras   = []
        if hs_pct: extras.append(f"HS: {hs_pct}%")
        if matches: extras.append(f"{matches} matches")
        value = f"**{kills} kills** • {kd_str}" + (f" • {' • '.join(extras)}" if extras else "")
        embed.add_field(name=f"{medal} {name}", value=value, inline=False)
    
    await inter.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="recentmatches", description="Show recent MatchZy matches")
async def recentmatches_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    matches = get_matchzy_recent_matches(5)
    if not matches:
        return await inter.followup.send(
            "❌ No match data found. Make sure MatchZy is configured with your MySQL DB.",
            ephemeral=True
        )
    embed = discord.Embed(title="🏟️ Recent Matches", color=0x3498DB)
    for m in matches:
        t1       = m.get("team1_name", "Team 1")
        t2       = m.get("team2_name", "Team 2")
        s1       = m.get("team1_score", 0)
        s2       = m.get("team2_score", 0)
        mapname  = m.get("mapname") or "?"
        winner   = m.get("winner", "")
        end_time = m.get("end_time")
        date_str = end_time.strftime("%b %d %H:%M") if isinstance(end_time, datetime) else str(end_time or "?")
        result   = f"**{t1}** {s1} : {s2} **{t2}**"
        if winner:
            result += f" — 🏆 **{winner}**"
        embed.add_field(
            name=f"🗺️ {mapname} — {date_str}",
            value=result,
            inline=False
        )
    await inter.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="demos", description="View server demos")
async def demos_cmd(inter: discord.Interaction):
    if SERVER_DEMOS_CHANNEL_ID and inter.channel_id != SERVER_DEMOS_CHANNEL_ID:
        return await inter.response.send_message("Wrong channel!", ephemeral=True)
    await inter.response.defer(ephemeral=True)
    result = fetch_demos(0, 5)
    embed = discord.Embed(
        title="🎥 Server Demos",
        description="\n\n".join(result["demos"]),
        color=0x9B59B6
    )
    if result.get("total"):
        embed.set_footer(text=f"Showing {result['showing']} of {result['total']} demos")
    view = DemosView(offset=0)
    if not result.get("has_more", False):
        for item in view.children:
            if item.custom_id == "next":
                item.disabled = True
    await inter.followup.send(embed=embed, view=view, ephemeral=True)

@bot.tree.command(name="elo", description="Get FACEIT stats for CS2")
async def faceit_cmd(inter: discord.Interaction, nickname: str):
    await inter.response.defer(ephemeral=True)
    try:
        stats = await fetch_faceit_stats_cs2(nickname)
    except Exception as e:
        return await inter.followup.send(f"❌ {e}", ephemeral=True)
    embed = discord.Embed(
        title=f"{stats['country_flag']} {stats['nickname']} — FACEIT CS2", color=0xFF8800
    )
    embed.set_thumbnail(url=stats["avatar"])
    embed.add_field(name="Level", value=stats["level"], inline=True)
    embed.add_field(name="ELO", value=stats["elo"], inline=True)
    embed.add_field(name="Win Rate", value=f"{stats['win_rate']}%", inline=True)
    embed.add_field(name="Matches", value=stats["matches"], inline=True)
    embed.add_field(name="K/D", value=stats["kd_ratio"], inline=True)
    embed.set_footer(text=f"Player ID: {stats['player_id']}")
    await inter.followup.send(embed=embed, ephemeral=True)

# ========== ADMIN COMMANDS ==========
@bot.tree.command(name="csssay", description="Send center-screen message to all players")
@owner_only()
async def csssay(inter: discord.Interaction, message: str):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f"css_cssay {message}")
    await inter.followup.send(f"📢 **Message Sent**\n```{message}```\n{resp}", ephemeral=True)

@bot.tree.command(name="csshsay", description="Send hint message to all players")
@owner_only()
async def csshsay(inter: discord.Interaction, message: str):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f"css_hsay {message}")
    await inter.followup.send(f"💬 **Hint Sent**\n```{message}```\n{resp}", ephemeral=True)

@bot.tree.command(name="csskick", description="Kick a player from the server")
@owner_only()
async def csskick(inter: discord.Interaction, player: str):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f'css_kick "{player}"')
    await inter.followup.send(f"👢 **Kick Command**\nPlayer: `{player}`\n\n{resp}", ephemeral=True)

@bot.tree.command(name="cssban", description="Ban a player from the server")
@owner_only()
async def cssban(inter: discord.Interaction, player: str, minutes: int, reason: str = "No reason"):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f'css_ban "{player}" {minutes} "{reason}"')
    await inter.followup.send(
        f"🔨 **Ban**\nPlayer: `{player}` • Duration: `{minutes}m` • Reason: `{reason}`\n\n{resp}",
        ephemeral=True
    )

@bot.tree.command(name="csschangemap", description="Change the server map")
@owner_only()
async def csschangemap(inter: discord.Interaction, map: str):
    if map not in MAP_WHITELIST:
        return await inter.response.send_message(
            f"❌ Map `{map}` not allowed.\nAllowed: {', '.join(MAP_WHITELIST)}", ephemeral=True
        )
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f"css_changemap {map}")
    await inter.followup.send(f"🗺️ Changing to `{map}`\n\n{resp}", ephemeral=True)

@csschangemap.autocomplete("map")
async def autocomplete_map(inter, current: str):
    return [app_commands.Choice(name=m, value=m) for m in MAP_WHITELIST if current.lower() in m.lower()]

@bot.tree.command(name="cssreload")
@owner_only()
async def cssreload(inter):
    resp = send_rcon("css_reloadplugins")
    await inter.response.send_message(resp, ephemeral=True)

@bot.tree.command(name="debugdb", description="Debug database + MatchZy connection")
@owner_only()
async def debugdb_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    lines = []
    try:
        conn = get_db()
        c = conn.cursor()
        
        # Check MatchZy
        has_mz = matchzy_tables_exist(conn)
        lines.append(f"**MatchZy tables:** {'✅ Found' if has_mz else '❌ Not found'}")
        
        if has_mz:
            c.execute(f"SELECT COUNT(*) FROM {MATCHZY_TABLES['players']}")
            rows = c.fetchone()[0]
            lines.append(f"**MatchZy player rows:** {rows}")
            c.execute(f"SELECT COUNT(DISTINCT matchid) FROM {MATCHZY_TABLES['players']}")
            matches = c.fetchone()[0]
            lines.append(f"**MatchZy matches:** {matches}")
        
        # Bot tables
        c.execute("SELECT COUNT(*) FROM player_sessions")
        lines.append(f"**Bot sessions:** {c.fetchone()[0]}")
        c.execute("SELECT COUNT(*) FROM server_snapshots")
        lines.append(f"**Snapshots:** {c.fetchone()[0]}")
        
        c.close()
        conn.close()
    except Exception as e:
        lines.append(f"❌ DB Error: {e}")
    
    await inter.followup.send("\n".join(lines), ephemeral=True)

@bot.tree.command(name="notifications", description="Toggle join/leave notifications")
@owner_only()
async def notifications_cmd(inter: discord.Interaction, enabled: bool,
                             channel: discord.TextChannel = None):
    await inter.response.defer(ephemeral=True)
    if enabled:
        if channel:
            msg = f"✅ Notifications enabled in {channel.mention}\nSet `NOTIFICATIONS_CHANNEL_ID={channel.id}` in Railway to persist."
        elif NOTIFICATIONS_CHANNEL_ID:
            msg = f"✅ Notifications active in <#{NOTIFICATIONS_CHANNEL_ID}>"
        else:
            return await inter.followup.send(
                "❌ Specify a channel: `/notifications enabled:True channel:#your-channel`",
                ephemeral=True
            )
    else:
        msg = "✅ Notifications disabled. Remove `NOTIFICATIONS_CHANNEL_ID` from Railway to persist."
    await inter.followup.send(msg, ephemeral=True)

if not TOKEN:
    raise SystemExit("TOKEN missing.")

bot.run(TOKEN)