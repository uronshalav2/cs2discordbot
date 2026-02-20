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

# Maps accountid (str) → {name, steamid, team, kills, deaths} — built from log lines
_player_registry: dict = {}

# In-memory kill/death counter keyed by player name (reset each round)
# name → {"kills": int, "deaths": int}
_round_kd: dict = {}

# Bot keywords defined early so all helpers below can use them
_BOT_KEYWORDS = ["CSTV", "BOT", "GOTV", "SourceTV"]

def _is_bot_name(name: str) -> bool:
    u = name.upper()
    return any(kw in u for kw in _BOT_KEYWORDS)

# Regex to extract every "name<slot><steamid><team>" actor token in a log line
_ACTOR_RE = re.compile(
    r'"(?P<n>[^"<]+)<\d+><(?P<steamid>[^>]+)><(?P<team>[^>]*)>"'
)

def _update_registry(line: str):
    """
    Parse all actor tokens in a log line and upsert into _player_registry.
    Includes bots (steamid == "BOT"), keyed by "BOT_<name>" so they appear in team tables.
    Real players keyed by accountid from [U:1:ACCOUNTID].
    """
    for m in _ACTOR_RE.finditer(line):
        name    = m.group("n").strip()
        steamid = m.group("steamid").strip()
        team    = m.group("team").strip()
        is_bot  = (steamid == "BOT")

        if is_bot:
            # Key bots by name so each bot is unique
            accountid = f"BOT_{name}"
        else:
            acct_m = re.search(r"U:1:(\d+)", steamid)
            if not acct_m:
                continue
            accountid = acct_m.group(1)

        existing = _player_registry.get(accountid, {})
        if team and team not in ("", "Unassigned"):
            resolved_team = team
        else:
            resolved_team = existing.get("team", "")

        _player_registry[accountid] = {
            "name":      name,
            "steamid":   steamid,
            "accountid": accountid,
            "team":      resolved_team,
            "is_bot":    is_bot,
            "kills":     existing.get("kills",   "0"),
            "deaths":    existing.get("deaths",  "0"),
            "assists":   existing.get("assists", "0"),
            "adr":       existing.get("adr",     "0"),
        }
        print(f"[REG] {'🤖' if is_bot else '👤'} {name} | {steamid} | {resolved_team or '—'}")

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
    Receives two types of POST from MatchZy:
      1. Plain text log lines (prefixed with "L date - time:")
         Contains kills, round_stats block, match events, player actions.
      2. Raw JSON webhook body ({"reason":8,"winner":...,"event":"round_end"})
         Sent by MatchZy after each round — has full player stats with names.
    """
    global pending_kill_events

    try:
        remote       = request.remote
        content_type = request.headers.get("Content-Type", "")
        text         = await request.text()

        if not text.strip():
            return web.Response(text="OK")

        # Ring buffer for /logs debug viewer
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        _recent_log_lines.append(f"── {ts}  {remote}  ({len(text)} bytes) ──")
        for ln in text.splitlines():
            if ln.strip():
                _recent_log_lines.append(f"  {ln}")

        # ════════════════════════════════════════════════════════════════════
        # PATH A: MatchZy JSON round_end webhook
        # Body starts with { and contains "event":"round_end"
        # ════════════════════════════════════════════════════════════════════
        stripped_text = text.strip()
        if stripped_text.startswith("{"):
            try:
                wh = json.loads(stripped_text)
            except json.JSONDecodeError:
                wh = None

            if wh and wh.get("event") == "round_end":
                rnd      = wh.get("round_number", 0)
                map_num  = wh.get("map_number",   0)
                matchid  = wh.get("matchid",      -1)
                winner   = wh.get("winner", {})
                t1       = wh.get("team1", {})
                t2       = wh.get("team2", {})

                # Scores: team1 is always CT side in MatchZy JSON
                score_ct = t1.get("score", _match_state["score_ct"])
                score_t  = t2.get("score", _match_state["score_t"])

                # Build enriched player list from webhook (has real names!)
                players = []
                for team_data, team_code in [(t1, "3"), (t2, "2")]:
                    for p in team_data.get("players", []):
                        steamid64 = str(p.get("steamid", ""))
                        name      = p.get("name", "?")
                        stats     = p.get("stats", {})
                        # Derive accountid from steamid64
                        # steamid64 = 76561197960265728 + accountid
                        try:
                            acct = str(int(steamid64) - 76561197960265728)
                        except (ValueError, TypeError):
                            acct = steamid64

                        # Update registry with authoritative name
                        if acct not in ("0", ""):
                            existing = _player_registry.get(acct, {})
                            _player_registry[acct] = {
                                "name":      name,
                                "steamid":   f"[U:1:{acct}]",
                                "accountid": acct,
                                "team":      "CT" if team_code == "3" else "TERRORIST",
                            }

                        players.append({
                            "accountid": acct,
                            "name":      name,
                            "steamid":   steamid64,
                            "team":      team_code,
                            "kills":     str(stats.get("kills",   0)),
                            "deaths":    str(stats.get("deaths",  0)),
                            "assists":   str(stats.get("assists", 0)),
                            "dmg":       str(stats.get("damage",  0)),
                            "hsp":       str(round(stats.get("headshot_kills", 0) /
                                           max(stats.get("kills", 1), 1) * 100, 1)),
                            "adr":       str(round(stats.get("damage", 0) /
                                           max(rnd, 1), 1)),
                            "mvp":       str(stats.get("mvp", 0)),
                        })

                _match_state.update({
                    "active":   True,
                    "warmup":   False,
                    "round":    rnd,
                    "score_ct": score_ct,
                    "score_t":  score_t,
                })
                if not _match_state["started_at"]:
                    _match_state["started_at"] = datetime.utcnow().isoformat()

                print(f"[WH] round_end  round={rnd}  CT:{score_ct} T:{score_t}  "
                      f"winner={winner.get('team','?')}  players={len(players)}")

                _sse_broadcast("round_stats", {
                    "round":    rnd,
                    "score_t":  score_t,
                    "score_ct": score_ct,
                    "map":      _match_state["map"],
                    "server":   _match_state["server"],
                    "players":  players,
                })
            return web.Response(text="OK")

        # ════════════════════════════════════════════════════════════════════
        # PATH B: Plain text log lines
        # ════════════════════════════════════════════════════════════════════
        raw_lines = text.splitlines()

        # Update player registry from every line
        for line in raw_lines:
            _update_registry(_strip_prefix(line))

        # ── MatchZy round_stats block (JSON_BEGIN…JSON_END) ──────────────────
        block = _parse_matchzy_block(raw_lines)
        if block:
            rnd      = int(block.get("round_number", 0) or 0)
            score_t  = int(block.get("score_t",  0) or 0)
            score_ct = int(block.get("score_ct", 0) or 0)
            map_name = block.get("map",    _match_state["map"])
            server   = block.get("server", _match_state["server"])
            players_raw = block.get("players", {})
            fields      = [f.strip() for f in block.get("fields", "").split(",")]

            players = []
            for pval in players_raw.values():
                vals = [v.strip() for v in pval.split(",")]
                if len(vals) != len(fields):
                    continue
                row  = dict(zip(fields, vals))
                acct = row.get("accountid", "0").strip()
                if acct == "0":
                    continue
                reg = _player_registry.get(acct, {})
                row["name"]    = reg.get("name", acct)
                row["steamid"] = reg.get("steamid", "")
                # Normalise team code using registry (more reliable)
                reg_team = reg.get("team", "")
                if "CT" in reg_team.upper():
                    row["team"] = "3"
                elif "TERRORIST" in reg_team.upper():
                    row["team"] = "2"
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

            print(f"[LOG] round_stats  round={rnd}  {map_name}  T:{score_t} CT:{score_ct}  players={len(players)}")
            _sse_broadcast("round_stats", {
                "round":    rnd,
                "score_t":  score_t,
                "score_ct": score_ct,
                "map":      map_name,
                "server":   server,
                "players":  players,
            })

        # ── Parse MatchStatus score lines as fallback score source ────────────
        # "MatchStatus: Score: 9:2 on map "de_dust2" RoundsPlayed: 11"
        score_re = re.compile(r'MatchStatus: Score: (\d+):(\d+) on map "([^"]+)"')
        for line in raw_lines:
            c = _strip_prefix(line).strip()
            sm = score_re.search(c)
            if sm:
                s_ct = int(sm.group(1))
                s_t  = int(sm.group(2))
                mn   = sm.group(3)
                # Only update if score actually changed (avoid stale repeated lines)
                if (s_ct != _match_state["score_ct"] or
                    s_t  != _match_state["score_t"]  or
                    mn   != _match_state["map"]):
                    _match_state["score_ct"] = s_ct
                    _match_state["score_t"]  = s_t
                    _match_state["map"]      = mn
                    _match_state["active"]   = True
                    print(f"[LOG] score  CT:{s_ct} T:{s_t}  map={mn}")
                    _sse_broadcast("score", {"score_ct": s_ct, "score_t": s_t, "map": mn})

        # ── Kill lines ────────────────────────────────────────────────────────
        kill_re = re.compile(
            r'"([^"<]+)<\d+><[^>]*><([^>]*)>" \[.*?\] killed '
            r'"([^"<]+)<\d+><[^>]*><[^>]*>" \[.*?\] with "([^"]+)"'
        )
        for line in raw_lines:
            c = _strip_prefix(line).strip()
            if " killed " not in c:
                continue
            km = kill_re.search(c)
            if not km:
                # Fallback simpler pattern (no coords)
                km = re.search(
                    r'"([^"<]+)<\d+><[^>]*><([^>]*)>" killed '
                    r'"([^"<]+)<\d+><[^>]*><[^>]*>" with "([^"]+)"', c)
            if km:
                killer      = km.group(1).strip()
                killer_team = km.group(2).strip()
                victim      = km.group(3).strip()
                weapon      = km.group(4).strip()
                side        = "CT" if "CT" in killer_team.upper() else "T"
                # Label bots with 🤖 prefix for display, but still show them
                killer_disp = f"🤖 {killer}" if _is_bot_name(killer) else killer
                victim_disp = f"🤖 {victim}" if _is_bot_name(victim) else victim
                entry = {
                    "killer": killer_disp,
                    "victim": victim_disp,
                    "weapon": weapon,
                    "side":   side,
                    "ts":     datetime.utcnow().strftime("%H:%M:%S"),
                }
                _kill_feed.appendleft(entry)
                pending_kill_events.append((killer, victim))
                # Track kills/deaths in memory for live player table
                _round_kd.setdefault(killer, {"kills": 0, "deaths": 0})["kills"] += 1
                _round_kd.setdefault(victim, {"kills": 0, "deaths": 0})["deaths"] += 1
                print(f"[LOG] kill  {killer_disp}({side}) → {victim_disp} [{weapon}]")
                _sse_broadcast("kill", entry)

        # ── Broadcast live player list from registry after each log batch ─────
        # This populates team tables in real-time without waiting for round_stats
        if _player_registry:
            live_players = []
            for acct, p in _player_registry.items():
                team = p.get("team", "")
                team_code = "3" if "CT" in team.upper() else ("2" if "TERRORIST" in team.upper() else "")
                if not team_code:
                    continue
                pname = p["name"]
                # Merge in-memory kill/death counts (updated on every kill line)
                kd = _round_kd.get(pname, {"kills": 0, "deaths": 0})
                live_players.append({
                    "accountid": acct,
                    "name":      ("🤖 " + pname) if p.get("is_bot") else pname,
                    "steamid":   p.get("steamid", ""),
                    "team":      team_code,
                    "kills":     str(kd["kills"]),
                    "deaths":    str(kd["deaths"]),
                    "assists":   "0",
                    "adr":       "0",
                })
            if live_players:
                _sse_broadcast("players", {"players": live_players})

        # ── Match lifecycle events ────────────────────────────────────────────
        for line in raw_lines:
            c = _strip_prefix(line).strip()

            if "Match_Start" in c:
                map_m = re.search(r'on "([^"]+)"', c)
                mn    = map_m.group(1) if map_m else _match_state["map"]
                _match_state.update({"active": True, "warmup": False, "map": mn,
                                     "score_t": 0, "score_ct": 0, "round": 0,
                                     "started_at": datetime.utcnow().isoformat()})
                _kill_feed.clear()
                _round_kd.clear()
                print(f"[LOG] match_start  map={mn}")
                _sse_broadcast("match_start", {"map": mn})

            elif "Match_Over" in c or "Game_Over" in c:
                _match_state["active"] = False
                print(f"[LOG] match_over  CT:{_match_state['score_ct']} T:{_match_state['score_t']}")
                _sse_broadcast("match_end", {
                    "score_t":  _match_state["score_t"],
                    "score_ct": _match_state["score_ct"],
                    "map":      _match_state["map"],
                })

            elif "Warmup_Start" in c:
                _match_state["warmup"] = True
                _sse_broadcast("warmup", {})

        return web.Response(text="OK")

    except Exception as e:
        print(f"[LOG] ❌ Error: {e}")
        import traceback; traceback.print_exc()
        return web.Response(text="Error", status=500)


async def handle_health_check(request):
    return web.Response(text='Bot is running')

async def handle_api_state(request):
    """GET /api/state — returns full match state + player registry as JSON."""
    return web.Response(
        text=json.dumps(_match_state_snapshot()),
        content_type='application/json',
        headers={"Access-Control-Allow-Origin": "*"},
    )



# ─────────────────────────────────────────────────────────────────────────────
# STATS WEBSITE API ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

def _json_response(data):
    return web.Response(
        text=json.dumps(data, default=str),
        content_type='application/json',
        headers={"Access-Control-Allow-Origin": "*"},
    )

async def handle_api_leaderboard(request):
    """GET /api/leaderboard — top players from MatchZy"""
    try:
        limit = int(request.rel_url.query.get('limit', 50))
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT name, steamid64,
                COUNT(DISTINCT matchid) AS matches,
                SUM(kills) AS kills, SUM(deaths) AS deaths, SUM(assists) AS assists,
                SUM(head_shot_kills) AS headshots,
                SUM(damage) AS total_damage,
                SUM(enemies5k) AS aces,
                SUM(v1_wins) AS clutch_wins,
                ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2) AS kd,
                ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1) AS hs_pct,
                ROUND(SUM(damage)/NULLIF(COUNT(DISTINCT CONCAT(matchid,mapnumber)),0)/30,1) AS adr
            FROM {MATCHZY_TABLES['players']}
            WHERE steamid64 != '0' AND steamid64 != ''
            GROUP BY steamid64, name
            ORDER BY kills DESC
            LIMIT %s
        """, (limit,))
        rows = c.fetchall()
        c.close(); conn.close()
        return _json_response(rows)
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_api_player(request):
    """GET /api/player/{name} — full career stats for a player"""
    name = request.match_info.get('name', '')
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        # Career totals
        c.execute(f"""
            SELECT name, steamid64,
                COUNT(DISTINCT matchid) AS matches,
                SUM(kills) AS kills, SUM(deaths) AS deaths, SUM(assists) AS assists,
                SUM(head_shot_kills) AS headshots, SUM(damage) AS total_damage,
                SUM(enemies5k) AS aces, SUM(enemies4k) AS quads,
                SUM(v1_wins) AS clutch_1v1, SUM(v2_wins) AS clutch_1v2,
                SUM(entry_wins) AS entry_wins, SUM(entry_count) AS entry_attempts,
                SUM(flash_successes) AS flashes_thrown,
                ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2) AS kd,
                ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1) AS hs_pct,
                ROUND(SUM(damage)/NULLIF(COUNT(DISTINCT CONCAT(matchid,mapnumber)),0)/30,1) AS adr
            FROM {MATCHZY_TABLES['players']}
            WHERE name = %s AND steamid64 != '0'
            GROUP BY steamid64, name
        """, (name,))
        career = c.fetchone()
        if not career:
            c.close(); conn.close()
            return _json_response({"error": "Player not found"})
        # Recent matches
        c.execute(f"""
            SELECT p.matchid, p.mapnumber, p.team,
                p.kills, p.deaths, p.assists, p.damage, p.head_shot_kills,
                p.enemies5k, p.v1_wins,
                m.mapname, m.winner, m.team1_score, m.team2_score,
                mm.team1_name, mm.team2_name,
                ROUND(p.damage/30,1) AS adr,
                ROUND(p.head_shot_kills/NULLIF(p.kills,0)*100,1) AS hs_pct
            FROM {MATCHZY_TABLES['players']} p
            LEFT JOIN {MATCHZY_TABLES['maps']} m ON p.matchid=m.matchid AND p.mapnumber=m.mapnumber
            LEFT JOIN {MATCHZY_TABLES['matches']} mm ON p.matchid=mm.matchid
            WHERE p.name = %s AND p.steamid64 != '0'
            ORDER BY p.matchid DESC, p.mapnumber DESC
            LIMIT 20
        """, (name,))
        recent = c.fetchall()
        c.close(); conn.close()
        return _json_response({"career": career, "recent_matches": recent})
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_api_matches(request):
    """GET /api/matches — recent matches with full scoreboard"""
    try:
        limit = int(request.rel_url.query.get('limit', 20))
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT mm.matchid, mm.team1_name, mm.team2_name,
                mm.team1_score, mm.team2_score, mm.winner,
                mm.start_time, mm.end_time,
                m.mapname, m.mapnumber
            FROM {MATCHZY_TABLES['matches']} mm
            LEFT JOIN {MATCHZY_TABLES['maps']} m ON mm.matchid=m.matchid
            WHERE mm.end_time IS NOT NULL
            ORDER BY mm.end_time DESC
            LIMIT %s
        """, (limit,))
        matches = c.fetchall()
        c.close(); conn.close()
        return _json_response(matches)
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_api_match(request):
    """GET /api/match/{matchid} — full scoreboard for a match"""
    matchid = request.match_info.get('matchid', '')
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        # Match meta
        c.execute(f"SELECT * FROM {MATCHZY_TABLES['matches']} WHERE matchid=%s", (matchid,))
        meta = c.fetchone()
        # Map(s)
        c.execute(f"SELECT * FROM {MATCHZY_TABLES['maps']} WHERE matchid=%s ORDER BY mapnumber", (matchid,))
        maps = c.fetchall()
        # All players
        c.execute(f"""
            SELECT p.*,
                ROUND(p.head_shot_kills/NULLIF(p.kills,0)*100,1) AS hs_pct,
                ROUND(p.damage/30,1) AS adr
            FROM {MATCHZY_TABLES['players']} p
            WHERE p.matchid=%s
            ORDER BY p.mapnumber, p.team, p.kills DESC
        """, (matchid,))
        players = c.fetchall()
        c.close(); conn.close()
        return _json_response({"meta": meta, "maps": maps, "players": players})
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_stats_page(request):
    """GET /stats — main stats website (SPA)"""
    html = _build_stats_html()
    return web.Response(text=html, content_type='text/html')

def _build_stats_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>CS2 Stats</title>
<link href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#0d0f13;--surface:#141720;--surface2:#1a1f2e;--border:#212535;--border2:#2d3347;
  --orange:#ff5500;--orange2:#ff7733;--orange-glow:rgba(255,85,0,.15);
  --ct:#5bc4f5;--t:#f0a842;--win:#22c55e;--loss:#ef4444;
  --text:#bcc4d0;--white:#eef1f6;--muted:#4b5568;--muted2:#6b7a94;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;font-size:13px}

/* NAV */
nav{background:var(--surface);border-bottom:2px solid var(--border);display:flex;align-items:center;padding:0 20px;height:50px;position:sticky;top:0;z-index:200;gap:0}
.logo{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:18px;letter-spacing:2px;color:var(--orange);text-transform:uppercase;margin-right:28px;white-space:nowrap}
.tabs{display:flex;gap:0;height:100%}
.tab{height:100%;padding:0 16px;display:flex;align-items:center;font-family:'Rajdhani',sans-serif;font-weight:600;font-size:12px;letter-spacing:1.5px;text-transform:uppercase;color:var(--muted2);cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;transition:all .18s}
.tab:hover{color:var(--text);background:rgba(255,255,255,.03)}
.tab.active{color:var(--orange);border-bottom-color:var(--orange)}
.nav-right{margin-left:auto;display:flex;gap:8px}
.btn-sm{padding:5px 12px;border:1px solid var(--border2);border-radius:3px;font-size:11px;font-family:'Rajdhani',sans-serif;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:var(--muted2);cursor:pointer;transition:all .18s;white-space:nowrap}
.btn-sm:hover{border-color:var(--orange);color:var(--orange)}
.btn-sm.active-btn{border-color:var(--orange);color:var(--orange);background:var(--orange-glow)}

/* LAYOUT */
#app{max-width:1100px;margin:0 auto;padding:20px 16px}
.page-title{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:20px;letter-spacing:1px;text-transform:uppercase;color:var(--white);margin-bottom:16px;display:flex;align-items:center;gap:10px}
.page-title .sub{font-size:11px;color:var(--muted2);font-weight:500;letter-spacing:.5px;text-transform:none;font-family:'Inter',sans-serif}

/* CARDS */
.card{background:var(--surface);border:1px solid var(--border);border-radius:4px;overflow:hidden}

/* LEADERBOARD */
.lb-wrap{overflow-x:auto}
.lb-table{width:100%;border-collapse:collapse;min-width:680px}
.lb-table thead tr{background:rgba(0,0,0,.4)}
.lb-table th{padding:8px 12px;text-align:right;font-family:'Rajdhani',sans-serif;font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--muted2);border-bottom:1px solid var(--border);white-space:nowrap}
.lb-table th:first-child,.lb-table th:nth-child(2){text-align:left}
.lb-table td{padding:9px 12px;text-align:right;border-bottom:1px solid var(--border);font-size:13px;white-space:nowrap}
.lb-table td:first-child{text-align:center;width:40px;font-family:'Rajdhani',sans-serif;font-weight:700;color:var(--muted)}
.lb-table td:nth-child(2){text-align:left}
.lb-table tbody tr{cursor:pointer;transition:background .12s}
.lb-table tbody tr:hover td{background:rgba(255,85,0,.04)}
.lb-table tbody tr:last-child td{border-bottom:none}
.rank-gold td:first-child{color:#f5b942}
.rank-silver td:first-child{color:#a0aec0}
.rank-bronze td:first-child{color:#b87333}
.pname{font-weight:600;color:var(--white);font-family:'Rajdhani',sans-serif;font-size:15px;letter-spacing:.5px}
.pname:hover{color:var(--orange)}
.kd-num{font-family:'Rajdhani',sans-serif;font-weight:600;font-size:15px}
.kd-g{color:var(--win)}.kd-n{color:var(--text)}.kd-b{color:var(--loss)}
.hs-bar-wrap{display:flex;align-items:center;gap:6px;justify-content:flex-end}
.hs-bar{height:4px;background:var(--border2);border-radius:2px;width:50px;overflow:hidden;flex-shrink:0}
.hs-bar-fill{height:100%;background:var(--orange);border-radius:2px;transition:width .4s}
.hs-val{font-size:11px;color:var(--orange);width:34px;text-align:right}

/* MVP CARD */
.mvp-card{background:linear-gradient(135deg,#141c2e 0%,#1a1030 50%,#0d1a1a 100%);border:1px solid var(--border);border-radius:4px;padding:20px 24px;display:flex;align-items:center;gap:24px;margin-bottom:12px;position:relative;overflow:hidden}
.mvp-card::before{content:'MVP';position:absolute;right:20px;top:16px;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:13px;letter-spacing:3px;color:var(--orange);border:1px solid var(--orange);padding:2px 10px;border-radius:2px}
.mvp-avatar{width:64px;height:64px;border-radius:50%;background:linear-gradient(135deg,var(--orange),var(--orange2));display:flex;align-items:center;justify-content:center;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:24px;color:#000;flex-shrink:0;border:2px solid rgba(255,85,0,.4)}
.mvp-info{flex:1}
.mvp-name{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:22px;color:var(--white);letter-spacing:.5px;margin-bottom:4px}
.mvp-stats{display:flex;gap:20px;flex-wrap:wrap}
.mvp-stat{display:flex;flex-direction:column}
.mvp-val{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:20px;color:var(--white);line-height:1}
.mvp-lbl{font-size:10px;letter-spacing:1px;text-transform:uppercase;color:var(--muted2);margin-top:2px}

/* AWARD CARDS */
.awards-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px}
.award-card{background:var(--surface2);border:1px solid var(--border);border-radius:4px;padding:12px 14px;display:flex;align-items:center;gap:12px}
.award-avatar{width:36px;height:36px;border-radius:50%;background:var(--surface);border:1px solid var(--border2);display:flex;align-items:center;justify-content:center;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:14px;color:var(--text);flex-shrink:0}
.award-name{font-family:'Rajdhani',sans-serif;font-weight:600;font-size:14px;color:var(--white)}
.award-val{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:22px;color:var(--white);margin-left:auto;line-height:1}
.award-lbl{font-size:10px;color:var(--muted2);text-align:right;margin-top:1px}

/* MATCH SCOREBOARD */
.match-header-card{background:var(--surface);border:1px solid var(--border);border-radius:4px;padding:18px 22px;margin-bottom:12px}
.match-score-row{display:flex;align-items:center;gap:12px;margin-bottom:6px}
.team-block{flex:1}
.team-nm{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:17px;letter-spacing:.5px;text-transform:uppercase}
.team-sc{font-family:'Rajdhani',sans-serif;font-weight:800;font-size:52px;line-height:1}
.score-mid{display:flex;flex-direction:column;align-items:center;gap:2px;padding:0 12px}
.score-colon{font-family:'Rajdhani',sans-serif;font-size:32px;color:var(--muted)}
.match-meta-row{display:flex;gap:16px;margin-top:8px}
.meta-chip{font-size:11px;color:var(--muted2);display:flex;align-items:center;gap:5px}
.meta-chip strong{color:var(--text)}
.winner-tag{display:inline-block;padding:2px 10px;border:1px solid var(--win);border-radius:2px;font-family:'Rajdhani',sans-serif;font-weight:600;font-size:11px;letter-spacing:1px;text-transform:uppercase;color:var(--win);background:rgba(34,197,94,.08)}

/* TEAM TABLE FILTER */
.filter-bar{display:flex;gap:6px;margin-bottom:10px;align-items:center}
.f-btn{padding:4px 12px;border:1px solid var(--border2);border-radius:2px;font-size:11px;font-family:'Rajdhani',sans-serif;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:var(--muted2);cursor:pointer;transition:all .15s}
.f-btn:hover{color:var(--text);border-color:var(--border2)}
.f-btn.on{color:var(--orange);border-color:var(--orange);background:var(--orange-glow)}

/* SCOREBOARD TABLE */
.sb-table{width:100%;border-collapse:collapse;min-width:620px}
.sb-table th{padding:7px 10px;text-align:right;font-family:'Rajdhani',sans-serif;font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--muted2);background:rgba(0,0,0,.35);border-bottom:1px solid var(--border);white-space:nowrap}
.sb-table th:first-child{text-align:left}
.sb-table td{padding:9px 10px;text-align:right;border-bottom:1px solid var(--border);font-size:13px}
.sb-table td:first-child{text-align:left;color:var(--white);font-family:'Rajdhani',sans-serif;font-weight:600;font-size:14px;cursor:pointer;letter-spacing:.3px}
.sb-table td:first-child:hover{color:var(--orange)}
.sb-table tbody tr:last-child td{border-bottom:none}
.sb-table tbody tr:hover td{background:rgba(255,255,255,.02)}
.team-divider td{padding:5px 10px;background:rgba(0,0,0,.3);font-family:'Rajdhani',sans-serif;font-weight:700;font-size:11px;letter-spacing:2px;text-transform:uppercase}
.ct-div td{color:var(--ct);border-top:2px solid rgba(91,196,245,.2)}
.t-div td{color:var(--t);border-top:2px solid rgba(240,168,66,.2)}
.kda-cell{font-family:'Rajdhani',sans-serif;font-weight:600;font-size:14px}
.adr-highlight{color:var(--orange)}

/* MATCHES LIST */
.matches-list .match-item{display:flex;align-items:center;gap:14px;padding:12px 16px;border-bottom:1px solid var(--border);cursor:pointer;transition:background .13s}
.matches-list .match-item:hover{background:rgba(255,85,0,.03)}
.matches-list .match-item:last-child{border-bottom:none}
.m-id{font-size:11px;color:var(--muted);width:42px;font-family:'Rajdhani',sans-serif;font-weight:600}
.m-map{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:16px;color:var(--white);width:110px}
.m-teams{flex:1}
.m-teams-str{font-size:12px;color:var(--muted2);margin-bottom:2px}
.m-score{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:20px;color:var(--white)}
.m-winner{padding:2px 8px;border:1px solid rgba(34,197,94,.3);border-radius:2px;font-size:10px;font-family:'Rajdhani',sans-serif;font-weight:700;letter-spacing:1px;text-transform:uppercase;color:var(--win);background:rgba(34,197,94,.07)}
.m-date{font-size:11px;color:var(--muted);text-align:right;white-space:nowrap}

/* PROFILE */
.profile-top{background:linear-gradient(135deg,var(--surface) 0%,#141c28 100%);border:1px solid var(--border);border-radius:4px;padding:22px 24px;margin-bottom:12px;display:flex;align-items:center;gap:20px}
.p-avatar{width:68px;height:68px;border-radius:4px;background:linear-gradient(135deg,var(--orange),#c43a00);display:flex;align-items:center;justify-content:center;font-family:'Rajdhani',sans-serif;font-weight:800;font-size:26px;color:#000;flex-shrink:0}
.p-name{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:28px;color:var(--white);letter-spacing:.5px;line-height:1;margin-bottom:4px}
.p-sub{font-size:11px;color:var(--muted2)}
.stats-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:var(--border);border-top:1px solid var(--border)}
@media(max-width:600px){.stats-grid{grid-template-columns:repeat(3,1fr)}}
.stat-box{background:var(--surface);padding:14px 16px}
.stat-val{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:22px;line-height:1;color:var(--white);margin-bottom:3px}
.stat-lbl{font-size:10px;letter-spacing:1px;text-transform:uppercase;color:var(--muted2)}

/* PLAYER MATCH HISTORY */
.ph-table{width:100%;border-collapse:collapse;min-width:560px}
.ph-table th{padding:7px 10px;text-align:right;font-family:'Rajdhani',sans-serif;font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--muted2);background:rgba(0,0,0,.3);border-bottom:1px solid var(--border)}
.ph-table th:first-child{text-align:left}
.ph-table td{padding:9px 10px;text-align:right;border-bottom:1px solid var(--border);font-size:13px}
.ph-table td:first-child{text-align:left;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:14px;color:var(--white)}
.ph-table tbody tr{cursor:pointer;transition:background .12s}
.ph-table tbody tr:hover td{background:rgba(255,85,0,.03)}
.ph-table tbody tr:last-child td{border-bottom:none}
.side-ct{display:inline-block;padding:1px 6px;border-radius:2px;font-size:10px;font-family:'Rajdhani',sans-serif;font-weight:700;letter-spacing:1px;background:rgba(91,196,245,.1);color:var(--ct);border:1px solid rgba(91,196,245,.25)}
.side-t{display:inline-block;padding:1px 6px;border-radius:2px;font-size:10px;font-family:'Rajdhani',sans-serif;font-weight:700;letter-spacing:1px;background:rgba(240,168,66,.1);color:var(--t);border:1px solid rgba(240,168,66,.25)}

/* BACK */
.back-btn{display:inline-flex;align-items:center;gap:5px;padding:5px 12px;border:1px solid var(--border2);border-radius:3px;font-size:11px;color:var(--muted2);cursor:pointer;margin-bottom:14px;font-family:'Rajdhani',sans-serif;font-weight:600;letter-spacing:1px;text-transform:uppercase;transition:all .18s}
.back-btn:hover{color:var(--orange);border-color:var(--orange)}

/* UTIL */
.loading{padding:48px;text-align:center;color:var(--muted2)}
.spin{display:inline-block;width:18px;height:18px;border:2px solid var(--border2);border-top-color:var(--orange);border-radius:50%;animation:spin .6s linear infinite;margin-bottom:8px}
@keyframes spin{to{transform:rotate(360deg)}}
.empty{padding:40px;text-align:center;color:var(--muted2)}
.ovx{overflow-x:auto}
</style>
</head>
<body>
<nav>
  <div class="logo">⚡ CS2 Stats</div>
  <div class="tabs">
    <div class="tab active" data-p="lb">Leaderboard</div>
    <div class="tab" data-p="matches">Matches</div>
  </div>
  <div class="nav-right">
    <div class="btn-sm" onclick="location.href='/'">◉ Live</div>
  </div>
</nav>

<div id="app">
  <div id="p-lb"></div>
  <div id="p-matches" style="display:none"></div>
  <div id="p-player" style="display:none"></div>
  <div id="p-match" style="display:none"></div>
</div>

<script>
// ── Router ────────────────────────────────────────────────────────────────────
let _back = null;
function go(page, params={}, back=null) {
  ['lb','matches','player','match'].forEach(p => {
    document.getElementById('p-'+p).style.display = (p===page)?'':'none';
  });
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.dataset.p===page));
  _back = back;
  if(page==='lb')      loadLB();
  if(page==='matches') loadMatches();
  if(page==='player')  loadPlayer(params.name);
  if(page==='match')   loadMatch(params.id);
}
document.querySelectorAll('.tab').forEach(t=>t.addEventListener('click',()=>go(t.dataset.p)));

// ── Helpers ───────────────────────────────────────────────────────────────────
const esc = s=>String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
const num = n=>n!=null?Number(n).toLocaleString():'-';
const kdc = v=>{const f=parseFloat(v);return f>=1.3?'kd-g':f>=0.9?'kd-n':'kd-b'};
const kdf = v=>{const f=parseFloat(v);return f>=1.3?'win':f>=0.9?'text':'loss'};
const fmtDate = s=>{if(!s)return'-';const d=new Date(s);return d.toLocaleDateString('en-GB',{day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'})};
const initials = n=>(n||'?').split(/[ _]/).slice(0,2).map(w=>w[0]||'').join('').toUpperCase().slice(0,2)||'?';
function spin(id){document.getElementById(id).innerHTML='<div class="loading"><div class="spin"></div><br>Loading…</div>'}

// ── Leaderboard ───────────────────────────────────────────────────────────────
async function loadLB() {
  spin('p-lb');
  const data = await fetch('/api/leaderboard?limit=50').then(r=>r.json()).catch(()=>[]);
  if(!data.length){document.getElementById('p-lb').innerHTML='<div class="empty">No stats yet — play a completed match first.</div>';return}
  const rows = data.map((p,i)=>{
    const rc = i===0?'rank-gold':i===1?'rank-silver':i===2?'rank-bronze':'';
    const hs = Math.min(parseFloat(p.hs_pct||0),100);
    return `<tr class="${rc}" onclick="go('player',{name:'${esc(p.name)}'},'lb')">
      <td>${i+1}</td>
      <td><span class="pname">${esc(p.name)}</span></td>
      <td>${p.matches||0}</td>
      <td>${p.kills||0}</td><td>${p.deaths||0}</td><td>${p.assists||0}</td>
      <td class="kd-num ${kdc(p.kd)}">${p.kd||'—'}</td>
      <td>${p.adr||'—'}</td>
      <td><div class="hs-bar-wrap"><div class="hs-bar"><div class="hs-bar-fill" style="width:${hs}%"></div></div><span class="hs-val">${p.hs_pct||0}%</span></div></td>
      <td>${p.aces||0}</td>
    </tr>`;
  }).join('');
  document.getElementById('p-lb').innerHTML = `
    <div class="page-title">Leaderboard <span class="sub">${data.length} players</span></div>
    <div class="card"><div class="lb-wrap">
    <table class="lb-table">
      <thead><tr>
        <th>#</th><th>Player</th><th>M</th><th>K</th><th>D</th><th>A</th>
        <th>K/D</th><th>ADR</th><th>HS%</th><th>Aces</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
    </div></div>`;
}

// ── Matches ───────────────────────────────────────────────────────────────────
async function loadMatches() {
  spin('p-matches');
  const data = await fetch('/api/matches?limit=30').then(r=>r.json()).catch(()=>[]);
  if(!data.length){document.getElementById('p-matches').innerHTML='<div class="empty">No completed matches yet.</div>';return}
  const items = data.map(m=>`
    <div class="match-item" onclick="go('match',{id:'${m.matchid}'},'matches')">
      <div class="m-id">#${m.matchid}</div>
      <div class="m-map">${esc(m.mapname||'—')}</div>
      <div class="m-teams">
        <div class="m-teams-str">${esc(m.team1_name||'Team 1')} vs ${esc(m.team2_name||'Team 2')}</div>
        <div class="m-score">${m.team1_score??0} : ${m.team2_score??0}</div>
      </div>
      ${m.winner?`<div class="m-winner">${esc(m.winner)}</div>`:''}
      <div class="m-date">${fmtDate(m.end_time)}</div>
    </div>`).join('');
  document.getElementById('p-matches').innerHTML = `
    <div class="page-title">Match History <span class="sub">${data.length} matches</span></div>
    <div class="card matches-list">${items}</div>`;
}

// ── Match Detail ──────────────────────────────────────────────────────────────
async function loadMatch(id) {
  spin('p-match');
  const d = await fetch('/api/match/'+id).then(r=>r.json()).catch(()=>({error:'fetch failed'}));
  if(d.error){document.getElementById('p-match').innerHTML=`<div class="empty">${d.error}</div>`;return}
  const meta=d.meta||{}, maps=d.maps||[], players=d.players||[];

  // Find MVP = highest kills overall
  const mvp = players.reduce((best,p)=>(!best||parseInt(p.kills||0)>parseInt(best.kills||0)?p:best),null);

  // Awards
  const byKills=[...players].sort((a,b)=>parseInt(b.kills||0)-parseInt(a.kills||0));
  const byDmg=[...players].sort((a,b)=>parseInt(b.damage||0)-parseInt(a.damage||0));

  const mvpHtml = mvp ? `
    <div class="mvp-card">
      <div class="mvp-avatar">${initials(mvp.name)}</div>
      <div class="mvp-info">
        <div class="mvp-name">${esc(mvp.name)}</div>
        <div class="mvp-stats">
          <div class="mvp-stat"><div class="mvp-val">${mvp.kills||0} / ${mvp.deaths||0} / ${mvp.assists||0}</div><div class="mvp-lbl">K / D / A</div></div>
          <div class="mvp-stat"><div class="mvp-val">${mvp.adr||'—'}</div><div class="mvp-lbl">ADR</div></div>
          <div class="mvp-stat"><div class="mvp-val">${mvp.hs_pct||0}%</div><div class="mvp-lbl">HS%</div></div>
          <div class="mvp-stat"><div class="mvp-val" style="color:var(--${kdf(mvp.kills&&mvp.deaths?(mvp.kills/mvp.deaths).toFixed(2):'0')})">${mvp.kills&&mvp.deaths?(mvp.kills/mvp.deaths).toFixed(2):'—'}</div><div class="mvp-lbl">K/D</div></div>
        </div>
      </div>
    </div>` : '';

  const awardsHtml = `<div class="awards-grid">
    ${byKills[0]?`<div class="award-card"><div class="award-avatar">${initials(byKills[0].name)}</div><div><div class="award-name">${esc(byKills[0].name)}</div><div style="font-size:10px;color:var(--muted2)">Most Kills</div></div><div style="margin-left:auto;text-align:right"><div class="award-val">${byKills[0].kills}</div></div></div>`:''}
    ${byDmg[0]?`<div class="award-card"><div class="award-avatar">${initials(byDmg[0].name)}</div><div><div class="award-name">${esc(byDmg[0].name)}</div><div style="font-size:10px;color:var(--muted2)">Most Damage</div></div><div style="margin-left:auto;text-align:right"><div class="award-val">${num(byDmg[0].damage)}</div></div></div>`:''}
  </div>`;

  // Scoreboard per map
  const mapsHtml = maps.map(m=>{
    const mp = players.filter(p=>p.mapnumber===m.mapnumber);
    const ct = mp.filter(p=>p.team==='CT');
    const t  = mp.filter(p=>p.team==='TERRORIST');
    return `
      <div style="margin-bottom:14px">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
          <div style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:15px;letter-spacing:1px;text-transform:uppercase;color:var(--white)">${esc(m.mapname||'Map '+m.mapnumber)}</div>
          <div style="font-size:12px;color:var(--muted2)">${m.team1_score??0} : ${m.team2_score??0}</div>
        </div>
        <div class="card ovx">
          <table class="sb-table">
            <thead><tr>
              <th>Player</th><th>K</th><th>D</th><th>A</th>
              <th>K/D</th><th>ADR</th><th>HS%</th><th>Dmg</th><th>5K</th><th>Clutch</th>
            </tr></thead>
            <tbody>
              <tr class="team-divider ct-div"><td colspan="10">🔵 Counter-Terrorists</td></tr>
              ${sbRows(ct)}
              <tr class="team-divider t-div"><td colspan="10">🔴 Terrorists</td></tr>
              ${sbRows(t)}
            </tbody>
          </table>
        </div>
      </div>`;
  }).join('');

  const t1=meta.team1_name||'Team 1', t2=meta.team2_name||'Team 2';
  const s1=meta.team1_score??0, s2=meta.team2_score??0;
  const won = s1>s2?t1:s2>s1?t2:null;

  document.getElementById('p-match').innerHTML = `
    <div class="back-btn" onclick="go(_back||'matches')">← Back</div>
    <div class="match-header-card" style="margin-bottom:12px">
      <div class="match-score-row">
        <div class="team-block">
          <div class="team-nm" style="color:var(--ct)">${esc(t1)}</div>
          <div class="team-sc" style="color:var(--ct)">${s1}</div>
        </div>
        <div class="score-mid"><div class="score-colon">:</div></div>
        <div class="team-block" style="text-align:right">
          <div class="team-nm" style="color:var(--t);text-align:right">${esc(t2)}</div>
          <div class="team-sc" style="color:var(--t)">${s2}</div>
        </div>
      </div>
      <div class="match-meta-row">
        <div class="meta-chip">Match <strong>#${meta.matchid||id}</strong></div>
        ${fmtDate(meta.end_time)?`<div class="meta-chip">${fmtDate(meta.end_time)}</div>`:''}
        ${won?`<div class="meta-chip"><span class="winner-tag">Winner: ${esc(won)}</span></div>`:''}
      </div>
    </div>
    ${mvpHtml}
    ${awardsHtml}
    <div class="page-title" style="font-size:16px;margin-bottom:10px">Scoreboard</div>
    ${mapsHtml}`;
}

function sbRows(arr) {
  if(!arr.length) return '<tr><td colspan="10" style="text-align:center;color:var(--muted);padding:12px">—</td></tr>';
  return [...arr].sort((a,b)=>parseInt(b.kills||0)-parseInt(a.kills||0)).map(p=>{
    const kd = p.deaths>0?(p.kills/p.deaths).toFixed(2):parseFloat(p.kills||0).toFixed(2);
    return `<tr>
      <td onclick="go('player',{name:'${esc(p.name)}'},'match')">${esc(p.name)}</td>
      <td class="kda-cell">${p.kills||0}</td>
      <td class="kda-cell">${p.deaths||0}</td>
      <td class="kda-cell">${p.assists||0}</td>
      <td class="kda-cell ${kdc(kd)}">${kd}</td>
      <td class="adr-highlight">${p.adr||0}</td>
      <td>${p.hs_pct||0}%</td>
      <td>${num(p.damage)}</td>
      <td>${p.enemies5k||0}</td>
      <td>${p.v1_wins||0}</td>
    </tr>`;
  }).join('');
}

// ── Player Profile ────────────────────────────────────────────────────────────
async function loadPlayer(name) {
  spin('p-player');
  const d = await fetch('/api/player/'+encodeURIComponent(name)).then(r=>r.json()).catch(()=>({error:'fetch failed'}));
  if(d.error){document.getElementById('p-player').innerHTML=`<div class="empty">${d.error}</div>`;return}
  const c=d.career, matches=d.recent_matches||[];
  const kd=c.kd||'0.00';

  const statsGrid = [
    [c.kills,'Kills'],[c.deaths,'Deaths'],
    [c.kd,'K/D Ratio',kdc(kd)],[`${c.hs_pct||0}%`,'HS%',''],
    [c.adr,'ADR','adr-highlight'],[c.assists,'Assists'],
    [c.matches,'Matches'],[c.aces,'Aces'],
    [c.clutch_1v1,'1v1 Wins'],[c.entry_wins,'Entry Wins'],
    [num(c.total_damage),'Total Dmg'],[c.headshots,'Headshots'],
  ].map(([v,l,cls=''])=>`<div class="stat-box"><div class="stat-val ${cls}">${v??'-'}</div><div class="stat-lbl">${l}</div></div>`).join('');

  const matchRows = matches.map(m=>{
    const kd2=m.deaths>0?(m.kills/m.deaths).toFixed(2):parseFloat(m.kills||0).toFixed(2);
    const side=m.team==='CT'?'<span class="side-ct">CT</span>':'<span class="side-t">T</span>';
    return `<tr onclick="go('match',{id:'${m.matchid}'},'player')">
      <td>${esc(m.mapname||'—')}</td>
      <td>${side}</td>
      <td>${m.kills||0}</td><td>${m.deaths||0}</td><td>${m.assists||0}</td>
      <td class="${kdc(kd2)}">${kd2}</td>
      <td>${m.hs_pct||0}%</td>
      <td class="adr-highlight">${m.adr||0}</td>
      <td>${(m.team1_score??0)} : ${(m.team2_score??0)}</td>
    </tr>`;
  }).join('');

  document.getElementById('p-player').innerHTML = `
    <div class="back-btn" onclick="go(_back||'lb')">← Back</div>
    <div class="profile-top">
      <div class="p-avatar">${initials(c.name)}</div>
      <div>
        <div class="p-name">${esc(c.name)}</div>
        <div class="p-sub">SteamID64: ${c.steamid64||'N/A'} &nbsp;·&nbsp; ${c.matches||0} matches played</div>
      </div>
    </div>
    <div class="card" style="margin-bottom:14px">
      <div class="stats-grid">${statsGrid}</div>
    </div>
    <div class="page-title" style="font-size:16px;margin-bottom:8px">Match History</div>
    <div class="card ovx">
      <table class="ph-table">
        <thead><tr>
          <th>Map</th><th>Side</th><th>K</th><th>D</th><th>A</th>
          <th>K/D</th><th>HS%</th><th>ADR</th><th>Score</th>
        </tr></thead>
        <tbody>${matchRows||'<tr><td colspan="9" style="text-align:center;color:var(--muted);padding:14px">No matches yet</td></tr>'}</tbody>
      </table>
    </div>`;
}

// Check for ?match= URL param to deep-link
const urlParams = new URLSearchParams(location.search);
if(urlParams.get('match')) go('match',{id:urlParams.get('match')});
else go('lb');
</script>
</body>
</html>"""
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
    """Return current match state + kill feed + player registry for initial page load."""
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
        "registry":  _player_registry,  # name/steamid lookup for the UI
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
                    border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }
  #killfeed-wrap h3 { padding: 10px 14px; font-size: 11px; font-weight: 700;
                       text-transform: uppercase; letter-spacing: 1px; color: var(--muted);
                       border-bottom: 1px solid var(--border); display: flex;
                       align-items: center; justify-content: space-between; }
  #round-kill-count { font-size: 10px; color: var(--muted); font-weight: 400; }
  #killfeed { padding: 4px 0; position: relative; }
  .kf-row {
    display: flex; align-items: center; gap: 8px; padding: 6px 14px;
    font-size: 13px; transition: opacity 0.5s ease, transform 0.3s ease;
    border-bottom: 1px solid rgba(255,255,255,0.03);
  }
  .kf-row[data-age="0"] { opacity: 1;    background: rgba(255,255,255,0.05); }
  .kf-row[data-age="1"] { opacity: 0.85; }
  .kf-row[data-age="2"] { opacity: 0.70; }
  .kf-row[data-age="3"] { opacity: 0.55; }
  .kf-row[data-age="4"] { opacity: 0.40; }
  .kf-row[data-age="5"] { opacity: 0.28; }
  .kf-row[data-age="6"] { opacity: 0.18; }
  .kf-row[data-age="7"] { opacity: 0.10; }
  .kf-row[data-age="8"] { opacity: 0.06; }
  .kf-row[data-age="9"] { opacity: 0.03; }
  @keyframes killpop {
    0%   { opacity: 0; transform: translateY(-8px) scale(0.97); }
    60%  { opacity: 1; transform: translateY(2px)  scale(1.01); }
    100% { opacity: 1; transform: translateY(0)    scale(1);    }
  }
  .kf-new { animation: killpop 0.35s cubic-bezier(.22,.68,0,1.2) forwards; }
  @keyframes roundclear {
    to { opacity: 0; transform: translateY(6px); }
  }
  .kf-clearing { animation: roundclear 0.4s ease forwards; }
  .kf-killer { font-weight: 700; }
  .kf-killer.ct { color: var(--ct); }
  .kf-killer.t  { color: var(--t); }
  .kf-arrow  { color: var(--muted); font-size: 9px; flex-shrink: 0; }
  .kf-victim { color: var(--text); flex: 1; }
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
  <h3>💀 Kill Feed <span id="round-kill-count"></span></h3>
  <div id="killfeed"><div class="kf-empty">No kills yet this round</div></div>
</div>

<script>
const $ = id => document.getElementById(id);
let elapsedTimer = null;
let _registry = {};  // accountid → {name, steamid, team}

function nameOf(accountid) {
  return (_registry[accountid] && _registry[accountid].name) || accountid || '?';
}

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
      const k    = parseInt(p.kills  || 0);
      const d    = parseInt(p.deaths || 0);
      const adr  = parseFloat(p.adr  || 0).toFixed(0);
      const name = p.name || p.accountid || '?';
      const isBot = name.startsWith('🤖');
      const nameStyle = isBot ? 'color:var(--muted);font-style:italic;' : '';
      const id   = p.accountid || '';
      return `<tr style="${isBot ? 'opacity:0.6' : ''}">
        <td title="${id}" style="${nameStyle}">${name}</td>
        <td>${k}</td><td>${d}</td><td>${adr}</td>
        <td>${fmtKD(k,d)}</td>
      </tr>`;
    }).join('');
  }
  $('ct-body').innerHTML = rows(ct);
  $('t-body').innerHTML  = rows(t);
}

const MAX_KILLS_PER_ROUND = 10;  // 5v5: max 5 kills per side

function weaponIcon(w) {
  const icons = {
    ak47:'🔫', m4a1:'🔫', m4a4:'🔫', awp:'🎯', deagle:'🔫',
    glock:'🔫', usp_silencer:'🔫', p250:'🔫', knife:'🔪',
    he_grenade:'💣', molotov:'🔥', incgrenade:'🔥', flashbang:'💡',
    sg556:'🔫', aug:'🔫', famas:'🔫', galil:'🔫', mp9:'🔫',
    mac10:'🔫', ump45:'🔫', p90:'🔫', nova:'🔫', xm1014:'🔫',
    ssg08:'🎯', g3sg1:'🎯', scar20:'🎯'
  };
  const key = (w||'').toLowerCase().replace('-','_').replace(' ','_');
  return icons[key] || '🔫';
}

function updateAgeAttributes() {
  const rows = $('killfeed').querySelectorAll('.kf-row');
  rows.forEach((row, i) => {
    row.setAttribute('data-age', Math.min(i, 9));
  });
  // Update counter
  const cnt = $('round-kill-count');
  if (cnt) cnt.textContent = rows.length > 0 ? rows.length + '/' + MAX_KILLS_PER_ROUND : '';
}

function renderKillFeed(kills) {
  if (!kills || !kills.length) {
    $('killfeed').innerHTML = '<div class="kf-empty">No kills yet this round</div>';
    const cnt = $('round-kill-count'); if (cnt) cnt.textContent = '';
    return;
  }
  $('killfeed').innerHTML = kills.slice(0, MAX_KILLS_PER_ROUND).map((k, i) =>
    `<div class="kf-row" data-age="${Math.min(i, 9)}">
      <span class="kf-killer ${(k.side||'').toLowerCase()}">${k.killer}</span>
      <span class="kf-arrow">›</span>
      <span class="kf-victim">${k.victim}</span>
      <span class="kf-weapon">${weaponIcon(k.weapon)} ${k.weapon||''}</span>
    </div>`
  ).join('');
  updateAgeAttributes();
}

function addKill(k) {
  const feed = $('killfeed');
  const empty = feed.querySelector('.kf-empty');
  if (empty) feed.innerHTML = '';

  // Remove oldest if at max
  while (feed.children.length >= MAX_KILLS_PER_ROUND)
    feed.removeChild(feed.lastChild);

  const row = document.createElement('div');
  row.className = 'kf-row kf-new';
  row.setAttribute('data-age', '0');
  row.innerHTML =
    `<span class="kf-killer ${(k.side||'').toLowerCase()}">${k.killer}</span>` +
    `<span class="kf-arrow">›</span>` +
    `<span class="kf-victim">${k.victim}</span>` +
    `<span class="kf-weapon">${weaponIcon(k.weapon)} ${k.weapon||''}</span>`;

  feed.insertBefore(row, feed.firstChild);

  // Remove animation class after it finishes so transition CSS takes over
  setTimeout(() => row.classList.remove('kf-new'), 400);

  // Update ages for all rows
  updateAgeAttributes();
}

function clearKillFeedAnimated(msg) {
  const rows = $('killfeed').querySelectorAll('.kf-row');
  rows.forEach((r, i) => {
    r.style.transitionDelay = (i * 30) + 'ms';
    r.classList.add('kf-clearing');
  });
  setTimeout(() => {
    $('killfeed').innerHTML = `<div class="kf-empty">${msg}</div>`;
    const cnt = $('round-kill-count'); if (cnt) cnt.textContent = '';
  }, 450);
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
  if (s.registry) _registry = s.registry;
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
  es.addEventListener('score', e => {
    const d = JSON.parse(e.data);
    $('score-ct').textContent = d.score_ct;
    $('score-t').textContent  = d.score_t;
    if (d.map) $('map-name').textContent = d.map.toUpperCase();
    setStatus('LIVE', 'live');
  });
  es.addEventListener('round_stats', e => {
    const d = JSON.parse(e.data);
    $('score-ct').textContent  = d.score_ct;
    $('score-t').textContent   = d.score_t;
    $('round-num').textContent = d.round;
    if (d.map)    $('map-name').textContent    = d.map.toUpperCase();
    if (d.server) $('server-name').textContent = d.server;
    setStatus('LIVE', 'live');
    renderPlayers(d.players || []);
    // Only clear kill feed on new round (round number changed)
    const prevRound = parseInt($('round-num').dataset.round || '0');
    if (d.round > prevRound) {
      clearKillFeedAnimated('New round — ' + (d.round || '') + ' starting…');
      $('round-num').dataset.round = d.round;
    }
  });
  // Live player list from registry (fires on every log batch during warmup/game)
  es.addEventListener('players', e => {
    const d = JSON.parse(e.data);
    renderPlayers(d.players || []);
  });
  es.addEventListener('kill', e => {
    addKill(JSON.parse(e.data));
  });
  es.addEventListener('match_start', e => {
    const d = JSON.parse(e.data);
    $('map-name').textContent  = (d.map || '').toUpperCase();
    $('score-ct').textContent  = 0;
    $('score-t').textContent   = 0;
    $('round-num').textContent = 0;
    $('round-num').dataset.round = 0;
    setStatus('LIVE', 'live');
    clearKillFeedAnimated('Match started!');
    startElapsed(new Date().toISOString());
  });
  es.addEventListener('match_end', e => {
    const d = JSON.parse(e.data);
    $('score-ct').textContent = d.score_ct;
    $('score-t').textContent  = d.score_t;
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
    app.router.add_post('/logs',          handle_log_post)
    app.router.add_get('/events',         handle_sse)
    app.router.add_get('/api/state',      handle_api_state)
    app.router.add_get('/api/leaderboard', handle_api_leaderboard)
    app.router.add_get('/api/player/{name}', handle_api_player)
    app.router.add_get('/api/matches',    handle_api_matches)
    app.router.add_get('/api/match/{matchid}', handle_api_match)
    app.router.add_get('/stats',          handle_stats_page)
    app.router.add_get('/',               handle_live_page)
    app.router.add_get('/health',         handle_health_check)
    
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
            return []

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
        return []
    finally:
        conn.close()


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






# ========== BACKGROUND TASKS ==========
@tasks.loop(minutes=1)
async def update_server_stats():
    try:
        # Kill events are broadcast via SSE only — MatchZy handles all DB writes
        global pending_kill_events
        pending_kill_events = []  # clear without writing to DB
        
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
    except Exception as e:
        print(f"Error in update_server_stats: {e}")



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


@bot.tree.command(name="profile", description="View player stats from MatchZy")
async def profile_cmd(inter: discord.Interaction, player_name: str):
    await inter.response.defer(ephemeral=True)
    mz = get_matchzy_player_stats(player_name=player_name)
    if not mz:
        return await inter.followup.send(
            f"❌ No MatchZy stats found for **{player_name}**\n"
            f"Player must have completed at least one match.",
            ephemeral=True
        )
    kills        = int(mz.get("kills") or 0)
    deaths       = int(mz.get("deaths") or 0)
    assists      = int(mz.get("assists") or 0)
    hs           = int(mz.get("headshots") or 0)
    total_damage = int(mz.get("total_damage") or 0)
    hs_pct       = float(mz.get("hs_pct") or 0)
    aces         = int(mz.get("aces") or 0)
    clutch_wins  = int(mz.get("clutch_wins") or 0)
    entry_wins   = int(mz.get("entry_wins") or 0)
    matches      = int(mz.get("matches_played") or 0)
    kd_ratio     = kills / deaths if deaths > 0 else float(kills)

    embed = discord.Embed(
        title=f"👤 {mz.get('name', player_name)}",
        description=f"📊 MatchZy Career Stats • {matches} match{'es' if matches != 1 else ''}",
        color=0x2ECC71
    )
    embed.add_field(name="💀 Kills",        value=f"**{kills}**",              inline=True)
    embed.add_field(name="☠️ Deaths",       value=f"**{deaths}**",             inline=True)
    embed.add_field(name="📊 K/D",          value=f"**{kd_ratio:.2f}**",       inline=True)
    embed.add_field(name="🤝 Assists",      value=f"**{assists}**",             inline=True)
    embed.add_field(name="🎯 Headshots",    value=f"**{hs}** ({hs_pct:.1f}%)", inline=True)
    embed.add_field(name="💥 Total Damage", value=f"**{total_damage:,}**",      inline=True)
    if aces:
        embed.add_field(name="⭐ Aces (5K)",  value=f"**{aces}**",     inline=True)
    if clutch_wins:
        embed.add_field(name="🔥 1vX Wins",   value=f"**{clutch_wins}**", inline=True)
    if entry_wins:
        embed.add_field(name="🚪 Entry Wins", value=f"**{entry_wins}**", inline=True)

    embed.set_footer(text=f"SteamID64: {mz.get('steamid64', 'N/A')}")
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

@bot.tree.command(name="match", description="Get link to match stats page")
async def match_cmd(inter: discord.Interaction, match_id: str):
    await inter.response.defer(ephemeral=True)
    # Verify match exists
    conn = get_db()
    c = conn.cursor(dictionary=True)
    c.execute(f"SELECT matchid, team1_name, team2_name, team1_score, team2_score, mapname FROM {MATCHZY_TABLES['matches']} mm LEFT JOIN {MATCHZY_TABLES['maps']} mp ON mm.matchid=mp.matchid WHERE mm.matchid=%s LIMIT 1", (match_id,))
    row = c.fetchone()
    c.close(); conn.close()
    if not row:
        return await inter.followup.send(f"❌ Match `#{match_id}` not found.", ephemeral=True)
    # Build URL
    base_url = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    if base_url:
        url = f"https://{base_url}/stats?match={match_id}"
    else:
        port = os.getenv("PORT", "8080")
        url = f"http://localhost:{port}/stats?match={match_id}"
    t1 = row.get("team1_name","Team 1")
    t2 = row.get("team2_name","Team 2")
    s1 = row.get("team1_score",0)
    s2 = row.get("team2_score",0)
    mapname = row.get("mapname","?")
    embed = discord.Embed(
        title=f"🏟️ Match #{match_id} — {mapname}",
        description=f"**{t1}** `{s1} : {s2}` **{t2}**",
        color=0xff5500,
        url=url
    )
    embed.add_field(name="📊 Stats Page", value=f"[View Full Scoreboard]({url})", inline=False)
    await inter.followup.send(embed=embed, ephemeral=False)

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
        
        
        c.close()
        conn.close()
    except Exception as e:
        lines.append(f"❌ DB Error: {e}")
    
    await inter.followup.send("\n".join(lines), ephemeral=True)


if not TOKEN:
    raise SystemExit("TOKEN missing.")

bot.run(TOKEN)