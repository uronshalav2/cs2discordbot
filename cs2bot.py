import os
import pathlib
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

# Path to stats.html — served directly as a static file
HTML_PATH = pathlib.Path(__file__).parent / "stats.html"

# =============================================================================
# CONFIGURATION & DATABASE — must be at the top so all helpers can reference them
# =============================================================================

# ── Environment variables ─────────────────────────────────────────────────────
TOKEN = os.getenv("TOKEN")
STEAM_API_KEY  = os.getenv("STEAM_API_KEY", "")
EDIT_PASSWORD  = os.getenv("EDIT_PASSWORD", "changeme")   # set in Railway env vars
SERVER_IP = os.getenv("SERVER_IP", "127.0.0.1")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))
RCON_IP = os.getenv("RCON_IP", SERVER_IP)
RCON_PORT = int(os.getenv("RCON_PORT", 27015))
RCON_PASSWORD = os.getenv("RCON_PASSWORD", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
SERVER_DEMOS_CHANNEL_ID = int(os.getenv("SERVER_DEMOS_CHANNEL_ID", 0))
DEMOS_JSON_URL = os.getenv("DEMOS_JSON_URL")
GUILD_ID = int(os.getenv("GUILD_ID", "0") or "0")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
SERVER_LOG_PATH = os.getenv("SERVER_LOG_PATH", "")
MAP_WHITELIST = [
    "de_inferno", "de_mirage", "de_dust2", "de_overpass",
    "de_nuke", "de_ancient", "de_vertigo", "de_anubis"
]

# ── MySQL / Database ──────────────────────────────────────────────────────────
import mysql.connector
from mysql.connector import pooling

def _mysql_cfg() -> dict:
    """Build MySQL connection kwargs from Railway env vars."""
    url = os.getenv("MYSQL_URL") or os.getenv("DATABASE_URL", "")
    if url.startswith("mysql://") or url.startswith("mysql+pymysql://"):
        url = url.replace("mysql+pymysql://", "mysql://").replace("mysql://", "")
        userpass, rest = url.split("@", 1)
        user, password = userpass.split(":", 1)
        hostport, database = rest.split("/", 1)
        host, port = (hostport.split(":", 1) if ":" in hostport else (hostport, "3306"))
        return dict(host=host, port=int(port), user=user, password=password,
                    database=database, autocommit=False)
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

# ── MatchZy table names ───────────────────────────────────────────────────────
MATCHZY_TABLES = {
    "matches": "matchzy_stats_matches",
    "maps":    "matchzy_stats_maps",
    "players": "matchzy_stats_players",
}




# Regex to extract every "name<slot><steamid><team>" actor token in a log line
async def handle_health_check(request):
    return web.Response(text='Bot is running')


# ─────────────────────────────────────────────────────────────────────────────
# STATS WEBSITE API ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

def _json_response(data, max_age=0):
    headers = {"Access-Control-Allow-Origin": "*"}
    if max_age > 0:
        headers["Cache-Control"] = f"public, max-age={max_age}"
    else:
        headers["Cache-Control"] = "no-cache"
    return web.Response(
        text=json.dumps(data, default=str),
        content_type='application/json',
        headers=headers,
    )

# ─────────────────────────────────────────────────────────────────────────────
# SERVER-SIDE IN-MEMORY CACHE
# ─────────────────────────────────────────────────────────────────────────────
import time as _time

_API_CACHE: dict = {}
_API_CACHE_TTL = {
    'matches':      30,
    'matches_full': 30,
    'leaderboard':  60,
    'specialists':  60,
    'mapstats':     60,
    'teams':        60,
}

def _cache_get(key: str):
    entry = _API_CACHE.get(key)
    if entry and (_time.monotonic() - entry['ts']) < _API_CACHE_TTL.get(key, 30):
        return entry['data']
    return None

def _cache_set(key: str, data):
    _API_CACHE[key] = {'data': data, 'ts': _time.monotonic()}
    return data

def _cache_bust(*keys):
    for k in keys:
        _API_CACHE.pop(k, None)

# ─────────────────────────────────────────────────────────────────────────────
# PLAYER PROFILE — shared SQL + helpers used by both /api/player/{name} and
# /api/player/sid/{steamid64}
# ─────────────────────────────────────────────────────────────────────────────

_CAREER_SQL = """
    SELECT
        SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS name,
        COUNT(DISTINCT matchid)                                       AS matches,
        SUM(kills)                                                    AS kills,
        SUM(deaths)                                                   AS deaths,
        SUM(assists)                                                  AS assists,
        SUM(head_shot_kills)                                          AS headshots,
        SUM(damage)                                                   AS total_damage,
        SUM(enemies5k)                                                AS aces,
        SUM(enemies4k)                                                AS quads,
        SUM(v1_wins)                                                  AS clutch_1v1,
        SUM(v2_wins)                                                  AS clutch_1v2,
        SUM(entry_wins)                                               AS entry_wins,
        SUM(entry_count)                                              AS entry_attempts,
        SUM(flash_successes)                                          AS flashes_thrown,
        ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2)                    AS kd,
        ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1)       AS hs_pct,
        ROUND(SUM(damage)/NULLIF(COUNT(DISTINCT CONCAT(matchid,mapnumber)),0)/30,1) AS adr
    FROM {players}
    WHERE steamid64 = %s AND steamid64 != '0'
"""

_RECENT_SQL = """
    SELECT p.matchid, p.mapnumber, p.team, p.steamid64,
        p.kills, p.deaths, p.assists, p.damage, p.head_shot_kills,
        p.enemies5k, p.v1_wins,
        m.mapname, m.team1_score, m.team2_score,
        mm.team1_name, mm.team2_name, mm.winner,
        ROUND(p.damage/30,1) AS adr,
        ROUND(p.head_shot_kills/NULLIF(p.kills,0)*100,1) AS hs_pct,
        CASE
            WHEN LOWER(p.team) = LOWER(mm.team1_name) THEN 'team1'
            WHEN LOWER(p.team) = LOWER(mm.team2_name) THEN 'team2'
            WHEN LOWER(p.team) IN ('team1','team_1','1') THEN 'team1'
            WHEN LOWER(p.team) IN ('team2','team_2','2') THEN 'team2'
            ELSE NULL
        END AS player_team,
        CASE
            WHEN LOWER(p.team) = LOWER(mm.team1_name)
                THEN CASE WHEN LOWER(mm.winner)=LOWER(mm.team1_name) THEN 1 ELSE 0 END
            WHEN LOWER(p.team) = LOWER(mm.team2_name)
                THEN CASE WHEN LOWER(mm.winner)=LOWER(mm.team2_name) THEN 1 ELSE 0 END
            ELSE NULL
        END AS player_won
    FROM {players} p
    LEFT JOIN {maps} m  ON p.matchid=m.matchid AND p.mapnumber=m.mapnumber
    LEFT JOIN {matches} mm ON p.matchid=mm.matchid
    WHERE p.steamid64 = %s AND p.steamid64 != '0'
    ORDER BY p.matchid DESC, p.mapnumber DESC
    LIMIT 20
"""

_MAPSTATS_SQL = """
    SELECT m.mapname,
        COUNT(DISTINCT p.matchid)                                            AS matches,
        SUM(p.kills) AS kills, SUM(p.deaths) AS deaths,
        SUM(p.assists) AS assists, SUM(p.damage) AS damage,
        SUM(p.head_shot_kills) AS headshots,
        ROUND(SUM(p.kills)/NULLIF(SUM(p.deaths),0),2)                       AS kd,
        ROUND(SUM(p.head_shot_kills)/NULLIF(SUM(p.kills),0)*100,1)          AS hs_pct,
        ROUND(SUM(p.damage)/NULLIF(COUNT(DISTINCT p.matchid),0)/30,1)       AS adr,
        SUM(CASE WHEN LOWER(mm.winner)=LOWER(p.team) THEN 1 ELSE 0 END)    AS wins
    FROM {players} p
    LEFT JOIN {maps} m  ON p.matchid=m.matchid AND p.mapnumber=m.mapnumber
    LEFT JOIN {matches} mm ON p.matchid=mm.matchid
    WHERE p.steamid64 = %s AND p.steamid64 != '0'
      AND m.mapname IS NOT NULL AND m.mapname != ''
    GROUP BY m.mapname
    ORDER BY matches DESC
"""


def _db_player_career_and_recent(sid64: str) -> tuple:
    """Return (career_dict, recent_list) for sid64, or (None, []) on miss/error."""
    t = MATCHZY_TABLES
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute(_CAREER_SQL.format(players=t['players']), (sid64,))
        career = c.fetchone()
        if career:
            career = dict(career)
            career['steamid64'] = sid64
            nm = _edited_name_map()
            if sid64 in nm:
                career['name'] = nm[sid64]
            c.execute(_RECENT_SQL.format(
                players=t['players'], maps=t['maps'], matches=t['matches']), (sid64,))
            recent = _patch_recent_matches(c.fetchall())
        else:
            recent = []
        c.close(); conn.close()
        return career, recent
    except Exception as e:
        print(f"[db_player] {sid64}: {e}")
        return None, []


def _db_player_mapstats(sid64: str) -> list:
    """Return per-map stats for sid64."""
    t = MATCHZY_TABLES
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute(_MAPSTATS_SQL.format(
            players=t['players'], maps=t['maps'], matches=t['matches']), (sid64,))
        rows = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
        return rows
    except Exception as e:
        print(f"[db_mapstats] {sid64}: {e}")
        return []


def _fshost_career_for(lookup_name: str) -> tuple:
    """Build (career, recent) from fshost JSON data — fallback when not in DB."""
    all_rows = _aggregate_stats_from_fshost()
    nm = _edited_name_map()
    edited_sid = next((s for s, n in nm.items() if n == lookup_name), None)
    row = next((r for r in all_rows if r['name'] == lookup_name
                or (edited_sid and str(r['steamid64']) == str(edited_sid))), None)
    if not row:
        return None, []
    sid = to_steamid64(str(row['steamid64']))
    career = {
        'name': nm.get(sid, row['name']), 'steamid64': sid,
        'matches': row['matches'], 'kills': row['kills'], 'deaths': row['deaths'],
        'assists': row['assists'], 'headshots': row['headshots'],
        'total_damage': row['damage'], 'aces': row['aces'], 'quads': row['quads'],
        'clutch_1v1': row['clutch_1v1'], 'clutch_1v2': row['clutch_1v2'],
        'entry_wins': row['entry_wins'], 'kd': row['kd'],
        'hs_pct': row['hs_pct'], 'adr': row['adr'],
    }
    recent = []
    for mid, entry in _load_matches_from_db().items():
        meta = entry.get('metadata')
        if not meta:
            continue
        for team_key in ('team1', 'team2'):
            for fp in meta.get(team_key, {}).get('players', []):
                if to_steamid64(str(fp.get('steam_id') or fp.get('steamid64') or '0')) != sid:
                    continue
                k   = int(fp.get('kills', 0) or 0)
                d   = int(fp.get('deaths', 0) or 0)
                hs  = int(fp.get('headshot_kills', 0) or fp.get('head_shot_kills', 0) or 0)
                dmg = int(fp.get('damage', 0) or 0)
                t1  = meta.get('team1', {}); t2 = meta.get('team2', {})
                recent.append({
                    'matchid': str(mid), 'mapnumber': 1, 'team': team_key,
                    'steamid64': sid, 'kills': k, 'deaths': d,
                    'assists': int(fp.get('assists', 0) or 0), 'damage': dmg,
                    'head_shot_kills': hs, 'enemies5k': int(fp.get('5k', 0) or 0),
                    'v1_wins': 0, 'mapname': meta.get('map', '?'),
                    'winner': meta.get('winner', ''),
                    'team1_score': t1.get('score', 0), 'team2_score': t2.get('score', 0),
                    'team1_name': t1.get('name', 'Team 1'), 'team2_name': t2.get('name', 'Team 2'),
                    'adr': round(dmg / 30, 1), 'hs_pct': round(hs / k * 100, 1) if k else 0.0,
                })
    recent = _patch_recent_matches(recent)
    recent.sort(key=lambda r: str(r.get('matchid', '')), reverse=True)
    return career, recent[:20]


async def handle_api_player(request):
    """GET /api/player/{name} — career stats by name; MatchZy primary, fshost fallback."""
    name = request.match_info.get('name', '')
    # Resolve SteamID64 from name (edit map has priority over raw DB name)
    nm = _edited_name_map()
    sid64 = next((s for s, n in nm.items() if n == name), None)
    if not sid64:
        try:
            conn = get_db(); c = conn.cursor(dictionary=True)
            c.execute(
                f"SELECT steamid64 FROM {MATCHZY_TABLES['players']} WHERE name=%s AND steamid64!='0' LIMIT 1",
                (name,))
            row = c.fetchone(); c.close(); conn.close()
            if row:
                sid64 = to_steamid64(str(row['steamid64']))
        except Exception as e:
            print(f"[api/player] SID lookup: {e}")

    career, recent = _db_player_career_and_recent(sid64) if sid64 else (None, [])
    if not career:
        career, recent = await asyncio.get_running_loop().run_in_executor(
            None, _fshost_career_for, name)
    if not career:
        return _json_response({"error": "Player not found"})
    return _json_response({"career": career, "recent_matches": recent})


async def handle_api_player_by_sid(request):
    """GET /api/player/sid/{steamid64} — career stats by SteamID64."""
    sid64 = to_steamid64(request.match_info.get('steamid64', ''))
    career, recent = _db_player_career_and_recent(sid64)
    if not career:
        return _json_response({"error": "Player not found"})
    return _json_response({"career": career, "recent_matches": recent})


async def handle_api_player_mapstats_by_sid(request):
    """GET /api/player/sid/{steamid64}/mapstats"""
    sid64 = to_steamid64(request.match_info.get('steamid64', ''))
    return _json_response(_db_player_mapstats(sid64))


def _get_steam_openid_url(return_to: str) -> str:
    """Build Steam OpenID redirect URL."""
    import urllib.parse
    params = {
        "openid.ns":         "http://specs.openid.net/auth/2.0",
        "openid.mode":       "checkid_setup",
        "openid.return_to":  return_to,
        "openid.realm":      return_to.split("/auth/")[0] + "/",
        "openid.identity":   "http://specs.openid.net/auth/2.0/identifier_select",
        "openid.claimed_id": "http://specs.openid.net/auth/2.0/identifier_select",
    }
    return "https://steamcommunity.com/openid/login?" + urllib.parse.urlencode(params)

def _verify_steam_openid(params: dict, return_to: str) -> str | None:
    """Verify Steam OpenID response. Returns steamid64 or None."""
    import urllib.parse, re
    check_params = dict(params)
    check_params["openid.mode"] = "check_authentication"
    body = urllib.parse.urlencode(check_params).encode()
    try:
        r = requests.post("https://steamcommunity.com/openid/login",
                          data=body, timeout=10,
                          headers={"Content-Type": "application/x-www-form-urlencoded"})
        if "is_valid:true" not in r.text:
            return None
        identity = params.get("openid.claimed_id", "")
        m = re.search(r"https://steamcommunity\.com/openid/id/(\d+)", identity)
        return m.group(1) if m else None
    except Exception:
        return None





# ── Player Steam login (any Steam user) ──────────────────────────────────────

async def handle_player_steam_login(request):
    """GET /auth/steam/player — redirect to Steam OpenID (open to all players)."""
    scheme = request.headers.get("X-Forwarded-Proto", "https")
    host   = request.headers.get("X-Forwarded-Host", request.host)
    return_to = f"{scheme}://{host}/auth/steam/player/callback"
    raise web.HTTPFound(location=_get_steam_openid_url(return_to))

async def handle_player_steam_callback(request):
    """GET /auth/steam/player/callback — verify Steam OpenID, set player session cookie."""
    import secrets
    scheme = request.headers.get("X-Forwarded-Proto", "https")
    host   = request.headers.get("X-Forwarded-Host", request.host)
    return_to = f"{scheme}://{host}/auth/steam/player/callback"
    params = dict(request.rel_url.query)
    steamid = await asyncio.get_running_loop().run_in_executor(
        None, _verify_steam_openid, params, return_to
    )
    if not steamid:
        raise web.HTTPFound(location="/?error=auth_failed")
    token = secrets.token_hex(32)
    _PLAYER_SESSIONS[token] = steamid
    response = web.HTTPFound(location="/")
    response.set_cookie("rg_player", token, max_age=86400*30, httponly=True, samesite="Lax")
    return response

async def handle_player_steam_logout(request):
    """POST /auth/steam/player/logout"""
    token = request.cookies.get("rg_player", "")
    _PLAYER_SESSIONS.pop(token, None)
    response = web.HTTPFound(location="/")
    response.del_cookie("rg_player")
    return response

def _get_player_steamid(request) -> str | None:
    """Return steamid64 if request has valid player session, else None."""
    token = request.cookies.get("rg_player", "")
    return _PLAYER_SESSIONS.get(token)

async def handle_player_api_me(request):
    """GET /api/auth/steam/me — return player session info (any logged-in Steam user)."""
    sid = _get_player_steamid(request)
    if not sid:
        return web.Response(text=json.dumps({"ok": False}), content_type="application/json", status=401)
    return _json_response({"ok": True, "steamid": sid})













# =============================================================================
# MISSING HELPERS
# =============================================================================

# In-memory player session store  {token: steamid64}
_PLAYER_SESSIONS: dict = {}

# Edit-session tokens  {token: True}  (admin only)
_EDIT_SESSIONS: dict = {}

# Simple edit-overlay cache so every request doesn't hit the DB
_EDITS_CACHE: dict = {}   # matchid -> edits dict
_EDITS_CACHE_TS: float = 0.0
_EDITS_CACHE_TTL = 60     # seconds

# Bot/fake-player name fragments to exclude from stats
BOT_FILTER = ["BOT", "GOTV", "[BOT]"]


def to_steamid64(raw: str) -> str:
    """Return a normalised SteamID64 string, or '' on failure."""
    if not raw or raw in ('0', '', 'None'):
        return ''
    raw = str(raw).strip()
    # Already a 64-bit Steam ID
    if raw.isdigit() and len(raw) >= 15:
        return raw
    return ''


def _edited_name_map() -> dict:
    """Return {steamid64: display_name} from the match_edits table."""
    nm = {}
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute("SELECT matchid, edits_json FROM match_edits")
        for row in c.fetchall():
            try:
                edits = json.loads(row['edits_json'] or '{}')
                for sid, name in edits.get('player_names', {}).items():
                    if name:
                        nm[sid] = name
            except Exception:
                pass
        c.close(); conn.close()
    except Exception:
        pass
    return nm


def _bust_edits_cache():
    global _EDITS_CACHE, _EDITS_CACHE_TS
    _EDITS_CACHE = {}
    _EDITS_CACHE_TS = 0.0


def _get_edits_for(matchid: str) -> dict:
    """Return the edit overlay dict for a match (cached)."""
    global _EDITS_CACHE_TS
    now = _time.monotonic()
    if now - _EDITS_CACHE_TS > _EDITS_CACHE_TTL:
        _EDITS_CACHE.clear()
        _EDITS_CACHE_TS = now
    if matchid in _EDITS_CACHE:
        return _EDITS_CACHE[matchid]
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute("SELECT edits_json FROM match_edits WHERE matchid=%s", (matchid,))
        row = c.fetchone(); c.close(); conn.close()
        edits = json.loads(row['edits_json']) if row else {}
    except Exception:
        edits = {}
    _EDITS_CACHE[matchid] = edits
    return edits


def _apply_edits(meta: dict, edits: dict) -> dict:
    """Overlay edit data onto a match meta dict (non-destructive copy)."""
    if not edits:
        return meta
    m = dict(meta)
    for field in ('team1_name', 'team2_name', 'winner', 'mapname'):
        if field in edits:
            m[field] = edits[field]
    # Per-player name overrides
    pn = edits.get('player_names', {})
    if pn and 'team1' in m:
        m['team1'] = dict(m['team1'])
        for p in m['team1'].get('players', []):
            sid = to_steamid64(str(p.get('steam_id') or p.get('steamid64') or ''))
            if sid and sid in pn:
                p['name'] = pn[sid]
    if pn and 'team2' in m:
        m['team2'] = dict(m['team2'])
        for p in m['team2'].get('players', []):
            sid = to_steamid64(str(p.get('steam_id') or p.get('steamid64') or ''))
            if sid and sid in pn:
                p['name'] = pn[sid]
    return m


def _patch_recent_matches(rows) -> list:
    """Apply edit overlays (name corrections etc.) to a list of recent-match rows."""
    nm = _edited_name_map()
    out = []
    for r in rows:
        r = dict(r)
        sid = str(r.get('steamid64', ''))
        if sid in nm:
            r['name'] = nm[sid]
        out.append(r)
    return out


def _load_matches_from_db() -> dict:
    """
    Read all synced fshost matches from the fshost_matches DB table.
    Returns the same shape as build_matchid_to_demo_map() so all callers
    are drop-in compatible — but with zero external HTTP calls.

    Falls back to live fshost fetch if the table is empty (e.g. first boot
    before the background sync has run).
    """
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute("""
            SELECT matchid, raw_json, demo_name, demo_url, demo_size, demo_modified
            FROM fshost_matches
            ORDER BY CAST(matchid AS UNSIGNED) DESC
        """)
        rows = c.fetchall(); c.close(); conn.close()
    except Exception as e:
        print(f"[load_matches_db] DB error: {e}")
        rows = []

    if not rows:
        # Table empty — fall back to live fetch once so the page isn't blank
        print("[load_matches_db] table empty, falling back to live fshost fetch")
        return build_matchid_to_demo_map()

    result = {}
    for row in rows:
        try:
            metadata = json.loads(row['raw_json'])
        except Exception:
            metadata = {}
        result[str(row['matchid'])] = {
            'metadata':       metadata,
            'name':           row.get('demo_name', ''),
            'download_url':   row.get('demo_url', ''),
            'size_formatted': row.get('demo_size', ''),
            'modified_at':    row.get('demo_modified', ''),
        }
    return result


def _aggregate_stats_from_fshost() -> list:
    """
    Aggregate per-player career stats from the fshost_matches DB table.
    Returns a list of dicts compatible with the leaderboard / career shape.
    """
    matchid_map = _load_matches_from_db()
    players: dict = {}   # steamid64 -> aggregated dict

    nm = _edited_name_map()

    for mid, entry in matchid_map.items():
        meta = entry.get('metadata')
        if not meta:
            continue
        for team_key in ('team1', 'team2'):
            for fp in meta.get(team_key, {}).get('players', []):
                raw_sid = str(fp.get('steam_id') or fp.get('steamid64') or '0')
                sid = to_steamid64(raw_sid) or raw_sid
                if not sid or sid == '0':
                    continue

                k   = int(fp.get('kills',   0) or 0)
                d   = int(fp.get('deaths',  0) or 0)
                a   = int(fp.get('assists', 0) or 0)
                hs  = int(fp.get('headshot_kills', 0) or fp.get('head_shot_kills', 0) or 0)
                dmg = int(fp.get('damage',  0) or 0)
                ace = int(fp.get('5k',      0) or 0)
                q   = int(fp.get('4k',      0) or 0)
                c1  = int(fp.get('1v1',     0) or fp.get('v1_wins', 0) or 0)
                c2  = int(fp.get('1v2',     0) or fp.get('v2_wins', 0) or 0)
                ew  = int(fp.get('entry_wins', 0) or 0)

                pname = nm.get(sid) or fp.get('name', sid)

                if sid not in players:
                    players[sid] = {
                        'steamid64': sid, 'name': pname,
                        'matches': 0, 'kills': 0, 'deaths': 0, 'assists': 0,
                        'headshots': 0, 'damage': 0, 'aces': 0, 'quads': 0,
                        'clutch_1v1': 0, 'clutch_1v2': 0, 'entry_wins': 0,
                        'flashes_thrown': 0,
                        '_match_ids': set(),
                    }
                p = players[sid]
                p['name'] = nm.get(sid, pname)  # keep most recent / edited name
                p['_match_ids'].add(str(mid))
                p['kills']    += k
                p['deaths']   += d
                p['assists']  += a
                p['headshots']+= hs
                p['damage']   += dmg
                p['aces']     += ace
                p['quads']    += q
                p['clutch_1v1'] += c1
                p['clutch_1v2'] += c2
                p['entry_wins'] += ew

    result = []
    for sid, p in players.items():
        match_count = len(p.pop('_match_ids', set()))
        p['matches'] = match_count
        k = p['kills']; d = p['deaths']; hs = p['headshots']
        p['kd']     = round(k / d, 2)     if d else float(k)
        p['hs_pct'] = round(hs / k * 100, 1) if k else 0.0
        p['adr']    = round(p['damage'] / max(match_count * 30, 1), 1)
        result.append(p)

    return result


# =============================================================================
# STEAM API HELPER
# =============================================================================

_STEAM_CACHE: dict = {}
_STEAM_CACHE_TTL = 3600   # 1 hour


def _fetch_steam_summary(sid64: str) -> dict:
    """Return {name, avatar} for a SteamID64, or {} on failure."""
    now = _time.monotonic()
    if sid64 in _STEAM_CACHE:
        entry = _STEAM_CACHE[sid64]
        if now - entry['ts'] < _STEAM_CACHE_TTL:
            return entry['data']
    if not STEAM_API_KEY or not sid64:
        return {}
    try:
        url = (f"https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
               f"?key={STEAM_API_KEY}&steamids={sid64}")
        r = requests.get(url, timeout=8)
        r.raise_for_status()
        players = r.json().get('response', {}).get('players', [])
        if players:
            p = players[0]
            data = {'name': p.get('personaname', ''), 'avatar': p.get('avatarfull', '')}
        else:
            data = {}
    except Exception:
        data = {}
    _STEAM_CACHE[sid64] = {'data': data, 'ts': now}
    return data


# =============================================================================
# HTTP HANDLERS — all the missing ones, now restored
# =============================================================================


async def handle_stats_page(request):
    """Serve the main stats HTML page."""
    if not HTML_PATH.exists():
        return web.Response(text="stats.html not found", status=404)
    return web.FileResponse(HTML_PATH)


def handle_options(request):
    """CORS pre-flight handler."""
    return web.Response(
        status=204,
        headers={
            "Access-Control-Allow-Origin":  "*",
            "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        }
    )


async def handle_api_steam(request):
    """GET /api/steam/{steamid64} — return Steam name + avatar (cached)."""
    sid64 = request.match_info.get('steamid64', '')
    data = await asyncio.get_running_loop().run_in_executor(
        None, _fetch_steam_summary, sid64)
    return _json_response(data, max_age=3600)


# ── /api/matches ──────────────────────────────────────────────────────────────

def _build_matches_list(limit: int = 30) -> list:
    """
    Build the matches list from fshost JSON (metadata + demo info),
    enriched with per-map scores from the MatchZy DB where available.
    Returns a list sorted newest-first.
    """
    matchid_map = _load_matches_from_db()
    if not matchid_map:
        return []

    t = MATCHZY_TABLES
    # Grab all DB map rows in one query so we can join without N+1 calls
    db_maps: dict = {}   # matchid -> list of map rows
    db_matches: dict = {}
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute(f"SELECT matchid, mapnumber, mapname, team1_score, team2_score FROM {t['maps']}")
        for row in c.fetchall():
            db_maps.setdefault(str(row['matchid']), []).append(dict(row))
        c.execute(f"SELECT matchid, team1_name, team2_name, winner, end_time FROM {t['matches']}")
        for row in c.fetchall():
            db_matches[str(row['matchid'])] = dict(row)
        c.close(); conn.close()
    except Exception:
        pass

    nm = _edited_name_map()
    items = []

    for mid_str, entry in matchid_map.items():
        meta = entry.get('metadata') or {}
        edits = _get_edits_for(mid_str)
        meta = _apply_edits(meta, edits)

        t1 = meta.get('team1', {})
        t2 = meta.get('team2', {})

        # Prefer DB names (more reliable), fall back to fshost
        db_match = db_matches.get(mid_str, {})
        team1_name = nm.get('_t1_' + mid_str) or db_match.get('team1_name') or t1.get('name', 'Team 1')
        team2_name = nm.get('_t2_' + mid_str) or db_match.get('team2_name') or t2.get('name', 'Team 2')
        winner     = edits.get('winner') or db_match.get('winner') or meta.get('winner', '')

        # Scores — from fshost JSON (authoritative match result)
        team1_score = int(t1.get('score', 0) or 0)
        team2_score = int(t2.get('score', 0) or 0)

        # Per-map breakdown from DB (may have multiple maps)
        maps_rows = db_maps.get(mid_str, [])
        # Build a flat player list from fshost (for leaderboard streak calc)
        players = []
        for tk, team_obj in [('team1', t1), ('team2', t2)]:
            for fp in team_obj.get('players', []):
                sid = to_steamid64(str(fp.get('steam_id') or fp.get('steamid64') or '0'))
                pname = nm.get(sid, fp.get('name', ''))
                players.append({
                    'steamid64': sid, 'name': pname,
                    'team': tk,
                    'kills': int(fp.get('kills', 0) or 0),
                    'deaths': int(fp.get('deaths', 0) or 0),
                    'assists': int(fp.get('assists', 0) or 0),
                    'damage': int(fp.get('damage', 0) or 0),
                })

        # Timestamp: prefer DB end_time, fall back to fshost / demo filename
        end_time = db_match.get('end_time') or meta.get('end_time') or entry.get('modified_at', '')

        mapname = (maps_rows[0]['mapname'] if maps_rows
                   else meta.get('map') or meta.get('mapname', ''))

        items.append({
            'matchid':        mid_str,
            'mapname':        mapname,
            'team1_name':     team1_name,
            'team2_name':     team2_name,
            'team1_score':    team1_score,
            'team2_score':    team2_score,
            'map_team1_score': maps_rows[0]['team1_score'] if maps_rows else team1_score,
            'map_team2_score': maps_rows[0]['team2_score'] if maps_rows else team2_score,
            'winner':         winner,
            'end_time':       str(end_time),
            'demo':           {
                'name':           entry.get('name', ''),
                'download_url':   entry.get('download_url', ''),
                'size_formatted': entry.get('size_formatted', ''),
            },
            'maps':    maps_rows,
            'players': players,
        })

    # Sort newest first (by matchid descending — matchid is numeric)
    items.sort(key=lambda x: int(x['matchid']) if str(x['matchid']).isdigit() else 0, reverse=True)
    return items[:limit]


async def handle_api_matches(request):
    """GET /api/matches?limit=30 — match list (fshost metadata + DB enrichment)."""
    cached = _cache_get('matches')
    if cached is not None:
        return _json_response(cached)
    limit = min(int(request.rel_url.query.get('limit', 30)), 100)
    data = await asyncio.get_running_loop().run_in_executor(
        None, _build_matches_list, limit)
    _cache_set('matches', data)
    return _json_response(data)


async def handle_api_matches_full(request):
    """GET /api/matches/full — full match list (all matches, no limit)."""
    cached = _cache_get('matches_full')
    if cached is not None:
        return _json_response(cached)
    data = await asyncio.get_running_loop().run_in_executor(
        None, _build_matches_list, 999)
    _cache_set('matches_full', data)
    return _json_response(data)


# ── /api/match/{matchid} ──────────────────────────────────────────────────────

def _build_match_detail(matchid: str) -> dict:
    """
    Build a full match detail object for a single match.
    Shell (teams, score, demo) from fshost JSON.
    Per-player stats rows from the MatchZy DB.
    """
    matchid_map = _load_matches_from_db()
    entry = matchid_map.get(str(matchid))
    if not entry:
        return {'error': 'Match not found'}

    meta = dict(entry.get('metadata') or {})
    edits = _get_edits_for(str(matchid))
    meta = _apply_edits(meta, edits)
    nm = _edited_name_map()

    t = MATCHZY_TABLES
    maps_rows = []
    players_rows = []
    db_match = {}

    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        # Match-level info
        c.execute(f"SELECT * FROM {t['matches']} WHERE matchid=%s", (matchid,))
        db_match = dict(c.fetchone() or {})
        # Map rows
        c.execute(f"SELECT * FROM {t['maps']} WHERE matchid=%s ORDER BY mapnumber", (matchid,))
        maps_rows = [dict(r) for r in c.fetchall()]
        # Player stat rows (all maps)
        c.execute(f"""
            SELECT p.*, m.mapname
            FROM {t['players']} p
            LEFT JOIN {t['maps']} m ON p.matchid=m.matchid AND p.mapnumber=m.mapnumber
            WHERE p.matchid=%s
            ORDER BY p.mapnumber, p.kills DESC
        """, (matchid,))
        players_rows = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
    except Exception as e:
        print(f"[match_detail] DB error for {matchid}: {e}")

    # Apply name edits to DB player rows
    for p in players_rows:
        sid = to_steamid64(str(p.get('steamid64', '')))
        if sid and sid in nm:
            p['name'] = nm[sid]
        # Compute derived fields
        k = int(p.get('kills', 0) or 0)
        d = int(p.get('deaths', 0) or 0)
        rounds = int(maps_rows[0]['team1_score'] + maps_rows[0]['team2_score']) if maps_rows else 30
        p['adr']    = round(int(p.get('damage', 0) or 0) / max(rounds, 1), 1)
        p['kd']     = round(k / d, 2) if d else float(k)
        p['hs_pct'] = round(int(p.get('head_shot_kills', 0) or 0) / k * 100, 1) if k else 0.0
        # Simple HLTV-style rating approximation
        if rounds and d is not None:
            kpr  = k / max(rounds, 1)
            dpr  = d / max(rounds, 1)
            p['rating'] = round(0.0073 + 0.3591 * kpr - 0.5329 * dpr + 0.2372 * kpr + 0.0032, 2)

    t1_fshost = meta.get('team1', {})
    t2_fshost = meta.get('team2', {})
    team1_name = db_match.get('team1_name') or t1_fshost.get('name', 'Team 1')
    team2_name = db_match.get('team2_name') or t2_fshost.get('name', 'Team 2')

    return {
        'meta': {
            'matchid':     str(matchid),
            'team1_name':  team1_name,
            'team2_name':  team2_name,
            'team1_score': int(t1_fshost.get('score', db_match.get('team1_score', 0)) or 0),
            'team2_score': int(t2_fshost.get('score', db_match.get('team2_score', 0)) or 0),
            'winner':      edits.get('winner') or db_match.get('winner') or meta.get('winner', ''),
            'end_time':    str(db_match.get('end_time', '')),
        },
        'maps':    maps_rows,
        'players': players_rows,
        'demo': {
            'name':           entry.get('name', ''),
            'url':            entry.get('download_url', ''),
            'download_url':   entry.get('download_url', ''),
            'size_formatted': entry.get('size_formatted', ''),
            'size':           entry.get('size_formatted', ''),
        },
    }


async def handle_api_match(request):
    """GET /api/match/{matchid} — full detail for a single match."""
    matchid = request.match_info.get('matchid', '')
    data = await asyncio.get_running_loop().run_in_executor(
        None, _build_match_detail, matchid)
    return _json_response(data)


# ── /api/leaderboard ─────────────────────────────────────────────────────────

def _build_leaderboard() -> list:
    """
    Career leaderboard: DB stats primary, fshost aggregation as fallback
    for players not yet in the DB.  Returns list sorted by kills desc.
    """
    t = MATCHZY_TABLES
    db_players: dict = {}  # steamid64 -> row
    nm = _edited_name_map()

    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT
                steamid64,
                SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS name,
                COUNT(DISTINCT matchid)                                            AS matches,
                SUM(kills)                                                         AS kills,
                SUM(deaths)                                                        AS deaths,
                SUM(assists)                                                       AS assists,
                SUM(head_shot_kills)                                               AS headshots,
                SUM(damage)                                                        AS damage,
                SUM(enemies5k)                                                     AS aces,
                SUM(v1_wins) + SUM(v2_wins)                                        AS clutch_wins,
                SUM(v1_wins)                                                       AS clutch_1v1,
                SUM(v2_wins)                                                       AS clutch_1v2,
                SUM(entry_wins)                                                    AS entry_wins,
                SUM(entry_count)                                                   AS entry_attempts,
                SUM(flash_successes)                                               AS flash_successes,
                ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2)                         AS kd,
                ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1)            AS hs_pct,
                ROUND(SUM(damage)/NULLIF(COUNT(DISTINCT CONCAT(matchid,mapnumber)),0)/30,1) AS adr
            FROM {t['players']}
            WHERE steamid64 != '0'
            GROUP BY steamid64
            ORDER BY SUM(kills) DESC
        """)
        for row in c.fetchall():
            row = dict(row)
            sid = to_steamid64(str(row['steamid64']))
            if sid and sid in nm:
                row['name'] = nm[sid]
            db_players[str(row['steamid64'])] = row
        c.close(); conn.close()
    except Exception as e:
        print(f"[leaderboard] DB error: {e}")

    # Supplement with fshost players not in DB
    fshost_rows = _aggregate_stats_from_fshost()
    for row in fshost_rows:
        sid = str(row.get('steamid64', ''))
        if sid and sid not in db_players:
            row['clutch_wins'] = row.get('clutch_1v1', 0) + row.get('clutch_1v2', 0)
            db_players[sid] = row

    result = list(db_players.values())
    result.sort(key=lambda r: int(r.get('kills') or 0), reverse=True)
    return result


async def handle_api_leaderboard(request):
    """GET /api/leaderboard"""
    cached = _cache_get('leaderboard')
    if cached is not None:
        return _json_response(cached)
    data = await asyncio.get_running_loop().run_in_executor(None, _build_leaderboard)
    _cache_set('leaderboard', data)
    return _json_response(data)


# ── /api/specialists ──────────────────────────────────────────────────────────

def _build_specialists() -> list:
    """Specialist stats: clutch kings, entry fraggers, flash heroes etc."""
    t = MATCHZY_TABLES
    result = []
    nm = _edited_name_map()
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT
                steamid64,
                SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS name,
                COUNT(DISTINCT matchid)                                            AS matches,
                SUM(kills)                                                         AS kills,
                SUM(v1_wins)                                                       AS clutch_1v1,
                SUM(v2_wins)                                                       AS clutch_1v2,
                SUM(v1_wins) + SUM(v2_wins)                                        AS clutch_total,
                SUM(entry_wins)                                                    AS entry_wins,
                SUM(entry_count)                                                   AS entry_attempts,
                SUM(flash_successes)                                               AS flash_successes,
                SUM(enemies5k)                                                     AS aces,
                ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2)                         AS kd,
                ROUND(SUM(damage)/NULLIF(COUNT(DISTINCT CONCAT(matchid,mapnumber)),0)/30,1) AS adr,
                ROUND(SUM(entry_wins)/NULLIF(SUM(entry_count),0)*100,1)           AS entry_rate,
                ROUND(SUM(flash_successes)/NULLIF(COUNT(DISTINCT matchid),0),1)   AS flashes_per_map,
                ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1)            AS hs_pct
            FROM {t['players']}
            WHERE steamid64 != '0'
            GROUP BY steamid64
            HAVING matches >= 1
            ORDER BY clutch_total DESC
        """)
        for row in c.fetchall():
            row = dict(row)
            sid = to_steamid64(str(row['steamid64']))
            if sid and sid in nm:
                row['name'] = nm[sid]
            result.append(row)
        c.close(); conn.close()
    except Exception as e:
        print(f"[specialists] DB error: {e}")
    return result


async def handle_api_specialists(request):
    """GET /api/specialists"""
    cached = _cache_get('specialists')
    if cached is not None:
        return _json_response(cached)
    data = await asyncio.get_running_loop().run_in_executor(None, _build_specialists)
    _cache_set('specialists', data)
    return _json_response(data)


# ── /api/mapstats ─────────────────────────────────────────────────────────────

def _build_mapstats() -> list:
    """Global per-map stats: avg rounds, T1/T2 win rates etc."""
    t = MATCHZY_TABLES
    result = []
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT
                m.mapname,
                COUNT(DISTINCT m.matchid)                                         AS matches,
                ROUND(AVG(m.team1_score + m.team2_score), 1)                     AS avg_rounds,
                MAX(m.team1_score + m.team2_score)                                AS max_rounds,
                SUM(CASE WHEN m.team1_score > m.team2_score THEN 1 ELSE 0 END)   AS t1_wins,
                SUM(CASE WHEN m.team2_score > m.team1_score THEN 1 ELSE 0 END)   AS t2_wins,
                SUM(CASE WHEN m.team1_score = m.team2_score THEN 1 ELSE 0 END)   AS draws
            FROM {t['maps']} m
            WHERE m.mapname IS NOT NULL AND m.mapname != ''
            GROUP BY m.mapname
            ORDER BY matches DESC
        """)
        result = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
    except Exception as e:
        print(f"[mapstats] DB error: {e}")
    return result


async def handle_api_mapstats(request):
    """GET /api/mapstats"""
    cached = _cache_get('mapstats')
    if cached is not None:
        return _json_response(cached)
    data = await asyncio.get_running_loop().run_in_executor(None, _build_mapstats)
    _cache_set('mapstats', data)
    return _json_response(data)


# ── /api/player/{name}/mapstats ───────────────────────────────────────────────

async def handle_api_player_mapstats(request):
    """GET /api/player/{name}/mapstats"""
    name = request.match_info.get('name', '')
    nm = _edited_name_map()
    sid64 = next((s for s, n in nm.items() if n == name), None)
    if not sid64:
        try:
            conn = get_db(); c = conn.cursor(dictionary=True)
            c.execute(
                f"SELECT steamid64 FROM {MATCHZY_TABLES['players']} WHERE name=%s AND steamid64!='0' LIMIT 1",
                (name,))
            row = c.fetchone(); c.close(); conn.close()
            if row:
                sid64 = to_steamid64(str(row['steamid64']))
        except Exception:
            pass
    return _json_response(_db_player_mapstats(sid64) if sid64 else [])


# ── /api/demos ────────────────────────────────────────────────────────────────

def _build_demos_list() -> list:
    """
    Build the demos list from fshost, shaped for the HTML demos page.
    Each item has: name, mapname, download_url, size_formatted, modified_at,
                   team1_name, team2_name, team1_score, team2_score, matchid
    """
    matchid_map = _load_matches_from_db()
    demos = []
    nm = _edited_name_map()

    for mid_str, entry in matchid_map.items():
        meta = entry.get('metadata') or {}
        edits = _get_edits_for(mid_str)
        meta = _apply_edits(meta, edits)

        t1 = meta.get('team1', {})
        t2 = meta.get('team2', {})
        demos.append({
            'matchid':        mid_str,
            'name':           entry.get('name', ''),
            'download_url':   entry.get('download_url', ''),
            'size_formatted': entry.get('size_formatted', ''),
            'modified_at':    entry.get('modified_at', ''),
            'mapname':        meta.get('map') or meta.get('mapname', ''),
            'team1_name':     t1.get('name', 'Team 1'),
            'team2_name':     t2.get('name', 'Team 2'),
            'team1_score':    int(t1.get('score', 0) or 0),
            'team2_score':    int(t2.get('score', 0) or 0),
        })

    demos.sort(key=lambda x: int(x['matchid']) if str(x['matchid']).isdigit() else 0, reverse=True)
    return demos


async def handle_api_demos(request):
    """GET /api/demos"""
    data = await asyncio.get_running_loop().run_in_executor(None, _build_demos_list)
    return _json_response(data)


# ── /api/h2h ─────────────────────────────────────────────────────────────────

def _build_h2h(name1: str, name2: str) -> dict:
    """Head-to-head comparison between two players (career stats)."""
    lb = _build_leaderboard()
    nm = _edited_name_map()

    def find_player(name):
        # Try steamid match first, then name
        for p in lb:
            sid = to_steamid64(str(p.get('steamid64', '')))
            if sid and nm.get(sid) == name:
                return p
        for p in lb:
            if p.get('name', '').lower() == name.lower():
                return p
        return None

    p1 = find_player(name1)
    p2 = find_player(name2)
    if not p1 or not p2:
        return {'error': 'One or both players not found'}
    return {'p1': p1, 'p2': p2}


async def handle_api_h2h(request):
    """GET /api/h2h?p1=name&p2=name"""
    p1 = request.rel_url.query.get('p1', '')
    p2 = request.rel_url.query.get('p2', '')
    data = await asyncio.get_running_loop().run_in_executor(None, _build_h2h, p1, p2)
    return _json_response(data)


# ── /api/teams + /api/teamh2h ─────────────────────────────────────────────────

def _build_teams() -> list:
    """Return a list of all team names seen across matches."""
    matchid_map = _load_matches_from_db()
    teams: set = set()
    for entry in matchid_map.values():
        meta = entry.get('metadata') or {}
        n1 = meta.get('team1', {}).get('name', '')
        n2 = meta.get('team2', {}).get('name', '')
        if n1: teams.add(n1)
        if n2: teams.add(n2)
    # Also pull from DB
    try:
        conn = get_db(); c = conn.cursor()
        t = MATCHZY_TABLES
        c.execute(f"SELECT DISTINCT team1_name, team2_name FROM {t['matches']}")
        for row in c.fetchall():
            if row[0]: teams.add(row[0])
            if row[1]: teams.add(row[1])
        c.close(); conn.close()
    except Exception:
        pass
    return sorted(teams)


async def handle_api_teams(request):
    """GET /api/teams"""
    cached = _cache_get('teams')
    if cached is not None:
        return _json_response(cached)
    data = await asyncio.get_running_loop().run_in_executor(None, _build_teams)
    _cache_set('teams', data)
    return _json_response(data)


def _build_team_h2h(t1_name: str, t2_name: str) -> dict:
    """Head-to-head record between two named teams across all matches."""
    matches = _build_matches_list(999)
    t1n_l = t1_name.lower(); t2n_l = t2_name.lower()
    t1_wins = t2_wins = draws = 0
    shared = []

    for m in matches:
        mn1 = (m.get('team1_name') or '').lower()
        mn2 = (m.get('team2_name') or '').lower()
        if not ({mn1, mn2} >= {t1n_l, t2n_l}):
            continue
        # Determine sides
        if mn1 == t1n_l:
            s1, s2 = int(m.get('team1_score', 0)), int(m.get('team2_score', 0))
        else:
            s1, s2 = int(m.get('team2_score', 0)), int(m.get('team1_score', 0))
        if s1 > s2:   t1_wins += 1
        elif s2 > s1: t2_wins += 1
        else:         draws   += 1
        shared.append(m)

    return {
        't1': t1_name, 't2': t2_name,
        't1_wins': t1_wins, 't2_wins': t2_wins, 'draws': draws,
        'matches': shared,
    }


async def handle_api_team_h2h(request):
    """GET /api/teamh2h?t1=name&t2=name"""
    t1 = request.rel_url.query.get('t1', '')
    t2 = request.rel_url.query.get('t2', '')
    data = await asyncio.get_running_loop().run_in_executor(None, _build_team_h2h, t1, t2)
    return _json_response(data)


# ── /api/search ───────────────────────────────────────────────────────────────

def _search(q: str) -> dict:
    """Full-text search across players and matches."""
    q = q.strip().lower()
    if not q:
        return {'players': [], 'matches': []}

    lb = _build_leaderboard()
    players = [p for p in lb if q in (p.get('name') or '').lower()][:10]

    matches = [m for m in _build_matches_list(100)
               if q in (m.get('team1_name') or '').lower()
               or q in (m.get('team2_name') or '').lower()
               or q in str(m.get('matchid', '')).lower()][:10]

    return {'players': players, 'matches': matches}


async def handle_api_search(request):
    """GET /api/search?q=query"""
    q = request.rel_url.query.get('q', '')
    data = await asyncio.get_running_loop().run_in_executor(None, _search, q)
    return _json_response(data)


# ── /api/status ───────────────────────────────────────────────────────────────

async def handle_api_status(request):
    """GET /api/status — live server status (player count, map, etc.)."""
    import a2s as _a2s
    try:
        loop = asyncio.get_running_loop()
        addr = (SERVER_IP, SERVER_PORT)
        info = await loop.run_in_executor(None, _a2s.info, addr)
        return _json_response({
            'online':       True,
            'server_name':  info.server_name,
            'map':          info.map_name,
            'players':      info.player_count,
            'max_players':  info.max_players,
            'connect':      f"{SERVER_IP}:{SERVER_PORT}",
        })
    except Exception:
        return _json_response({'online': False})


# ── Edit endpoints ────────────────────────────────────────────────────────────

async def handle_api_auth_edit(request):
    """POST /api/auth/edit — validate edit password, return a session token."""
    import secrets
    try:
        body = await request.json()
    except Exception:
        return web.Response(status=400, text='Bad JSON')
    if body.get('password') != EDIT_PASSWORD:
        return web.Response(status=401, text='Wrong password',
                            headers={"Access-Control-Allow-Origin": "*"})
    token = secrets.token_hex(32)
    _EDIT_SESSIONS[token] = True
    return _json_response({'ok': True, 'token': token})


def _edit_auth_ok(request) -> bool:
    """Return True if request carries a valid edit session token."""
    auth = request.headers.get('Authorization', '')
    token = auth.removeprefix('Bearer ').strip()
    return bool(token and _EDIT_SESSIONS.get(token))


async def handle_api_save_match(request):
    """POST /api/match/{matchid}/save — save fshost JSON edits into match_edits table."""
    if not _edit_auth_ok(request):
        return web.Response(status=401, text='Unauthorized',
                            headers={"Access-Control-Allow-Origin": "*"})
    matchid = request.match_info.get('matchid', '')
    try:
        edits = await request.json()
    except Exception:
        return web.Response(status=400, text='Bad JSON')
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("""
            INSERT INTO match_edits (matchid, edits_json)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE edits_json = VALUES(edits_json), edited_at = NOW()
        """, (matchid, json.dumps(edits, default=str)))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        return _json_response({'ok': False, 'error': str(e)})
    _bust_edits_cache()
    _cache_bust('matches', 'matches_full', 'leaderboard', 'specialists')
    return _json_response({'ok': True})


async def handle_api_get_edits(request):
    """GET /api/match/{matchid}/edits — return current edits for a match."""
    matchid = request.match_info.get('matchid', '')
    edits = _get_edits_for(matchid)
    return _json_response(edits)


async def handle_api_patch_match(request):
    """PATCH /api/match/{matchid}/edit — merge partial edits."""
    if not _edit_auth_ok(request):
        return web.Response(status=401, text='Unauthorized',
                            headers={"Access-Control-Allow-Origin": "*"})
    matchid = request.match_info.get('matchid', '')
    try:
        patch = await request.json()
    except Exception:
        return web.Response(status=400, text='Bad JSON')
    existing = dict(_get_edits_for(matchid))
    existing.update(patch)
    if 'player_names' in patch:
        existing.setdefault('player_names', {}).update(patch['player_names'])
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("""
            INSERT INTO match_edits (matchid, edits_json)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE edits_json = VALUES(edits_json), edited_at = NOW()
        """, (matchid, json.dumps(existing, default=str)))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        return _json_response({'ok': False, 'error': str(e)})
    _bust_edits_cache()
    _cache_bust('matches', 'matches_full', 'leaderboard', 'specialists')
    return _json_response({'ok': True, 'edits': existing})


async def handle_api_revert_match(request):
    """DELETE /api/match/{matchid}/edit — remove all edits for a match."""
    if not _edit_auth_ok(request):
        return web.Response(status=401, text='Unauthorized',
                            headers={"Access-Control-Allow-Origin": "*"})
    matchid = request.match_info.get('matchid', '')
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("DELETE FROM match_edits WHERE matchid=%s", (matchid,))
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        return _json_response({'ok': False, 'error': str(e)})
    _bust_edits_cache()
    _cache_bust('matches', 'matches_full', 'leaderboard', 'specialists')
    return _json_response({'ok': True})


async def start_http_server():
    app = web.Application()
    app.router.add_get('/api/specialists',             handle_api_specialists)
    app.router.add_get('/api/player/sid/{steamid64}',         handle_api_player_by_sid)
    app.router.add_get('/api/player/sid/{steamid64}/mapstats', handle_api_player_mapstats_by_sid)
    app.router.add_get('/api/player/{name}',                   handle_api_player)
    app.router.add_get('/api/steam/{steamid64}',       handle_api_steam)
    app.router.add_get('/api/matches',                 handle_api_matches)
    app.router.add_get('/api/matches/full',            handle_api_matches_full)
    app.router.add_get('/api/match/{matchid}',         handle_api_match)
    app.router.add_get('/api/demos',                   handle_api_demos)
    app.router.add_get('/api/leaderboard',             handle_api_leaderboard)
    app.router.add_get('/api/mapstats',                handle_api_mapstats)
    app.router.add_get('/api/h2h',                     handle_api_h2h)
    app.router.add_get('/api/teamh2h',                 handle_api_team_h2h)
    app.router.add_get('/api/teams',                   handle_api_teams)
    app.router.add_get('/api/search',                  handle_api_search)
    app.router.add_get('/api/player/{name}/mapstats',  handle_api_player_mapstats)
    app.router.add_get('/api/status',                  handle_api_status)
    # ── Edit endpoints ────────────────────────────────────────────────────────
    app.router.add_post('/api/auth/edit',              handle_api_auth_edit)
    app.router.add_post('/api/match/{matchid}/save',   handle_api_save_match)
    app.router.add_get('/api/match/{matchid}/edits',   handle_api_get_edits)
    app.router.add_route('PATCH',  '/api/match/{matchid}/edit', handle_api_patch_match)
    app.router.add_route('DELETE', '/api/match/{matchid}/edit', handle_api_revert_match)
    app.router.add_route('OPTIONS', '/api/auth/edit',             handle_options)
    app.router.add_route('OPTIONS', '/api/match/{matchid}/save',  handle_options)
    app.router.add_route('OPTIONS', '/api/match/{matchid}/edit',  handle_options)
    app.router.add_route('OPTIONS', '/api/match/{matchid}/edits', handle_options)
    # ── Static ────────────────────────────────────────────────────────────────

    # ── Player Steam login (open to any Steam user) ───────────────────────────
    app.router.add_get('/auth/steam/player',            handle_player_steam_login)
    app.router.add_get('/auth/steam/player/callback',   handle_player_steam_callback)
    app.router.add_post('/auth/steam/player/logout',    handle_player_steam_logout)
    app.router.add_get('/api/auth/steam/me',            handle_player_api_me)
    app.router.add_get('/stats',   handle_stats_page)
    app.router.add_get('/',        handle_stats_page)
    app.router.add_get('/health',  handle_health_check)
    app.router.add_static('/assets', path=pathlib.Path(__file__).parent / "assets", name='assets')

    port = int(os.getenv('PORT', 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"✓ HTTP server started on port {port}")
    return runner




intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
bot = commands.Bot(command_prefix="!", intents=intents, owner_id=ADMIN_ID)

# ========== DATABASE SETUP (Railway MySQL via mysql-connector-python) ==========
# Railway MySQL env vars:
#   MYSQL_URL  (mysql://user:pass@host:port/dbname)
# OR individual vars:
#   MYSQLHOST, MYSQLPORT, MYSQLUSER, MYSQLPASSWORD, MYSQLDATABASE


def init_database():
    conn = get_db()
    c = conn.cursor()

    # ── fshost match cache ───────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS fshost_matches (
            matchid      VARCHAR(64)   PRIMARY KEY,
            raw_json     LONGTEXT      NOT NULL,
            demo_name    VARCHAR(512)  NOT NULL DEFAULT '',
            demo_url     VARCHAR(1024) NOT NULL DEFAULT '',
            demo_size    VARCHAR(64)   NOT NULL DEFAULT '',
            demo_modified VARCHAR(64)  NOT NULL DEFAULT '',
            fetched_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at   DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4
    """)
    # Migrate existing installs that are missing the demo columns
    for col, definition in [
        ('demo_name',     "VARCHAR(512)  NOT NULL DEFAULT ''"),
        ('demo_url',      "VARCHAR(1024) NOT NULL DEFAULT ''"),
        ('demo_size',     "VARCHAR(64)   NOT NULL DEFAULT ''"),
        ('demo_modified', "VARCHAR(64)   NOT NULL DEFAULT ''"),
    ]:
        try:
            c.execute(f"ALTER TABLE fshost_matches ADD COLUMN {col} {definition}")
        except Exception:
            pass  # column already exists — ignore

    # ── edit overlay: partial JSON diff stored per match ────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS match_edits (
            matchid    VARCHAR(64) PRIMARY KEY,
            edits_json LONGTEXT    NOT NULL,
            edited_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4
    """)

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



def matchzy_tables_exist(conn) -> bool:
    """Return True if MatchZy tables are present in the database."""
    c = conn.cursor()
    c.execute("SHOW TABLES LIKE 'matchzy_stats_players'")
    result = c.fetchone()
    c.close()
    return result is not None




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
        return interaction.user.id == ADMIN_ID
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

# Demo filename format: YYYY-MM-DD_HH-MM-SS_<matchnum>_<mapname>_<team1>_vs_<team2>.dem
DEMO_TS_RE = re.compile(r'^(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})')

# Cache for match ID to demo mapping (refreshed every 5 minutes)
_MATCHID_DEMO_CACHE = None
_MATCHID_CACHE_TIME = None
_CACHE_TTL_SECONDS = 300  # 5 minutes

def build_matchid_to_demo_map(force_refresh=False):
    """
    Build a mapping of matchid -> match data from ALL fshost .json files.
    Every .json is indexed by its match_id field — no .dem required.
    Demo info (download_url, size) is attached if a matching .dem exists.

    Returns dict: {
      "26": {
        "metadata":      {...full fshost JSON...},
        "name":          "2026-02-21_21-18-57_26_de_mirage_...dem"  (or ""),
        "download_url":  "https://..."  (or ""),
        "size_formatted":"286.37MB"     (or ""),
      }
    }
    """
    global _MATCHID_DEMO_CACHE, _MATCHID_CACHE_TIME

    if not force_refresh and _MATCHID_DEMO_CACHE is not None and _MATCHID_CACHE_TIME is not None:
        if (datetime.now() - _MATCHID_CACHE_TIME).total_seconds() < _CACHE_TTL_SECONDS:
            return _MATCHID_DEMO_CACHE

    print("[Demo Map] Building matchid map from all fshost .json files...")
    all_files = fetch_all_demos_raw()

    # Index .dem files by base name for quick lookup
    dem_by_base = {}
    for f in all_files:
        n = f.get("name", "")
        if n.endswith(".dem"):
            dem_by_base[n[:-4]] = f  # strip .dem

    matchid_map = {}
    headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://fshost.me/'}

    for file_obj in all_files:
        name = file_obj.get("name", "")
        if not name.endswith(".json"):
            continue
        url = file_obj.get("download_url", "")
        if not url:
            continue
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            metadata = resp.json()
        except Exception as e:
            print(f"[Demo Map] ✗ {name}: {e}")
            continue

        matchid = str(metadata.get("match_id") or metadata.get("matchid") or metadata.get("id") or "")
        if not matchid:
            print(f"[Demo Map] ✗ {name}: no match_id field")
            continue

        # Find matching .dem (same base name)
        # fshost names JSON files as "<base>_stats.json", so strip "_stats" too
        base = name[:-5]  # strip .json
        if base.endswith("_stats"):
            base = base[:-6]  # strip _stats
        dem_entry = dem_by_base.get(base, {})

        matchid_map[matchid] = {
            "metadata":      metadata,
            "name":          dem_entry.get("name", ""),
            "download_url":  dem_entry.get("download_url", ""),
            "size_formatted":dem_entry.get("size_formatted", ""),
            "modified_at":   dem_entry.get("modified_at", ""),
        }
        print(f"[Demo Map] ✓ match {matchid} ← {name}" + (f" + {dem_entry.get('name')}" if dem_entry else " (no demo)"))

    print(f"[Demo Map] Total: {len(matchid_map)} matches indexed from {sum(1 for f in all_files if f.get('name','').endswith('.json'))} JSONs")

    _MATCHID_DEMO_CACHE = matchid_map
    _MATCHID_CACHE_TIME = datetime.now()
    return matchid_map

def fetch_all_demos_raw():
    """Return the raw list of demo dicts from fshost, sorted newest first."""
    if not DEMOS_JSON_URL:
        return []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Referer': 'https://fshost.me/'
    }
    try:
        r = requests.get(DEMOS_JSON_URL, headers=headers, timeout=15)
        r.raise_for_status()
        demos = r.json().get("demos", [])
        return sorted(demos, key=lambda x: x.get("modified_at", ""), reverse=True)
    except Exception:
        return []

def find_demo_for_match(match_end_time_or_id, window_minutes=10):
    """
    Try to match a demo file to a match.
    Can accept either:
    - A matchid string: will look for exact match in JSON metadata (PREFERRED)
    - A datetime object: will use timestamp matching as fallback
    
    Match ID Method (EXACT):
    - Looks up matchid in .json files
    - Returns the exact .dem file for that match
    - Example: find_demo_for_match("11") → "2024-02-20_18-23-00_match11.dem"
    
    Timestamp Method (FALLBACK):
    - Compares demo filename timestamp with match end_time
    - Finds demos within ±10 minutes
    - Less accurate, may match wrong demo
    
    Returns (name, download_url) or (None, None).
    """
    # Method 1: Match ID lookup (preferred - exact matching)
    if isinstance(match_end_time_or_id, str):
        try:
            matchid_map = _load_matches_from_db()
            if match_end_time_or_id in matchid_map:
                demo = matchid_map[match_end_time_or_id]
                print(f"[Demo Match] ✓ Found exact match for ID {match_end_time_or_id}: {demo.get('name')}")
                return demo.get("name"), demo.get("download_url", "#")
            else:
                print(f"[Demo Match] ✗ No exact match found for ID {match_end_time_or_id}")
        except Exception as e:
            print(f"[Demo Match] Error using matchid map: {e}")
        return None, None
    
    # Method 2: Timestamp matching (fallback - less accurate)
    end_time = match_end_time_or_id
    if not end_time:
        return None, None
    if not isinstance(end_time, datetime):
        return None, None
    
    print(f"[Demo Match] Using timestamp matching (fallback) for {end_time}")
    
    # Make end_time timezone-aware (UTC) if it isn't already
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=pytz.utc)
    
    demos = fetch_all_demos_raw()
    # Filter to only .dem files for timestamp matching
    demos = [d for d in demos if d.get("name", "").endswith(".dem")]
    
    best = None
    best_delta = None
    for demo in demos:
        name = demo.get("name", "")
        m = DEMO_TS_RE.match(name)
        if not m:
            continue
        try:
            demo_dt = datetime.strptime(m.group(1), "%Y-%m-%d_%H-%M-%S").replace(tzinfo=pytz.utc)
        except ValueError:
            continue
        delta = abs((demo_dt - end_time).total_seconds())
        if delta <= window_minutes * 60:
            if best_delta is None or delta < best_delta:
                best = demo
                best_delta = delta
    
    if best:
        print(f"[Demo Match] ✓ Found timestamp match: {best.get('name')} (within {best_delta:.0f}s)")
        return best.get("name"), best.get("download_url", "#")
    
    print(f"[Demo Match] ✗ No timestamp match found within {window_minutes} minutes")
    return None, None

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

def _sync_fshost_to_db_blocking():
    """Pull every fshost JSON + demo info into fshost_matches. Runs in a thread executor."""
    inserted = skipped = errors = 0
    try:
        matchid_map = build_matchid_to_demo_map(force_refresh=True)
        if not matchid_map:
            return 0, 0, 0
        conn = get_db(); c = conn.cursor()
        for matchid, entry in matchid_map.items():
            metadata = entry.get('metadata')
            if not metadata:
                skipped += 1; continue
            try:
                c.execute("""
                    INSERT INTO fshost_matches
                        (matchid, raw_json, demo_name, demo_url, demo_size, demo_modified, fetched_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        raw_json      = VALUES(raw_json),
                        demo_name     = VALUES(demo_name),
                        demo_url      = VALUES(demo_url),
                        demo_size     = VALUES(demo_size),
                        demo_modified = VALUES(demo_modified),
                        updated_at    = NOW()
                """, (
                    str(matchid),
                    json.dumps(metadata, default=str),
                    entry.get('name', ''),
                    entry.get('download_url', ''),
                    entry.get('size_formatted', ''),
                    entry.get('modified_at', ''),
                ))
                inserted += 1
            except Exception as e:
                print(f"[fshost-sync] match {matchid}: {e}"); errors += 1
        conn.commit(); c.close(); conn.close()
    except Exception as e:
        print(f"[fshost-sync] fatal: {e}"); errors += 1
    return inserted, skipped, errors


@tasks.loop(minutes=30)
async def sync_fshost_to_db():
    """Sync all fshost JSONs into the DB every 30 min so edit modal always has raw data."""
    try:
        loop = asyncio.get_running_loop()
        inserted, skipped, errors = await loop.run_in_executor(None, _sync_fshost_to_db_blocking)
        print(f"[fshost-sync] {inserted} upserted, {skipped} skipped, {errors} errors")
        _bust_edits_cache()
        _cache_bust('matches', 'matches_full', 'leaderboard', 'specialists', 'mapstats', 'teams')
        print("[fshost-sync] API caches cleared")
    except Exception as e:
        print(f"[fshost-sync] task error: {e}")

@sync_fshost_to_db.before_loop
async def before_sync():
    await bot.wait_until_ready()


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
    print(f"Owner ID from env: {ADMIN_ID}")
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
    sync_fshost_to_db.start()
    print("✓ fshost → DB sync started (runs now + every 30 min)")

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    if message.content.startswith('!') and message.author.id == ADMIN_ID:
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
    # Resolve name -> SteamID64 -> career stats
    sid64 = None
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute(
            f"SELECT steamid64 FROM {MATCHZY_TABLES['players']} WHERE name=%s AND steamid64!='0' LIMIT 1",
            (player_name,))
        row = c.fetchone(); c.close(); conn.close()
        if row: sid64 = to_steamid64(str(row['steamid64']))
    except Exception: pass
    mz, _ = _db_player_career_and_recent(sid64) if sid64 else (None, [])
    if not mz:
        return await inter.followup.send(
            f"❌ No stats found for **{player_name}**. Player must have completed at least one match.",
            ephemeral=True)
    kills   = int(mz.get("kills") or 0);  deaths = int(mz.get("deaths") or 0)
    kd      = kills / deaths if deaths else float(kills)
    embed = discord.Embed(
        title=f"👤 {mz.get('name', player_name)}",
        description=f"📊 Career Stats • {mz.get('matches',0)} matches",
        color=0x2ECC71)
    embed.add_field(name="💀 Kills",        value=f"**{kills}**",                              inline=True)
    embed.add_field(name="☠️ Deaths",       value=f"**{deaths}**",                             inline=True)
    embed.add_field(name="📊 K/D",          value=f"**{kd:.2f}**",                             inline=True)
    embed.add_field(name="🤝 Assists",      value=f"**{int(mz.get('assists') or 0)}**",        inline=True)
    embed.add_field(name="🎯 Headshots",    value=f"**{int(mz.get('headshots') or 0)}** ({float(mz.get('hs_pct') or 0):.1f}%)", inline=True)
    embed.add_field(name="💥 ADR",          value=f"**{float(mz.get('adr') or 0):.1f}**",     inline=True)
    if mz.get("aces"):   embed.add_field(name="⭐ Aces",      value=f"**{mz['aces']}**",       inline=True)
    if mz.get("clutch_1v1"): embed.add_field(name="🔥 1v1 Wins", value=f"**{mz['clutch_1v1']}**", inline=True)
    if mz.get("entry_wins"): embed.add_field(name="🚪 Entry Wins", value=f"**{mz['entry_wins']}**", inline=True)
    embed.set_footer(text=f"SteamID64: {mz.get('steamid64', 'N/A')}")
    await inter.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="leaderboard", description="Top players (MatchZy kills leaderboard)")
async def leaderboard_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT name, steamid64, COUNT(DISTINCT matchid) AS matches,
                SUM(kills) AS kills, SUM(deaths) AS deaths,
                ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2) AS kd,
                ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1) AS hs_pct
            FROM {MATCHZY_TABLES['players']} WHERE steamid64 != '0'
            GROUP BY steamid64
            ORDER BY SUM(kills) DESC LIMIT 10
        """)
        leaderboard = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
    except Exception: leaderboard = []
    if not leaderboard:
        return await inter.followup.send("❌ No player data available yet.", ephemeral=True)
    
    embed = discord.Embed(
        title="🏆 Top Players",
        description="*Sorted by kills • Bots excluded*",
        color=0xF1C40F
    )
    medals = ["🥇", "🥈", "🥉"]
    for i, row in enumerate(leaderboard, 1):
        name     = row.get("team1_name", "?")
        kills    = int(row.get("kills") or 0)
        deaths   = int(row.get("deaths") or 0)
        kd       = row.get("kd")
        damage   = row.get("kills")
        hs_pct   = row.get("hs_pct")
        matches  = row.get("matches")
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
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT m.matchid, m.end_time, m.winner, m.team1_name, m.team2_name,
                   mp.mapname, mp.team1_score, mp.team2_score
            FROM {MATCHZY_TABLES['matches']} m
            LEFT JOIN {MATCHZY_TABLES['maps']} mp ON m.matchid=mp.matchid
            ORDER BY m.end_time DESC LIMIT 5
        """)
        matches = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
    except Exception: matches = []
    if not matches:
        return await inter.followup.send("❌ No match data found.", ephemeral=True)
    
    # Build matchid -> demo mapping from DB cache
    try:
        matchid_map = _load_matches_from_db()
        debug_lines = [f"📂 Demos mapped via DB cache: **{len(matchid_map)}**"]
    except Exception as e:
        matchid_map = {}
        debug_lines = [f"⚠️ Could not load match map: {e}"]

    embed = discord.Embed(title="🏟️ Recent Matches", color=0x3498DB)
    embed.set_footer(text=" | ".join(debug_lines))
    
    for m in matches:
        matchid  = m.get("matchid")
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
        
        # Try to find demo using matchid first (EXACT via .json), then timestamp fallback
        demo_name, demo_url = find_demo_for_match(str(matchid))  # Try match ID first
        if not demo_url or demo_url == "#":
            # Fallback to timestamp matching
            if end_time:
                demo_name, demo_url = find_demo_for_match(end_time)
        
        if demo_url and demo_url != "#":
            result += f"\n📥 [Download Demo](<{demo_url}>)"
        else:
            result += f"\n*(no demo matched)*"
        
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
    c.execute(f"SELECT matchid, team1_name, team2_name, team1_score, team2_score, mapname, end_time FROM {MATCHZY_TABLES['matches']} mm LEFT JOIN {MATCHZY_TABLES['maps']} mp ON mm.matchid=mp.matchid WHERE mm.matchid=%s LIMIT 1", (match_id,))
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
    
    # Try to find demo using matchid first (EXACT MATCH via .json), then fall back to timestamp
    demo_name, demo_url = find_demo_for_match(match_id)  # Try match ID first
    if not demo_url or demo_url == "#":
        # Fallback to timestamp matching
        end_time = row.get("end_time")
        if end_time:
            demo_name, demo_url = find_demo_for_match(end_time)
    
    if demo_url and demo_url != "#":
        embed.add_field(name="📥 Demo", value=f"[Download Demo](<{demo_url}>)", inline=False)
    
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

@bot.tree.command(name="debugmatch", description="Debug a specific match data")
@owner_only()
async def debugmatch_cmd(inter: discord.Interaction, match_id: str):
    await inter.response.defer(ephemeral=True)
    lines = [f"**Debugging Match #{match_id}**\n"]
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        
        # Get match data
        c.execute(f"SELECT * FROM {MATCHZY_TABLES['matches']} WHERE matchid=%s", (match_id,))
        match = c.fetchone()
        if match:
            lines.append(f"✅ Match found")
            lines.append(f"Teams: {match.get('team1_name')} vs {match.get('team2_name')}")
            lines.append(f"Score: {match.get('team1_score')} : {match.get('team2_score')}")
        else:
            lines.append(f"❌ Match not found")
            c.close(); conn.close()
            return await inter.followup.send("\n".join(lines), ephemeral=True)
        
        # Get player data
        c.execute(f"SELECT name, team, kills, deaths, mapnumber FROM {MATCHZY_TABLES['players']} WHERE matchid=%s", (match_id,))
        players = c.fetchall()
        lines.append(f"\n**Players ({len(players)} total):**")
        for p in players:
            lines.append(f"• {p['name']} | Team: `{p['team']}` | Map: {p['mapnumber']} | K/D: {p['kills']}/{p['deaths']}")
        
        # Get map data
        c.execute(f"SELECT * FROM {MATCHZY_TABLES['maps']} WHERE matchid=%s", (match_id,))
        maps = c.fetchall()
        lines.append(f"\n**Maps ({len(maps)} total):**")
        for m in maps:
            lines.append(f"• Map {m.get('mapnumber')}: {m.get('mapname')} ({m.get('team1_score')} : {m.get('team2_score')})")
        
        c.close()
        conn.close()
    except Exception as e:
        lines.append(f"\n❌ Error: {e}")
    
    # Split into multiple messages if too long
    message = "\n".join(lines)
    if len(message) > 2000:
        chunks = [message[i:i+2000] for i in range(0, len(message), 2000)]
        for chunk in chunks:
            await inter.followup.send(chunk, ephemeral=True)
    else:
        await inter.followup.send(message, ephemeral=True)

@bot.tree.command(name="debugdemos", description="Show match ID to demo mapping from .json files")
@owner_only()
async def debugdemos_cmd(inter: discord.Interaction, refresh: bool = False):
    """
    Shows how .json files are mapped to match IDs.
    
    Args:
        refresh: If True, clears the cache and rebuilds the mapping
    """
    await inter.response.defer(ephemeral=True)
    lines = ["**Match ID → Demo Mapping** (from .json files)\n"]
    
    try:
        if refresh:
            lines.append("🔄 Refreshing cache...\n")
        
        matchid_map = build_matchid_to_demo_map(force_refresh=refresh)
        
        if not matchid_map:
            lines.append("❌ No demos with .json metadata found")
            lines.append("\nMake sure your demo files have corresponding .json files with 'matchid' field:")
            lines.append("```json")
            lines.append('{"matchid": "11", "team1_name": "...", ...}')
            lines.append("```")
        else:
            lines.append(f"✅ Found {len(matchid_map)} matches with demos:\n")
            
            # Sort by match ID (numeric)
            sorted_matches = sorted(matchid_map.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0, reverse=True)
            
            for matchid, demo in sorted_matches[:15]:  # Show last 15
                name = demo.get('name', '?')
                size = demo.get('size_formatted', '?')
                lines.append(f"**Match #{matchid}**")
                lines.append(f"  └─ {name} ({size})")
            
            if len(matchid_map) > 15:
                lines.append(f"\n...and {len(matchid_map) - 15} more")
            
            # Show cache info
            if _MATCHID_CACHE_TIME:
                age = (datetime.now() - _MATCHID_CACHE_TIME).total_seconds()
                lines.append(f"\n📊 Cache age: {age:.0f}s (refreshes every {_CACHE_TTL_SECONDS}s)")
    
    except Exception as e:
        lines.append(f"\n❌ Error: {e}")
    
    message = "\n".join(lines)
    if len(message) > 2000:
        chunks = [message[i:i+2000] for i in range(0, len(message), 2000)]
        for chunk in chunks:
            await inter.followup.send(chunk, ephemeral=True)
    else:
        await inter.followup.send(message, ephemeral=True)


@bot.tree.command(name="syncdemos", description="Force re-sync all fshost JSONs into the database now")
@owner_only()
async def syncdemos_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    try:
        loop = asyncio.get_running_loop()
        inserted, skipped, errors = await loop.run_in_executor(None, _sync_fshost_to_db_blocking)
        _bust_edits_cache()
        await inter.followup.send(
            f"✅ **fshost → DB sync complete**\n"
            f"• `{inserted}` matches upserted\n"
            f"• `{skipped}` skipped (no metadata)\n"
            f"• `{errors}` errors",
            ephemeral=True
        )
    except Exception as e:
        await inter.followup.send(f"❌ Sync failed: {e}", ephemeral=True)


if not TOKEN:
    raise SystemExit("TOKEN missing.")

bot.run(TOKEN)
