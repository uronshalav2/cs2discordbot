import os
import re
import json
import pytz
import a2s
import asyncio
import discord
import requests
import io
import pathlib
from datetime import datetime, timedelta
from discord.ext import commands, tasks
from discord import app_commands
from typing import Literal, Optional
from mcrcon import MCRcon
from collections import defaultdict
from aiohttp import web
from discord.ui import Button, View
# Path to stats.html — always relative to this file, safe regardless of working directory
HTML_PATH = pathlib.Path(__file__).parent / 'stats.html'
async def handle_health_check(request):
    return web.Response(text='Bot is running')
# ─────────────────────────────────────────────────────────────────────────────
# STATS WEBSITE API ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────
def _json_response(data):
    return web.Response(
        text=json.dumps(data, default=str),
        content_type='application/json',
        headers={"Access-Control-Allow-Origin": "*"},
    )
async def handle_api_player(request):
    """GET /api/player/{name} — full career stats for a player"""
    name = request.match_info.get('name', '')
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
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
        c.execute(f"""
SELECT p.matchid, p.mapnumber, p.team,
p.kills, p.deaths, p.assists, p.damage, p.head_shot_kills,
p.enemies5k, p.v1_wins,
m.mapname, m.winner, m.team1_score, m.team2_score,
mm.team1_name, mm.team2_name,
ROUND(p.damage/30,1) AS adr,
ROUND(p.head_shot_kills/NULLIF(p.kills,0)*100,1) AS hs_pct
FROM {MATCHZY_TABLES['players']} p
LEFT JOIN {MATCHZY_TABLES['maps']} m ON p.matchid=m.matchid AND p.mapnumber=m.map
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
    """GET /api/matches — primary from fshost JSONs, fallback from DB"""
    try:
        limit = int(request.rel_url.query.get('limit', 50))
        loop = asyncio.get_running_loop()
        matchid_map = await loop.run_in_executor(None, build_matchid_to_demo_map)
        results = []
        for mid, entry in matchid_map.items():
            meta = entry.get('metadata') or {}
            if not meta:
                continue
            t1 = meta.get('team1', {})
            t2 = meta.get('team2', {})
            results.append({
            'matchid': str(meta.get('match_id') or mid),
            'team1_name': t1.get('name', 'Team 1'),
            'team2_name': t2.get('name', 'Team 2'),
            'team1_score': t1.get('score', 0),
            'team2_score': t2.get('score', 0),
            'winner': meta.get('winner', ''),
            'end_time': meta.get('date', ''),
            'mapname': meta.get('map', '?'),
            'total_rounds':meta.get('total_rounds'),
            'demo_url': entry.get('download_url', ''),
            'demo_size': entry.get('size_formatted', ''),
            })
        fshost_ids = {r['matchid'] for r in results}
        try:
                conn = get_db()
                c = conn.cursor(dictionary=True)
                c.execute(f"""
SELECT mm.matchid, mm.team1_name, mm.team2_name, mm.winner,
mm.end_time, m.mapname, m.team1_score, m.team2_score
FROM {MATCHZY_TABLES['matches']} mm
LEFT JOIN {MATCHZY_TABLES['maps']} m ON mm.matchid = m.matchid
WHERE mm.end_time IS NOT NULL
ORDER BY mm.end_time DESC
LIMIT %s
""", (limit,))
                for row in c.fetchall():
                    mid = str(row['matchid'])
                    if mid not in fshost_ids:
                        results.append({
                            'matchid': mid,
                            'team1_name': row.get('team1_name', 'Team 1'),
                            'team2_name': row.get('team2_name', 'Team 2'),
                            'team1_score': row.get('team1_score', 0),
                            'team2_score': row.get('team2_score', 0),
                            'winner': row.get('winner', ''),
                            'end_time': str(row.get('end_time', '')),
                            'mapname': row.get('mapname', '?'),
                            'demo_url': '',
                            'demo_size': '',
                        })
                c.close(); conn.close()
        except Exception as e:
            print(f"[api/matches] DB fallback error: {e}")
        results.sort(key=lambda r: str(r.get('end_time') or ''), reverse=True)
        return _json_response(results[:limit])
    except Exception as e:
        return _json_response({"error": str(e)})
async def handle_api_match(request):
    """GET /api/match/{matchid} — pure fshost JSON source"""
    matchid = request.match_info.get('matchid', '')
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, _fetch_fshost_match_json, matchid)
        if not data:
            return _json_response({"error": f"No fshost data found for match {matchid}"})
        t1 = data.get('team1', {})
        t2 = data.get('team2', {})
        meta = {
        'matchid': data.get('match_id') or matchid,
        'team1_name': t1.get('name', 'Team 1'),
        'team2_name': t2.get('name', 'Team 2'),
        'team1_score': t1.get('score', 0),
        'team2_score': t2.get('score', 0),
        'winner': data.get('winner', ''),
        'end_time': data.get('date'),
        'start_time': data.get('date'),
        }
        maps = [{
        'matchid': matchid,
        'mapnumber': 1,
        'mapname': data.get('map', 'unknown'),
        'team1_score': t1.get('score', 0),
        'team2_score': t2.get('score', 0),
        'winner': data.get('winner', ''),
        'total_rounds': data.get('total_rounds'),
        }]
        players = _players_from_fshost(data, matchid)
        matchid_map = build_matchid_to_demo_map()
        entry = matchid_map.get(str(matchid), {})
        demo = {
        'name': entry.get('name', ''),
        'url': entry.get('download_url', ''),
        'size': entry.get('size_formatted', ''),
        }
        return _json_response({"meta": meta, "maps": maps, "players": players, "demo": demo})
    except Exception as e:
        return _json_response({"error": str(e)})
def _players_from_fshost(data: dict, matchid: str) -> list:
    """Flatten fshost team1/team2 players into a unified list."""
    def cw(s):
        try: return int(str(s).split('/')[0])
        except: return 0
    players = []
    for team_key in ('team1', 'team2'):
        team_data = data.get(team_key, {})
    team_name = team_data.get('name', team_key)
    for fp in team_data.get('players', []):
        kills = int(fp.get('kills', 0) or 0)
        deaths = int(fp.get('deaths', 0) or 0)
        hs_kills = int(fp.get('headshot_kills', 0) or 0)
        hs_pct = fp.get('hs_percent')
        if hs_pct is None:
            hs_pct = round(hs_kills / kills * 100, 1) if kills else 0
            players.append({
            'matchid': matchid,
            'mapnumber': 1,
            'steamid64': str(fp.get('steam_id', '0')),
            'name': fp.get('name', '?'),
            'team': team_key,
            'team_name': team_name,
            'kills': kills,
            'deaths': deaths,
            'assists': int(fp.get('assists', 0) or 0),
            'damage': int(fp.get('damage', 0) or 0),
            'head_shot_kills':hs_kills,
            'hs_pct': hs_pct,
            'adr': fp.get('adr'),
            'enemies5k': int(fp.get('5k', 0) or 0),
            'enemies4k': int(fp.get('4k', 0) or 0),
            'enemies3k': int(fp.get('3k', 0) or 0),
            'v1_wins': cw(fp.get('1v1', 0)),
            'v2_wins': cw(fp.get('1v2', 0)),
            'clutch_1v1': fp.get('1v1', '0/0'),
            'clutch_1v2': fp.get('1v2', '0/0'),
            'rating': fp.get('rating'),
            'kast': fp.get('kast'),
            'multi_kills': fp.get('multi_kills'),
            'opening_kills': fp.get('opening_kills'),
            'opening_deaths': fp.get('opening_deaths'),
            'trade_kills': fp.get('trade_kills'),
            'flash_assists': fp.get('flash_assists'),
            'utility_damage': fp.get('utility_damage'),
            })
            return players
def _fetch_fshost_match_json(matchid: str) -> dict | None:
    """Return the fshost JSON for a given match ID via the demo cache."""
    try:
        matchid_map = build_matchid_to_demo_map()
        entry = matchid_map.get(str(matchid))
        if not entry:
            return None
        return entry.get('metadata')
    except Exception as e:
        print(f"[fshost JSON] Error fetching match {matchid}: {e}")
        return None
def _parse_demo_filename(name: str) -> dict:
    """Parse a demo filename to extract timestamp, mapname, and team names."""
    import re as _re
    result = {}
    stem = name.replace('.dem', '')
    ts_m = _re.match(r'^(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})', stem)
    if ts_m:
        try:
            dt_str = f"{ts_m.group(1)} {ts_m.group(2).replace('-', ':')}"
            result['filename_ts'] = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").isoformat()
        except ValueError:
            pass
    rest = _re.sub(r'^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_[^_]+_', '', stem)
    map_m = _re.match(r'((?:de|cs|gg|ar|dm)_\w+?)_(.+)', rest)
    if map_m:
        result['mapname'] = map_m.group(1)
        teams_part = map_m.group(2)
        vs_split = _re.split(r'_vs_', teams_part, flags=_re.IGNORECASE)
        if len(vs_split) == 2:
            result['team1_name'] = vs_split[0].replace('_', ' ').strip()
            result['team2_name'] = vs_split[1].replace('_', ' ').strip()
    return result
async def handle_api_demos(request):
    """GET /api/demos — all demos from fshost with parsed timestamps and match metadata"""
    demos = fetch_all_demos_raw()
    matchid_map = build_matchid_to_demo_map()
    result = []
    for d in demos:
        name = d.get("name", "")
        if not name.endswith(".dem"):
            continue
        parsed = _parse_demo_filename(name)
        ts = parsed.get('filename_ts')
        meta = {
            'matchid': '',
            'mapname': parsed.get('mapname', ''),
            'team1_name': parsed.get('team1_name', ''),
            'team2_name': parsed.get('team2_name', ''),
            'team1_score': '',
            'team2_score': '',
        }
        for mid, entry in matchid_map.items():
            entry_name = entry.get('name', '')
            if entry_name == name or entry_name.replace('.json', '') == name.replace('.dem', ''):
                raw_meta = entry.get('metadata') or {}
                meta['matchid'] = str(raw_meta.get('match_id', mid))
                meta['mapname'] = raw_meta.get('map', '') or meta['mapname']
                meta['team1_name'] = (raw_meta.get('team1') or {}).get('name', '') or meta['team1_name']
                meta['team2_name'] = (raw_meta.get('team2') or {}).get('name', '') or meta['team2_name']
                meta['team1_score'] = (raw_meta.get('team1') or {}).get('score', '')
                meta['team2_score'] = (raw_meta.get('team2') or {}).get('score', '')
                break
        result.append({
            "name": name,
            "download_url": d.get("download_url", ""),
            "size_formatted": d.get("size_formatted", ""),
            "modified_at": d.get("modified_at", ""),
            "filename_ts": ts,
            **meta,
        })
    result.sort(key=lambda x: x.get('filename_ts') or x.get('modified_at') or '', reverse=True)
    return _json_response(result)
async def handle_api_leaderboard(request):
    """GET /api/leaderboard — career stats aggregated from matchzy_stats_players"""
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
SELECT
steamid64,
SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS name,
COUNT(DISTINCT matchid) AS matches,
SUM(kills) AS kills,
SUM(deaths) AS deaths,
SUM(assists) AS assists,
SUM(head_shot_kills) AS headshots,
SUM(damage) AS damage,
SUM(enemies5k) AS aces,
SUM(enemies4k) AS quads,
SUM(enemies3k) AS triples,
SUM(v1_wins) AS clutch_wins,
SUM(entry_wins) AS entry_wins,
ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2) AS kd,
ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1) AS hs_pct,
ROUND(SUM(damage)/NULLIF(
COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)),0)/30,1) AS adr
FROM {MATCHZY_TABLES['players']}
WHERE steamid64 != '0' AND steamid64 IS NOT NULL
AND name != '' AND name IS NOT NULL
GROUP BY steamid64
ORDER BY kills DESC
""")
        rows = c.fetchall()
        c.close(); conn.close()
        return _json_response(rows)
    except Exception as e:
        return _json_response({"error": str(e)})
async def handle_api_specialists(request):
    """GET /api/specialists — clutch, entry, flash, utility specialist boards"""
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
SELECT
steamid64,
SUM(v1_wins) AS clutch_1v1,
SUM(v2_wins) AS clutch_1v2,
SUM(v1_wins) + SUM(v2_wins) AS clutch_total,
SUM(entry_wins) AS entry_wins,
SUM(entry_count) AS entry_attempts,
SUM(entry_wins) AS entry_wins
SUM(flash_successes) AS flash_successes,
ROUND(SUM(flash_successes)/NULLIF(COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)),0),2) AS flash_per_map,
SUM(utility_damage) AS utility_damage,
ROUND(SUM(utility_damage)/NULLIF(COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)),0),1) AS utility_per_map
SUM(utility_damage) AS utility_da
ROUND(SUM(utility_damage)/NULLIF(COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)
FROM {MATCHZY_TABLES['players']}
WHERE steamid64 != '0' AND steamid64 IS NOT NULL
AND name != '' AND name IS NOT NULL
GROUP BY steamid64
HAVING matches >= 1
ORDER BY clutch_total DESC
""")
        rows = c.fetchall()
        c.close(); conn.close()
        return _json_response(rows)
    except Exception as e:
        return _json_response({"error": str(e)})
STEAMID64_BASE = 76561197960265728
def to_steamid64(raw: str) -> str:
    try:
        val = int(raw)
        if val < 0x100000000:
            val += STEAMID64_BASE
        return str(val)
    except (ValueError, TypeError):
        return raw
async def handle_api_steam(request):
    """GET /api/steam/{steamid64} — Steam profile via Steam Web API"""
    steamid = request.match_info.get('steamid64', '')
    if not steamid or not STEAM_API_KEY:
        return _json_response({"error": "Steam API not configured"})
    try:
        steamid64 = to_steamid64(steamid)
        loop = asyncio.get_running_loop()
        def fetch():
            url = (
                f"https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
                f"?key={STEAM_API_KEY}&steamids={steamid64}"
            )
            r = requests.get(url, timeout=8)
            r.raise_for_status()
            players = r.json().get("response", {}).get("players", [])
            if not players:
                return {}
            p = players[0]
            return {
                "steamid": p.get("steamid"),
                "name": p.get("personaname"),
                "avatar": p.get("avatarfull"),
                "profile_url": p.get("profileurl"),
                "country": p.get("loccountrycode", ""),
                "real_name": p.get("realname", ""),
            }
        data = await loop.run_in_executor(None, fetch)
        return _json_response(data)
    except Exception as e:
        return _json_response({"error": str(e)})
async def handle_api_mapstats(request):
    """GET /api/mapstats — win rates and avg scores per map"""
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
SELECT
mp.mapname,
COUNT(*) AS total_matches,
ROUND(AVG(mp.team1_score + mp.team2_score), 1) AS avg_rounds,
ROUND(AVG(mp.team1_score), 1) AS avg_t1_score,
ROUND(AVG(mp.team2_score), 1) AS avg_t2_score,
MAX(mp.team1_score + mp.team2_score) AS max_rounds,
SUM(CASE WHEN mp.team1_score > mp.team2_score THEN 1 ELSE 0 END) AS t1_wins,
SUM(CASE WHEN mp.team2_score > mp.team1_score THEN 1 ELSE 0 END) AS t2_wins
FROM {MATCHZY_TABLES['maps']} mp
WHERE mp.mapname IS NOT NULL AND mp.mapname != ''
GROUP BY mp.mapname
ORDER BY total_matches DESC
""")
        rows = c.fetchall()
        c.close(); conn.close()
        return _json_response(rows)
    except Exception as e:
        return _json_response({"error": str(e)})
async def handle_api_h2h(request):
    """GET /api/h2h?p1=name&p2=name — head to head career stats"""
    p1 = request.rel_url.query.get('p1', '')
    p2 = request.rel_url.query.get('p2', '')
    if not p1 or not p2:
        return _json_response({"error": "Need p1 and p2 query params"})
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        def fetch_player(name):
            c.execute(f"""
SELECT name, steamid64,
COUNT(DISTINCT matchid) AS matches,
SUM(kills) AS kills,
SUM(deaths) AS deaths,
SUM(assists) AS assists,
SUM(head_shot_kills) AS headshots,
SUM(damage) AS damage,
SUM(v1_wins) AS clutch_wins,
SUM(entry_wins) AS entry_wins,
SUM(v1_wins) AS clutch_win
SUM(entry_wins) AS entry_wins
ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2) AS kd,
ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1) AS hs_pct,
ROUND(SUM(damage)/NULLIF(
COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)),0)/30,1) AS adr
FROM {MATCHZY_TABLES['players']}
WHERE name = %s AND steamid64 != '0'
GROUP BY steamid64, name
LIMIT 1
""", (name,))
            return c.fetchone()
        r1 = fetch_player(p1)
        r2 = fetch_player(p2)
        c.close(); conn.close()
        return _json_response({"p1": r1, "p2": r2})
    except Exception as e:
        return _json_response({"error": str(e)})
async def handle_api_status(request):
    """GET /api/status — live CS2 server status via a2s"""
    try:
        loop = asyncio.get_running_loop()
        addr = (SERVER_IP, SERVER_PORT)
        info = await loop.run_in_executor(None, a2s.info, addr)
        try:
            a2s_players = await asyncio.wait_for(
            loop.run_in_executor(None, a2s.players, addr), 5
            )
        except Exception:
            a2s_players = []
        player_list = []
        if a2s_players and any(getattr(p, "name", "") for p in a2s_players):
            for p in a2s_players:
                name = getattr(p, "name", "") or ""
                if name:
                    player_list.append({
                        "name": name,
                        "score": getattr(p, "score", 0),
                        "duration": round(getattr(p, "duration", 0)),
                    })
        else:
            rcon_players = rcon_list_players()
            for p in rcon_players:
                player_list.append({"name": p.get("name", ""), "score": 0, "duration": 0})
        return _json_response({
            "online": True,
            "server_name": info.server_name,
            "map": info.map_name,
            "players": info.player_count,
            "max_players": info.max_players,
            "connect": f"{SERVER_IP}:{SERVER_PORT}",
            "player_list": player_list,
        })
    except Exception:
        return _json_response({
            "online": False,
            "server_name": "",
            "map": "",
            "players": 0,
            "max_players": 10,
            "connect": f"{SERVER_IP}:{SERVER_PORT}",
            "player_list": [],
        })
async def handle_stats_page(request):
    """GET /stats — serve the standalone stats.html SPA"""
    return web.FileResponse(HTML_PATH)
async def start_http_server():
    app = web.Application()
    app.router.add_get('/api/specialists', handle_api_specialists)
    app.router.add_get('/api/player/{name}', handle_api_player)
    app.router.add_get('/api/steam/{steamid64}', handle_api_steam)
    app.router.add_get('/api/matches', handle_api_matches)
    app.router.add_get('/api/match/{matchid}', handle_api_match)
    app.router.add_get('/api/demos', handle_api_demos)
    app.router.add_get('/api/leaderboard', handle_api_leaderboard)
    app.router.add_get('/api/mapstats', handle_api_mapstats)
    app.router.add_get('/api/h2h', handle_api_h2h)
    app.router.add_get('/api/status', handle_api_status)
    app.router.add_get('/stats', handle_stats_page)
    app.router.add_get('/', handle_stats_page)
    app.router.add_get('/health', handle_health_check)
    port = int(os.getenv('PORT', 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"✓ HTTP server started on port {port}")
    return runner
# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
TOKEN = os.getenv("TOKEN")
STEAM_API_KEY = os.getenv("STEAM_API_KEY", "")
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
intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
bot = commands.Bot(command_prefix="!", intents=intents, owner_id=OWNER_ID)
# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────
import mysql.connector
from mysql.connector import pooling
def _mysql_cfg() -> dict:
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
    return mysql.connector.connect(**_DB_CFG)
def init_database():
    conn = get_db()
    c = conn.cursor()
    conn.commit()
    c.close()
    conn.close()
    print("✓ Database initialized (Railway MySQL)")
    try:
        init_database()
    except Exception as e:
        print(f" Database init error: {e}")
# ─────────────────────────────────────────────────────────────────────────────
# MATCHZY INTEGRATION
# ─────────────────────────────────────────────────────────────────────────────
MATCHZY_TABLES = {
"matches": "matchzy_stats_matches",
"maps": "matchzy_stats_maps",
"players": "matchzy_stats_players",
}
def matchzy_tables_exist(conn) -> bool:
    c = conn.cursor()
    c.execute("SHOW TABLES LIKE 'matchzy_stats_players'")
    result = c.fetchone()
    c.close()
    return result is not None
def get_matchzy_player_stats(steamid64: str = None, player_name: str = None) -> dict | None:
    conn = get_db()
    try:
        if not matchzy_tables_exist(conn):
            return None
        c = conn.cursor(dictionary=True)
        table = MATCHZY_TABLES["players"]
        if steamid64:
            where, param = "steamid64 = %s", steamid64
        elif player_name:
            where, param = "name = %s", player_name
        else:
            return None
        c.execute(f'''
SELECT name, steamid64,
COUNT(DISTINCT matchid) AS matches_played,
SUM(kills) AS kills,
SUM(deaths) AS deaths,
SUM(assists) AS assists,
SUM(head_shot_kills) AS headshots,
SUM(damage) AS total_damage,
SUM(enemies5k) AS aces,
SUM(v1_wins) AS clutch_wins,
SUM(entry_wins) AS entry_wins,
ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2) AS kd_ratio,
ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1) AS hs_pct
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
    """Return top players by kills from MatchZy stats."""
    conn = get_db()
    try:
        if not matchzy_tables_exist(conn):
            return []
        c = conn.cursor(dictionary=True)
        c.execute(f'''
SELECT
    SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS player_name,
    steamid64,
    COUNT(DISTINCT matchid) AS matches_played,
SUM(deaths) AS deaths,
SUM(assists) AS assists,
SUM(head_shot_kills) AS headshots,
SUM(damage) AS total_damage,
SUM(enemies5k) AS aces,
SUM(v1_wins) AS clutch_wins,
ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2) AS kd_ratio,
ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1) AS hs_pct
FROM {MATCHZY_TABLES["players"]}
WHERE steamid64 != "0" AND steamid64 IS NOT NULL
AND name != "" AND name IS NOT NULL
GROUP BY steamid64
ORDER BY kills DESC
LIMIT %s
''', (limit,))
        rows = c.fetchall()
        c.close()
        return rows
    except Exception as e:
        print(f"[MatchZy] Leaderboard error: {e}")
        return []
    finally:
        conn.close()
def get_matchzy_match_mvp(matchid: str, mapnumber: int = None) -> dict | None:
    """Return the top-kill player for a given match."""
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
# ─────────────────────────────────────────────────────────────────────────────
# DEMO HELPERS
# ─────────────────────────────────────────────────────────────────────────────
_MATCHID_DEMO_CACHE = None
_MATCHID_CACHE_TIME = None
_CACHE_TTL_SECONDS = 300 # 5 minutes
def build_matchid_to_demo_map(force_refresh=False):
    """
Build mapping: matchid -> {metadata, name, download_url, size_formatted}
Indexes all .json files from fshost; attaches .dem info if found.
Cached for 5 minutes.
"""
    global _MATCHID_DEMO_CACHE, _MATCHID_CACHE_TIME
    if not force_refresh and _MATCHID_DEMO_CACHE is not None and _MATCHID_CACHE_TIME is not None:
        if (datetime.now() - _MATCHID_CACHE_TIME).total_seconds() < _CACHE_TTL_SECONDS:
            return _MATCHID_DEMO_CACHE
    print("[Demo Map] Building matchid map from all fshost .json files...")
    all_files = fetch_all_demos_raw()
    dem_by_base = {}
    for f in all_files:
        n = f.get("name", "")
        if n.endswith(".dem"):
            dem_by_base[n[:-4]] = f
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
                matchid = str(metadata.get("match_id") or metadata.get("matchid") or metadata.get("id", ""))
                if not matchid:
                    print(f"[Demo Map] ✗ {name}: no match_id field")
                    continue
                base = name[:-5]
                if base.endswith("_stats"):
                    base = base[:-6]
                dem_entry = dem_by_base.get(base, {})
                matchid_map[matchid] = {
                    "metadata": metadata,
                    "name": dem_entry.get("name", ""),
                    "download_url": dem_entry.get("download_url", ""),
                    "size_formatted": dem_entry.get("size_formatted", ""),
                    "modified_at": dem_entry.get("modified_at", ""),
                }
                print(f"[Demo Map] ✓ match {matchid} ← {name}" + (f" + {dem_entry.get('name')}" if dem_entry else ""))
    print(f"[Demo Map] Total: {len(matchid_map)} matches indexed")
    _MATCHID_DEMO_CACHE = matchid_map
    _MATCHID_CACHE_TIME = datetime.now()
    return matchid_map
def fetch_all_demos_raw():
    """Return raw list of demo dicts from fshost, sorted newest first."""
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
# ─────────────────────────────────────────────────────────────────────────────
# RCON / SERVER HELPERS
# ─────────────────────────────────────────────────────────────────────────────
STATUS_NAME_RE = re.compile(r'^#\s*\d+\s+"(?P<n>.*?)"\s+')
CSS_LIST_RE = re.compile(r'^\s*•\s*\[#\d+\]\s*"(?P<n>[^"]*)"')
def sanitize(s: str) -> str:
    if not s:
        return "-"
    for ch in ['*', '_', '`', '~', '|', '>', '@']:
        s = s.replace(ch, f"\\{ch}")
        return s.replace("\x00", "").strip()
def send_rcon(command: str) -> str:
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            resp = rcon.command(command)
            if not resp or resp.strip() == "":
                return " Command executed successfully"
            if any(i in resp.lower() for i in ["success", "completed", "done"]):
                return f" {resp[:1000]}"
        response_text = resp[:2000] if len(resp) > 2000 else resp
        if any(e in resp.lower() for e in ["error", "failed", "invalid", "unknown"]):
            return f" {response_text}"
        return response_text
    except Exception as e:
        return f" RCON Connection Error: {e}"
def send_rcon_silent(command: str):
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            rcon.command(command)
    except:
        pass
def rcon_list_players():
    txt = send_rcon("css_players")
    if "Unknown command" in txt or "Error" in txt:
        txt = send_rcon("status")
    players = []
    for line in txt.splitlines():
        line = line.strip()
        css = CSS_LIST_RE.match(line)
        if css:
            continue
        m = STATUS_NAME_RE.match(line)
        if m:
            players.append({"name": sanitize(m.group("name")), "ping": "-", "time": "-"})
            name = sanitize(m.group("name"))
            time_match = re.search(r'\b(\d{1,2}:\d{2})\b', line)
            ping_match = re.search(r'(\d+)\s*$', line.split('"')[-1])
            players.append({
                "name": name,
                "time": time_match.group(1) if time_match else "-",
                "ping": ping_match.group(1) if ping_match else "-",
            })
    uniq, seen = [], set()
    for p in players:
        if p["name"] not in seen:
            uniq.append(p); seen.add(p["name"])
    return uniq
def flag(cc):
    if not cc or len(cc) != 2:
        return " "
    return "".join(chr(ord(c.upper()) + 127397) for c in cc)
async def fetch_faceit_stats_cs2(nickname: str) -> dict:
    if not FACEIT_API_KEY:
        raise ValueError("FACEIT_API_KEY missing")
    headers = {"Authorization": f"Bearer {FACEIT_API_KEY}"}
    r = requests.get(f"https://open.faceit.com/data/v4/players?nickname={nickname}", headers=headers)
    if r.status_code == 404:
        raise ValueError("Player not found on FACEIT.")
    r.raise_for_status()
    pdata = r.json()
    pid = pdata.get("player_id")
    s = requests.get(f"https://open.faceit.com/data/v4/players/{pid}/stats/{FACEIT_GAME_ID}", headers=headers)
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
            color = 0x95A5A6 if player_count == 0 else (
            0xE74C3C if player_count < info.max_players / 3 else
            0xF39C12 if player_count < info.max_players * 2 / 3 else
            0x2ECC71
            )
            embed = discord.Embed(
            title=" CS2 Server Status",
            description=f"**{info.server_name}**",
            color=color,
            timestamp=datetime.now()
            )
            embed.add_field(name=" Current Map", value=f"`{info.map_name}`", inline=True)
        embed.add_field(name=" Players", value=f"`{player_count}/{info.max_players}`", inline=True)
        embed.add_field(name=" Connect", value=f"`connect {SERVER_IP}:{SERVER_PORT}`", inline=True)
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
            name=f" Players Online ({player_count})",
            value=listing if len(listing) < 1024 else listing[:1020] + "...",
            inline=False
        )
        embed.set_footer(
            text="Last updated",
            icon_url="https://cdn.cloudflare.steamstatic.com/steamcommunity/public/images/apps/730/69891d4a612de5b26eb5ac5a4c58d3de74d78ad1/capsule_sm_120.jpg"
        )
    except:
        pass  # fall through to offline embed
    else:
        return embed, info
    embed = discord.Embed(
        title=" Server Offline",
        description="The server appears to be offline or unreachable.",
        color=0xFF0000,
        timestamp=datetime.now()
    )
# ─────────────────────────────────────────────────────────────────────────────
# DEMOS PAGINATION VIEW
# ─────────────────────────────────────────────────────────────────────────────
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
        refresh_btn = Button(label=" Refresh", style=discord.ButtonStyle.success, custom_id="refresh")
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
            title=" Server Demos",
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
        page_demos = demos_sorted[offset:offset + limit]
        has_more = (offset + limit) < len(demos_sorted)
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
                f" [{name}](<{url}>)\n {date_display} • {size}"
            )
        return {
            "demos": formatted_demos,
            "has_more": has_more,
            "total": len(demos_sorted),
            "showing": f"{offset + 1}-{min(offset + limit, len(demos_sorted))}"
        }
    except Exception as e:
        return {"demos": [f"Error: {str(e)}"], "has_more": False}
# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND TASKS
# ─────────────────────────────────────────────────────────────────────────────
@tasks.loop(minutes=1)
async def update_server_stats():
    try:
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
        # Player list is available here if you need it for future use
    except Exception as e:
        print(f"Error in update_server_stats: {e}")
# ─────────────────────────────────────────────────────────────────────────────
# BOT EVENTS
# ─────────────────────────────────────────────────────────────────────────────
@bot.event
async def on_ready():
    print(f"Bot online as {bot.user.name} (ID: {bot.user.id})")
    try:
        await start_http_server()
    except Exception as e:
        print(f" Failed to start HTTP server: {e}")
    try:
        send_rcon_silent("mp_logdetail 3")
        send_rcon_silent("log on")
        print("✓ Server kill logging enabled")
    except Exception as e:
        print(f" Could not enable kill logging: {e}")
    print("Syncing slash commands...")
    try:
        synced = await bot.tree.sync()
        print(f"✓ Synced {len(synced)} commands globally")
    except Exception as e:
        print(f"✗ Failed to sync commands: {e}")
    try:
        conn = get_db()
        has_mz = matchzy_tables_exist(conn)
        conn.close()
        print(f"✓ MatchZy tables {'found' if has_mz else 'NOT found'}")
    except Exception as e:
        print(f" Could not check MatchZy tables: {e}")
    update_server_stats.start()
@bot.event
async def on_message(message):
    if message.author == bot.user:
        return
    await bot.process_commands(message)
# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def owner_only():
    async def predicate(interaction: discord.Interaction):
        return interaction.user.id == OWNER_ID
    return app_commands.check(predicate)
# ─────────────────────────────────────────────────────────────────────────────
# PREFIX COMMANDS
# ─────────────────────────────────────────────────────────────────────────────
@bot.command()
@commands.guild_only()
@commands.is_owner()
async def sync(ctx, guilds: commands.Greedy[discord.Object] = None,
        spec: Optional[Literal["~", "*", "^"]] = None):
    await ctx.send(" Syncing commands...", delete_after=5)
    if not guilds:
        if spec == "~":
            synced = await bot.tree.sync(guild=ctx.guild)
        elif spec == "*":
            bot.tree.copy_global_to(guild=ctx.guild)
            synced = await bot.tree.sync(guild=ctx.guild)
        elif spec == "^":
            bot.tree.clear_commands(guild=ctx.guild)
            await bot.tree.sync(guild=ctx.guild)
            await ctx.send(" Cleared all commands from this server.")
            return
        else:
            synced = await bot.tree.sync()
        await ctx.send(f" Synced {len(synced)} commands.")
        return
    count = sum(1 for guild in guilds if (await bot.tree.sync(guild=guild)) is not None)
    await ctx.send(f" Synced to {count}/{len(guilds)} guilds.")
@bot.command()
async def ping(ctx):
    await ctx.send(f" Pong! Latency: {round(bot.latency * 1000)}ms")
# ─────────────────────────────────────────────────────────────────────────────
# SLASH COMMANDS
# ─────────────────────────────────────────────────────────────────────────────
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
    f" No MatchZy stats found for **{player_name}**\n"
    f"Player must have completed at least one match.",
    ephemeral=True
    )
    kills = int(mz.get("kills") or 0)
    deaths = int(mz.get("deaths") or 0)
    assists = int(mz.get("assists") or 0)
    hs = int(mz.get("headshots") or 0)
    total_damage = int(mz.get("total_damage") or 0)
    hs_pct = float(mz.get("hs_pct") or 0)
    aces = int(mz.get("aces") or 0)
    clutch_wins = int(mz.get("clutch_wins") or 0)
    entry_wins = int(mz.get("entry_wins") or 0)
    matches = int(mz.get("matches_played") or 0)
    kd_ratio = kills / deaths if deaths > 0 else float(kills)
    embed = discord.Embed(
        title=f" {mz.get('name', player_name)}",
        description=f" MatchZy Career Stats • {matches} match{'es' if matches != 1 else ''}",
        color=0x2ECC71
    )
    embed.add_field(name=" Kills", value=f"**{kills}**", inline=True)
    embed.add_field(name=" Deaths", value=f"**{deaths}**", inline=True)
    embed.add_field(name=" K/D", value=f"**{kd_ratio:.2f}**", inline=True)
    embed.add_field(name=" Assists", value=f"**{assists}**", inline=True)
    embed.add_field(name=" Headshots", value=f"**{hs}** ({hs_pct:.1f}%)", inline=True)
    embed.add_field(name=" Total Damage", value=f"**{total_damage:,}**", inline=True)
    if aces: embed.add_field(name=" Aces (5K)", value=f"**{aces}**", inline=True)
    if clutch_wins: embed.add_field(name=" 1vX Wins", value=f"**{clutch_wins}**", inline=True)
    if entry_wins: embed.add_field(name=" Entry Wins", value=f"**{entry_wins}**", inline=True)
    embed.set_footer(text=f"SteamID64: {mz.get('steamid64', 'N/A')}")
    await inter.followup.send(embed=embed, ephemeral=True)
@bot.tree.command(name="leaderboard", description="Top players by kills")
async def leaderboard_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    leaderboard = get_matchzy_leaderboard(10)
    if not leaderboard:
        return await inter.followup.send(" No player data available yet.", ephemeral=True)
    embed = discord.Embed(
    title=" Top Players",
    description="*Sorted by kills*",
    color=0xF1C40F
    )
    medals = [" ", " ", " "]
    for i, row in enumerate(leaderboard, 1):
        name = row.get("player_name", "Unknown")
        kills = int(row.get("kills") or 0)
        deaths = int(row.get("deaths") or 0)
        kd = row.get("kd_ratio")
        hs_pct = row.get("hs_pct")
        matches = row.get("matches_played")
        medal = medals[i-1] if i <= 3 else f"`{i}.`"
        kd_str = f"K/D: {kd:.2f}" if kd else f"K/D: {kills}/{deaths}"
        extras = []
        if hs_pct: extras.append(f"HS: {hs_pct}%")
        if matches: extras.append(f"{matches} matches")
        value = f"**{kills} kills** • {kd_str}" + (f" • {' • '.join(extras)}" if extras else "")
        embed.add_field(name=f"{medal} {name}", value=value, inline=False)
    await inter.followup.send(embed=embed, ephemeral=True)
@bot.tree.command(name="demos", description="View server demos")
async def demos_cmd(inter: discord.Interaction):
    if SERVER_DEMOS_CHANNEL_ID and inter.channel_id != SERVER_DEMOS_CHANNEL_ID:
        return await inter.response.send_message("Wrong channel!", ephemeral=True)
    await inter.response.defer(ephemeral=True)
    result = fetch_demos(0, 5)
    embed = discord.Embed(
    title=" Server Demos",
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
        return await inter.followup.send(f" {e}", ephemeral=True)
    embed = discord.Embed(
    title=f"{stats['country_flag']} {stats['nickname']} — FACEIT CS2", color=0xFF8800
    )
    embed.set_thumbnail(url=stats["avatar"])
    embed.add_field(name="Level", value=stats["level"], inline=True)
    embed.add_field(name="ELO", value=stats["elo"], inline=True)
    embed.add_field(name="Win Rate", value=f"{stats['win_rate']}%",inline=True)
    embed.add_field(name="Matches", value=stats["matches"], inline=True)
    embed.add_field(name="K/D", value=stats["kd_ratio"], inline=True)
    embed.set_footer(text=f"Player ID: {stats['player_id']}")
    await inter.followup.send(embed=embed, ephemeral=True)
# ─────────────────────────────────────────────────────────────────────────────
# ADMIN COMMANDS
# ─────────────────────────────────────────────────────────────────────────────
@bot.tree.command(name="csssay", description="Send center-screen message to all players")
@owner_only()
async def csssay(inter: discord.Interaction, message: str):
    await inter.followup.send(f" **Message Sent**\n```{message}```\n{resp}", ephemeral=True)
    resp = send_rcon(f"css_cssay {message}")
    await inter.followup.send(f" **Message Sent**\n```{message}```\n{resp}", ephemeral=True)
@bot.tree.command(name="csshsay", description="Send hint message to all players")
@owner_only()
async def csshsay(inter: discord.Interaction, message: str):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f"css_hsay {message}")
    await inter.followup.send(f" **Hint Sent**\n```{message}```\n{resp}", ephemeral=True)
@bot.tree.command(name="csskick", description="Kick a player from the server")
@owner_only()
async def csskick(inter: discord.Interaction, player: str):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f'css_kick "{player}"')
    await inter.followup.send(f" **Kick Command**\nPlayer: `{player}`\n\n{resp}", ephemeral=True)
@bot.tree.command(name="cssban", description="Ban a player from the server")
@owner_only()
async def cssban(inter: discord.Interaction, player: str, minutes: int, reason: str = "No reason"):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f'css_ban "{player}" {minutes} "{reason}"')
    await inter.followup.send(
        f" **Ban**\nPlayer: `{player}` • Duration: `{minutes}m` • Reason: `{reason}`\n\n{resp}",
        ephemeral=True
    )
@bot.tree.command(name="csschangemap", description="Change the server map")
@owner_only()
async def csschangemap(inter: discord.Interaction, map: str):
    if map not in MAP_WHITELIST:
        return await inter.response.send_message(
        f" Map `{map}` not allowed.\nAllowed: {', '.join(MAP_WHITELIST)}", ephemeral=True
    )
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f"css_changemap {map}")
    await inter.followup.send(f" Changing to `{map}`\n\n{resp}", ephemeral=True)
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
        has_mz = matchzy_tables_exist(conn)
        lines.append(f"**MatchZy tables:** {' Found' if has_mz else ' Not found'}")
        if has_mz:
            c.execute(f"SELECT COUNT(*) FROM {MATCHZY_TABLES['players']}")
            lines.append(f"**MatchZy player rows:** {c.fetchone()[0]}")
            c.execute(f"SELECT COUNT(DISTINCT matchid) FROM {MATCHZY_TABLES['players']}")
            lines.append(f"**MatchZy matches:** {c.fetchone()[0]}")
            c.close(); conn.close()
    except Exception as e:
        lines.append(f" DB Error: {e}")
    await inter.followup.send("\n".join(lines), ephemeral=True)
@bot.tree.command(name="debugmatch", description="Debug a specific match data")
@owner_only()
async def debugmatch_cmd(inter: discord.Interaction, match_id: str):
    await inter.response.defer(ephemeral=True)
    lines = [f"**Debugging Match #{match_id}**\n"]
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"SELECT * FROM {MATCHZY_TABLES['matches']} WHERE matchid=%s", (match_id,))
        match = c.fetchone()
        if match:
            lines.append(f" Match found")
            lines.append(f"Teams: {match.get('team1_name')} vs {match.get('team2_name')}")
            lines.append(f"Score: {match.get('team1_score')} : {match.get('team2_score')}")
        else:
            lines.append(f" Match not found")
        c.close(); conn.close()
        return await inter.followup.send("\n".join(lines), ephemeral=True)
        c.execute(f"SELECT name, team, kills, deaths, mapnumber FROM {MATCHZY_TABLES['players']} WHERE matchid=%s ORDER BY kills DESC", (match_id,))
        players = c.fetchall()
        lines.append(f"\n**Players ({len(players)} total):**")
        for p in players:
            lines.append(f"• {p['name']} | Team: `{p['team']}` | Map: {p['mapnumber']} | K/D: {p['kills']}/{p['deaths']}")
        c.execute(f"SELECT * FROM {MATCHZY_TABLES['maps']} WHERE matchid=%s", (match_id,))
        maps = c.fetchall()
        lines.append(f"\n**Maps ({len(maps)} total):")
        for m in maps:
            lines.append(f"• Map {m.get('mapnumber')}: {m.get('mapname')} ({m.get('team1_score')}-{m.get('team2_score')})")
        c.close(); conn.close()
    except Exception as e:
        lines.append(f"\n Error: {e}")
    message = "\n".join(lines)
    if len(message) > 2000:
        for chunk in [message[i:i+2000] for i in range(0, len(message), 2000)]:
            await inter.followup.send(chunk, ephemeral=True)
    else:
        await inter.followup.send(message, ephemeral=True)
@bot.tree.command(name="debugdemos", description="Show match ID to demo mapping from .json files")
@owner_only()
async def debugdemos_cmd(inter: discord.Interaction, refresh: bool = False):
    await inter.response.defer(ephemeral=True)
    lines = ["**Match ID → Demo Mapping** (from .json files)\n"]
    try:
        if refresh:
            lines.append(" Refreshing cache...\n")
        matchid_map = build_matchid_to_demo_map(force_refresh=refresh)
        if not matchid_map:
            lines.append(" No demos with .json metadata found")
        else:
            lines.append(f" Found {len(matchid_map)} matches with demos:\n")
            sorted_matches = sorted(matchid_map.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0, reverse=True)
            for matchid, demo in sorted_matches[:15]:
                name = demo.get('name', '?')
                size = demo.get('size_formatted', '?')
                lines.append(f"**Match #{matchid}**\n └─ {name} ({size})")
            if len(matchid_map) > 15:
                lines.append(f"\n...and {len(matchid_map) - 15} more")
        if _MATCHID_CACHE_TIME:
            age = (datetime.now() - _MATCHID_CACHE_TIME).total_seconds()
            lines.append(f"\n Cache age: {age:.0f}s (refreshes every {_CACHE_TTL_SECONDS}s)")
    except Exception as e:
        lines.append(f"\n Error: {e}")
    message = "\n".join(lines)
    if len(message) > 2000:
        for chunk in [message[i:i+2000] for i in range(0, len(message), 2000)]:
            await inter.followup.send(chunk, ephemeral=True)
    else:
        await inter.followup.send(message, ephemeral=True)
if not TOKEN:
    raise SystemExit("TOKEN missing.")
bot.run(TOKEN)
