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



# Regex to extract every "name<slot><steamid><team>" actor token in a log line
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
    """
    GET /api/matches
    Primary source: fshost JSONs (all matches, correct round scores, correct dates).
    Fallback: DB rows for any matchid not found in fshost JSONs.
    """
    try:
        limit = int(request.rel_url.query.get('limit', 50))
        loop  = asyncio.get_running_loop()

        # ── Build match list from fshost JSONs ────────────────────────────────
        matchid_map = await loop.run_in_executor(None, build_matchid_to_demo_map)

        results = []
        for mid, entry in matchid_map.items():
            meta = entry.get('metadata') or {}
            if not meta:
                continue
            t1 = meta.get('team1', {})
            t2 = meta.get('team2', {})

            # Parse date from JSON — format: "2026-02-21T21:46:19.4524841Z"
            raw_date = meta.get('date', '')
            results.append({
                'matchid':     str(meta.get('match_id') or mid),
                'team1_name':  t1.get('name', 'Team 1'),
                'team2_name':  t2.get('name', 'Team 2'),
                'team1_score': t1.get('score', 0),
                'team2_score': t2.get('score', 0),
                'winner':      meta.get('winner', ''),
                'end_time':    raw_date,
                'mapname':     meta.get('map', '?'),
                'total_rounds':meta.get('total_rounds'),
                'demo_url':    entry.get('download_url', ''),
                'demo_size':   entry.get('size_formatted', ''),
            })

        # ── Merge any DB-only matches not in fshost ───────────────────────────
        fshost_ids = {r['matchid'] for r in results}
        try:
            conn = get_db()
            c = conn.cursor(dictionary=True)
            c.execute(f"""
                SELECT mm.matchid, mm.team1_name, mm.team2_name, mm.winner,
                       mm.end_time, m.mapname,
                       m.team1_score, m.team2_score
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
                        'matchid':     mid,
                        'team1_name':  row.get('team1_name', 'Team 1'),
                        'team2_name':  row.get('team2_name', 'Team 2'),
                        'team1_score': row.get('team1_score', 0),
                        'team2_score': row.get('team2_score', 0),
                        'winner':      row.get('winner', ''),
                        'end_time':    str(row.get('end_time', '')),
                        'mapname':     row.get('mapname', '?'),
                        'demo_url':    '',
                        'demo_size':   '',
                    })
            c.close(); conn.close()
        except Exception as e:
            print(f"[api/matches] DB fallback error: {e}")

        # Sort newest first — ISO date strings sort correctly lexicographically
        def sort_key(r):
            d = r.get('end_time') or ''
            return str(d)
        results.sort(key=sort_key, reverse=True)
        return _json_response(results[:limit])
    except Exception as e:
        return _json_response({"error": str(e)})


async def handle_api_match(request):
    """GET /api/match/{matchid} - pure fshost JSON source."""
    matchid = request.match_info.get('matchid', '')
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, _fetch_fshost_match_json, matchid)
        if not data:
            return _json_response({"error": f"No fshost data found for match {matchid}"})
        t1 = data.get('team1', {})
        t2 = data.get('team2', {})
        meta = {
            'matchid':     data.get('match_id') or matchid,
            'team1_name':  t1.get('name', 'Team 1'),
            'team2_name':  t2.get('name', 'Team 2'),
            'team1_score': t1.get('score', 0),
            'team2_score': t2.get('score', 0),
            'winner':      data.get('winner', ''),
            'end_time':    data.get('date'),
            'start_time':  data.get('date'),
        }
        maps = [{
            'matchid':      matchid,
            'mapnumber':    1,
            'mapname':      data.get('map', 'unknown'),
            'team1_score':  t1.get('score', 0),
            'team2_score':  t2.get('score', 0),
            'winner':       data.get('winner', ''),
            'total_rounds': data.get('total_rounds'),
        }]
        players = _players_from_fshost(data, matchid)

        # Build demo info directly from the matchid cache
        matchid_map = build_matchid_to_demo_map()
        entry = matchid_map.get(str(matchid), {})
        demo = {
            'name': entry.get('name', ''),
            'url':  entry.get('download_url', ''),
            'size': entry.get('size_formatted', ''),
        }
        return _json_response({"meta": meta, "maps": maps, "players": players, "demo": demo})
    except Exception as e:
        return _json_response({"error": str(e)})


def _players_from_fshost(data: dict, matchid: str) -> list:
    """Flatten fshost team1/team2 players into a unified list with all available fields."""
    players = []
    for team_key in ('team1', 'team2'):
        team_data = data.get(team_key, {})
        team_name = team_data.get('name', team_key)
        for fp in team_data.get('players', []):
            kills    = int(fp.get('kills', 0) or 0)
            deaths   = int(fp.get('deaths', 0) or 0)
            hs_kills = int(fp.get('headshot_kills', 0) or 0)
            hs_pct   = fp.get('hs_percent')
            if hs_pct is None:
                hs_pct = round(hs_kills / kills * 100, 1) if kills else 0
            def cw(s):
                try: return int(str(s).split('/')[0])
                except: return 0
            players.append({
                'matchid':        matchid,
                'mapnumber':      1,
                'steamid64':      str(fp.get('steam_id', '0')),
                'name':           fp.get('name', '?'),
                'team':           team_key,
                'team_name':      team_name,
                'kills':          kills,
                'deaths':         deaths,
                'assists':        int(fp.get('assists', 0) or 0),
                'damage':         int(fp.get('damage', 0) or 0),
                'head_shot_kills':hs_kills,
                'hs_pct':         hs_pct,
                'adr':            fp.get('adr'),
                # Multi-kills
                'enemies5k':      int(fp.get('5k', 0) or 0),
                'enemies4k':      int(fp.get('4k', 0) or 0),
                'enemies3k':      int(fp.get('3k', 0) or 0),
                # Clutches
                'v1_wins':        cw(fp.get('1v1', 0)),
                'v2_wins':        cw(fp.get('1v2', 0)),
                'clutch_1v1':     fp.get('1v1', '0/0'),
                'clutch_1v2':     fp.get('1v2', '0/0'),
                # Rating / impact
                'rating':         fp.get('rating'),
                'kast':           fp.get('kast'),
                'multi_kills':    fp.get('multi_kills'),
                # Opening duels
                'opening_kills':  fp.get('opening_kills'),
                'opening_deaths': fp.get('opening_deaths'),
                # Utility
                'trade_kills':    fp.get('trade_kills'),
                'flash_assists':  fp.get('flash_assists'),
                'utility_damage': fp.get('utility_damage'),
            })
    return players


def _fetch_fshost_match_json(matchid: str) -> dict | None:
    """Return the fshost JSON for a given match ID, using the cache built by build_matchid_to_demo_map."""
    try:
        matchid_map = build_matchid_to_demo_map()
        entry = matchid_map.get(str(matchid))
        if not entry:
            return None
        # metadata is always stored now
        return entry.get('metadata')
    except Exception as e:
        print(f"[fshost JSON] Error fetching match {matchid}: {e}")
        return None


def _parse_demo_filename(name: str) -> dict:
    """
    Parse a demo filename like:
      2026-02-20_15-58-15_-1_de_dust2_team_Miksen_vs_TERRORISTS.dem
    Returns dict with: filename_ts, mapname, team1_name, team2_name
    """
    import re as _re
    result = {}
    stem = name.replace('.dem', '')
    # Extract date + time: YYYY-MM-DD_HH-MM-SS
    ts_m = _re.match(r'^(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})', stem)
    if ts_m:
        try:
            dt_str = f"{ts_m.group(1)} {ts_m.group(2).replace('-', ':')}"
            result['filename_ts'] = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").isoformat()
        except ValueError:
            pass
        # Everything after date_time_<something>_
        rest = _re.sub(r'^\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_[^_]+_', '', stem)
        # Find map: look for de_ or cs_ or gg_ pattern
        map_m = _re.match(r'((?:de|cs|gg|ar|dm)_\w+?)_(.+)', rest)
        if map_m:
            result['mapname'] = map_m.group(1)
            teams_part = map_m.group(2)
            # Split on _vs_ (case-insensitive)
            vs_split = _re.split(r'_vs_', teams_part, flags=_re.IGNORECASE)
            if len(vs_split) == 2:
                result['team1_name'] = vs_split[0].replace('_', ' ').strip()
                result['team2_name'] = vs_split[1].replace('_', ' ').strip()
    return result

async def handle_api_demos(request):
    """GET /api/demos — returns all demos from fshost with parsed timestamps and match metadata"""
    demos = fetch_all_demos_raw()
    matchid_map = build_matchid_to_demo_map()
    result = []
    for d in demos:
        name = d.get("name", "")
        if not name.endswith(".dem"):
            continue
        # Start with filename-parsed data as base (works for all demos)
        parsed = _parse_demo_filename(name)
        ts = parsed.get('filename_ts')
        meta = {
            'matchid':     '',
            'mapname':     parsed.get('mapname', ''),
            'team1_name':  parsed.get('team1_name', ''),
            'team2_name':  parsed.get('team2_name', ''),
            'team1_score': '',
            'team2_score': '',
        }
        # Try to enrich with JSON match metadata (scores, exact names)
        for mid, entry in matchid_map.items():
            entry_name = entry.get('name', '')
            if entry_name == name or entry_name.replace('.json', '') == name.replace('.dem', ''):
                raw_meta = entry.get('metadata') or {}
                meta['matchid']     = str(raw_meta.get('match_id', mid))
                meta['mapname']     = raw_meta.get('map', '') or meta['mapname']
                meta['team1_name']  = (raw_meta.get('team1') or {}).get('name', '') or meta['team1_name']
                meta['team2_name']  = (raw_meta.get('team2') or {}).get('name', '') or meta['team2_name']
                meta['team1_score'] = (raw_meta.get('team1') or {}).get('score', '')
                meta['team2_score'] = (raw_meta.get('team2') or {}).get('score', '')
                break
        result.append({
            "name":           name,
            "download_url":   d.get("download_url", ""),
            "size_formatted": d.get("size_formatted", ""),
            "modified_at":    d.get("modified_at", ""),
            "filename_ts":    ts,
            **meta,
        })
    # Sort newest first
    result.sort(key=lambda x: x.get('filename_ts') or x.get('modified_at') or '', reverse=True)
    return _json_response(result)

async def handle_api_leaderboard(request):
    """GET /api/leaderboard — career stats aggregated from matchzy_stats_players"""
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT
                name,
                steamid64,
                COUNT(DISTINCT matchid)                                      AS matches,
                SUM(kills)                                                   AS kills,
                SUM(deaths)                                                  AS deaths,
                SUM(assists)                                                 AS assists,
                SUM(head_shot_kills)                                         AS headshots,
                SUM(damage)                                                  AS damage,
                SUM(enemies5k)                                               AS aces,
                SUM(enemies4k)                                               AS quads,
                SUM(enemies3k)                                               AS triples,
                SUM(v1_wins)                                                 AS clutch_wins,
                SUM(entry_wins)                                              AS entry_wins,
                ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2)                   AS kd,
                ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1)      AS hs_pct,
                ROUND(SUM(damage)/NULLIF(
                    COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)),0)/30,1)   AS adr
            FROM {MATCHZY_TABLES['players']}
            WHERE steamid64 != '0' AND name != '' AND name IS NOT NULL
            GROUP BY steamid64, name
            ORDER BY kills DESC
        """)
        rows = c.fetchall()
        c.close(); conn.close()
        return _json_response(rows)
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_api_specialists(request):
    """GET /api/specialists — specialist stat boards: clutch, entry, flash, utility"""
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT
                name, steamid64,
                COUNT(DISTINCT matchid)                                         AS matches,
                SUM(v1_wins)                                                    AS clutch_1v1,
                SUM(v2_wins)                                                    AS clutch_1v2,
                SUM(v1_wins) + SUM(v2_wins)                                    AS clutch_total,
                SUM(entry_wins)                                                 AS entry_wins,
                SUM(entry_count)                                                AS entry_attempts,
                ROUND(SUM(entry_wins)/NULLIF(SUM(entry_count),0)*100,1)       AS entry_rate,
                SUM(flash_successes)                                            AS flash_successes,
                ROUND(SUM(flash_successes)/NULLIF(COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)),0),1) AS flashes_per_map,
                SUM(utility_damage)                                             AS utility_damage,
                ROUND(SUM(utility_damage)/NULLIF(COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)),0),1) AS util_dmg_per_map
            FROM {MATCHZY_TABLES['players']}
            WHERE steamid64 != '0' AND name != '' AND name IS NOT NULL
            GROUP BY steamid64, name
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
    """Convert SteamID32 or SteamID64 to SteamID64 string."""
    try:
        val = int(raw)
        # SteamID32 values are < 2^32, SteamID64 values are much larger
        if val < 0x100000000:
            val += STEAMID64_BASE
        return str(val)
    except (ValueError, TypeError):
        return raw

async def handle_api_steam(request):
    """GET /api/steam/{steamid64} — fetch Steam profile info via Steam Web API"""
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
                "steamid":     p.get("steamid"),
                "name":        p.get("personaname"),
                "avatar":      p.get("avatarfull"),
                "profile_url": p.get("profileurl"),
                "country":     p.get("loccountrycode", ""),
                "real_name":   p.get("realname", ""),
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
                COUNT(*)                                            AS total_matches,
                ROUND(AVG(mp.team1_score + mp.team2_score), 1)    AS avg_rounds,
                ROUND(AVG(mp.team1_score), 1)                     AS avg_t1_score,
                ROUND(AVG(mp.team2_score), 1)                     AS avg_t2_score,
                MAX(mp.team1_score + mp.team2_score)              AS max_rounds,
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
    """GET /api/h2h?p1=name&p2=name — head to head career stats for two players"""
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
                    COUNT(DISTINCT matchid)                                     AS matches,
                    SUM(kills)                                                  AS kills,
                    SUM(deaths)                                                 AS deaths,
                    SUM(assists)                                                AS assists,
                    SUM(head_shot_kills)                                        AS headshots,
                    SUM(damage)                                                 AS damage,
                    SUM(enemies5k)                                              AS aces,
                    SUM(enemies4k)                                              AS quads,
                    SUM(v1_wins)                                                AS clutch_wins,
                    SUM(entry_wins)                                             AS entry_wins,
                    ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2)                  AS kd,
                    ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1)     AS hs_pct,
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

        # Build player list
        player_list = []
        if a2s_players and any(getattr(p, "name", "") for p in a2s_players):
            for p in a2s_players:
                name = getattr(p, "name", "") or ""
                if name:
                    player_list.append({
                        "name":  name,
                        "score": getattr(p, "score", 0),
                        "duration": round(getattr(p, "duration", 0)),
                    })
        else:
            # fallback to rcon
            rcon_players = rcon_list_players()
            for p in rcon_players:
                player_list.append({"name": p.get("name",""), "score": 0, "duration": 0})

        return _json_response({
            "online":       True,
            "server_name":  info.server_name,
            "map":          info.map_name,
            "players":      info.player_count,
            "max_players":  info.max_players,
            "connect":      f"{SERVER_IP}:{SERVER_PORT}",
            "player_list":  player_list,
        })
    except Exception:
        return _json_response({
            "online":      False,
            "server_name": "",
            "map":         "",
            "players":     0,
            "max_players": 10,
            "connect":     f"{SERVER_IP}:{SERVER_PORT}",
            "player_list": [],
        })

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
<title>Reshtan Gaming</title>
<link href="https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;500;600;700&family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#080a0c;--surface:#0f1012;--surface2:#141618;--border:#1c1e21;--border2:#252729;
  --orange:#ff5500;--orange2:#ff7733;--orange-glow:rgba(255,85,0,.15);
  --ct:#5bc4f5;--t:#f0a842;--win:#22c55e;--loss:#ef4444;
  --text:#a8b0b8;--white:#dde1e7;--muted:#3a3e44;--muted2:#575e68;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;font-size:13px}
/* NAV */
nav{background:#09090b;border-bottom:2px solid var(--border);display:flex;align-items:center;padding:0 20px;height:50px;position:sticky;top:0;z-index:200;gap:0}
.logo{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:26px;letter-spacing:-2px;color:var(--orange);text-transform:uppercase;margin-right:28px;white-space:nowrap;display:flex;align-items:center;line-height:1}
.tabs{display:flex;gap:0;height:100%;position:relative}
.tab{height:100%;padding:0 16px;display:flex;align-items:center;font-family:'Rajdhani',sans-serif;font-weight:600;font-size:12px;letter-spacing:1.5px;text-transform:uppercase;color:var(--muted2);cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;transition:color .18s,background .18s;position:relative}
.tab:hover{color:var(--text);background:rgba(255,255,255,.03)}
.tab.active{color:var(--orange)}
/* Sliding underline indicator */
.tab-indicator{position:absolute;bottom:-2px;height:2px;background:var(--orange);transition:left .25s cubic-bezier(.4,0,.2,1),width .25s cubic-bezier(.4,0,.2,1);pointer-events:none;box-shadow:0 0 8px rgba(255,85,0,.6);border-radius:1px}
.nav-right{margin-left:auto;display:flex;gap:8px;align-items:center}
/* Hamburger */
.hamburger{display:none;flex-direction:column;justify-content:center;gap:5px;width:36px;height:36px;cursor:pointer;padding:4px;border-radius:3px;margin-left:auto;flex-shrink:0}
.hamburger span{display:block;height:2px;background:var(--orange);border-radius:2px;transition:all .25s}
.hamburger.open span:nth-child(1){transform:translateY(7px) rotate(45deg)}
.hamburger.open span:nth-child(2){opacity:0}
.hamburger.open span:nth-child(3){transform:translateY(-7px) rotate(-45deg)}
/* Mobile drawer */
.mobile-menu{display:none;position:fixed;top:50px;left:0;right:0;bottom:0;background:rgba(0,0,0,.6);z-index:199;backdrop-filter:blur(4px)}
.mobile-menu.open{display:block}
.mobile-drawer{background:#09090b;border-bottom:2px solid var(--border);padding:8px 0}
.mobile-tab{padding:14px 24px;font-family:'Rajdhani',sans-serif;font-weight:600;font-size:14px;letter-spacing:1.5px;text-transform:uppercase;color:var(--muted2);cursor:pointer;border-left:3px solid transparent;transition:all .15s}
.mobile-tab:hover{color:var(--text);background:rgba(255,255,255,.03)}
.mobile-tab.active{color:var(--orange);border-left-color:var(--orange);background:var(--orange-glow)}
.mobile-skins{display:block;padding:14px 24px;font-family:'Rajdhani',sans-serif;font-weight:600;font-size:14px;letter-spacing:1.5px;text-transform:uppercase;color:var(--muted2);text-decoration:none;border-top:1px solid var(--border);margin-top:4px}
@media(max-width:768px){
  .tabs{display:none}
  .nav-right{display:none}
  .hamburger{display:flex}
}
.btn-sm{padding:5px 12px;border:1px solid var(--border2);border-radius:3px;font-size:11px;font-family:'Rajdhani',sans-serif;font-weight:600;letter-spacing:1px;text-transform:uppercase;color:var(--muted2);cursor:pointer;transition:all .18s;white-space:nowrap}
.btn-sm:hover{border-color:var(--orange);color:var(--orange)}
.btn-sm.active-btn{border-color:var(--orange);color:var(--orange);background:var(--orange-glow)}

/* LAYOUT */
#app{max-width:1100px;margin:0 auto;padding:20px 16px}
.page-title{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:20px;letter-spacing:1px;text-transform:uppercase;color:var(--white);margin-bottom:16px;display:flex;align-items:center;gap:10px}
.page-title .sub{font-size:11px;color:var(--muted2);font-weight:500;letter-spacing:.5px;text-transform:none;font-family:'Inter',sans-serif}

/* PAGE TRANSITIONS */
.page-panel{opacity:1;transform:translateY(0);transition:opacity .22s ease,transform .22s ease}
.page-panel.page-out{opacity:0;transform:translateY(6px);pointer-events:none}
.page-panel.page-in{animation:pageIn .22s ease forwards}
@keyframes pageIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}

/* EMPTY STATE */
.empty{padding:52px 24px;text-align:center;color:var(--muted2);display:flex;flex-direction:column;align-items:center;gap:12px}
.empty svg{opacity:.25}
.empty-title{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:16px;letter-spacing:1px;text-transform:uppercase;color:var(--muted2)}
.empty-sub{font-size:12px;color:var(--muted2);opacity:.7}

/* CARDS */
.card{background:#0c0e10;border:1px solid var(--border);border-radius:4px;overflow:hidden}

/* LEADERBOARD */
.lb-wrap{overflow-x:auto;border:1px solid rgba(255,85,0,.3)!important;box-shadow:0 0 20px rgba(255,85,0,.06),inset 0 1px 0 rgba(255,255,255,.04)!important;border-radius:4px}
.lb-table{width:100%;border-collapse:collapse;min-width:680px}
.lb-table thead tr{background:rgba(0,0,0,.6)}
.lb-table th{padding:8px 12px;text-align:right;font-family:'Rajdhani',sans-serif;font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--muted2);border-bottom:1px solid var(--border);white-space:nowrap}
.lb-table th:first-child,.lb-table th:nth-child(2){text-align:left}
.lb-table td{padding:9px 12px;text-align:right;border-bottom:1px solid var(--border);font-size:13px;white-space:nowrap}
.lb-table td:first-child{text-align:center;width:40px;font-family:'Rajdhani',sans-serif;font-weight:700;color:var(--muted)}
.lb-table td:nth-child(2){text-align:left}
.lb-table tbody tr{cursor:pointer;transition:background .12s}
.lb-table tbody tr:hover td{background:rgba(255,85,0,.04)}
.lb-table tbody tr:last-child td{border-bottom:none}
.rank-gold td:first-child{color:var(--orange);background:linear-gradient(90deg,rgba(255,85,0,.14) 0%,rgba(255,85,0,.05) 100%)}
.rank-gold td:nth-child(2){background:linear-gradient(90deg,rgba(255,85,0,.05) 0%,transparent 100%)}
.rank-silver td:first-child{color:#a0aec0;background:linear-gradient(90deg,rgba(160,174,192,.12) 0%,rgba(160,174,192,.04) 100%)}
.rank-silver td:nth-child(2){background:linear-gradient(90deg,rgba(160,174,192,.04) 0%,transparent 100%)}
.rank-bronze td:first-child{color:#b87333;background:linear-gradient(90deg,rgba(184,115,51,.13) 0%,rgba(184,115,51,.05) 100%)}
.rank-bronze td:nth-child(2){background:linear-gradient(90deg,rgba(184,115,51,.05) 0%,transparent 100%)}
.spec-rank-gold td:first-child{color:var(--orange);background:linear-gradient(90deg,rgba(255,85,0,.14) 0%,rgba(255,85,0,.05) 100%)}
.spec-rank-gold td:nth-child(2){background:linear-gradient(90deg,rgba(255,85,0,.05) 0%,transparent 100%)}
.spec-rank-silver td:first-child{color:#a0aec0;background:linear-gradient(90deg,rgba(160,174,192,.12) 0%,rgba(160,174,192,.04) 100%)}
.spec-rank-silver td:nth-child(2){background:linear-gradient(90deg,rgba(160,174,192,.04) 0%,transparent 100%)}
.spec-rank-bronze td:first-child{color:#b87333;background:linear-gradient(90deg,rgba(184,115,51,.13) 0%,rgba(184,115,51,.05) 100%)}
.spec-rank-bronze td:nth-child(2){background:linear-gradient(90deg,rgba(184,115,51,.05) 0%,transparent 100%)}
.pname{font-weight:600;color:var(--white);font-family:'Rajdhani',sans-serif;font-size:15px;letter-spacing:.5px}
.pname:hover{color:var(--orange)}
.kd-num{font-family:'Rajdhani',sans-serif;font-weight:600;font-size:15px}
.kd-g{color:var(--win)}.kd-n{color:var(--text)}.kd-b{color:var(--loss)}
.hs-bar-wrap{display:flex;align-items:center;gap:6px;justify-content:flex-end}
.hs-bar{height:4px;background:var(--border2);border-radius:2px;width:50px;overflow:hidden;flex-shrink:0}
.hs-bar-fill{height:100%;background:var(--orange);border-radius:2px;transition:width .4s}
.hs-val{font-size:11px;color:var(--orange);width:34px;text-align:right}

/* MVP CARD */
.mvp-card{--sx:50%;--sy:50%;background:linear-gradient(135deg,rgba(255,85,0,.12) 0%,rgba(10,12,14,.7) 50%,rgba(255,85,0,.06) 100%);backdrop-filter:blur(24px);-webkit-backdrop-filter:blur(24px);border:1px solid rgba(255,85,0,.35);border-radius:10px;padding:20px 24px;display:flex;align-items:center;gap:24px;margin-bottom:12px;position:relative;overflow:hidden;box-shadow:0 0 40px rgba(255,85,0,.15),0 8px 32px rgba(0,0,0,.6),inset 0 1px 0 rgba(255,255,255,.08)}
.mvp-card::after{content:'';position:absolute;inset:0;background:radial-gradient(circle at var(--sx) var(--sy),rgba(255,140,60,.22) 0%,rgba(255,85,0,.08) 35%,transparent 65%);pointer-events:none;opacity:0;transition:opacity .4s ease}
.mvp-card::before{content:'MVP';position:absolute;right:20px;top:16px;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:13px;letter-spacing:3px;color:var(--orange);border:1px solid var(--orange);padding:2px 10px;border-radius:2px;background:rgba(255,85,0,.18);box-shadow:0 0 12px rgba(255,85,0,.3),inset 0 1px 0 rgba(255,255,255,.1);z-index:1}
.mvp-avatar{width:64px;height:64px;border-radius:50%;background:linear-gradient(135deg,var(--orange),var(--orange2));display:flex;align-items:center;justify-content:center;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:24px;color:#000;flex-shrink:0;border:2px solid rgba(255,85,0,.4)}
.mvp-info{flex:1}
.mvp-name{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:22px;color:var(--white);letter-spacing:.5px;margin-bottom:4px}
.mvp-stats{display:flex;gap:20px;flex-wrap:wrap}
.mvp-stat{display:flex;flex-direction:column}
.mvp-val{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:20px;color:var(--white);line-height:1}
.mvp-lbl{font-size:10px;letter-spacing:1px;text-transform:uppercase;color:var(--muted2);margin-top:2px}

/* AWARD CARDS */
.awards-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:12px}
.award-card{--sx:50%;--sy:50%;background:linear-gradient(135deg,rgba(255,85,0,.09) 0%,rgba(10,12,14,.65) 60%,rgba(255,85,0,.04) 100%);backdrop-filter:blur(18px);-webkit-backdrop-filter:blur(18px);border:1px solid rgba(255,85,0,.22);border-radius:8px;padding:12px 14px;display:flex;align-items:center;gap:12px;box-shadow:0 0 20px rgba(255,85,0,.08),0 4px 16px rgba(0,0,0,.45),inset 0 1px 0 rgba(255,255,255,.06);position:relative;overflow:hidden}
.award-card::after{content:'';position:absolute;inset:0;background:radial-gradient(circle at var(--sx) var(--sy),rgba(255,140,60,.2) 0%,rgba(255,85,0,.06) 40%,transparent 65%);pointer-events:none;opacity:0;transition:opacity .4s ease}
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
.sb-table th{padding:7px 10px;text-align:right;font-family:'Rajdhani',sans-serif;font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--muted2);background:rgba(0,0,0,.55);border-bottom:1px solid var(--border);white-space:nowrap}
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
.matches-list .match-item{position:relative;overflow:hidden;display:flex;align-items:center;gap:14px;padding:0;border-bottom:3px solid rgba(0,0,0,.8);cursor:pointer;transition:transform .22s ease,box-shadow .22s ease,border-left-color .22s ease;min-height:96px;border-left:3px solid transparent;will-change:transform}
.matches-list .match-item:hover{transform:translateX(4px);border-left-color:var(--orange);box-shadow:inset 4px 0 20px rgba(255,85,0,.1)}
.matches-list .match-item:last-child{border-bottom:none}
.match-item .m-bg{position:absolute;inset:0;background-size:cover;background-position:center;z-index:0;transition:transform .35s ease;will-change:transform}
.matches-list .match-item:hover .m-bg{transform:scale(1.04)}
.match-item .m-overlay{position:absolute;inset:0;background:linear-gradient(90deg,rgba(6,8,14,.97) 0%,rgba(6,8,14,.92) 30%,rgba(6,8,14,.65) 60%,rgba(6,8,14,.25) 100%);z-index:1}
.match-item .m-hover-layer{position:absolute;inset:0;z-index:1;background:linear-gradient(90deg,rgba(255,85,0,.07) 0%,rgba(255,85,0,.03) 30%,transparent 70%);opacity:0;transition:opacity .22s ease}
.matches-list .match-item:hover .m-hover-layer{opacity:1}
.matches-list .match-item:hover .m-score{text-shadow:0 0 18px rgba(255,120,40,.45),0 2px 8px rgba(0,0,0,.9)}
.matches-list .match-item:hover .m-id{color:var(--orange)}
.match-item .m-content{position:relative;z-index:2;display:flex;align-items:center;gap:14px;width:100%;padding:18px 24px}
.m-id{font-size:11px;color:rgba(255,255,255,.45);width:42px;font-family:'Rajdhani',sans-serif;font-weight:600;flex-shrink:0;transition:color .22s ease}
.m-teams{flex:1}
.m-teams-str{font-size:13px;color:rgba(255,255,255,.8);margin-bottom:4px;font-weight:500}
.m-score{font-family:'Rajdhani',sans-serif;font-weight:800;font-size:26px;color:#fff;text-shadow:0 2px 8px rgba(0,0,0,.9),0 0 20px rgba(0,0,0,.5);transition:text-shadow .22s ease}
.m-date{font-size:11px;color:rgba(255,255,255,.5);text-align:right;white-space:nowrap;text-shadow:0 1px 4px rgba(0,0,0,.9)}

/* DEMO CARDS */
.demo-card{position:relative;overflow:hidden;height:110px;margin-bottom:2px;cursor:pointer;border-left:3px solid transparent;transition:transform .22s ease,box-shadow .22s ease,border-left-color .22s ease;will-change:transform}
.demo-card:hover{transform:translateX(4px);border-left-color:var(--orange);box-shadow:inset 4px 0 20px rgba(255,85,0,.1)}
.demo-card:hover .demo-bg-img{transform:scale(1.04)}
.demo-card:hover .demo-map-label{color:rgba(255,255,255,.7)}
.demo-hover-layer{position:absolute;inset:0;z-index:1;background:linear-gradient(90deg,rgba(255,85,0,.07) 0%,rgba(255,85,0,.03) 30%,transparent 70%);opacity:0;transition:opacity .22s ease;pointer-events:none}
.demo-card:hover .demo-hover-layer{opacity:1}
.demo-bg-img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;opacity:0.75;transition:transform .35s ease;will-change:transform}
.demo-overlay{position:absolute;inset:0;background:linear-gradient(90deg,rgba(4,5,7,.95) 0%,rgba(4,5,7,.88) 35%,rgba(4,5,7,.55) 65%,rgba(4,5,7,.1) 100%)}
.demo-content{position:relative;z-index:2;height:100%;display:flex;align-items:center;padding:0 22px;gap:18px}
.demo-map-label{font-family:'Rajdhani',sans-serif;font-weight:800;font-size:12px;letter-spacing:3px;text-transform:uppercase;color:rgba(255,255,255,.45);margin-bottom:4px;text-shadow:0 1px 4px rgba(0,0,0,.8);transition:color .22s ease}

/* PROFILE */
.profile-top{--sx:50%;--sy:50%;background:linear-gradient(135deg,rgba(255,85,0,.11) 0%,rgba(10,12,14,.7) 50%,rgba(255,85,0,.05) 100%);backdrop-filter:blur(24px);-webkit-backdrop-filter:blur(24px);border:1px solid rgba(255,85,0,.28);border-radius:10px;padding:22px 24px;margin-bottom:12px;display:flex;align-items:center;gap:20px;box-shadow:0 0 40px rgba(255,85,0,.12),0 8px 32px rgba(0,0,0,.55),inset 0 1px 0 rgba(255,255,255,.08);position:relative;overflow:hidden}
.profile-top::after{content:'';position:absolute;inset:0;background:radial-gradient(circle at var(--sx) var(--sy),rgba(255,140,60,.18) 0%,rgba(255,85,0,.06) 40%,transparent 65%);pointer-events:none;opacity:0;transition:opacity .4s ease}
.p-avatar{width:68px;height:68px;border-radius:4px;background:linear-gradient(135deg,var(--orange),#c43a00);display:flex;align-items:center;justify-content:center;font-family:'Rajdhani',sans-serif;font-weight:800;font-size:26px;color:#000;flex-shrink:0}
.p-name{font-family:'Rajdhani',sans-serif;font-weight:700;font-size:28px;color:var(--white);letter-spacing:.5px;line-height:1;margin-bottom:4px}
.p-sub{font-size:11px;color:var(--muted2)}
.stats-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:var(--border);border-top:1px solid var(--border)}
@media(max-width:600px){.stats-grid{grid-template-columns:repeat(3,1fr)}}
.stat-box{background:#0a0c0e;padding:14px 16px;transition:background .15s}
.stat-box:hover{background:#0e1012}
.stat-box:nth-child(-n+6){background:linear-gradient(180deg,rgba(255,85,0,.07) 0%,#0a0c0e 100%)}
.stat-box:nth-child(-n+6):hover{background:linear-gradient(180deg,rgba(255,85,0,.13) 0%,#0c0e10 100%)}
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

/* POINTER SHINE */
.shine-card{transition:box-shadow .2s,border-color .2s}
.shine-card:hover{border-color:rgba(255,85,0,.55)!important;box-shadow:0 0 40px rgba(255,85,0,.18),0 8px 32px rgba(0,0,0,.6),inset 0 1px 0 rgba(255,255,255,.08)!important}
.mvp-card.shine-active::after,
.award-card.shine-active::after,
.profile-top.shine-active::after{opacity:1}

/* PODIUM CARDS */
.podium-card{transition:transform .2s,box-shadow .2s,border-color .2s}
.podium-card:hover{transform:translateY(-3px);box-shadow:0 0 30px rgba(255,85,0,.2),0 12px 40px rgba(0,0,0,.6)!important}
.podium-shine{position:absolute;inset:0;pointer-events:none;z-index:0;opacity:0;transition:opacity .4s ease;border-radius:inherit;background:radial-gradient(circle at var(--sx,50%) var(--sy,50%),rgba(255,200,100,.25) 0%,rgba(255,85,0,.08) 40%,transparent 65%)}
.podium-card:hover .podium-shine{opacity:1}

/* H2H RESULT EXPAND */
.h2h-result-body{overflow:hidden;max-height:0;transition:max-height .4s ease;position:relative;z-index:1}
.h2h-result-body.open{max-height:1200px}
.h2h-result-divider{border-bottom:1px solid rgba(255,85,0,.15);margin:0 -20px;display:none}
.h2h-result-body.open ~ .h2h-result-divider{display:block}

/* H2H TOP CARD */
.h2h-top-card{--sx:50%;--sy:50%;background:linear-gradient(135deg,rgba(255,85,0,.12) 0%,rgba(10,12,14,.7) 50%,rgba(255,85,0,.06) 100%);backdrop-filter:blur(24px);-webkit-backdrop-filter:blur(24px);border:1px solid rgba(255,85,0,.35);border-radius:10px;padding:20px 22px;position:relative;overflow:hidden;box-shadow:0 0 40px rgba(255,85,0,.15),0 8px 32px rgba(0,0,0,.6),inset 0 1px 0 rgba(255,255,255,.08)}
.h2h-top-card::after{content:'';position:absolute;inset:0;background:radial-gradient(circle at var(--sx) var(--sy),rgba(255,140,60,.22) 0%,rgba(255,85,0,.08) 35%,transparent 65%);pointer-events:none;opacity:0;transition:opacity .4s ease}
.h2h-top-card.shine-active::after{opacity:1}

/* H2H CLEAR BUTTON */
.h2h-clear-btn{padding:3px 10px;background:transparent;border:1px solid var(--border);border-radius:3px;color:var(--muted2);font-size:11px;font-family:'Rajdhani',sans-serif;cursor:pointer;letter-spacing:1px;transition:all .2s;position:relative;overflow:hidden}
.h2h-clear-btn::before{content:'';position:absolute;inset:0;background:linear-gradient(135deg,rgba(255,255,255,.07) 0%,transparent 60%);pointer-events:none}
.h2h-clear-btn:hover{background:rgba(255,85,0,.45);border-color:var(--orange2);color:#fff;box-shadow:0 0 18px rgba(255,85,0,.5),0 0 6px rgba(255,85,0,.3),inset 0 1px 0 rgba(255,255,255,.15);text-shadow:0 0 8px rgba(255,180,80,.7);transform:translateY(-1px)}
.h2h-clear-btn:active{transform:translateY(0);box-shadow:0 0 10px rgba(255,85,0,.35)}

/* H2H PLAYER ROWS */
.h2h-row{display:flex;align-items:center;gap:10px;padding:10px 14px;border-radius:4px;cursor:pointer;border:1px solid transparent;transition:background .15s ease,border-color .15s ease,box-shadow .15s ease,transform .15s ease}
.h2h-row:hover{background:rgba(255,85,0,.06);border-color:rgba(255,85,0,.25);box-shadow:0 0 12px rgba(255,85,0,.08),inset 0 1px 0 rgba(255,255,255,.04);transform:translateX(2px)}
.h2h-row.selected{background:rgba(255,255,255,.05);border-color:rgba(255,255,255,.12)}
.h2h-row.selected:hover{background:rgba(255,85,0,.09);border-color:rgba(255,85,0,.35);box-shadow:0 0 16px rgba(255,85,0,.12),inset 0 1px 0 rgba(255,255,255,.05)}

/* COMPARE BUTTON */
.compare-btn{width:100%;padding:11px;border-radius:3px;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:13px;letter-spacing:2px;text-transform:uppercase;cursor:pointer;transition:all .2s;position:relative;overflow:hidden}
.compare-btn::before{content:'';position:absolute;inset:0;background:linear-gradient(135deg,rgba(255,255,255,.08) 0%,transparent 60%);pointer-events:none}
.compare-btn.active{border:1px solid var(--orange);background:var(--orange-glow);color:var(--orange)}
.compare-btn.active:hover{background:rgba(255,85,0,.45);border-color:var(--orange2);color:#fff;box-shadow:0 0 24px rgba(255,85,0,.6),0 0 8px rgba(255,85,0,.4),inset 0 1px 0 rgba(255,255,255,.15);text-shadow:0 0 10px rgba(255,180,80,.8);transform:translateY(-1px)}
.compare-btn.active:active{transform:translateY(0);box-shadow:0 0 12px rgba(255,85,0,.4)}
.compare-btn.inactive{border:1px solid var(--border);background:transparent;color:var(--muted2);cursor:default}

/* DEMO DOWNLOAD BUTTON */
.demo-dl-btn{padding:10px 20px;background:rgba(255,85,0,.15);backdrop-filter:blur(8px);-webkit-backdrop-filter:blur(8px);border:1px solid var(--orange);border-radius:0;color:var(--orange);font-family:'Rajdhani',sans-serif;font-weight:700;font-size:12px;letter-spacing:1.5px;text-transform:uppercase;cursor:pointer;white-space:nowrap;transition:all .2s;position:relative;overflow:hidden}.demo-dl-btn::before{content:'';position:absolute;inset:0;background:linear-gradient(135deg,rgba(255,255,255,.08) 0%,transparent 60%);pointer-events:none}
.demo-dl-btn:hover{background:rgba(255,85,0,.45);border-color:var(--orange2);color:#fff;box-shadow:0 0 24px rgba(255,85,0,.6),0 0 8px rgba(255,85,0,.4),inset 0 1px 0 rgba(255,255,255,.15);text-shadow:0 0 10px rgba(255,180,80,.8);transform:translateY(-1px)}
.demo-dl-btn:active{transform:translateY(0);box-shadow:0 0 12px rgba(255,85,0,.4)}

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
  <div class="logo"><span style="font-size:22px;font-weight:800;letter-spacing:-1px">R</span><span style="font-size:22px;font-weight:300;letter-spacing:-1px;color:var(--orange2)">G</span></div>
  <div class="tabs">
    <div class="tab active" data-p="matches">Matches</div>
    <div class="tab" data-p="leaderboard">Leaderboard</div>
    <div class="tab" data-p="maps">Maps</div>
    <div class="tab" data-p="h2h">Head-to-Head</div>
    <div class="tab" data-p="specialists">Specialists</div>
    <div class="tab" data-p="demos">Demos</div>
    <div class="tab-indicator" id="tab-indicator"></div>
  </div>
  <div class="nav-right">
    <a href="https://skins.fsho.st" target="_blank" class="btn-sm" style="text-decoration:none">🎨 Skins</a>
  </div>
  <div class="hamburger" id="hamburger" onclick="toggleMenu()">
    <span></span><span></span><span></span>
  </div>
</nav>
<div class="mobile-menu" id="mobile-menu" onclick="closeMenu()">
  <div class="mobile-drawer" onclick="event.stopPropagation()">
    <div class="mobile-tab active" data-p="matches" onclick="go('matches');closeMenu()">Matches</div>
    <div class="mobile-tab" data-p="leaderboard" onclick="go('leaderboard');closeMenu()">Leaderboard</div>
    <div class="mobile-tab" data-p="maps" onclick="go('maps');closeMenu()">Maps</div>
    <div class="mobile-tab" data-p="h2h" onclick="go('h2h');closeMenu()">Head-to-Head</div>
    <div class="mobile-tab" data-p="specialists" onclick="go('specialists');closeMenu()">Specialists</div>
    <div class="mobile-tab" data-p="demos" onclick="go('demos');closeMenu()">Demos</div>
    <a href="https://skins.fsho.st" target="_blank" class="mobile-skins" onclick="closeMenu()">🎨 Skins</a>
  </div>
</div>

<div id="app">
  <div id="p-matches" class="page-panel"></div>
  <div id="p-leaderboard" class="page-panel" style="display:none"></div>
  <div id="p-maps" class="page-panel" style="display:none"></div>
  <div id="p-h2h" class="page-panel" style="display:none"></div>
  <div id="p-specialists" class="page-panel" style="display:none"></div>
  <div id="p-demos" class="page-panel" style="display:none"></div>
  <div id="p-player" class="page-panel" style="display:none"></div>
  <div id="p-match" class="page-panel" style="display:none"></div>
</div>

<script>
// ── Router ────────────────────────────────────────────────────────────────────
// Shared CS2 map image URLs (Steam CDN)
const MAP_IMG_BASE = 'https://raw.githubusercontent.com/ghostcap-gaming/cs2-map-images/refs/heads/main/cs2';
const MAP_IMGS = {
  'de_mirage':   `${MAP_IMG_BASE}/de_mirage.png`,
  'de_dust2':    `${MAP_IMG_BASE}/de_dust2.png`,
  'de_inferno':  `${MAP_IMG_BASE}/de_inferno.png`,
  'de_nuke':     `${MAP_IMG_BASE}/de_nuke.png`,
  'de_ancient':  `${MAP_IMG_BASE}/de_ancient.png`,
  'de_anubis':   `${MAP_IMG_BASE}/de_anubis.png`,
  'de_vertigo':  `${MAP_IMG_BASE}/de_vertigo.png`,
  'de_overpass': `${MAP_IMG_BASE}/de_overpass.png`,
};
function mapThumb(mapname, h=48, w=80) {
  const url = MAP_IMGS[mapname];
  if (!url) return `<div style="width:${w}px;height:${h}px;background:var(--surface2);border-radius:3px;display:flex;align-items:center;justify-content:center;flex-shrink:0"><span style="font-size:9px;color:var(--muted2);font-family:'Rajdhani',sans-serif;font-weight:700;letter-spacing:1px;text-transform:uppercase">${esc(mapname||'?')}</span></div>`;
  return `<div style="width:${w}px;height:${h}px;border-radius:3px;overflow:hidden;flex-shrink:0;position:relative">
    <img src="${url}" style="width:100%;height:100%;object-fit:cover" onerror="this.parentElement.style.background='var(--surface2)'">
  </div>`;
}

function toggleMenu() {
  document.getElementById('hamburger').classList.toggle('open');
  document.getElementById('mobile-menu').classList.toggle('open');
}
function closeMenu() {
  document.getElementById('hamburger').classList.remove('open');
  document.getElementById('mobile-menu').classList.remove('open');
}

// ── Tab indicator ─────────────────────────────────────────────────────────────
function moveTabIndicator(page) {
  const indicator = document.getElementById('tab-indicator');
  const activeTab = document.querySelector(`.tab[data-p="${page}"]`);
  if (!indicator || !activeTab) return;
  const tabsEl = activeTab.parentElement;
  const tabsRect = tabsEl.getBoundingClientRect();
  const tabRect = activeTab.getBoundingClientRect();
  indicator.style.left = (tabRect.left - tabsRect.left) + 'px';
  indicator.style.width = tabRect.width + 'px';
}

const _pages = ['matches','leaderboard','maps','h2h','specialists','demos','player','match'];

let _back = null;
function go(page, params, back) {
  params = params || {};
  back   = back   || null;

  // Find currently visible page to fade out
  const current = _pages.find(function(pg){ return document.getElementById('p-'+pg).style.display !== 'none'; });
  const incoming = document.getElementById('p-'+page);

  function doSwitch() {
    _pages.forEach(function(pg) {
      const el = document.getElementById('p-'+pg);
      el.style.display = (pg===page) ? '' : 'none';
      el.classList.remove('page-in','page-out');
    });
    incoming.classList.add('page-in');
    document.querySelectorAll('.tab').forEach(function(t){ t.classList.toggle('active', t.dataset.p===page); });
    document.querySelectorAll('.mobile-tab').forEach(function(t){ t.classList.toggle('active', t.dataset.p===page); });
    moveTabIndicator(page);
    _back = back;
    if(page==='matches')     loadMatches();
    if(page==='leaderboard') loadLeaderboard();
    if(page==='maps')        loadMaps();
    if(page==='h2h')         loadH2H();
    if(page==='specialists') loadSpecialists();
    if(page==='demos')       loadDemos();
    if(page==='player')      loadPlayer(params.name);
    if(page==='match')       loadMatch(params.id);
  }

  if (current && current !== page) {
    var outEl = document.getElementById('p-'+current);
    outEl.classList.add('page-out');
    setTimeout(doSwitch, 200);
  } else {
    doSwitch();
  }
}
document.querySelectorAll('.tab').forEach(t=>t.addEventListener('click',()=>go(t.dataset.p)));

// ── Animated number counter ───────────────────────────────────────────────────
// Usage: animateCount(el, targetValue, {duration, decimals, suffix, prefix})
function animateCount(el, target, opts={}) {
  const duration = opts.duration || 900;
  const decimals = opts.decimals ?? 0;
  const suffix   = opts.suffix   || '';
  const prefix   = opts.prefix   || '';
  const start    = performance.now();
  const from     = 0;
  const isFloat  = decimals > 0;

  function step(now) {
    const t = Math.min((now - start) / duration, 1);
    // Ease out expo
    const ease = t === 1 ? 1 : 1 - Math.pow(2, -10 * t);
    const val = from + (target - from) * ease;
    el.textContent = prefix + (isFloat ? val.toFixed(decimals) : Math.floor(val).toLocaleString()) + suffix;
    if (t < 1) requestAnimationFrame(step);
    else el.textContent = prefix + (isFloat ? target.toFixed(decimals) : Number(target).toLocaleString()) + suffix;
  }
  requestAnimationFrame(step);
}

// Attach counters to any element with data-count attribute after DOM injection
function attachCounters(root) {
  (root || document).querySelectorAll('[data-count]').forEach(el => {
    if (el._counted) return;
    el._counted = true;
    const target   = parseFloat(el.dataset.count);
    const decimals = parseInt(el.dataset.dec  || '0');
    const suffix   = el.dataset.suffix || '';
    animateCount(el, target, {decimals, suffix, duration: 800});
  });
}

// Hook into MutationObserver to auto-attach counters whenever content loads
const _counterObs = new MutationObserver(() => attachCounters());
_counterObs.observe(document.getElementById('app'), {childList:true, subtree:true});

// ── Empty state helper ────────────────────────────────────────────────────────
const EMPTY_ICONS = {
  matches: `<svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="8" y="14" width="48" height="36" rx="3" stroke="#ff5500" stroke-width="2" fill="none"/><path d="M8 22h48" stroke="#ff5500" stroke-width="2"/><circle cx="32" cy="38" r="7" stroke="#ff5500" stroke-width="2" fill="none"/><path d="M32 35v3l2 2" stroke="#ff5500" stroke-width="1.5" stroke-linecap="round"/></svg>`,
  leaderboard: `<svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="10" y="30" width="12" height="24" rx="2" stroke="#ff5500" stroke-width="2" fill="none"/><rect x="26" y="20" width="12" height="34" rx="2" stroke="#ff5500" stroke-width="2" fill="none"/><rect x="42" y="38" width="12" height="16" rx="2" stroke="#ff5500" stroke-width="2" fill="none"/><path d="M32 10l1.5 3h3.5l-2.8 2.1 1 3.4L32 16.7l-3.2 1.8 1-3.4L27 13h3.5z" fill="#ff5500" opacity=".6"/></svg>`,
  maps: `<svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M10 18l14-6 16 8 14-6v30l-14 6-16-8-14 6V18z" stroke="#ff5500" stroke-width="2" fill="none"/><path d="M24 12v32M40 20v32" stroke="#ff5500" stroke-width="1.5" stroke-dasharray="3 3"/><circle cx="32" cy="30" r="4" stroke="#ff5500" stroke-width="2" fill="none"/></svg>`,
  h2h: `<svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="20" cy="22" r="8" stroke="#ff5500" stroke-width="2" fill="none"/><circle cx="44" cy="22" r="8" stroke="#ff5500" stroke-width="2" fill="none"/><path d="M8 46c0-6.6 5.4-12 12-12h4M44 34h4c6.6 0 12 5.4 12 12" stroke="#ff5500" stroke-width="2" stroke-linecap="round" fill="none"/><path d="M28 32h8M32 28v8" stroke="#ff5500" stroke-width="2" stroke-linecap="round"/></svg>`,
  specialists: `<svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M32 10l4 12h13l-10 8 4 12-11-8-11 8 4-12-10-8h13z" stroke="#ff5500" stroke-width="2" fill="none"/><circle cx="32" cy="46" r="4" stroke="#ff5500" stroke-width="2" fill="none"/><path d="M32 50v6" stroke="#ff5500" stroke-width="2" stroke-linecap="round"/></svg>`,
  demos: `<svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="10" y="14" width="44" height="30" rx="3" stroke="#ff5500" stroke-width="2" fill="none"/><path d="M20 50h24M32 44v6" stroke="#ff5500" stroke-width="2" stroke-linecap="round"/><path d="M27 24l12 7-12 7V24z" stroke="#ff5500" stroke-width="2" fill="none" stroke-linejoin="round"/></svg>`,
  default: `<svg width="64" height="64" viewBox="0 0 64 64" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="32" cy="32" r="22" stroke="#ff5500" stroke-width="2" fill="none"/><path d="M32 22v12M32 38v4" stroke="#ff5500" stroke-width="2.5" stroke-linecap="round"/></svg>`,
};

function emptyState(type, title, sub) {
  const icon = EMPTY_ICONS[type] || EMPTY_ICONS.default;
  return `<div class="empty">${icon}<div class="empty-title">${title}</div><div class="empty-sub">${sub||''}</div></div>`;
}

// ── Helpers ───────────────────────────────────────────────────────────────────
const esc = s=>String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
const num = n=>n!=null?Number(n).toLocaleString():'-';
const kdc = v=>{const f=parseFloat(v);return f>=1.3?'kd-g':f>=0.9?'kd-n':'kd-b'};
const kdf = v=>{const f=parseFloat(v);return f>=1.3?'win':f>=0.9?'text':'loss'};
const fmtDate = s=>{
  if(!s)return'-';
  // Normalize ISO strings with >6 fractional seconds digits (e.g. "...4524841Z" → "...452Z")
  const norm = String(s).replace(/(\.\d{3})\d*(Z?)$/, '$1$2');
  const d = new Date(norm);
  if(isNaN(d)) return String(s).replace('T',' ').slice(0,16);
  return d.toLocaleDateString('en-GB',{day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'});
};
const initials = n=>(n||'?').split(/[ _]/).slice(0,2).map(w=>w[0]||'').join('').toUpperCase().slice(0,2)||'?';
function spin(id){document.getElementById(id).innerHTML='<div class="loading"><div class="spin"></div><br>Loading…</div>'}

// ── Demo helpers ─────────────────────────────────────────────────────────────
let _demosCache = null;
async function getDemos() {
  if (_demosCache) return _demosCache;
  _demosCache = await fetch('/api/demos').then(r=>r.json()).catch(()=>[]);
  return _demosCache;
}
function findDemo(endTimeStr, windowMs=10*60*1000) {
  if (!_demosCache || !endTimeStr) return null;
  const matchEnd = new Date(endTimeStr).getTime();
  if (isNaN(matchEnd)) return null;
  let best = null, bestDelta = Infinity;
  for (const d of _demosCache) {
    if (!d.filename_ts) continue;
    const demoTs = new Date(d.filename_ts).getTime();
    const delta = Math.abs(demoTs - matchEnd);
    if (delta <= windowMs && delta < bestDelta) { best = d; bestDelta = delta; }
  }
  return best;
}
function demoBtn(demo, small=false) {
  if (!demo || !demo.download_url) return '';
  const size = demo.size_formatted ? ` (${demo.size_formatted})` : '';
  // small = match list row chip; full = match detail meta row chip
  return `<a href="${demo.download_url}" target="_blank" onclick="event.stopPropagation()" style="display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border:1px solid rgba(255,85,0,.5);border-radius:2px;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:10px;letter-spacing:1px;text-transform:uppercase;color:var(--orange);background:var(--orange-glow);text-decoration:none;white-space:nowrap" title="${demo.name}">📥 Demo${size}</a>`;
}

// ── Matches ───────────────────────────────────────────────────────────────────
async function loadMatches() {
  spin('p-matches');
  const data = await fetch('/api/matches?limit=30').then(r=>r.json()).catch(()=>[]);
  if(!Array.isArray(data)||!data.length){
    document.getElementById('p-matches').innerHTML=emptyState('matches','No Matches Yet','Completed matches will appear here');return;
  }
  const items = data.map(m=>{
    const bgStyle = MAP_IMGS[m.mapname] ? `background-image:url('${MAP_IMGS[m.mapname]}')` : 'background:var(--surface2)';
    return `
    <div class="match-item" onclick="go('match',{id:'${m.matchid}'},'matches')">
      <div class="m-bg" style="${bgStyle}"></div>
      <div class="m-overlay"></div>
      <div class="m-hover-layer"></div>
      <div class="m-content">
        <div class="m-id">#${m.matchid}</div>
        <div class="m-teams">
          <div class="m-teams-str">${esc(m.team1_name||'Team 1')} <span style="color:var(--muted2)">vs</span> ${esc(m.team2_name||'Team 2')}</div>
          <div class="m-score">${m.map_team1_score??m.team1_score??0} : ${m.map_team2_score??m.team2_score??0}</div>
        </div>
        <div class="m-date">${fmtDate(m.end_time)}</div>
      </div>
    </div>`;
  }).join('');
  document.getElementById('p-matches').innerHTML = `
    <div class="matches-list" style="border-radius:4px;overflow:hidden">${items}</div>`;
}

// ── Match Detail ──────────────────────────────────────────────────────────────
async function loadMatch(id) {
  spin('p-match');
  const d = await fetch('/api/match/'+id).then(r=>r.json()).catch(()=>({error:'fetch failed'}));
  if(d.error){document.getElementById('p-match').innerHTML=`<div class="empty">${d.error}</div>`;return}

  const meta=d.meta||{}, maps=d.maps||[], players=d.players||[], demo=d.demo||{};

  // ── Team split: players have team='team1' or 'team2' from fshost ──────────
  function splitTeams(arr) {
    const t1 = arr.filter(p=>String(p.team||'').toLowerCase()==='team1');
    const t2 = arr.filter(p=>String(p.team||'').toLowerCase()==='team2');
    const n1  = (t1[0]||{}).team_name || meta.team1_name || 'Team 1';
    const n2  = (t2[0]||{}).team_name || meta.team2_name || 'Team 2';
    return {t1, t2, n1, n2};
  }

  // Always show full column set when data comes from fshost
  const hasExtra = true;
  const colCount  = 15;
  const extraHdr  = '<th>Rating</th><th>KAST</th><th>3K</th><th>4K</th><th>OK/OD</th>';

  // MVP = highest rating, fallback to kills
  const mvp = players.reduce((b,p)=>{
    const s = p.rating!=null?parseFloat(p.rating)*100:parseInt(p.kills||0);
    const bs= b?(b.rating!=null?parseFloat(b.rating)*100:parseInt(b.kills||0)):-1;
    return s>bs?p:b;
  }, null);

  const byKills  = [...players].sort((a,b)=>parseInt(b.kills||0)-parseInt(a.kills||0));
  const byDmg    = [...players].sort((a,b)=>parseInt(b.damage||0)-parseInt(a.damage||0));
  const byRating = [...players].filter(p=>p.rating!=null).sort((a,b)=>parseFloat(b.rating)-parseFloat(a.rating));

  // Fetch Steam avatars for all unique players in this match
  const steamCache = {};
  await Promise.all([...new Map(players.map(p=>[p.steamid64,p])).values()].map(async p => {
    if (p.steamid64 && p.steamid64 !== '0') {
      const s = await fetch('/api/steam/'+p.steamid64).then(r=>r.json()).catch(()=>({}));
      if (s.avatar) steamCache[p.steamid64] = s.avatar;
    }
  }));

  function playerAvatar(p, size=40) {
    const img = steamCache[p?.steamid64];
    if (img) return `<img src="${img}" style="width:${size}px;height:${size}px;border-radius:50%;object-fit:cover;border:2px solid var(--border2);flex-shrink:0" onerror="this.style.display='none'">`;
    return `<div style="width:${size}px;height:${size}px;border-radius:50%;background:linear-gradient(135deg,var(--orange),var(--orange2));display:flex;align-items:center;justify-content:center;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:${Math.floor(size*0.35)}px;color:#000;flex-shrink:0">${initials(p?.name)}</div>`;
  }

  const mvpHtml = mvp ? `
    <div class="mvp-card">
      ${playerAvatar(mvp, 64)}
      <div class="mvp-info">
        <div class="mvp-name">${esc(mvp.name)}</div>
        <div class="mvp-stats">
          <div class="mvp-stat"><div class="mvp-val">${mvp.kills??0} / ${mvp.deaths??0} / ${mvp.assists??0}</div><div class="mvp-lbl">K / D / A</div></div>
          <div class="mvp-stat"><div class="mvp-val"><span data-count="${mvp.adr!=null?parseFloat(mvp.adr):0}" data-dec="1">${mvp.adr!=null?parseFloat(mvp.adr).toFixed(1):'—'}</span></div><div class="mvp-lbl">ADR</div></div>
          <div class="mvp-stat"><div class="mvp-val"><span data-count="${mvp.hs_pct!=null?parseFloat(mvp.hs_pct):0}" data-dec="1" data-suffix="%">${mvp.hs_pct!=null?parseFloat(mvp.hs_pct).toFixed(1)+'%':'—'}</span></div><div class="mvp-lbl">HS%</div></div>
          <div class="mvp-stat"><div class="mvp-val" style="color:var(--${kdf(mvp.kills&&mvp.deaths?(mvp.kills/mvp.deaths).toFixed(2):'0')})"><span data-count="${mvp.kills&&mvp.deaths?(mvp.kills/mvp.deaths).toFixed(2):0}" data-dec="2">${mvp.kills&&mvp.deaths?(mvp.kills/mvp.deaths).toFixed(2):'—'}</span></div><div class="mvp-lbl">K/D</div></div>
          ${mvp.rating!=null?`<div class="mvp-stat"><div class="mvp-val" style="color:#a78bfa"><span data-count="${parseFloat(mvp.rating).toFixed(2)}" data-dec="2">${parseFloat(mvp.rating).toFixed(2)}</span></div><div class="mvp-lbl">Rating</div></div>`:''}
          ${mvp.kast!=null?`<div class="mvp-stat"><div class="mvp-val"><span data-count="${parseFloat(mvp.kast).toFixed(1)}" data-dec="1" data-suffix="%">${parseFloat(mvp.kast).toFixed(1)}%</span></div><div class="mvp-lbl">KAST</div></div>`:''}
        </div>
      </div>
    </div>` : '';

  const awardsHtml = `<div class="awards-grid">
    ${byKills[0]?`<div class="award-card">${playerAvatar(byKills[0],36)}<div><div class="award-name">${esc(byKills[0].name)}</div><div style="font-size:10px;color:var(--muted2)">Most Kills</div></div><div style="margin-left:auto;text-align:right"><div class="award-val" data-count="${byKills[0].kills}" data-dec="0">${byKills[0].kills}</div></div></div>`:''}
    ${byDmg[0]?`<div class="award-card">${playerAvatar(byDmg[0],36)}<div><div class="award-name">${esc(byDmg[0].name)}</div><div style="font-size:10px;color:var(--muted2)">Most Damage</div></div><div style="margin-left:auto;text-align:right"><div class="award-val" data-count="${byDmg[0].damage??0}" data-dec="0">${num(byDmg[0].damage)}</div></div></div>`:''}
    ${byRating[0]?`<div class="award-card">${playerAvatar(byRating[0],36)}<div><div class="award-name">${esc(byRating[0].name)}</div><div style="font-size:10px;color:var(--muted2)">Best Rating</div></div><div style="margin-left:auto;text-align:right"><div class="award-val" style="color:#a78bfa" data-count="${parseFloat(byRating[0].rating).toFixed(2)}" data-dec="2">${parseFloat(byRating[0].rating).toFixed(2)}</div></div></div>`:''}
  </div>`;

  const mapsHtml = maps.map(m=>{
    const mapPlayers = players.filter(p=>(p.mapnumber??1)===(m.mapnumber??1));
    const {t1, t2, n1, n2} = splitTeams(mapPlayers);
    return `
      <div style="margin-bottom:14px">
        <div class="card ovx">
          <table class="sb-table">
            <thead><tr>
              <th>Player</th><th>K</th><th>D</th><th>A</th>
              <th>K/D</th><th>ADR</th><th>HS%</th><th>Dmg</th><th>5K</th><th>1vX</th>
              <th>Rating</th><th>KAST</th><th>3K</th><th>4K</th><th title="Opening Kills / Deaths">OK/OD</th>
            </tr></thead>
            <tbody>
              <tr class="team-divider ct-div"><td colspan="15">${esc(n1)}</td></tr>
              ${sbRows(t1, steamCache)}
              <tr class="team-divider t-div"><td colspan="15">${esc(n2)}</td></tr>
              ${sbRows(t2, steamCache)}
            </tbody>
          </table>
        </div>
      </div>`;
  }).join('');

  const t1name = meta.team1_name||'Team 1';
  const t2name = meta.team2_name||'Team 2';
  const s1 = meta.team1_score??0;
  const s2 = meta.team2_score??0;
  const won = meta.winner || (s1>s2?t1name:s2>s1?t2name:null);
  // Demo button from fshost data directly
  const demoBtnHtml = demo.url
    ? `<div class="meta-chip"><a href="${demo.url}" target="_blank"
         style="display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border:1px solid rgba(255,85,0,.5);border-radius:2px;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:10px;letter-spacing:1px;text-transform:uppercase;color:var(--orange);background:var(--orange-glow);text-decoration:none">
         Demo${demo.size?' ('+demo.size+')':''}</a></div>`
    : '';

  document.getElementById('p-match').innerHTML = `
    <div class="back-btn" onclick="go(_back||'matches')">← Back</div>
    <div style="position:relative;border-radius:6px;overflow:hidden;margin-bottom:12px;min-height:200px">
      ${maps[0]?.mapname && MAP_IMGS[maps[0].mapname]
        ? `<img src="${MAP_IMGS[maps[0].mapname]}" style="position:absolute;inset:0;width:100%;height:100%;object-fit:cover;opacity:0.85">`
        : `<div style="position:absolute;inset:0;background:linear-gradient(135deg,#0d1117,#1a2332)"></div>`}
      <div style="position:absolute;inset:0;background:linear-gradient(to bottom,rgba(0,0,0,.15) 0%,rgba(0,0,0,.55) 100%)"></div>
      <div style="position:relative;z-index:2;padding:22px 26px">
        <div style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:12px;letter-spacing:3px;text-transform:uppercase;color:rgba(255,255,255,.6);margin-bottom:16px">${maps[0]?.mapname?esc(maps[0].mapname):''}</div>
        <div style="
          background:rgba(10,13,20,.45);
          backdrop-filter:blur(18px);
          -webkit-backdrop-filter:blur(18px);
          border:1px solid rgba(255,255,255,.1);
          border-radius:8px;
          padding:18px 22px;
        ">
          <div class="match-score-row">
            <div class="team-block">
              <div class="team-nm" style="color:var(--ct)">${esc(t1name)}</div>
              <div class="team-sc" style="color:var(--ct)">${s1}</div>
            </div>
            <div class="score-mid"><div class="score-colon">:</div></div>
            <div class="team-block" style="text-align:right">
              <div class="team-nm" style="color:var(--t);text-align:right">${esc(t2name)}</div>
              <div class="team-sc" style="color:var(--t)">${s2}</div>
            </div>
          </div>
          <div class="match-meta-row" style="margin-top:14px">
            <div class="meta-chip">Match <strong>#${meta.matchid||id}</strong></div>
            ${fmtDate(meta.end_time)?`<div class="meta-chip">${fmtDate(meta.end_time)}</div>`:''}
            ${won?`<div class="meta-chip"><span class="winner-tag">Winner: ${esc(won)}</span></div>`:''}
            ${demoBtnHtml}
          </div>
        </div>
      </div>
    </div>
    ${mvpHtml}
    ${awardsHtml}
    ${mapsHtml}`;
}

function sbRows(arr, steamCache={}) {
  if (!arr.length) return `<tr><td colspan="15" style="text-align:center;color:var(--muted);padding:12px">—</td></tr>`;
  return [...arr].sort((a,b)=>{
    const sa = a.rating!=null?parseFloat(a.rating)*100:parseInt(a.kills||0);
    const sb2 = b.rating!=null?parseFloat(b.rating)*100:parseInt(b.kills||0);
    return sb2-sa;
  }).map(p=>{
    const kd     = p.deaths>0?(p.kills/p.deaths).toFixed(2):parseFloat(p.kills||0).toFixed(2);
    const adrVal = p.adr!=null?parseFloat(p.adr).toFixed(1):'—';
    const hsVal  = p.hs_pct!=null?parseFloat(p.hs_pct).toFixed(1)+'%':'—';
    const kastVal= p.kast!=null?parseFloat(p.kast).toFixed(1)+'%':'—';
    const fiveK  = p.enemies5k??0;
    const fourK  = p.enemies4k??0;
    const threeK = p.enemies3k??0;
    const rSty   = p.rating!=null?(parseFloat(p.rating)>=1.15?'style="color:#a78bfa;font-weight:700"':parseFloat(p.rating)<0.85?'style="color:var(--loss)"':''):'';
    const avatar = steamCache[p.steamid64]
      ? `<img src="${steamCache[p.steamid64]}" style="width:22px;height:22px;border-radius:50%;object-fit:cover;vertical-align:middle;margin-right:7px;border:1px solid var(--border2)" onerror="this.style.display='none'">`
      : `<span style="display:inline-block;width:22px;height:22px;border-radius:50%;background:var(--surface2);line-height:22px;text-align:center;font-size:9px;font-family:'Rajdhani',sans-serif;font-weight:700;color:var(--muted2);vertical-align:middle;margin-right:7px">${initials(p.name)}</span>`;
    return `<tr>
      <td onclick="go('player',{name:'${esc(p.name)}'},'match')" style="cursor:pointer"><div style="display:flex;align-items:center">${avatar}${esc(p.name)}</div></td>
      <td class="kda-cell">${p.kills??0}</td>
      <td class="kda-cell">${p.deaths??0}</td>
      <td class="kda-cell">${p.assists??0}</td>
      <td class="kda-cell ${kdc(kd)}">${kd}</td>
      <td class="adr-highlight">${adrVal}</td>
      <td>${hsVal}</td>
      <td>${num(p.damage)}</td>
      <td>${fiveK}</td>
      <td>${p.v1_wins??0}</td>
      <td ${rSty}>${p.rating!=null?parseFloat(p.rating).toFixed(2):'—'}</td>
      <td style="color:var(--muted2)">${kastVal}</td>
      <td style="color:var(--muted2)">${threeK}</td>
      <td style="color:var(--muted2)">${fourK}</td>
      <td style="font-size:11px;color:var(--muted2)">${p.opening_kills??'—'}/${p.opening_deaths??'—'}</td>
    </tr>`;
  }).join('');
}

// ── Player Profile ────────────────────────────────────────────────────────────
async function loadPlayer(name) {
  spin('p-player');
  const [data, ] = await Promise.all([
    fetch('/api/player/'+encodeURIComponent(name)).then(r=>r.json()).catch(()=>({})),
  ]);
  if (data.error) {
    document.getElementById('p-player').innerHTML = `<div class="empty">${data.error}</div>`;
    return;
  }
  const c = data.career || {};
  const recent = data.recent_matches || [];

  // Fetch Steam profile if steamid64 available
  let steam = {};
  if (c.steamid64 && c.steamid64 !== '0') {
    steam = await fetch('/api/steam/'+c.steamid64).then(r=>r.json()).catch(()=>({}));
  }

  const kd = parseFloat(c.kd ?? 0);
  const kdCls = kd>=1.3?'kd-g':kd>=0.9?'kd-n':'kd-b';

  // Avatar: Steam photo or initials fallback
  const avatarHtml = steam.avatar
    ? `<img src="${steam.avatar}" style="width:68px;height:68px;border-radius:4px;object-fit:cover;border:2px solid var(--border2)" alt="${esc(name)}">`
    : `<div class="p-avatar">${initials(name)}</div>`;

  const profileLink = steam.profile_url
    ? `<a href="${steam.profile_url}" target="_blank" style="color:var(--orange);text-decoration:none;font-size:11px">View Steam Profile ↗</a>`
    : '';

  const countryFlag = steam.country
    ? `<img src="https://flagcdn.com/24x18/${steam.country.toLowerCase()}.png" style="border-radius:2px;vertical-align:middle;margin-right:6px;height:16px" onerror="this.style.display='none'" title="${steam.country}"> `
    : '';

  const statsGrid = [
    {label:'Kills',     raw: c.kills,         dec:0},
    {label:'Deaths',    raw: c.deaths,        dec:0},
    {label:'Assists',   raw: c.assists,       dec:0},
    {label:'K/D',       raw: kd,              dec:2, cls:kdCls},
    {label:'ADR',       raw: c.adr,           dec:1},
    {label:'HS%',       raw: c.hs_pct,        dec:1, suffix:'%'},
    {label:'Matches',   raw: c.matches,       dec:0},
    {label:'Damage',    raw: c.total_damage,  dec:0},
    {label:'Aces (5K)', raw: c.aces,          dec:0},
    {label:'Clutches',  raw: c.clutch_1v1,    dec:0},
    {label:'Entry Wins',raw: c.entry_wins,    dec:0},
    {label:'Headshots', raw: c.headshots,     dec:0},
  ].map(s => {
    const v = s.raw ?? 0;
    const cls = s.cls ? ` class="${s.cls}"` : '';
    const valEl = `<span${cls} data-count="${v}" data-dec="${s.dec}" data-suffix="${s.suffix||''}">${s.dec>0?parseFloat(v).toFixed(s.dec):Number(v).toLocaleString()}${s.suffix||''}</span>`;
    return `<div class="stat-box"><div class="stat-val">${valEl}</div><div class="stat-lbl">${s.label}</div></div>`;
  }).join('');

  const recentRows = recent.map(m => {
    const won = m.winner && (m.winner.toLowerCase().includes(m.team?.toLowerCase() ?? ''));
    const result = won ? `<span style="color:var(--win)">W</span>` : `<span style="color:var(--loss)">L</span>`;
    const kd2 = m.deaths>0?(m.kills/m.deaths).toFixed(2):parseFloat(m.kills||0).toFixed(2);
    return `<tr onclick="go('match',{id:'${m.matchid}'},'player')">
      <td>${result} <span style="color:var(--muted);font-size:11px">#${m.matchid}</span></td>
      <td>${esc(m.mapname||'?')}</td>
      <td>${m.kills??0} / ${m.deaths??0} / ${m.assists??0}</td>
      <td class="${m.deaths>0&&m.kills/m.deaths>=1.3?'kd-g':m.deaths>0&&m.kills/m.deaths>=0.9?'kd-n':'kd-b'}">${kd2}</td>
      <td class="adr-highlight">${m.adr!=null?parseFloat(m.adr).toFixed(1):'—'}</td>
      <td>${m.hs_pct!=null?parseFloat(m.hs_pct).toFixed(1)+'%':'—'}</td>
      <td>${num(m.damage)}</td>
      <td style="font-size:11px;color:var(--muted2)">${m.team1_name??'?'} vs ${m.team2_name??'?'}</td>
    </tr>`;
  }).join('');

  document.getElementById('p-player').innerHTML = `
    <div class="back-btn" onclick="go(_back||'leaderboard')">← Back</div>
    <div class="profile-top">
      ${avatarHtml}
      <div style="flex:1">
        <div class="p-name">${countryFlag}${esc(steam.name || name)}</div>
        <div class="p-sub">${steam.real_name ? esc(steam.real_name)+' • ' : ''}${c.matches??0} matches played</div>
        <div style="margin-top:6px">${profileLink}</div>
      </div>
    </div>
    <div class="card" style="margin-bottom:12px">
      <div class="stats-grid">${statsGrid}</div>
    </div>
    ${recent.length ? `
    <div class="page-title" style="font-size:16px;margin-bottom:10px">Match History</div>
    <div class="card ovx">
      <table class="ph-table">
        <thead><tr>
          <th>Match</th><th>Map</th><th>K/D/A</th><th>K/D</th><th>ADR</th><th>HS%</th><th>Dmg</th><th>Teams</th>
        </tr></thead>
        <tbody>${recentRows}</tbody>
      </table>
    </div>` : ''}`;
}

// ── Map Stats ─────────────────────────────────────────────────────────────────
async function loadMaps() {
  const el = document.getElementById('p-maps');
  el.innerHTML = '<div class="loading"><div class="spin"></div><br>Loading…</div>';
  const data = await fetch('/api/mapstats').then(r=>r.json()).catch(()=>[]);
  if (!Array.isArray(data) || !data.length) {
    el.innerHTML = emptyState('maps','No Map Data','Play some matches to see map statistics'); return;
  }
  const cards = data.map(m => {
    const total = parseInt(m.total_matches||0);
    const mapImg = MAP_IMGS[m.mapname];
    return `
    <div style="position:relative;border-radius:6px;overflow:hidden;margin-bottom:12px;height:160px;cursor:default">
      ${mapImg
        ? `<img src="${mapImg}" style="position:absolute;inset:0;width:100%;height:100%;object-fit:cover;opacity:0.82">`
        : `<div style="position:absolute;inset:0;background:linear-gradient(135deg,#0d1117,#1a2332)"></div>`}
      <div style="position:absolute;inset:0;background:linear-gradient(to bottom,rgba(0,0,0,.08) 0%,rgba(0,0,0,.45) 100%)"></div>
      <div style="position:relative;z-index:2;height:100%;display:flex;flex-direction:column;justify-content:flex-end;padding:16px 20px">
        <div style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:13px;letter-spacing:3px;text-transform:uppercase;color:rgba(255,255,255,.55);margin-bottom:8px;text-shadow:0 1px 4px rgba(0,0,0,.8)">${esc(m.mapname)}</div>
        <div style="
          display:inline-flex;align-items:center;gap:10px;
          background:rgba(10,13,20,.45);
          backdrop-filter:blur(14px);
          -webkit-backdrop-filter:blur(14px);
          border:1px solid rgba(255,255,255,.1);
          border-radius:6px;
          padding:10px 18px;
          align-self:flex-start;
        ">
          <div style="font-family:'Rajdhani',sans-serif;font-weight:900;font-size:38px;color:#fff;line-height:1">${total}</div>
          <div style="font-size:11px;color:rgba(255,255,255,.6);text-transform:uppercase;letter-spacing:1.5px;line-height:1.4">Games<br>Played</div>
        </div>
      </div>
    </div>`;
  }).join('');
  el.innerHTML = `${cards}`;
}

// ── Head-to-Head ──────────────────────────────────────────────────────────────
let _h2hPlayers = null;
let _h2hSel = [null, null]; // [player1, player2]

async function loadH2H() {
  const el = document.getElementById('p-h2h');
  el.innerHTML = '<div class="loading"><div class="spin"></div><br>Loading players…</div>';
  if (!_h2hPlayers) {
    const data = await fetch('/api/leaderboard').then(r=>r.json()).catch(()=>[]);
    // Fetch Steam avatars for all
    await Promise.all(data.slice(0,30).map(async p => {
      if (p.steamid64 && p.steamid64 !== '0') {
        const s = await fetch('/api/steam/'+p.steamid64).then(r=>r.json()).catch(()=>({}));
        if (s.avatar) p._steam_avatar = s.avatar;
        if (s.name)   p._steam_name   = s.name;
      }
    }));
    _h2hPlayers = data;
  }
  renderH2HPicker();
}

function renderH2HPicker() {
  const el = document.getElementById('p-h2h');
  const players = _h2hPlayers || [];
  const [p1, p2] = _h2hSel;

  const slotHtml = (slot, p) => {
    if (p) {
      const av = p._steam_avatar
        ? `<img src="${p._steam_avatar}" style="width:48px;height:48px;border-radius:50%;object-fit:cover;border:2px solid ${slot===0?'var(--ct)':'var(--t)'}">`
        : `<div style="width:48px;height:48px;border-radius:50%;background:var(--surface2);border:2px solid ${slot===0?'var(--ct)':'var(--t)'};display:flex;align-items:center;justify-content:center;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:16px;color:${slot===0?'var(--ct)':'var(--t)'}">${initials(p.name)}</div>`;
      return `<div style="display:flex;flex-direction:column;align-items:center;gap:6px;flex:1">
        <div style="font-size:10px;color:var(--muted2);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:2px">Player ${slot+1}</div>
        ${av}
        <div style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:14px;color:#fff">${esc(p._steam_name||p.name)}</div>
        <button onclick="_h2hSel[${slot}]=null;renderH2HPicker()" class="h2h-clear-btn">✕ Clear</button>
      </div>`;
    }
    return `<div style="display:flex;flex-direction:column;align-items:center;gap:6px;flex:1">
      <div style="font-size:10px;color:var(--muted2);letter-spacing:1.5px;text-transform:uppercase;margin-bottom:2px">Player ${slot+1}</div>
      <div style="width:48px;height:48px;border-radius:50%;border:2px dashed ${slot===0?'rgba(56,189,248,.4)':'rgba(251,146,60,.4)'};display:flex;align-items:center;justify-content:center">
        <span style="font-size:22px;color:var(--muted2)">?</span>
      </div>
      <div style="font-size:12px;color:var(--muted2)">Select below</div>
    </div>`;
  };

  const canCompare = p1 && p2 && p1.name !== p2.name;

  const cards = players.map((p,i) => {
    const isP1 = p1 && p1.name === p.name;
    const isP2 = p2 && p2.name === p.name;
    const selected = isP1 || isP2;
    const av = p._steam_avatar
      ? `<img src="${p._steam_avatar}" style="width:38px;height:38px;border-radius:50%;object-fit:cover;flex-shrink:0">`
      : `<div style="width:38px;height:38px;border-radius:50%;background:var(--surface2);flex-shrink:0;display:flex;align-items:center;justify-content:center;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:13px;color:var(--muted2)">${initials(p.name)}</div>`;
    const badge = isP1
      ? `<span style="font-size:9px;padding:1px 6px;border-radius:2px;background:rgba(56,189,248,.2);border:1px solid rgba(56,189,248,.4);color:var(--ct);font-family:'Rajdhani',sans-serif;font-weight:700;letter-spacing:1px">P1</span>`
      : isP2
      ? `<span style="font-size:9px;padding:1px 6px;border-radius:2px;background:rgba(251,146,60,.2);border:1px solid rgba(251,146,60,.4);color:var(--t);font-family:'Rajdhani',sans-serif;font-weight:700;letter-spacing:1px">P2</span>`
      : '';
    return `<div onclick="h2hSelect(${i})" class="h2h-row${selected?' selected':''}">
      ${av}
      <div style="flex:1;min-width:0">
        <div style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:14px;color:#fff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${esc(p._steam_name||p.name)}</div>
        <div style="font-size:11px;color:var(--muted2)">${p.kills??0} kills · ${parseFloat(p.kd??0).toFixed(2)} K/D · ${p.matches??0} matches</div>
      </div>
      ${badge}
    </div>`;
  }).join('');

  el.innerHTML = `
    <div class="h2h-top-card" style="margin-bottom:12px;overflow:hidden">
      <div id="h2h-picker-inner">
        <div style="display:grid;grid-template-columns:1fr 48px 1fr;align-items:center;gap:12px;margin-bottom:18px">
          ${slotHtml(0,p1)}
          <div style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:20px;color:var(--muted);text-align:center">VS</div>
          ${slotHtml(1,p2)}
        </div>
        <button onclick="runH2H()" ${canCompare?'':'disabled'} class="compare-btn ${canCompare?'active':'inactive'}">
          ${canCompare?'Compare':'Select 2 players to compare'}
        </button>
      </div>
      <div class="h2h-result-body" id="h2h-result-body"></div>
    </div>
    <div class="card" style="padding:8px 8px;max-height:380px;overflow-y:auto">${cards}</div>`;
}

function h2hSelect(idx) {
  const p = _h2hPlayers[idx];
  if (_h2hSel[0] && _h2hSel[0].name === p.name) { _h2hSel[0] = null; }
  else if (_h2hSel[1] && _h2hSel[1].name === p.name) { _h2hSel[1] = null; }
  else if (!_h2hSel[0]) { _h2hSel[0] = p; }
  else if (!_h2hSel[1]) { _h2hSel[1] = p; }
  else { _h2hSel[0] = _h2hSel[1]; _h2hSel[1] = p; }
  renderH2HPicker();
}

async function runH2H() {
  const [p1, p2] = _h2hSel;
  const body = document.getElementById('h2h-result-body');
  if (!p1 || !p2 || !body) return;

  // Show loading state inside the expanding body
  body.innerHTML = '<div class="loading" style="padding:20px"><div class="spin"></div><br>Loading…</div>';
  body.classList.add('open');

  // Collapse picker
  const picker = document.getElementById('h2h-picker-inner');
  if (picker) { picker.style.maxHeight = picker.scrollHeight + 'px'; picker.style.overflow='hidden'; picker.style.transition='max-height .35s ease,opacity .25s ease'; requestAnimationFrame(()=>{ picker.style.maxHeight='0'; picker.style.opacity='0'; }); }

  const data = await fetch(`/api/h2h?p1=${encodeURIComponent(p1.name)}&p2=${encodeURIComponent(p2.name)}`).then(r=>r.json()).catch(()=>({}));

  if (data.error || !data.p1 || !data.p2) {
    body.innerHTML = `<div class="empty" style="padding:16px">${data.error || 'One or both players not found.'}</div>`; return;
  }

  const d1 = data.p1, d2 = data.p2;
  const st1 = {avatar: p1._steam_avatar, name: p1._steam_name};
  const st2 = {avatar: p2._steam_avatar, name: p2._steam_name};

  function avatar(p, st, size=56) {
    return st.avatar
      ? `<img src="${st.avatar}" style="width:${size}px;height:${size}px;border-radius:50%;object-fit:cover;border:2px solid var(--border2)">`
      : `<div style="width:${size}px;height:${size}px;border-radius:50%;background:linear-gradient(135deg,var(--orange),var(--orange2));display:flex;align-items:center;justify-content:center;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:${Math.floor(size*.35)}px;color:#000">${initials(p.name)}</div>`;
  }

  const stats = [
    {label:'Matches',   k:'matches'},
    {label:'Kills',     k:'kills'},
    {label:'Deaths',    k:'deaths'},
    {label:'Assists',   k:'assists'},
    {label:'K/D',       k:'kd',      dec:2},
    {label:'ADR',       k:'adr',     dec:1},
    {label:'HS%',       k:'hs_pct',  dec:1, suffix:'%'},
    {label:'Damage',    k:'damage'},
    {label:'Aces',      k:'aces'},
    {label:'Clutches',  k:'clutch_wins'},
    {label:'Entry Wins',k:'entry_wins'},
  ];

  const rows = stats.map(s => {
    const v1 = parseFloat(d1[s.k]??0), v2 = parseFloat(d2[s.k]??0);
    const fmt = v => s.dec ? v.toFixed(s.dec)+(s.suffix||'') : Number(v).toLocaleString()+(s.suffix||'');
    const w1 = v1 > v2, w2 = v2 > v1;
    return `<tr style="border-bottom:1px solid var(--border)">
      <td style="padding:10px 14px;text-align:right;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:16px;color:${w1?'var(--win)':'var(--white)'}">${fmt(v1)}</td>
      <td style="padding:10px 14px;text-align:center;font-size:10px;color:var(--muted2);letter-spacing:1px;text-transform:uppercase;white-space:nowrap">${s.label}</td>
      <td style="padding:10px 14px;text-align:left;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:16px;color:${w2?'var(--win)':'var(--white)'}">${fmt(v2)}</td>
    </tr>`;
  }).join('');

  body.innerHTML = `
    <div style="border-top:1px solid rgba(255,85,0,.15);margin:0 -20px"></div>
    <div style="display:grid;grid-template-columns:1fr 60px 1fr;align-items:center;padding:20px 14px 14px">
      <div style="display:flex;flex-direction:column;align-items:center;gap:8px;cursor:pointer" onclick="go('player',{name:'${esc(d1.name)}'},'h2h')">
        ${avatar(d1,st1)}
        <div style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:16px;color:var(--ct);text-align:center">${esc(st1.name||d1.name)}</div>
      </div>
      <div style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:20px;color:var(--muted);text-align:center">VS</div>
      <div style="display:flex;flex-direction:column;align-items:center;gap:8px;cursor:pointer" onclick="go('player',{name:'${esc(d2.name)}'},'h2h')">
        ${avatar(d2,st2)}
        <div style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:16px;color:var(--t);text-align:center">${esc(st2.name||d2.name)}</div>
      </div>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <tbody>${rows}</tbody>
    </table>
    <div style="padding:12px 0 4px;text-align:center">
      <button onclick="h2hBack()" class="h2h-clear-btn" style="padding:6px 20px;font-size:12px;letter-spacing:1.5px">← Back</button>
    </div>`;
}

function h2hBack() {
  // Collapse result, restore picker
  const body = document.getElementById('h2h-result-body');
  const picker = document.getElementById('h2h-picker-inner');
  if (body) { body.classList.remove('open'); setTimeout(()=>{ body.innerHTML=''; },400); }
  if (picker) {
    picker.style.maxHeight = '500px';
    picker.style.opacity = '1';
  }
}

// ── Leaderboard ───────────────────────────────────────────────────────────────
let _lbSort = 'kills';
async function loadLeaderboard() {
  const el = document.getElementById('p-leaderboard');
  el.innerHTML = '<div class="loading"><div class="spin"></div><br>Loading…</div>';
  const data = await fetch('/api/leaderboard').then(r=>r.json()).catch(()=>[]);
  if (!Array.isArray(data) || !data.length) {
    el.innerHTML = emptyState('leaderboard','No Player Data','Stats will appear after matches are completed');
    return;
  }
  // Fetch Steam avatars in parallel (top 20 only to keep it fast)
  await Promise.all(data.slice(0, 20).map(async p => {
    if (p.steamid64 && p.steamid64 !== '0') {
      try {
        const s = await fetch('/api/steam/'+p.steamid64).then(r=>r.json()).catch(()=>({}));
        if (s.avatar) p._steam_avatar = s.avatar;
        if (s.name)   p._steam_name   = s.name;
      } catch(_) {}
    }
  }));
  renderLeaderboard(data, _lbSort);
}

function renderLeaderboard(data, sortKey) {
  _lbSort = sortKey;
  const el = document.getElementById('p-leaderboard');

  const sorted = [...data].sort((a,b) => {
    const av = parseFloat(a[sortKey]??0), bv = parseFloat(b[sortKey]??0);
    return bv - av;
  });

  const cols = [
    {key:'kills',      label:'Kills'},
    {key:'deaths',     label:'Deaths'},
    {key:'assists',    label:'Assists'},
    {key:'kd',         label:'K/D'},
    {key:'adr',        label:'ADR'},
    {key:'hs_pct',     label:'HS%'},
    {key:'damage',     label:'Damage'},
    {key:'matches',    label:'Matches'},
    {key:'aces',       label:'5K'},
    {key:'clutch_wins',label:'Clutches'},
  ];

  const thStyle = k => k===sortKey
    ? 'style="color:var(--orange);cursor:pointer"'
    : 'style="cursor:pointer"';

  const headers = cols.map(c =>
    `<th ${thStyle(c.key)} onclick="renderLeaderboard(window._lbData,'${c.key}')">${c.label}</th>`
  ).join('');

  const rows = sorted.map((p, i) => {
    const rank = i + 1;
    const rankCls = rank===1?'rank-gold':rank===2?'rank-silver':rank===3?'rank-bronze':'';
    const kd = parseFloat(p.kd??0);
    const kdCls = kd>=1.3?'kd-g':kd>=0.9?'kd-n':'kd-b';
    const hsPct = parseFloat(p.hs_pct??0);
    const hsBar = `<div class="hs-bar-wrap">
      <div class="hs-bar"><div class="hs-bar-fill" style="width:${Math.min(hsPct,100)}%"></div></div>
      <span class="hs-val">${hsPct.toFixed(1)}%</span>
    </div>`;
    const avatarEl = p._steam_avatar
      ? `<img src="${p._steam_avatar}" style="width:24px;height:24px;border-radius:50%;object-fit:cover;vertical-align:middle;margin-right:8px;border:1px solid var(--border2)" alt="">`
      : `<span style="display:inline-block;width:24px;height:24px;border-radius:50%;background:var(--surface2);vertical-align:middle;margin-right:8px;text-align:center;line-height:24px;font-size:10px;font-family:'Rajdhani',sans-serif;font-weight:700;color:var(--muted2)">${initials(p.name)}</span>`;
    const displayName = esc(p._steam_name || p.name);
    return `<tr class="${rankCls}" onclick="go('player',{name:'${esc(p.name)}'},'leaderboard')">
      <td style="${rank<=3?`color:${['var(--orange)','#a0aec0','#b87333'][rank-1]};font-family:'Rajdhani',sans-serif;font-weight:800;font-size:15px`:'color:var(--muted2)'}">${rank}</td>
      <td>${avatarEl}<span class="pname">${displayName}</span></td>
      <td>${p.kills??0}</td>
      <td>${p.deaths??0}</td>
      <td>${p.assists??0}</td>
      <td class="kd-num ${kdCls}">${kd.toFixed(2)}</td>
      <td class="adr-highlight">${p.adr!=null?parseFloat(p.adr).toFixed(1):'—'}</td>
      <td>${hsBar}</td>
      <td>${p.damage!=null?Number(p.damage).toLocaleString():'—'}</td>
      <td>${p.matches??0}</td>
      <td>${p.aces??0}</td>
      <td>${p.clutch_wins??0}</td>
    </tr>`;
  }).join('');

  // Top 3 podium cards
  const top3 = sorted.slice(0,3);
  const podiumCard = (p, rank) => {
    if (!p) return '';
    const colors      = ['var(--orange)','#a0aec0','#b87333'];
    const glowColors  = ['rgba(255,85,0,.18)','rgba(160,174,192,.14)','rgba(184,115,51,.15)'];
    const borderColors= ['rgba(255,85,0,.35)','rgba(160,174,192,.3)','rgba(184,115,51,.3)'];
    const rankLabels  = ['1ST','2ND','3RD'];
    const c = colors[rank];
    const avatarEl = p._steam_avatar
      ? `<img src="${p._steam_avatar}" style="width:56px;height:56px;border-radius:50%;object-fit:cover;border:2px solid ${c};margin-bottom:10px" alt="${esc(p.name)}">`
      : `<div style="width:56px;height:56px;border-radius:50%;background:var(--surface);border:2px solid ${c};display:flex;align-items:center;justify-content:center;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:20px;color:${c};margin:0 auto 10px">${initials(p.name)}</div>`;
    return `<div class="podium-card" data-rank="${rank}" style="--card-color:${c};--glow:${glowColors[rank]};--bdr:${borderColors[rank]};flex:1;background:${glowColors[rank]};backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border:1px solid ${borderColors[rank]};border-top:2px solid ${c};border-radius:6px;padding:18px 14px;text-align:center;cursor:pointer;position:relative;overflow:hidden;" onclick="go('player',{name:'${esc(p.name)}'},'leaderboard')">
      <div class="podium-shine"></div>
      <div style="position:absolute;top:10px;left:14px;font-family:'Rajdhani',sans-serif;font-weight:800;font-size:11px;letter-spacing:2px;color:${c};opacity:.9">${rankLabels[rank]}</div>
      <div style="margin-top:6px;position:relative;z-index:1">${avatarEl}</div>
      <div style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:15px;color:#fff;margin-bottom:12px;position:relative;z-index:1">${esc(p._steam_name||p.name)}</div>
      <div style="display:flex;justify-content:center;gap:16px;flex-wrap:wrap;position:relative;z-index:1">
        <div><div style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:20px;color:#fff" data-count="${p.kills??0}" data-dec="0">${p.kills??0}</div><div style="font-size:10px;color:rgba(255,255,255,.5);letter-spacing:1px;text-transform:uppercase">Kills</div></div>
        <div><div style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:20px;color:#fff" data-count="${parseFloat(p.kd??0).toFixed(2)}" data-dec="2">${parseFloat(p.kd??0).toFixed(2)}</div><div style="font-size:10px;color:rgba(255,255,255,.5);letter-spacing:1px;text-transform:uppercase">K/D</div></div>
        <div><div style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:20px;color:#fff" data-count="${p.matches??0}" data-dec="0">${p.matches??0}</div><div style="font-size:10px;color:rgba(255,255,255,.5);letter-spacing:1px;text-transform:uppercase">Matches</div></div>
      </div>
    </div>`;
  };

  window._lbData = data;

  const sortLabel = cols.find(c=>c.key===sortKey)?.label ?? sortKey;

  el.innerHTML = `
    <div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap">
      ${top3.map((p,i)=>podiumCard(p,i)).join('')}
    </div>
    <div class="card lb-wrap">
      <table class="lb-table">
        <thead><tr>
          <th>#</th><th>Player</th>${headers}
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

// ── Specialist Boards ─────────────────────────────────────────────────────────
let _specTab = 'clutch';
let _specData = null;
async function loadSpecialists() {
  const el = document.getElementById('p-specialists');
  if (_specData) { renderSpecialists(_specData, _specTab); return; }
  el.innerHTML = '<div class="loading"><div class="spin"></div><br>Loading…</div>';
  const data = await fetch('/api/specialists').then(r=>r.json()).catch(()=>[]);
  if (!Array.isArray(data) || !data.length) {
    el.innerHTML = emptyState('specialists','No Specialist Data','Clutch kings, entry fraggers and more will appear here'); return;
  }
  // Fetch Steam avatars for all players
  await Promise.all(data.map(async p => {
    if (p.steamid64 && p.steamid64 !== '0') {
      try {
        const s = await fetch('/api/steam/'+p.steamid64).then(r=>r.json()).catch(()=>({}));
        if (s.avatar) p._steam_avatar = s.avatar;
      } catch(_) {}
    }
  }));
  _specData = data;
  renderSpecialists(data, _specTab);
}
function renderSpecialists(data, tab) {
  _specTab = tab;
  const el = document.getElementById('p-specialists');
  const tabs = [
    {id:'clutch',  label:'Clutch Kings'},
    {id:'entry',   label:'Entry Fraggers'},
    {id:'flash',   label:'Flash Kings'},
    {id:'utility', label:'Utility'},
  ];
  const tabsHtml = tabs.map(t => `
    <div onclick="renderSpecialists(window._specData,'${t.id}')"
      style="padding:8px 20px;border-radius:3px;cursor:pointer;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:13px;letter-spacing:1.5px;text-transform:uppercase;transition:all .15s;${tab===t.id?'background:var(--orange);color:#000;':'background:var(--surface2);color:var(--muted2);border:1px solid var(--border);'}">${t.label}</div>
  `).join('');

  let sorted, cols, getValue;
  if (tab === 'clutch') {
    sorted = [...data].sort((a,b) => (b.clutch_total||0)-(a.clutch_total||0));
    cols = [{label:'1v1 Wins'},{label:'1v2 Wins'},{label:'Total Clutches'}];
    getValue = p => `<td style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:18px;color:var(--ct);text-align:center">${p.clutch_1v1??0}</td><td style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:18px;color:var(--t);text-align:center">${p.clutch_1v2??0}</td><td style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:20px;color:var(--orange);text-align:center">${p.clutch_total??0}</td>`;
  } else if (tab === 'entry') {
    sorted = [...data].sort((a,b) => (b.entry_wins||0)-(a.entry_wins||0));
    cols = [{label:'Entry Wins'},{label:'Attempts'},{label:'Success Rate'}];
    getValue = p => `<td style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:20px;color:var(--orange);text-align:center">${p.entry_wins??0}</td><td style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:18px;color:var(--muted2);text-align:center">${p.entry_attempts??0}</td><td style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:18px;color:var(--win);text-align:center">${p.entry_rate!=null?p.entry_rate+'%':'—'}</td>`;
  } else if (tab === 'flash') {
    sorted = [...data].sort((a,b) => (b.flash_successes||0)-(a.flash_successes||0));
    cols = [{label:'Total Flashes'},{label:'Per Map'},{label:'Matches'}];
    getValue = p => `<td style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:20px;color:var(--orange);text-align:center">${p.flash_successes??0}</td><td style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:18px;color:var(--win);text-align:center">${p.flashes_per_map??'—'}</td><td style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:18px;color:var(--muted2);text-align:center">${p.matches??0}</td>`;
  } else {
    sorted = [...data].sort((a,b) => (b.utility_damage||0)-(a.utility_damage||0));
    cols = [{label:'Total Util DMG'},{label:'Per Map'},{label:'Matches'}];
    getValue = p => `<td style="font-family:'Rajdhani',sans-serif;font-weight:800;font-size:20px;color:var(--orange);text-align:center">${p.utility_damage!=null?Number(p.utility_damage).toLocaleString():'—'}</td><td style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:18px;color:var(--win);text-align:center">${p.util_dmg_per_map??'—'}</td><td style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:18px;color:var(--muted2);text-align:center">${p.matches??0}</td>`;
  }

  const rankColors = ['var(--orange)','#a0aec0','#b87333'];
  const headerCols = cols.map(c=>`<th style="padding:8px 16px;text-align:center;font-family:'Rajdhani',sans-serif;font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--muted2)">${c.label}</th>`).join('');
  const rows = sorted.map((p,i) => {
    const rank = i+1;
    const rankStr = rank <= 3
      ? `<span style="font-size:18px;color:${rankColors[rank-1]};font-family:'Rajdhani',sans-serif;font-weight:800">${rank}</span>`
      : `<span style="font-size:14px;color:var(--muted2);font-family:'Rajdhani',sans-serif;font-weight:700">${rank}</span>`;
    const avatarEl = p._steam_avatar
      ? `<img src="${p._steam_avatar}" style="width:30px;height:30px;border-radius:50%;object-fit:cover;vertical-align:middle;margin-right:10px;border:1px solid var(--border2)" alt="">`
      : `<span style="display:inline-block;width:30px;height:30px;border-radius:50%;background:var(--surface2);vertical-align:middle;margin-right:10px;text-align:center;line-height:30px;font-size:11px;font-family:'Rajdhani',sans-serif;font-weight:700;color:var(--muted2)">${initials(p.name)}</span>`;
    const rankCls = rank===1?'spec-rank-gold':rank===2?'spec-rank-silver':rank===3?'spec-rank-bronze':'';
    return `<tr class="${rankCls}" onclick="go('player',{name:'${esc(p.name)}'},'specialists')" style="cursor:pointer;border-bottom:1px solid var(--border)">
      <td style="padding:10px 14px;text-align:center;width:48px">${rankStr}</td>
      <td style="padding:10px 14px">${avatarEl}<span style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:15px;color:var(--white);vertical-align:middle">${esc(p.name)}</span></td>
      ${getValue(p)}
    </tr>`;
  }).join('');

  window._specData = data;
  el.innerHTML = `
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px">${tabsHtml}</div>
    <div class="card lb-wrap">
      <table class="lb-table" style="width:100%;border-collapse:collapse">
        <thead><tr style="background:rgba(0,0,0,.35)">
          <th style="padding:8px 14px;width:48px"></th>
          <th style="padding:8px 14px;text-align:left;font-family:'Rajdhani',sans-serif;font-size:10px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:var(--muted2)">Player</th>
          ${headerCols}
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>`;
}

// ── Demo Browser ──────────────────────────────────────────────────────────────
let _demoData = null;
let _demoFilters = {map:''};
async function loadDemos() {
  const el = document.getElementById('p-demos');
  if (_demoData) { renderDemos(); return; }
  el.innerHTML = '<div class="loading"><div class="spin"></div><br>Loading demos…</div>';
  const data = await fetch('/api/demos').then(r=>r.json()).catch(()=>[]);
  if (!Array.isArray(data) || !data.length) {
    el.innerHTML = emptyState('demos','No Demos Found','Demo files will appear here once uploaded'); return;
  }
  _demoData = data;
  renderDemos();
}
function renderDemos() {
  const el = document.getElementById('p-demos');
  const data = _demoData || [];
  const maps = [...new Set(data.map(d=>d.mapname).filter(Boolean))].sort();
  const mapOpts = ['<option value="">All Maps</option>', ...maps.map(m=>`<option value="${esc(m)}" ${_demoFilters.map===m?'selected':''}>${esc(m)}</option>`)].join('');

  const filtered = data.filter(d => !_demoFilters.map || d.mapname === _demoFilters.map);

  const rows = filtered.map(d => {
    // Format date + time from filename_ts
    let dateStr = '—', timeStr = '';
    if (d.filename_ts) {
      const dt = new Date(d.filename_ts);
      dateStr = dt.toLocaleDateString('en-GB', {day:'2-digit', month:'short', year:'numeric'});
      timeStr = dt.toLocaleTimeString('en-GB', {hour:'2-digit', minute:'2-digit'});
    } else if (d.modified_at) {
      dateStr = d.modified_at;
    }

    const mapImg = d.mapname && MAP_IMGS[d.mapname] ? MAP_IMGS[d.mapname] : null;

    const score = (d.team1_score !== '' && d.team1_score != null)
      ? `<span style="color:var(--ct);font-family:'Rajdhani',sans-serif;font-weight:800;font-size:26px">${d.team1_score}</span><span style="color:rgba(255,255,255,.4);margin:0 8px;font-family:'Rajdhani',sans-serif;font-size:22px">:</span><span style="color:var(--t);font-family:'Rajdhani',sans-serif;font-weight:800;font-size:26px">${d.team2_score}</span>`
      : '';

    const teamsLine = (d.team1_name || d.team2_name)
      ? `<div style="font-size:13px;color:rgba(255,255,255,.7);margin-top:4px">
           <span style="color:var(--ct)">${esc(d.team1_name||'?')}</span>
           <span style="margin:0 6px;color:rgba(255,255,255,.35)">vs</span>
           <span style="color:var(--t)">${esc(d.team2_name||'?')}</span>
         </div>`
      : '';

    return `<div class="demo-card">
      ${mapImg
        ? `<img class="demo-bg-img" src="${mapImg}">`
        : `<div style="position:absolute;inset:0;background:linear-gradient(135deg,#0a0c0e,#141618)"></div>`}
      <div class="demo-overlay"></div>
      <div class="demo-hover-layer"></div>
      <div class="demo-content">
        <div style="flex:1;min-width:0">
          <div class="demo-map-label">${esc(d.mapname||d.name)}</div>
          <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap">
            ${score}
            ${teamsLine}
          </div>
          <div style="font-size:11px;color:rgba(255,255,255,.35);margin-top:5px;text-shadow:0 1px 3px rgba(0,0,0,.9)">${dateStr}${timeStr?' · '+timeStr:''} · ${esc(d.size_formatted||'')}</div>
        </div>
        <a href="${esc(d.download_url)}" download style="text-decoration:none;flex-shrink:0">
          <button class="demo-dl-btn">Download</button>
        </a>
      </div>
    </div>`;
  }).join('') || emptyState('demos','No Demos Match Your Filter','Try clearing the map filter');

  el.innerHTML = `
    <div style="padding:14px 0;margin-bottom:10px;display:flex;gap:10px;align-items:center">
      <select onchange="_demoFilters.map=this.value;renderDemos()"
        style="padding:9px 12px;background:var(--surface2);border:1px solid var(--border);border-radius:3px;color:var(--white);font-family:'Rajdhani',sans-serif;font-size:13px;cursor:pointer;min-width:160px">${mapOpts}</select>
      <button onclick="_demoFilters={map:''};renderDemos()"
        style="padding:9px 14px;background:transparent;border:1px solid var(--border);border-radius:3px;color:var(--muted2);font-family:'Rajdhani',sans-serif;font-size:12px;cursor:pointer;letter-spacing:1px">✕ Clear</button>
    </div>
    ${rows}`;
}

// ── Pointer shine effect on glassy cards ─────────────────────────────────────
function getShineTarget(el) {
  // For ::after pseudo-elements (mvp-card, award-card, profile-top) we set vars on el itself.
  // For podium-card we set on the .podium-shine child div.
  return el.classList.contains('podium-card') ? el.querySelector('.podium-shine') : el;
}

function setShinePos(el, x, y) {
  el.style.setProperty('--sx', x.toFixed(1) + '%');
  el.style.setProperty('--sy', y.toFixed(1) + '%');
}

function animateShineToCenter(el, fromX, fromY) {
  // Animate --sx/--sy from current position back to 50% 50% over ~400ms
  const duration = 400;
  const startTime = performance.now();
  const targetX = 50, targetY = 50;

  function step(now) {
    const t = Math.min((now - startTime) / duration, 1);
    const ease = 1 - Math.pow(1 - t, 3); // ease-out cubic
    const x = fromX + (targetX - fromX) * ease;
    const y = fromY + (targetY - fromY) * ease;
    setShinePos(el, x, y);
    if (t < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
}

function attachShineToEl(el) {
  if (el._shineAttached) return;
  el._shineAttached = true;

  let curX = 50, curY = 50;
  let leaveTimer = null;

  // Show glow
  function showGlow() {
    if (leaveTimer) { clearTimeout(leaveTimer); leaveTimer = null; }
    el.style.setProperty('--shine-opacity', '1');
    // For pseudo-element cards, force opacity via a class
    el.classList.add('shine-active');
  }

  // Hide glow with fade + position animation
  function hideGlow() {
    el.classList.remove('shine-active');
    animateShineToCenter(getShineTarget(el), curX, curY);
  }

  // Track position
  function track(clientX, clientY) {
    const r = el.getBoundingClientRect();
    curX = ((clientX - r.left) / r.width  * 100);
    curY = ((clientY - r.top)  / r.height * 100);
    setShinePos(getShineTarget(el), curX, curY);
  }

  // Mouse
  el.addEventListener('mouseenter', () => showGlow());
  el.addEventListener('mousemove',  e => track(e.clientX, e.clientY));
  el.addEventListener('mouseleave', () => hideGlow());

  // Touch
  el.addEventListener('touchstart', e => {
    const t = e.touches[0];
    track(t.clientX, t.clientY);
    showGlow();
  }, {passive:true});
  el.addEventListener('touchmove', e => {
    const t = e.touches[0];
    track(t.clientX, t.clientY);
  }, {passive:true});
  el.addEventListener('touchend', () => hideGlow());
}

function attachShine(selector) {
  document.querySelectorAll(selector).forEach(el => attachShineToEl(el));
}

// Re-attach whenever new content is injected
const _shineObs = new MutationObserver(() => {
  attachShine('.mvp-card');
  attachShine('.award-card');
  attachShine('.profile-top');
  attachShine('.podium-card');
  attachShine('.h2h-top-card');
});
_shineObs.observe(document.getElementById('app'), {childList:true, subtree:true});

// Check for ?match= URL param to deep-link
const urlParams = new URLSearchParams(location.search);
if(urlParams.get('match')) go('match',{id:urlParams.get('match')});
else go('matches');
</script>
</body>
</html>"""
async def start_http_server():
    app = web.Application()
    app.router.add_get('/api/specialists',    handle_api_specialists)
    app.router.add_get('/api/player/{name}', handle_api_player)
    app.router.add_get('/api/steam/{steamid64}', handle_api_steam)
    app.router.add_get('/api/matches',       handle_api_matches)
    app.router.add_get('/api/match/{matchid}', handle_api_match)
    app.router.add_get('/api/demos',          handle_api_demos)
    app.router.add_get('/api/leaderboard',   handle_api_leaderboard)
    app.router.add_get('/api/mapstats',      handle_api_mapstats)
    app.router.add_get('/api/h2h',           handle_api_h2h)
    app.router.add_get('/api/status',        handle_api_status)
    app.router.add_get('/stats',             handle_stats_page)
    app.router.add_get('/',                  handle_stats_page)
    app.router.add_get('/health',            handle_health_check)
    
    port = int(os.getenv('PORT', 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    print(f"✓ HTTP log endpoint started on port {port}")
    return runner

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
            matchid_map = build_matchid_to_demo_map()
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
    
    # Build matchid -> demo mapping once using .json files
    try:
        matchid_map = build_matchid_to_demo_map()
        debug_lines = [f"📂 Demos mapped via .json files: **{len(matchid_map)}**"]
    except Exception as e:
        matchid_map = {}
        debug_lines = [f"⚠️ Could not build matchid map: {e}"]

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


if not TOKEN:
    raise SystemExit("TOKEN missing.")

bot.run(TOKEN)
