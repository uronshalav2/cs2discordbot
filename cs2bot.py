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

async def handle_api_player(request):
    """GET /api/player/{name} — full career stats, MatchZy primary / fshost fallback"""
    name = request.match_info.get('name', '')
    loop = asyncio.get_running_loop()

    # ── Helper: build career + recent from fshost data ────────────────────────
    def _fshost_career_for(lookup_name):
        all_rows = _aggregate_stats_from_fshost()
        # Find by name or edited name
        name_map = _edited_name_map()
        # reverse edited map: edited_name -> steamid
        edited_sid = next((s for s, n in name_map.items() if n == lookup_name), None)
        row = next((r for r in all_rows if r['name'] == lookup_name
                    or to_steamid64(str(r['steamid64'])) == to_steamid64(str(edited_sid or ''))), None)
        if not row:
            return None, []
        row_sid64 = to_steamid64(str(row['steamid64']))
        career = {
            'name':           name_map.get(row_sid64, row['name']),
            'steamid64':      row_sid64,
            'matches':        row['matches'],
            'kills':          row['kills'],
            'deaths':         row['deaths'],
            'assists':        row['assists'],
            'headshots':      row['headshots'],
            'total_damage':   row['damage'],
            'aces':           row['aces'],
            'quads':          row['quads'],
            'clutch_1v1':     row['clutch_1v1'],
            'clutch_1v2':     row['clutch_1v2'],
            'entry_wins':     row['entry_wins'],
            'kd':             row['kd'],
            'hs_pct':         row['hs_pct'],
            'adr':            row['adr'],
        }
        # Build recent matches from fshost
        recent = []
        matchid_map = build_matchid_to_demo_map()
        sid = to_steamid64(str(row['steamid64']))
        for mid, entry in matchid_map.items():
            meta = entry.get('metadata')
            if not meta:
                continue
            for team_key in ('team1', 'team2'):
                team = meta.get(team_key, {})
                for fp in team.get('players', []):
                    fp_sid = to_steamid64(str(fp.get('steam_id') or fp.get('steamid64') or '0'))
                    if fp_sid != sid:
                        continue
                    k = int(fp.get('kills',0) or 0)
                    d = int(fp.get('deaths',0) or 0)
                    hs = int(fp.get('headshot_kills',0) or fp.get('head_shot_kills',0) or 0)
                    dmg = int(fp.get('damage',0) or 0)
                    t1 = meta.get('team1', {})
                    t2 = meta.get('team2', {})
                    recent.append({
                        'matchid':    str(mid),
                        'mapnumber':  1,
                        'team':       team_key,
                        'steamid64':  sid,
                        'kills':      k,
                        'deaths':     d,
                        'assists':    int(fp.get('assists',0) or 0),
                        'damage':     dmg,
                        'head_shot_kills': hs,
                        'enemies5k':  int(fp.get('5k',0) or 0),
                        'v1_wins':    0,
                        'mapname':    meta.get('map','?'),
                        'winner':     meta.get('winner',''),
                        'team1_score': t1.get('score',0),
                        'team2_score': t2.get('score',0),
                        'team1_name':  t1.get('name','Team 1'),
                        'team2_name':  t2.get('name','Team 2'),
                        'adr':        round(dmg/30, 1),
                        'hs_pct':     round(hs/k*100,1) if k else 0.0,
                    })
        recent = _patch_recent_matches(recent)
        recent.sort(key=lambda r: str(r.get('matchid','')), reverse=True)
        return career, recent[:20]

    # ── Try MatchZy first ─────────────────────────────────────────────────────
    career = None
    recent = []
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)

        # Step 1: resolve steamid64 for this player name.
        # A player may have changed their name, so rows exist under multiple names
        # all sharing the same steamid64. We resolve the SID first, then aggregate
        # ALL rows by SID so no matches are missed.
        name_map = _edited_name_map()

        # Check edited name map first (highest priority)
        resolved_sid = next((sid for sid, n in name_map.items() if n == name), None)

        # If not in edit map, look up any row with this exact name
        if not resolved_sid:
            c.execute(f"""
                SELECT steamid64 FROM {MATCHZY_TABLES['players']}
                WHERE name = %s AND steamid64 != '0'
                LIMIT 1
            """, (name,))
            row = c.fetchone()
            if row:
                resolved_sid = to_steamid64(str(row['steamid64']))

        # Aggregate ALL rows for this steamid64 regardless of name changes.
        # Use sid_variants() so we match both SteamID64 and SteamID32 stored forms.
        if resolved_sid:
            sid64, sid32 = sid_variants(resolved_sid)
            c.execute(f"""
                SELECT
                    SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS name,
                    COUNT(DISTINCT matchid)                                      AS matches,
                    SUM(kills)                                                   AS kills,
                    SUM(deaths)                                                  AS deaths,
                    SUM(assists)                                                 AS assists,
                    SUM(head_shot_kills)                                         AS headshots,
                    SUM(damage)                                                  AS total_damage,
                    SUM(enemies5k)                                               AS aces,
                    SUM(enemies4k)                                               AS quads,
                    SUM(v1_wins)                                                 AS clutch_1v1,
                    SUM(v2_wins)                                                 AS clutch_1v2,
                    SUM(entry_wins)                                              AS entry_wins,
                    SUM(entry_count)                                             AS entry_attempts,
                    SUM(flash_successes)                                         AS flashes_thrown,
                    ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2)                   AS kd,
                    ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1)      AS hs_pct,
                    ROUND(SUM(damage)/NULLIF(
                        COUNT(DISTINCT CONCAT(matchid,mapnumber)),0)/30,1)      AS adr
                FROM {MATCHZY_TABLES['players']}
                WHERE CAST(steamid64 AS UNSIGNED) IN (%s, %s) AND steamid64 != '0'
            """, (int(sid64), int(sid32)))
            career = c.fetchone()

        if career:
            name_map = _edited_name_map()
            sid = sid64  # always SteamID64 form, set above
            if sid in name_map:
                career['name'] = name_map[sid]
            c.execute(f"""
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
                        WHEN LOWER(p.team) = LOWER(mm.team1_name) THEN
                            CASE WHEN LOWER(mm.winner) = LOWER(mm.team1_name) THEN 1 ELSE 0 END
                        WHEN LOWER(p.team) = LOWER(mm.team2_name) THEN
                            CASE WHEN LOWER(mm.winner) = LOWER(mm.team2_name) THEN 1 ELSE 0 END
                        ELSE NULL
                    END AS player_won
                FROM {MATCHZY_TABLES['players']} p
                LEFT JOIN {MATCHZY_TABLES['maps']} m ON p.matchid=m.matchid AND p.mapnumber=m.mapnumber
                LEFT JOIN {MATCHZY_TABLES['matches']} mm ON p.matchid=mm.matchid
                WHERE CAST(p.steamid64 AS UNSIGNED) IN (%s, %s) AND p.steamid64 != '0'
                ORDER BY p.matchid DESC, p.mapnumber DESC
                LIMIT 20
            """, (int(sid_variants(sid)[0]), int(sid_variants(sid)[1])))
            recent = _patch_recent_matches(c.fetchall())
        c.close(); conn.close()
    except Exception as _e:
        print(f"[api/player] MatchZy query error for '{name}': {_e}")

    # ── Fallback to fshost ────────────────────────────────────────────────────
    if not career:
        career, recent = await loop.run_in_executor(None, _fshost_career_for, name)

    if not career:
        return _json_response({"error": "Player not found"})

    return _json_response({"career": career, "recent_matches": recent})

async def handle_api_player_by_sid(request):
    """GET /api/player/sid/{steamid64} — look up player by SteamID (either form)."""
    raw_sid = request.match_info.get('steamid64', '')
    loop = asyncio.get_running_loop()

    sid64, sid32 = sid_variants(to_steamid64(raw_sid))

    career = None
    recent = []
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)

        # WHERE IN (sid64, sid32) covers both forms stored in DB.
        # No GROUP BY needed — we already filter to one player's rows.
        c.execute(f"""
            SELECT
                SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS name,
                COUNT(DISTINCT matchid)                                      AS matches,
                SUM(kills)                                                   AS kills,
                SUM(deaths)                                                  AS deaths,
                SUM(assists)                                                 AS assists,
                SUM(head_shot_kills)                                         AS headshots,
                SUM(damage)                                                  AS total_damage,
                SUM(enemies5k)                                               AS aces,
                SUM(enemies4k)                                               AS quads,
                SUM(v1_wins)                                                 AS clutch_1v1,
                SUM(v2_wins)                                                 AS clutch_1v2,
                SUM(entry_wins)                                              AS entry_wins,
                SUM(entry_count)                                             AS entry_attempts,
                SUM(flash_successes)                                         AS flashes_thrown,
                ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2)                   AS kd,
                ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1)      AS hs_pct,
                ROUND(SUM(damage)/NULLIF(
                    COUNT(DISTINCT CONCAT(matchid,mapnumber)),0)/30,1)      AS adr
            FROM {MATCHZY_TABLES['players']}
            WHERE CAST(steamid64 AS UNSIGNED) IN (%s, %s)
              AND steamid64 != '0' AND steamid64 IS NOT NULL
        """, (int(sid64), int(sid32)))
        career = c.fetchone()

        if career:
            career = dict(career)
            # Always expose the real SteamID64 regardless of what DB stores
            career['steamid64'] = sid64
            name_map = _edited_name_map()
            if sid64 in name_map:
                career['name'] = name_map[sid64]

            c.execute(f"""
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
                        WHEN LOWER(p.team) = LOWER(mm.team1_name) THEN
                            CASE WHEN LOWER(mm.winner) = LOWER(mm.team1_name) THEN 1 ELSE 0 END
                        WHEN LOWER(p.team) = LOWER(mm.team2_name) THEN
                            CASE WHEN LOWER(mm.winner) = LOWER(mm.team2_name) THEN 1 ELSE 0 END
                        ELSE NULL
                    END AS player_won
                FROM {MATCHZY_TABLES['players']} p
                LEFT JOIN {MATCHZY_TABLES['maps']} m ON p.matchid=m.matchid AND p.mapnumber=m.mapnumber
                LEFT JOIN {MATCHZY_TABLES['matches']} mm ON p.matchid=mm.matchid
                WHERE CAST(p.steamid64 AS UNSIGNED) IN (%s, %s)
                  AND p.steamid64 != '0'
                ORDER BY p.matchid DESC, p.mapnumber DESC
                LIMIT 20
            """, (int(sid64), int(sid32)))
            recent = _patch_recent_matches(c.fetchall())
        c.close(); conn.close()
    except Exception as _e:
        print(f"[api/player/sid] error for '{raw_sid}': {_e}")

    if not career:
        return _json_response({"error": "Player not found"})

    return _json_response({"career": career, "recent_matches": recent})


async def handle_api_player_mapstats_by_sid(request):
    """GET /api/player/sid/{steamid64}/mapstats"""
    raw_sid = request.match_info.get('steamid64', '')
    sid64, sid32 = sid_variants(to_steamid64(raw_sid))
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT m.mapname,
                COUNT(DISTINCT p.matchid)                                           AS matches,
                SUM(p.kills) AS kills, SUM(p.deaths) AS deaths,
                SUM(p.assists) AS assists, SUM(p.damage) AS damage,
                SUM(p.head_shot_kills) AS headshots,
                ROUND(SUM(p.kills)/NULLIF(SUM(p.deaths),0),2)                      AS kd,
                ROUND(SUM(p.head_shot_kills)/NULLIF(SUM(p.kills),0)*100,1)         AS hs_pct,
                ROUND(SUM(p.damage)/NULLIF(COUNT(DISTINCT p.matchid),0)/30,1)      AS adr,
                SUM(CASE WHEN LOWER(mm.winner) = LOWER(p.team) THEN 1 ELSE 0 END) AS wins
            FROM {MATCHZY_TABLES['players']} p
            LEFT JOIN {MATCHZY_TABLES['maps']} m  ON p.matchid=m.matchid AND p.mapnumber=m.mapnumber
            LEFT JOIN {MATCHZY_TABLES['matches']} mm ON p.matchid=mm.matchid
            WHERE CAST(p.steamid64 AS UNSIGNED) IN (%s, %s) AND p.steamid64 != '0'
              AND m.mapname IS NOT NULL AND m.mapname != ''
            GROUP BY m.mapname
            ORDER BY matches DESC
        """, (int(sid64), int(sid32)))
        rows = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
        return _json_response(rows)
    except Exception as e:
        return _json_response({"error": str(e)})


async def handle_api_debug_player(request):
    """GET /api/debug/player/{steamid64} — raw DB lookup for debugging"""
    raw_sid = request.match_info.get('steamid64', '')
    sid64, sid32 = sid_variants(to_steamid64(raw_sid))
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        # Show all raw rows for both SID forms
        c.execute(f"""
            SELECT steamid64, name, matchid, kills, deaths
            FROM {MATCHZY_TABLES['players']}
            WHERE steamid64 IN (%s, %s)
            LIMIT 20
        """, (sid64, sid32))
        rows = [dict(r) for r in c.fetchall()]
        # Also show count with each form individually
        c.execute(f"SELECT COUNT(*) AS cnt FROM {MATCHZY_TABLES['players']} WHERE steamid64 = %s", (sid64,))
        cnt64 = c.fetchone()['cnt']
        c.execute(f"SELECT COUNT(*) AS cnt FROM {MATCHZY_TABLES['players']} WHERE steamid64 = %s", (sid32,))
        cnt32 = c.fetchone()['cnt']
        c.close(); conn.close()
        return _json_response({
            "input": raw_sid,
            "sid64": sid64,
            "sid32": sid32,
            "count_by_sid64": cnt64,
            "count_by_sid32": cnt32,
            "rows": rows
        })
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
        cached = _cache_get('matches')
        if cached is not None:
            return _json_response(cached[:limit], max_age=30)
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
                'filename':    entry.get('name', ''),   # e.g. 2026-02-23_20-11-44_29_de_ancient_..._stats.json
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
        results = results[:limit]

        # ── Apply edits to match list (team names, scores, map) ──────────────
        all_edits = _get_all_edits()
        edited_ids = set(all_edits.keys())
        for r in results:
            mid   = str(r['matchid'])
            edits = all_edits.get(mid, {})
            r['is_edited'] = mid in edited_ids
            if edits:
                t1e = edits.get('team1', {})
                t2e = edits.get('team2', {})
                if t1e.get('name'):  r['team1_name']  = t1e['name']
                if t1e.get('score') is not None: r['team1_score'] = t1e['score']
                if t2e.get('name'):  r['team2_name']  = t2e['name']
                if t2e.get('score') is not None: r['team2_score'] = t2e['score']
                if edits.get('map'):    r['mapname'] = edits['map']
                if edits.get('winner'): r['winner']  = edits['winner']

        _cache_set('matches', results)
        return _json_response(results, max_age=30)
    except Exception as e:
        return _json_response({"error": str(e)})


async def handle_api_match(request):
    """GET /api/match/{matchid} — fshost JSON merged with edits + MatchZy gap-fill."""
    matchid = request.match_info.get('matchid', '')
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, _fetch_fshost_match_json, matchid)
        if not data:
            return _json_response({"error": f"No fshost data found for match {matchid}"})

        # Auto-save raw fshost JSON to DB (best-effort)
        asyncio.ensure_future(loop.run_in_executor(None, _save_raw_to_db, matchid, data))

        # Load edits once
        edits  = _get_edits(matchid)
        t1e    = edits.get('team1', {})
        t2e    = edits.get('team2', {})

        t1 = data.get('team1', {})
        t2 = data.get('team2', {})

        t1name  = t1e.get('name')  or t1.get('name', 'Team 1')
        t2name  = t2e.get('name')  or t2.get('name', 'Team 2')
        t1score = t1e['score'] if 'score' in t1e else t1.get('score', 0)
        t2score = t2e['score'] if 'score' in t2e else t2.get('score', 0)
        mapname = edits.get('map') or data.get('map', 'unknown')
        winner  = edits.get('winner') or data.get('winner', '')

        meta = {
            'matchid':     data.get('match_id') or matchid,
            'team1_name':  t1name,
            'team2_name':  t2name,
            'team1_score': t1score,
            'team2_score': t2score,
            'winner':      winner,
            'end_time':    data.get('date'),
            'start_time':  data.get('date'),
            'is_edited':   bool(edits),
        }
        maps = [{
            'matchid':      matchid,
            'mapnumber':    1,
            'mapname':      mapname,
            'team1_score':  t1score,
            'team2_score':  t2score,
            'winner':       winner,
            'total_rounds': data.get('total_rounds'),
        }]

        # Build base player list from fshost, apply stat edits
        players = _players_from_fshost(data, matchid)
        players = _apply_match_player_edits(players, matchid)
        # Patch team_name if team name was edited
        for p in players:
            if p.get('team') == 'team1' and t1e.get('name'):
                p['team_name'] = t1name
            elif p.get('team') == 'team2' and t2e.get('name'):
                p['team_name'] = t2name

        # ── Merge manually-added players from edits ──────────────────────────
        added_players = edits.get('added_players', [])
        added_sids    = {to_steamid64(str(ap.get('steamid64') or '')) for ap in added_players}
        for ap in added_players:
            ap = dict(ap)
            ap['source']    = 'added'
            ap['matchid']   = matchid
            ap['mapnumber'] = 1
            # Apply team_name based on current (possibly edited) team names
            if ap.get('team') == 'team1':
                ap['team_name'] = t1name
            elif ap.get('team') == 'team2':
                ap['team_name'] = t2name
            # Recalculate derived fields
            kills  = int(ap.get('kills', 0) or 0)
            deaths = int(ap.get('deaths', 0) or 0)
            hs     = int(ap.get('head_shot_kills', 0) or 0)
            dmg    = int(ap.get('damage', 0) or 0)
            ap.setdefault('kd',     round(kills / deaths, 2) if deaths else float(kills))
            ap.setdefault('hs_pct', round(hs / kills * 100, 1) if kills else 0.0)
            ap.setdefault('adr',    round(dmg / 30, 1))
            players.append(ap)

        # ── Fetch MatchZy players to surface "available to add" list ─────────
        matchzy_players = await loop.run_in_executor(None, _get_matchzy_players_for_match, matchid)
        _, missing_players = _merge_missing_players(players, matchzy_players, added_sids)
        # Patch team names on missing players too
        for mp in missing_players:
            if mp.get('team') == 'team1':
                mp['team_name'] = t1name
            elif mp.get('team') == 'team2':
                mp['team_name'] = t2name

        matchid_map = build_matchid_to_demo_map()
        entry = matchid_map.get(str(matchid), {})
        demo = {
            'name': entry.get('name', ''),
            'url':  entry.get('download_url', ''),
            'size': entry.get('size_formatted', ''),
        }
        # Include filename in meta so team stats page can use it as unique match key
        meta['filename'] = entry.get('name', '')
        return _json_response({
            "meta":            meta,
            "maps":            maps,
            "players":         players,
            "demo":            demo,
            "missing_players": missing_players,   # MatchZy players not in fshost
        })
    except Exception as e:
        return _json_response({"error": str(e)})


async def handle_api_matches_full(request):
    """
    GET /api/matches/full — all matches WITH player data in one shot.
    Eliminates the N+1 fetch pattern on the team stats page.
    """
    try:
        cached = _cache_get('matches_full')
        if cached is not None:
            return _json_response(cached, max_age=30)

        loop = asyncio.get_running_loop()
        matchid_map = await loop.run_in_executor(None, build_matchid_to_demo_map)
        all_edits   = _get_all_edits()

        results = []
        for mid, entry in matchid_map.items():
            meta = entry.get('metadata') or {}
            if not meta:
                continue
            matchid = str(meta.get('match_id') or mid)
            edits   = all_edits.get(matchid, {})
            t1e     = edits.get('team1', {})
            t2e     = edits.get('team2', {})

            t1 = meta.get('team1', {})
            t2 = meta.get('team2', {})

            t1name  = t1e.get('name')  or t1.get('name', 'Team 1')
            t2name  = t2e.get('name')  or t2.get('name', 'Team 2')
            t1score = t1e['score'] if 'score' in t1e else t1.get('score', 0)
            t2score = t2e['score'] if 'score' in t2e else t2.get('score', 0)
            mapname = edits.get('map')    or meta.get('map', 'unknown')
            winner  = edits.get('winner') or meta.get('winner', '')

            players = _players_from_fshost(meta, matchid)
            players = _apply_match_player_edits(players, matchid)
            for p in players:
                if p.get('team') == 'team1' and t1e.get('name'):
                    p['team_name'] = t1name
                elif p.get('team') == 'team2' and t2e.get('name'):
                    p['team_name'] = t2name

            added_players = edits.get('added_players', [])
            for ap in list(added_players):
                ap = dict(ap)
                ap['source'] = 'added'; ap['matchid'] = matchid; ap['mapnumber'] = 1
                if ap.get('team') == 'team1': ap['team_name'] = t1name
                elif ap.get('team') == 'team2': ap['team_name'] = t2name
                kills = int(ap.get('kills', 0) or 0); deaths = int(ap.get('deaths', 0) or 0)
                hs = int(ap.get('head_shot_kills', 0) or 0); dmg = int(ap.get('damage', 0) or 0)
                ap.setdefault('kd',     round(kills / deaths, 2) if deaths else float(kills))
                ap.setdefault('hs_pct', round(hs / kills * 100, 1) if kills else 0.0)
                ap.setdefault('adr',    round(dmg / 30, 1))
                players.append(ap)

            results.append({
                'meta': {
                    'matchid':     matchid,
                    'team1_name':  t1name,
                    'team2_name':  t2name,
                    'team1_score': t1score,
                    'team2_score': t2score,
                    'winner':      winner,
                    'end_time':    meta.get('date'),
                    'mapname':     mapname,
                    'filename':    entry.get('name', ''),
                    'is_edited':   bool(edits),
                },
                'maps': [{
                    'matchid':      matchid,
                    'mapnumber':    1,
                    'mapname':      mapname,
                    'team1_score':  t1score,
                    'team2_score':  t2score,
                    'winner':       winner,
                    'total_rounds': meta.get('total_rounds'),
                }],
                'players': players,
                'demo': {
                    'name': entry.get('name', ''),
                    'url':  entry.get('download_url', ''),
                    'size': entry.get('size_formatted', ''),
                },
            })

        results.sort(key=lambda r: str(r['meta'].get('end_time') or ''), reverse=True)
        _cache_set('matches_full', results)
        return _json_response(results, max_age=30)
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
                'steamid64':      to_steamid64(str(fp.get('steam_id', '0'))),
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


# ─────────────────────────────────────────────────────────────────────────────
# EDIT OVERLAY HELPERS
# Every API endpoint calls these to apply match_edits on top of raw data.
# ─────────────────────────────────────────────────────────────────────────────

def _get_edits(matchid: str) -> dict:
    """Load edit overrides for one match from DB. Returns {} if none."""
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute("SELECT edits_json FROM match_edits WHERE matchid = %s", (str(matchid),))
        row = c.fetchone()
        c.close(); conn.close()
        if row and row['edits_json']:
            return json.loads(row['edits_json'])
    except Exception as e:
        print(f"[edits] _get_edits error for {matchid}: {e}")
    return {}


def _get_all_edits() -> dict:
    """
    Load ALL match edits in one query. Returns { matchid: edits_dict }.
    Simple 60-second in-process cache so hot endpoints don't hit the DB repeatedly.
    """
    import time as _time
    now = _time.monotonic()
    cache = _get_all_edits._cache
    if cache and now - cache['ts'] < 60:
        return cache['data']
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute("SELECT matchid, edits_json FROM match_edits")
        rows = c.fetchall()
        c.close(); conn.close()
        data = {}
        for row in rows:
            try:
                data[str(row['matchid'])] = json.loads(row['edits_json'])
            except Exception:
                pass
        _get_all_edits._cache = {'ts': now, 'data': data}
        return data
    except Exception as e:
        print(f"[edits] _get_all_edits error: {e}")
        return {}
_get_all_edits._cache = {}


def _bust_edits_cache():
    """Call this after saving edits so the cache refreshes immediately."""
    _get_all_edits._cache = {}
    _cache_bust('matches', 'matches_full', 'leaderboard', 'specialists', 'mapstats', 'teams')


def _edited_name_map():
    """
    Returns name_map = { steamid64: latest_edited_name }.
    Keys are always normalised to SteamID64 so lookups work regardless of
    whether the DB or edit JSON stored SteamID32 or SteamID64.
    """
    all_edits = _get_all_edits()
    name_map = {}
    for mid, edits in all_edits.items():
        for sid, pe in edits.get('players', {}).items():
            if 'name' in pe:
                name_map[to_steamid64(str(sid))] = pe['name']
    return name_map


def _apply_match_player_edits(players: list, matchid: str) -> list:
    """
    Patch a per-match player list with stored edits.
    Recalculates kd / hs_pct / adr when base stats are edited.
    """
    edits = _get_edits(matchid)
    if not edits:
        return players
    player_edits = edits.get('players', {})
    if not player_edits:
        return players
    out = []
    for p in players:
        sid = to_steamid64(str(p.get('steamid64') or p.get('steam_id') or ''))
        pe  = player_edits.get(sid, {})
        if pe:
            p = dict(p)
            p.update(pe)
            kills  = int(p.get('kills', 0) or 0)
            deaths = int(p.get('deaths', 0) or 0)
            hs     = int(p.get('head_shot_kills', 0) or 0)
            dmg    = int(p.get('damage', 0) or 0)
            if 'kills' in pe or 'deaths' in pe:
                p['kd'] = round(kills / deaths, 2) if deaths else float(kills)
            if 'kills' in pe or 'head_shot_kills' in pe:
                p['hs_pct'] = round(hs / kills * 100, 1) if kills else 0.0
            if 'damage' in pe:
                p['adr'] = round(dmg / 30, 1)
        out.append(p)
    return out


def _apply_meta_edits(meta: dict, matchid: str) -> dict:
    """Patch match-level meta (team names, scores, map, winner) from edits."""
    edits = _get_edits(matchid)
    if not edits:
        return meta
    meta = dict(meta)
    t1e = edits.get('team1', {})
    t2e = edits.get('team2', {})
    if t1e.get('name'):  meta['team1_name']  = t1e['name']
    if t1e.get('score') is not None: meta['team1_score'] = t1e['score']
    if t2e.get('name'):  meta['team2_name']  = t2e['name']
    if t2e.get('score') is not None: meta['team2_score'] = t2e['score']
    if edits.get('map'):    meta['mapname'] = edits['map']
    if edits.get('winner'): meta['winner']  = edits['winner']
    return meta


def _get_matchzy_players_for_match(matchid: str) -> list:
    """
    Fetch all player rows for a match from matchzy_stats_players.
    Returns a list of dicts with the same field names used elsewhere.
    Used to detect players that left early and weren't captured by fshost.
    """
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT
                p.steamid64, p.name, p.team,
                p.kills, p.deaths, p.assists, p.damage,
                p.head_shot_kills,
                p.enemies5k, p.enemies4k, p.enemies3k,
                p.v1_wins, p.v2_wins,
                p.entry_wins, p.entry_count,
                p.flash_successes,
                p.utility_damage,
                ROUND(p.damage / 30.0, 1)                              AS adr,
                ROUND(p.head_shot_kills / NULLIF(p.kills,0) * 100, 1) AS hs_pct,
                mm.team1_name, mm.team2_name
            FROM {MATCHZY_TABLES['players']} p
            LEFT JOIN {MATCHZY_TABLES['matches']} mm ON mm.matchid = p.matchid
            WHERE p.matchid = %s AND p.steamid64 != '0'
        """, (str(matchid),))
        rows = c.fetchall()
        c.close(); conn.close()
        # Normalise team field: MatchZy stores 'team1'/'team2' or 'CT'/'T' depending on version
        out = []
        for r in rows:
            r = dict(r)
            r['matchid']   = str(matchid)
            r['mapnumber'] = 1
            r['steamid64'] = to_steamid64(str(r.get('steamid64') or '0'))
            r['source']    = 'matchzy'
            # Determine team key for consistency with fshost convention
            team_raw = str(r.get('team') or '').lower()
            if team_raw in ('team1', 'team_1', '1'):
                r['team'] = 'team1'
            elif team_raw in ('team2', 'team_2', '2'):
                r['team'] = 'team2'
            # team_name from match row
            if r['team'] == 'team1':
                r['team_name'] = r.get('team1_name') or 'Team 1'
            else:
                r['team_name'] = r.get('team2_name') or 'Team 2'
            out.append(r)
        return out
    except Exception as e:
        print(f"[matchzy-players] Error for match {matchid}: {e}")
        return []


def _merge_missing_players(fshost_players: list, matchzy_players: list,
                           added_sids: set) -> tuple[list, list]:
    """
    Compare fshost player list vs MatchZy player list.
    Returns (merged_players, missing_players) where:
      - merged_players = fshost list (already has edits applied)
      - missing_players = MatchZy rows whose steamid64 is NOT in fshost list
        AND NOT already manually added via edits['added_players']
    """
    fshost_sids = {to_steamid64(str(p.get('steamid64') or '')) for p in fshost_players}
    missing = [
        p for p in matchzy_players
        if to_steamid64(str(p['steamid64'])) not in fshost_sids
        and to_steamid64(str(p['steamid64'])) not in added_sids
        and p['steamid64'] != '0'
    ]
    return fshost_players, missing



def _patch_aggregate_rows(rows: list) -> list:
    """
    Patch leaderboard / specialists / career aggregate rows with edited player names.
    Only names are patched here — aggregate stats (kills/deaths totals) stay as MatchZy
    calculated them, because individual-match stat edits are already reflected in the
    per-match scoreboard via _apply_match_player_edits.
    """
    name_map = _edited_name_map()
    if not name_map:
        return rows
    out = []
    for row in rows:
        # Normalise to SteamID64 before name_map lookup (DB may store SteamID32)
        sid = to_steamid64(str(row.get('steamid64') or ''))
        if sid in name_map:
            row = dict(row)
            row['name'] = name_map[sid]
        out.append(row)
    return out


def _patch_recent_matches(rows: list) -> list:
    """
    Patch recent-match rows (from handle_api_player / team stats) with edited
    team names, scores, mapname, winner, and the requesting player's own stats.
    """
    all_edits = _get_all_edits()
    if not all_edits:
        return rows
    out = []
    for row in rows:
        mid   = str(row.get('matchid') or '')
        edits = all_edits.get(mid, {})
        if not edits:
            out.append(row)
            continue
        row = dict(row)
        t1e = edits.get('team1', {})
        t2e = edits.get('team2', {})
        if t1e.get('name'):  row['team1_name']  = t1e['name']
        if t1e.get('score') is not None: row['team1_score'] = t1e['score']
        if t2e.get('name'):  row['team2_name']  = t2e['name']
        if t2e.get('score') is not None: row['team2_score'] = t2e['score']
        if edits.get('map'):    row['mapname'] = edits['map']
        if edits.get('winner'): row['winner']  = edits['winner']
        # Patch this player's own stats if they were edited
        sid = to_steamid64(str(row.get('steamid64') or ''))
        pe  = edits.get('players', {}).get(sid, {})
        if pe:
            row.update(pe)
            kills  = int(row.get('kills', 0) or 0)
            deaths = int(row.get('deaths', 0) or 0)
            hs     = int(row.get('head_shot_kills', 0) or 0)
            dmg    = int(row.get('damage', 0) or 0)
            row['kd']     = round(kills / deaths, 2) if deaths else float(kills)
            row['hs_pct'] = round(hs / kills * 100, 1) if kills else 0.0
            row['adr']    = round(dmg / 30, 1)
        out.append(row)
    return out


def _deep_merge(base: dict, overrides: dict) -> dict:
    """
    Recursively merge overrides into base dict.
    For team player lists: match by steam_id and merge per-player fields.
    """
    result = dict(base)
    for key, val in overrides.items():
        if key in ('team1', 'team2') and isinstance(val, dict) and isinstance(result.get(key), dict):
            merged_team = dict(result[key])
            for tk, tv in val.items():
                if tk == 'players' and isinstance(tv, list):
                    orig_players = {to_steamid64(str(p.get('steam_id', p.get('steamid64', '')))): p
                                    for p in merged_team.get('players', [])}
                    for op in tv:
                        pid = to_steamid64(str(op.get('steam_id', op.get('steamid64', ''))))
                        if pid and pid in orig_players:
                            orig_players[pid] = {**orig_players[pid], **op}
                        elif pid:
                            orig_players[pid] = op
                    merged_team['players'] = list(orig_players.values())
                else:
                    merged_team[tk] = tv
            result[key] = merged_team
        elif isinstance(val, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def _save_raw_to_db(matchid: str, raw: dict):
    """Upsert the raw fshost JSON into fshost_matches table."""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO fshost_matches (matchid, raw_json, fetched_at)
            VALUES (%s, %s, NOW())
            ON DUPLICATE KEY UPDATE raw_json = VALUES(raw_json), updated_at = NOW()
        """, (str(matchid), json.dumps(raw, default=str)))
        conn.commit()
        c.close(); conn.close()
    except Exception as e:
        print(f"[DB] Save raw error for {matchid}: {e}")


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

def _aggregate_stats_from_fshost() -> list:
    """
    Aggregate per-player career stats from ALL fshost JSONs.
    Used as fallback when MatchZy tables are empty or missing.
    Returns a list of dicts with the same fields as matchzy_stats_players aggregations.
    """
    try:
        matchid_map = build_matchid_to_demo_map()
        players_agg = {}   # steamid64 -> aggregated dict

        for mid, entry in matchid_map.items():
            meta = entry.get('metadata')
            if not meta:
                continue
            # Apply any edits so aggregates use the edited data
            edits = _get_edits(str(mid))
            if edits:
                meta = _deep_merge(meta, edits)

            for team_key in ('team1', 'team2'):
                team_data = meta.get(team_key, {})
                for fp in team_data.get('players', []):
                    sid = to_steamid64(str(fp.get('steam_id') or fp.get('steamid64') or '0'))
                    if sid == '0' or not sid:
                        continue
                    name = fp.get('name') or '?'

                    kills   = int(fp.get('kills', 0) or 0)
                    deaths  = int(fp.get('deaths', 0) or 0)
                    assists = int(fp.get('assists', 0) or 0)
                    damage  = int(fp.get('damage', 0) or 0)
                    hs      = int(fp.get('headshot_kills', 0) or fp.get('head_shot_kills', 0) or 0)

                    def cw(s):
                        try: return int(str(s).split('/')[0])
                        except: return 0

                    if sid not in players_agg:
                        players_agg[sid] = {
                            'steamid64':   sid,
                            'name':        name,
                            'matches':     0,
                            'kills':       0, 'deaths':    0, 'assists':    0,
                            'damage':      0, 'headshots': 0,
                            'aces':        0, 'quads':     0, 'triples':    0,
                            'clutch_wins': 0, 'clutch_1v1': 0, 'clutch_1v2': 0,
                            'entry_wins':  0, 'entry_attempts': 0,
                            'flash_successes': 0, 'utility_damage': 0,
                        }
                    p = players_agg[sid]
                    p['name']    = name   # keep most recent name
                    p['matches'] += 1
                    p['kills']   += kills
                    p['deaths']  += deaths
                    p['assists'] += assists
                    p['damage']  += damage
                    p['headshots'] += hs
                    p['aces']    += int(fp.get('5k', 0) or 0)
                    p['quads']   += int(fp.get('4k', 0) or 0)
                    p['triples'] += int(fp.get('3k', 0) or 0)
                    p['clutch_wins']  += cw(fp.get('1v1', 0)) + cw(fp.get('1v2', 0))
                    p['clutch_1v1']   += cw(fp.get('1v1', 0))
                    p['clutch_1v2']   += cw(fp.get('1v2', 0))
                    p['entry_wins']   += int(fp.get('entry_kills', 0) or fp.get('entry_wins', 0) or 0)
                    p['flash_successes'] += int(fp.get('flash_assists', 0) or 0)
                    p['utility_damage']  += int(fp.get('utility_damage', 0) or 0)

        rows = []
        for p in players_agg.values():
            k, d = p['kills'], p['deaths']
            hs   = p['headshots']
            m    = p['matches']
            rows.append({
                **p,
                'kd':     round(k / d, 2) if d else float(k),
                'hs_pct': round(hs / k * 100, 1) if k else 0.0,
                'adr':    round(p['damage'] / max(m, 1) / 30, 1),
                # Specialists fields
                'clutch_total':    p['clutch_wins'],
                'entry_rate':      round(p['entry_wins'] / p['entry_attempts'] * 100, 1)
                                   if p.get('entry_attempts') else 0.0,
                'flashes_per_map': round(p['flash_successes'] / max(m, 1), 1),
                'util_dmg_per_map':round(p['utility_damage']  / max(m, 1), 1),
            })

        return rows
    except Exception as e:
        print(f"[fshost-agg] Error: {e}")
        return []


async def handle_api_leaderboard(request):
    """GET /api/leaderboard — career stats from matchzy_stats_players only"""
    try:
        cached = _cache_get('leaderboard')
        if cached is not None:
            return _json_response(cached, max_age=60)
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT
                steamid64,
                SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS name,
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
            WHERE steamid64 != '0' AND steamid64 IS NOT NULL
              AND name != '' AND name IS NOT NULL
            GROUP BY steamid64
            ORDER BY kills DESC
        """)
        rows = c.fetchall()
        c.close(); conn.close()
        # Normalise any SteamID32 → SteamID64 in output (DB may store either form)
        for r in rows:
            if r.get('steamid64'):
                r['steamid64'] = to_steamid64(str(r['steamid64']))
        rows = _patch_aggregate_rows(rows)
        _cache_set('leaderboard', rows)
        return _json_response(rows, max_age=60)
    except Exception as e:
        return _json_response({"error": str(e)})


async def handle_api_specialists(request):
    """GET /api/specialists — specialist stat boards from matchzy_stats_players only"""
    try:
        cached = _cache_get('specialists')
        if cached is not None:
            return _json_response(cached, max_age=60)
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT
                steamid64,
                SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS name,
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
            WHERE steamid64 != '0' AND steamid64 IS NOT NULL
              AND name != '' AND name IS NOT NULL
            GROUP BY steamid64
            HAVING matches >= 1
            ORDER BY clutch_total DESC
        """)
        rows = c.fetchall()
        c.close(); conn.close()
        for r in rows:
            if r.get('steamid64'):
                r['steamid64'] = to_steamid64(str(r['steamid64']))
        rows = _patch_aggregate_rows(rows)
        _cache_set('specialists', rows)
        return _json_response(rows, max_age=60)
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

def sid_variants(raw: str) -> tuple[str, str]:
    """
    Return (steamid64, steamid32) for a given ID in either form.
    The DB may store either form in the steamid64 column depending on the source
    (MatchZy stores SteamID64, some fshost rows store SteamID32).
    Use WHERE steamid64 IN (%s, %s) with both variants to match either.
    """
    try:
        val = int(raw)
        if val < 0x100000000:
            # Input is SteamID32
            return str(val + STEAMID64_BASE), str(val)
        else:
            # Input is SteamID64
            return str(val), str(val - STEAMID64_BASE)
    except (ValueError, TypeError):
        return raw, raw

# ── Steam avatar local cache ─────────────────────────────────────────────────
# In-memory cache: steamid64 -> local URL or Steam CDN URL



async def handle_api_steam(request):
    """GET /api/steam/{steamid64} — fetch Steam profile and avatar from CDN."""
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
        return _json_response(data, max_age=3600)
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_api_mapstats(request):
    """GET /api/mapstats — win rates and avg scores per map"""
    try:
        cached = _cache_get('mapstats')
        if cached is not None:
            return _json_response(cached, max_age=60)
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
        _cache_set('mapstats', rows)
        return _json_response(rows, max_age=60)
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_api_h2h(request):
    """GET /api/h2h?p1=name&p2=name — head to head career stats from matchzy_stats_players only"""
    p1 = request.rel_url.query.get('p1', '')
    p2 = request.rel_url.query.get('p2', '')
    if not p1 or not p2:
        return _json_response({"error": "Need p1 and p2 query params"})

    r1 = r2 = None
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        name_map = _edited_name_map()

        def fetch_player(pname):
            # Resolve SID: edit map first, then any stored name row
            psid = next((s for s, n in name_map.items() if n == pname), None)
            if not psid:
                c.execute(f"SELECT steamid64 FROM {MATCHZY_TABLES['players']} WHERE name = %s AND steamid64 != '0' LIMIT 1", (pname,))
                r = c.fetchone()
                if r:
                    psid = to_steamid64(str(r['steamid64']))
            if not psid:
                return None
            # Aggregate ALL rows for this SID regardless of name changes
            c.execute(f"""
                SELECT
                    steamid64,
                    SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS name,
                    COUNT(DISTINCT matchid)                                      AS matches,
                    SUM(kills)                                                   AS kills,
                    SUM(deaths)                                                  AS deaths,
                    SUM(assists)                                                 AS assists,
                    SUM(head_shot_kills)                                         AS headshots,
                    SUM(damage)                                                  AS damage,
                    SUM(enemies5k)                                               AS aces,
                    SUM(enemies4k)                                               AS quads,
                    SUM(v1_wins)                                                 AS clutch_wins,
                    SUM(entry_wins)                                              AS entry_wins,
                    ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2)                   AS kd,
                    ROUND(SUM(head_shot_kills)/NULLIF(SUM(kills),0)*100,1)      AS hs_pct,
                    ROUND(SUM(damage)/NULLIF(
                        COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)),0)/30,1)  AS adr
                FROM {MATCHZY_TABLES['players']}
                WHERE CAST(steamid64 AS UNSIGNED) IN (%s, %s) AND steamid64 != '0'
            """, (int(sid_variants(psid)[0]), int(sid_variants(psid)[1])))
            row = c.fetchone()
            if row:
                row = dict(row)
                row['steamid64'] = to_steamid64(str(psid))
            return row

        r1 = fetch_player(p1)
        r2 = fetch_player(p2)
        c.close(); conn.close()

        # Patch edited names
        for r in [r1, r2]:
            if r:
                sid = to_steamid64(str(r.get('steamid64') or ''))
                r['steamid64'] = sid  # normalise output too
                if sid in name_map:
                    r['name'] = name_map[sid]
    except Exception as e:
        return _json_response({"error": str(e)})

    return _json_response({"p1": r1, "p2": r2})

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
    """GET /stats — serve stats.html as a static file"""
    return web.FileResponse(HTML_PATH)

# ─────────────────────────────────────────────────────────────────────────────
# MATCH EDIT API ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

async def handle_options(request):
    """CORS preflight."""
    return web.Response(status=204, headers={
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Methods": "GET, POST, PATCH, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
    })


async def handle_api_auth_edit(request):
    """POST /api/auth/edit — check Steam admin session, return ok."""
    if _get_admin_steamid(request):
        return _json_response({"ok": True, "admin": True})
    return web.Response(
        text=json.dumps({"ok": False, "error": "Not logged in as admin"}),
        content_type='application/json', status=401,
        headers={"Access-Control-Allow-Origin": "*"},
    )


def _verify_edit_token(request) -> bool:
    """Auth check: valid Steam admin session OR legacy bearer token."""    # Primary: Steam session cookie
    if _get_admin_steamid(request):
        return True
    # Legacy fallback: bearer token (keep working during transition)
    import base64
    auth = request.headers.get('Authorization', '')
    if not auth.startswith('Bearer '):
        return False
    try:
        return base64.b64decode(auth[7:].encode()).decode() == f"edit:{EDIT_PASSWORD}"
    except Exception:
        return False


async def handle_api_save_match(request):
    """POST /api/match/{matchid}/save — cache raw fshost JSON. No auth needed."""
    matchid = request.match_info.get('matchid', '')
    try:
        body = await request.json()
        raw  = body.get('raw_json') or body
        if not isinstance(raw, dict):
            return _json_response({"ok": False, "error": "No JSON body"})
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _save_raw_to_db, matchid, raw)
        return _json_response({"ok": True, "matchid": matchid})
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)})


async def handle_api_get_edits(request):
    """GET /api/match/{matchid}/edits — return stored edits for a match."""
    matchid = request.match_info.get('matchid', '')
    edits   = _get_edits(matchid)
    has_raw = False
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("SELECT 1 FROM fshost_matches WHERE matchid = %s", (str(matchid),))
        has_raw = c.fetchone() is not None
        c.close(); conn.close()
    except Exception:
        pass
    return _json_response({"matchid": matchid, "edits": edits, "has_raw": has_raw})


async def handle_api_patch_match(request):
    """PATCH /api/match/{matchid}/edit — save edits. Requires bearer token."""
    matchid = request.match_info.get('matchid', '')
    if not _verify_edit_token(request):
        return web.Response(
            text=json.dumps({"ok": False, "error": "Unauthorized"}),
            content_type='application/json', status=401,
            headers={"Access-Control-Allow-Origin": "*"},
        )
    try:
        body  = await request.json()
        edits = body.get('edits')
        if not isinstance(edits, dict):
            return _json_response({"ok": False, "error": "Missing 'edits' object"})
        conn = get_db(); c = conn.cursor()
        c.execute("""
            INSERT INTO match_edits (matchid, edits_json, edited_at)
            VALUES (%s, %s, NOW())
            ON DUPLICATE KEY UPDATE edits_json = VALUES(edits_json), edited_at = NOW()
        """, (str(matchid), json.dumps(edits, default=str)))
        conn.commit(); c.close(); conn.close()
        _bust_edits_cache()
        return _json_response({"ok": True, "matchid": matchid})
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)})


async def handle_api_revert_match(request):
    """DELETE /api/match/{matchid}/edit — remove edits, revert to fshost data."""
    matchid = request.match_info.get('matchid', '')
    if not _verify_edit_token(request):
        return web.Response(
            text=json.dumps({"ok": False, "error": "Unauthorized"}),
            content_type='application/json', status=401,
            headers={"Access-Control-Allow-Origin": "*"},
        )
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("DELETE FROM match_edits WHERE matchid = %s", (str(matchid),))
        conn.commit(); c.close(); conn.close()
        _bust_edits_cache()
        return _json_response({"ok": True, "matchid": matchid, "reverted": True})
    except Exception as e:
        return _json_response({"ok": False, "error": str(e)})



async def handle_api_team_h2h(request):
    """GET /api/teamh2h?t1=name&t2=name — head-to-head history between two teams"""
    t1 = request.rel_url.query.get('t1', '').strip()
    t2 = request.rel_url.query.get('t2', '').strip()
    if not t1 or not t2:
        return _json_response({"error": "Need t1 and t2 query params"})
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT mm.matchid, mm.team1_name, mm.team2_name, mm.winner,
                   m.mapname, m.team1_score, m.team2_score, mm.end_time
            FROM {MATCHZY_TABLES['matches']} mm
            LEFT JOIN {MATCHZY_TABLES['maps']} m ON mm.matchid = m.matchid
            WHERE (LOWER(mm.team1_name) = LOWER(%s) AND LOWER(mm.team2_name) = LOWER(%s))
               OR (LOWER(mm.team1_name) = LOWER(%s) AND LOWER(mm.team2_name) = LOWER(%s))
            ORDER BY mm.matchid DESC
        """, (t1, t2, t2, t1))
        rows = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
        all_edits = _get_all_edits()
        for r in rows:
            mid = str(r['matchid'])
            edits = all_edits.get(mid, {})
            if edits:
                t1e = edits.get('team1', {}); t2e = edits.get('team2', {})
                if t1e.get('name'):  r['team1_name']  = t1e['name']
                if t2e.get('name'):  r['team2_name']  = t2e['name']
                if t1e.get('score') is not None: r['team1_score'] = t1e['score']
                if t2e.get('score') is not None: r['team2_score'] = t2e['score']
                if edits.get('map'):    r['mapname'] = edits['map']
                if edits.get('winner'): r['winner']  = edits['winner']
        t1_wins = sum(1 for r in rows if (r.get('winner') or '').lower().strip() == t1.lower())
        t2_wins = sum(1 for r in rows if (r.get('winner') or '').lower().strip() == t2.lower())
        return _json_response({"t1": t1, "t2": t2, "t1_wins": t1_wins, "t2_wins": t2_wins,
                                "total": len(rows), "matches": rows})
    except Exception as e:
        return _json_response({"error": str(e)})


async def handle_api_teams(request):
    """GET /api/teams — distinct team names from matches table"""
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        c.execute(f"""
            SELECT DISTINCT team1_name AS name FROM {MATCHZY_TABLES['matches']}
            WHERE team1_name IS NOT NULL AND team1_name != ''
            UNION
            SELECT DISTINCT team2_name AS name FROM {MATCHZY_TABLES['matches']}
            WHERE team2_name IS NOT NULL AND team2_name != ''
            ORDER BY name
        """)
        rows = [r['name'] for r in c.fetchall()]
        c.close(); conn.close()
        return _json_response(rows)
    except Exception as e:
        return _json_response({"error": str(e)})


async def handle_api_search(request):
    """GET /api/search?q=query — search players and matches"""
    q = request.rel_url.query.get('q', '').strip()
    if not q or len(q) < 2:
        return _json_response({"players": [], "matches": []})
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        like = f"%{q}%"
        c.execute(f"""
            SELECT steamid64,
                SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS name,
                COUNT(DISTINCT matchid) AS matches,
                ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2) AS kd,
                ROUND(SUM(damage)/NULLIF(COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)),0)/30,1) AS adr
            FROM {MATCHZY_TABLES['players']}
            WHERE name LIKE %s AND steamid64 != '0'
            GROUP BY steamid64
            ORDER BY matches DESC
            LIMIT 8
        """, (like,))
        players = [dict(r) for r in c.fetchall()]
        # Normalise SteamID32 → SteamID64 in output
        for p in players:
            if p.get('steamid64'):
                p['steamid64'] = to_steamid64(str(p['steamid64']))
        name_map = _edited_name_map()
        for p in players:
            sid = str(p.get('steamid64') or '')
            if sid in name_map:
                p['name'] = name_map[sid]
        for sid, edited_name in [(s, n) for s, n in name_map.items() if q.lower() in n.lower()]:
            sid64_cmp = to_steamid64(str(sid))
            if not any(to_steamid64(str(p.get('steamid64',''))) == sid64_cmp for p in players):
                c.execute(f"""
                    SELECT steamid64, COUNT(DISTINCT matchid) AS matches,
                        ROUND(SUM(kills)/NULLIF(SUM(deaths),0),2) AS kd,
                        ROUND(SUM(damage)/NULLIF(COUNT(DISTINCT CONCAT(matchid,'_',mapnumber)),0)/30,1) AS adr
                    FROM {MATCHZY_TABLES['players']} WHERE CAST(steamid64 AS UNSIGNED) IN (%s, %s) LIMIT 1
                """, (int(sid_variants(sid)[0]), int(sid_variants(sid)[1])))
                row = c.fetchone()
                if row:
                    row = dict(row); row['name'] = edited_name; players.append(row)
        c.execute(f"""
            SELECT mm.matchid, mm.team1_name, mm.team2_name, mm.winner, mm.end_time,
                   m.mapname, m.team1_score, m.team2_score
            FROM {MATCHZY_TABLES['matches']} mm
            LEFT JOIN {MATCHZY_TABLES['maps']} m ON mm.matchid = m.matchid
            WHERE mm.team1_name LIKE %s OR mm.team2_name LIKE %s
               OR CAST(mm.matchid AS CHAR) LIKE %s
            ORDER BY mm.matchid DESC
            LIMIT 8
        """, (like, like, like))
        matches = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
        all_edits = _get_all_edits()
        for r in matches:
            mid = str(r['matchid'])
            edits = all_edits.get(mid, {})
            if edits:
                t1e = edits.get('team1', {}); t2e = edits.get('team2', {})
                if t1e.get('name'):  r['team1_name']  = t1e['name']
                if t2e.get('name'):  r['team2_name']  = t2e['name']
                if edits.get('map'):    r['mapname'] = edits['map']
                if edits.get('winner'): r['winner']  = edits['winner']
        return _json_response({"players": players, "matches": matches})
    except Exception as e:
        return _json_response({"error": str(e)})


async def handle_api_player_mapstats(request):
    """GET /api/player/{name}/mapstats — per-map career breakdown for a player"""
    name = request.match_info.get('name', '')
    try:
        conn = get_db()
        c = conn.cursor(dictionary=True)
        # Resolve SID via edit map first (handles renamed players), then by any stored name
        name_map = _edited_name_map()
        sid = next((s for s, n in name_map.items() if n == name), None)
        if not sid:
            c.execute(f"SELECT steamid64 FROM {MATCHZY_TABLES['players']} WHERE name = %s AND steamid64 != '0' LIMIT 1", (name,))
            row = c.fetchone()
            if row:
                sid = to_steamid64(str(row['steamid64']))
        if not sid:
            return _json_response([])
        c.execute(f"""
            SELECT m.mapname,
                COUNT(DISTINCT p.matchid)                                           AS matches,
                SUM(p.kills) AS kills, SUM(p.deaths) AS deaths,
                SUM(p.assists) AS assists, SUM(p.damage) AS damage,
                SUM(p.head_shot_kills) AS headshots,
                ROUND(SUM(p.kills)/NULLIF(SUM(p.deaths),0),2)                      AS kd,
                ROUND(SUM(p.head_shot_kills)/NULLIF(SUM(p.kills),0)*100,1)         AS hs_pct,
                ROUND(SUM(p.damage)/NULLIF(COUNT(DISTINCT p.matchid),0)/30,1)      AS adr,
                SUM(CASE WHEN LOWER(mm.winner) = LOWER(p.team) THEN 1 ELSE 0 END) AS wins
            FROM {MATCHZY_TABLES['players']} p
            LEFT JOIN {MATCHZY_TABLES['maps']} m  ON p.matchid=m.matchid AND p.mapnumber=m.mapnumber
            LEFT JOIN {MATCHZY_TABLES['matches']} mm ON p.matchid=mm.matchid
            WHERE CAST(p.steamid64 AS UNSIGNED) IN (%s, %s) AND p.steamid64 != '0'
              AND m.mapname IS NOT NULL AND m.mapname != ''
            GROUP BY m.mapname
            ORDER BY matches DESC
        """, (int(sid_variants(sid)[0]), int(sid_variants(sid)[1])))
        rows = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
        return _json_response(rows)
    except Exception as e:
        return _json_response({"error": str(e)})


ADMIN_HTML_PATH = pathlib.Path(__file__).parent / "admin.html"
# In-memory session store: {token: steamid64}
_ADMIN_SESSIONS: dict = {}
# Player session store (any Steam user, not just admin): {token: steamid64}
_PLAYER_SESSIONS: dict = {}

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

async def handle_admin_page(request):
    """GET /admin — serve admin.html"""
    if ADMIN_HTML_PATH.exists():
        return web.FileResponse(ADMIN_HTML_PATH)
    return web.Response(text="Admin panel not found", status=404)

async def handle_admin_steam_login(request):
    """GET /auth/steam — redirect to Steam OpenID."""
    scheme = request.headers.get("X-Forwarded-Proto", "https")
    host   = request.headers.get("X-Forwarded-Host", request.host)
    return_to = f"{scheme}://{host}/auth/steam/callback"
    raise web.HTTPFound(location=_get_steam_openid_url(return_to))

async def handle_admin_steam_callback(request):
    """GET /auth/steam/callback — verify Steam OpenID, set session cookie."""
    import secrets
    scheme = request.headers.get("X-Forwarded-Proto", "https")
    host   = request.headers.get("X-Forwarded-Host", request.host)
    return_to = f"{scheme}://{host}/auth/steam/callback"
    params = dict(request.rel_url.query)
    steamid = await asyncio.get_running_loop().run_in_executor(
        None, _verify_steam_openid, params, return_to
    )
    if not steamid:
        raise web.HTTPFound(location="/admin?error=auth_failed")
    if str(ADMIN_ID) and steamid != str(ADMIN_ID):
        raise web.HTTPFound(location="/admin?error=not_admin")
    token = secrets.token_hex(32)
    _ADMIN_SESSIONS[token] = steamid
    response = web.HTTPFound(location="/admin")
    response.set_cookie("rg_admin", token, max_age=86400*7, httponly=True, samesite="Lax")
    return response

async def handle_admin_logout(request):
    """POST /auth/steam/logout"""
    token = request.cookies.get("rg_admin", "")
    _ADMIN_SESSIONS.pop(token, None)
    response = web.HTTPFound(location="/admin")
    response.del_cookie("rg_admin")
    return response

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

def _get_admin_steamid(request) -> str | None:
    """Return steamid64 if request has valid admin session, else None."""
    token = request.cookies.get("rg_admin", "")
    return _ADMIN_SESSIONS.get(token)

async def handle_admin_api_me(request):
    """GET /api/admin/me — return session info."""
    sid = _get_admin_steamid(request)
    if not sid:
        return web.Response(text=json.dumps({"ok": False}), content_type="application/json", status=401)
    return _json_response({"ok": True, "steamid": sid})

async def handle_admin_api_matches(request):
    """GET /api/admin/matches — all matches for admin management."""
    if not _get_admin_steamid(request):
        return web.Response(text=json.dumps({"error": "Unauthorized"}), content_type="application/json", status=401)
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute("""
            SELECT f.matchid, f.fetched_at,
                   JSON_UNQUOTE(JSON_EXTRACT(f.raw_json, '$.map'))        AS map,
                   JSON_UNQUOTE(JSON_EXTRACT(f.raw_json, '$.winner'))     AS winner,
                   JSON_UNQUOTE(JSON_EXTRACT(f.raw_json, '$.team1.name')) AS team1,
                   JSON_UNQUOTE(JSON_EXTRACT(f.raw_json, '$.team2.name')) AS team2,
                   JSON_UNQUOTE(JSON_EXTRACT(f.raw_json, '$.team1.score')) AS score1,
                   JSON_UNQUOTE(JSON_EXTRACT(f.raw_json, '$.team2.score')) AS score2,
                   e.edited_at
            FROM fshost_matches f
            LEFT JOIN match_edits e ON e.matchid = f.matchid
            ORDER BY f.fetched_at DESC
        """)
        rows = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
        return _json_response(rows)
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_admin_api_delete_match(request):
    """DELETE /api/admin/match/{matchid} — delete match entirely."""
    if not _get_admin_steamid(request):
        return web.Response(text=json.dumps({"error": "Unauthorized"}), content_type="application/json", status=401)
    matchid = request.match_info.get("matchid", "")
    try:
        conn = get_db(); c = conn.cursor()
        c.execute("DELETE FROM match_edits WHERE matchid = %s", (matchid,))
        c.execute("DELETE FROM fshost_matches WHERE matchid = %s", (matchid,))
        conn.commit(); c.close(); conn.close()
        return _json_response({"ok": True})
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_admin_api_players(request):
    """GET /api/admin/players — all players for management."""
    if not _get_admin_steamid(request):
        return web.Response(text=json.dumps({"error": "Unauthorized"}), content_type="application/json", status=401)
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute("""
            SELECT steamid64, name,
                   COUNT(*) AS matches,
                   SUM(kills) AS kills, SUM(deaths) AS deaths
            FROM matchzy_stats_players
            GROUP BY steamid64, name
            ORDER BY matches DESC
        """)
        rows = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()
        return _json_response(rows)
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_admin_api_rename_player(request):
    """POST /api/admin/player/rename — rename player across all records."""
    if not _get_admin_steamid(request):
        return web.Response(text=json.dumps({"error": "Unauthorized"}), content_type="application/json", status=401)
    try:
        body = await request.json()
        old_name = body.get("old_name", "").strip()
        new_name = body.get("new_name", "").strip()
        if not old_name or not new_name:
            return _json_response({"error": "old_name and new_name required"})
        conn = get_db(); c = conn.cursor()
        c.execute("UPDATE matchzy_stats_players SET name = %s WHERE name = %s", (new_name, old_name))
        affected = c.rowcount
        conn.commit(); c.close(); conn.close()
        return _json_response({"ok": True, "rows_updated": affected})
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_admin_api_server(request):
    """GET /api/admin/server — live server status + player list."""
    if not _get_admin_steamid(request):
        return web.Response(text=json.dumps({"error": "Unauthorized"}), content_type="application/json", status=401)
    try:
        loop = asyncio.get_running_loop()
        players = await loop.run_in_executor(None, rcon_list_players)
        status_txt = await loop.run_in_executor(None, send_rcon, "status")
        return _json_response({"ok": True, "players": players, "status": status_txt})
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_admin_api_rcon(request):
    """POST /api/admin/rcon — run RCON command."""
    if not _get_admin_steamid(request):
        return web.Response(text=json.dumps({"error": "Unauthorized"}), content_type="application/json", status=401)
    try:
        body = await request.json()
        cmd = body.get("cmd", "").strip()
        if not cmd:
            return _json_response({"error": "cmd required"})
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, send_rcon, cmd)
        return _json_response({"ok": True, "result": result})
    except Exception as e:
        return _json_response({"error": str(e)})


async def handle_admin_api_suggest_merges(request):
    """GET /api/admin/players/suggest-merges — fuzzy duplicate detection."""
    if not _get_admin_steamid(request):
        return web.Response(text=json.dumps({"error": "Unauthorized"}), content_type="application/json", status=401)
    try:
        conn = get_db(); c = conn.cursor(dictionary=True)
        c.execute("""
            SELECT name, COUNT(*) AS matches, SUM(kills) AS kills
            FROM matchzy_stats_players
            GROUP BY name
            ORDER BY matches DESC
        """)
        players = [dict(r) for r in c.fetchall()]
        c.close(); conn.close()

        # Fuzzy matching using simple edit distance
        def edit_distance(a, b):
            a, b = a.lower().strip(), b.lower().strip()
            if a == b: return 0
            if len(a) > len(b): a, b = b, a
            distances = range(len(a) + 1)
            for i2, c2 in enumerate(b):
                new_distances = [i2 + 1]
                for i1, c1 in enumerate(a):
                    if c1 == c2:
                        new_distances.append(distances[i1])
                    else:
                        new_distances.append(1 + min((distances[i1], distances[i1 + 1], new_distances[-1])))
                distances = new_distances
            return distances[-1]

        names = [p["name"] for p in players]
        name_map = {p["name"]: p for p in players}
        suggestions = []
        seen = set()

        for i, n1 in enumerate(names):
            for n2 in names[i+1:]:
                key = tuple(sorted([n1, n2]))
                if key in seen:
                    continue
                seen.add(key)
                l1, l2 = len(n1), len(n2)
                max_len = max(l1, l2)
                if max_len == 0:
                    continue
                dist = edit_distance(n1, n2)
                similarity = 1 - dist / max_len
                # Also check if one starts with or contains the other
                n1l, n2l = n1.lower(), n2.lower()
                if n1l in n2l or n2l in n1l:
                    similarity = max(similarity, 0.75)
                if similarity >= 0.65:
                    p1, p2 = name_map[n1], name_map[n2]
                    suggestions.append({
                        "name1": n1, "matches1": p1["matches"], "kills1": int(p1["kills"] or 0),
                        "name2": n2, "matches2": p2["matches"], "kills2": int(p2["kills"] or 0),
                        "similarity": round(similarity, 2),
                    })

        # Sort by similarity desc
        suggestions.sort(key=lambda x: -x["similarity"])
        return _json_response(suggestions[:30])
    except Exception as e:
        return _json_response({"error": str(e)})


async def handle_admin_api_merge_players(request):
    """POST /api/admin/players/merge — merge duplicate names into one."""
    if not _get_admin_steamid(request):
        return web.Response(text=json.dumps({"error": "Unauthorized"}), content_type="application/json", status=401)
    try:
        body = await request.json()
        keep_name  = body.get("keep_name", "").strip()
        merge_names = [n.strip() for n in body.get("merge_names", []) if n.strip()]
        if not keep_name or not merge_names:
            return _json_response({"error": "keep_name and merge_names[] required"})
        conn = get_db(); c = conn.cursor()
        total = 0
        for name in merge_names:
            if name == keep_name:
                continue
            c.execute("UPDATE matchzy_stats_players SET name = %s WHERE name = %s", (keep_name, name))
            total += c.rowcount
        conn.commit(); c.close(); conn.close()
        return _json_response({"ok": True, "rows_updated": total, "keep_name": keep_name, "merged": merge_names})
    except Exception as e:
        return _json_response({"error": str(e)})

async def handle_admin_api_merge_by_steamid(request):
    """
    POST /api/admin/players/merge-by-steamid
    Two-step fix:
      1. Convert any SteamID32 values in the steamid64 column to real SteamID64
         (fshost JSONs store steam_id as SteamID32; MatchZy stores SteamID64 —
          this caused the same player to appear twice with different IDs).
      2. For each steamid64 with multiple name variants, unify all rows to
         the most recent name so renamed players are merged into one profile.
    Body (optional): { "steamid64": "765..." } to fix one player only.
    """
    if not _get_admin_steamid(request):
        return web.Response(text=json.dumps({"error": "Unauthorized"}),
                            content_type="application/json", status=401)
    try:
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        target_sid = (body.get("steamid64") or "").strip() or None

        conn = get_db()
        c = conn.cursor(dictionary=True)

        # Find all steamid64s that have more than one distinct name
        if target_sid:
            c.execute(f"""
                SELECT steamid64,
                    COUNT(DISTINCT name) AS name_count,
                    SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS latest_name
                FROM {MATCHZY_TABLES['players']}
                WHERE steamid64 IN (%s, %s) AND steamid64 != '0'
                GROUP BY steamid64
                HAVING name_count > 1
            """, sid_variants(target_sid))
        else:
            c.execute(f"""
                SELECT steamid64,
                    COUNT(DISTINCT name) AS name_count,
                    SUBSTRING_INDEX(GROUP_CONCAT(name ORDER BY matchid DESC), ',', 1) AS latest_name
                FROM {MATCHZY_TABLES['players']}
                WHERE steamid64 != '0' AND steamid64 IS NOT NULL
                GROUP BY steamid64
                HAVING name_count > 1
            """)

        duplicates = [dict(r) for r in c.fetchall()]

        # Apply edit-map overrides: if admin has explicitly renamed this player, use that
        name_map = _edited_name_map()
        total_updated = 0
        merged = []

        for dup in duplicates:
            sid = str(dup['steamid64'])
            canonical = name_map.get(sid) or dup['latest_name']

            # Fetch old names for reporting
            c.execute(f"""
                SELECT DISTINCT name FROM {MATCHZY_TABLES['players']}
                WHERE steamid64 IN (%s, %s) AND name != %s
            """, (*sid_variants(sid), canonical))
            old_names = [r['name'] for r in c.fetchall()]

            # Update all rows for this SID to the canonical name (both stored forms)
            c2 = conn.cursor()
            c2.execute(f"""
                UPDATE {MATCHZY_TABLES['players']}
                SET name = %s
                WHERE steamid64 IN (%s, %s) AND name != %s
            """, (canonical, *sid_variants(sid), canonical))
            rows_changed = c2.rowcount
            c2.close()

            total_updated += rows_changed
            merged.append({
                "steamid64":   sid,
                "canonical":   canonical,
                "old_names":   old_names,
                "rows_updated": rows_changed,
            })

        conn.commit()
        c.close()
        conn.close()

        # Bust all caches so leaderboard/profiles reflect the fix immediately
        _bust_edits_cache()

        return _json_response({
            "ok":            True,
            "players_fixed": len(merged),
            "rows_updated":  total_updated,
            "details":       merged,
        })
    except Exception as e:
        return _json_response({"error": str(e)})


async def start_http_server():
    app = web.Application()
    app.router.add_get('/api/specialists',             handle_api_specialists)
    app.router.add_get('/api/debug/player/{steamid64}',      handle_api_debug_player)
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
    # ── Admin ─────────────────────────────────────────────────────────────────
    app.router.add_get('/admin',                        handle_admin_page)
    app.router.add_get('/auth/steam',                   handle_admin_steam_login)
    app.router.add_get('/auth/steam/callback',          handle_admin_steam_callback)
    app.router.add_post('/auth/steam/logout',           handle_admin_logout)
    app.router.add_get('/api/admin/me',                 handle_admin_api_me)
    app.router.add_get('/api/admin/matches',            handle_admin_api_matches)
    app.router.add_route('DELETE', '/api/admin/match/{matchid}', handle_admin_api_delete_match)
    app.router.add_get('/api/admin/players',            handle_admin_api_players)
    app.router.add_post('/api/admin/player/rename',     handle_admin_api_rename_player)
    app.router.add_get('/api/admin/players/suggest-merges', handle_admin_api_suggest_merges)
    app.router.add_post('/api/admin/players/merge',             handle_admin_api_merge_players)
    app.router.add_post('/api/admin/players/merge-by-steamid', handle_admin_api_merge_by_steamid)
    app.router.add_get('/api/admin/server',             handle_admin_api_server)
    app.router.add_post('/api/admin/rcon',              handle_admin_api_rcon)
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


intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
bot = commands.Bot(command_prefix="!", intents=intents, owner_id=ADMIN_ID)

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

    # ── fshost match cache ───────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS fshost_matches (
            matchid    VARCHAR(64) PRIMARY KEY,
            raw_json   LONGTEXT    NOT NULL,
            fetched_at DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) CHARACTER SET utf8mb4
    """)

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
            where = "steamid64 IN (%s, %s)"
            param = sid_variants(steamid64)
        elif player_name:
            where = "name = %s"
            param = (player_name,)
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
        ''', param)

        row = c.fetchone()
        if row and steamid64:
            row = dict(row)
            row['steamid64'] = to_steamid64(str(row['steamid64']))
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
    """Pull every fshost JSON into fshost_matches. Runs in a thread executor."""
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
                    INSERT INTO fshost_matches (matchid, raw_json, fetched_at)
                    VALUES (%s, %s, NOW())
                    ON DUPLICATE KEY UPDATE raw_json = VALUES(raw_json), updated_at = NOW()
                """, (str(matchid), json.dumps(metadata, default=str)))
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
        _bust_edits_cache()  # also refresh edit cache after sync
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
