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
    """GET /api/matches - pure fshost JSON source."""
    try:
        limit = int(request.rel_url.query.get('limit', 30))
        loop  = asyncio.get_running_loop()
        matchid_map = await loop.run_in_executor(None, build_matchid_to_demo_map)
        results = []
        for mid, entry in matchid_map.items():
            data = entry.get('metadata') or await loop.run_in_executor(None, _fetch_fshost_match_json, mid)
            if not data:
                continue
            t1 = data.get('team1', {})
            t2 = data.get('team2', {})
            results.append({
                'matchid':      data.get('match_id') or mid,
                'team1_name':   t1.get('name', 'Team 1'),
                'team2_name':   t2.get('name', 'Team 2'),
                'team1_score':  t1.get('score', 0),
                'team2_score':  t2.get('score', 0),
                'winner':       data.get('winner', ''),
                'end_time':     data.get('date'),
                'start_time':   data.get('date'),
                'mapname':      data.get('map', '?'),
                'mapnumber':    1,
                'total_rounds': data.get('total_rounds'),
                'demo_url':     entry.get('download_url', ''),
                'demo_size':    entry.get('size_formatted', ''),
            })
        results.sort(key=lambda x: x.get('end_time') or '', reverse=True)
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
        matchid_map = build_matchid_to_demo_map()
        entry = matchid_map.get(str(matchid), {})
        demo  = {'name': entry.get('name',''), 'url': entry.get('download_url',''), 'size': entry.get('size_formatted','')}
        return _json_response({"meta": meta, "maps": maps, "players": players, "demo": demo})
    except Exception as e:
        return _json_response({"error": str(e)})


def _players_from_fshost(data: dict, matchid: str) -> list:
    """Flatten fshost team1/team2 players into a unified list."""
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
                'enemies5k':      int(fp.get('5k', 0) or 0),
                'enemies4k':      int(fp.get('4k', 0) or 0),
                'v1_wins':        cw(fp.get('1v1', 0)),
                'v2_wins':        cw(fp.get('1v2', 0)),
                'rating':         fp.get('rating'),
                'kast':           fp.get('kast'),
                'multi_kills':    fp.get('multi_kills'),
                'opening_kills':  fp.get('opening_kills'),
                'opening_deaths': fp.get('opening_deaths'),
                'trade_kills':    fp.get('trade_kills'),
                'clutch_1v1':     fp.get('1v1', '0/0'),
                'clutch_1v2':     fp.get('1v2', '0/0'),
                'flash_assists':  fp.get('flash_assists'),
                'utility_damage': fp.get('utility_damage'),
            })
    return players


def _fetch_fshost_match_json(matchid: str) -> dict | None:
    """Fetch the fshost .json stats file for a given match ID."""
    try:
        matchid_map = build_matchid_to_demo_map()
        entry = matchid_map.get(str(matchid))
        if not entry:
            return None
        cached = entry.get('metadata')
        if cached and cached.get('team1'):
            return cached
        dem_name = entry.get('name', '')
        if not dem_name.endswith('.dem'):
            return None
        json_name = dem_name.replace('.dem', '.json')
        all_files = fetch_all_demos_raw()
        json_url  = next((f.get('download_url') for f in all_files if f.get('name') == json_name), None)
        if not json_url:
            return None
        headers = {'User-Agent': 'Mozilla/5.0', 'Referer': 'https://fshost.me/'}
        r = requests.get(json_url, headers=headers, timeout=10)
        r.raise_for_status()
        result = r.json()
        entry['metadata'] = result
        return result
    except Exception as e:
        print(f"[fshost JSON] Error fetching match {matchid}: {e}")
        return None


async def handle_api_demos(request):
    """GET /api/demos — returns all demos from fshost with parsed timestamps (only .dem files)"""
    demos = fetch_all_demos_raw()
    result = []
    for d in demos:
        name = d.get("name", "")
        # Only include .dem files, not .json files
        if not name.endswith(".dem"):
            continue
        m = DEMO_TS_RE.match(name)
        ts = None
        if m:
            try:
                ts = datetime.strptime(m.group(1), "%Y-%m-%d_%H-%M-%S").isoformat()
            except ValueError:
                pass
        result.append({
            "name": name,
            "download_url": d.get("download_url", ""),
            "size_formatted": d.get("size_formatted", ""),
            "modified_at": d.get("modified_at", ""),
            "filename_ts": ts,
        })
    return _json_response(result)

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
  <div class="logo">5v5 Stats</div>
  <div class="tabs">
    <div class="tab active" data-p="matches">Matches</div>
  </div>
</nav>

<div id="app">
  <div id="p-matches"></div>
  <div id="p-player" style="display:none"></div>
  <div id="p-match" style="display:none"></div>
</div>

<script>
// ── Router ────────────────────────────────────────────────────────────────────
let _back = null;
function go(page, params={}, back=null) {
  ['matches','player','match'].forEach(p => {
    document.getElementById('p-'+p).style.display = (p===page)?'':'none';
  });
  document.querySelectorAll('.tab').forEach(t=>t.classList.toggle('active',t.dataset.p===page));
  _back = back;
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
    document.getElementById('p-matches').innerHTML='<div class="empty">No completed matches yet.</div>';return;
  }
  const items = data.map(m=>{
    // Demo comes directly in match data from fshost
    const demoHtml = m.demo_url
      ? `<a href="${m.demo_url}" target="_blank" onclick="event.stopPropagation()"
           style="display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border:1px solid rgba(255,85,0,.5);border-radius:2px;font-family:'Rajdhani',sans-serif;font-weight:700;font-size:10px;letter-spacing:1px;text-transform:uppercase;color:var(--orange);background:var(--orange-glow);text-decoration:none;white-space:nowrap">
           📥 Demo${m.demo_size?' ('+m.demo_size+')':''}</a>`
      : '';
    // Clean winner name (remove "team_" prefix for display)
    const winnerClean = (m.winner||'').replace(/^team_/i,'');
    return `
    <div class="match-item" onclick="go('match',{id:'${m.matchid}'},'matches')">
      <div class="m-id">#${m.matchid}</div>
      <div class="m-teams">
        <div class="m-teams-str">${esc(m.team1_name||'Team 1')} <span style="color:var(--muted2)">vs</span> ${esc(m.team2_name||'Team 2')}</div>
        <div class="m-score">${m.team1_score??0} : ${m.team2_score??0}</div>
      </div>
      <div class="m-map">${esc(m.mapname||'—')}</div>
      ${winnerClean?`<div class="m-winner">W: ${esc(winnerClean)}</div>`:''}
      ${demoHtml}
      <div class="m-date">${fmtDate(m.end_time)}</div>
    </div>`;
  }).join('');
  document.getElementById('p-matches').innerHTML = `
    <div class="page-title">Match History <span class="sub">${data.length} matches</span></div>
    <div class="card matches-list">${items}</div>`;
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

  const hasExtra = players.some(p=>p.rating!=null||p.kast!=null);
  const colCount  = hasExtra ? 13 : 10;
  const extraHdr  = hasExtra ? '<th>Rating</th><th>KAST</th><th title="Opening K/D">OK/OD</th>' : '';

  // MVP = highest rating, fallback to kills
  const mvp = players.reduce((b,p)=>{
    const s = p.rating!=null?parseFloat(p.rating)*100:parseInt(p.kills||0);
    const bs= b?(b.rating!=null?parseFloat(b.rating)*100:parseInt(b.kills||0)):-1;
    return s>bs?p:b;
  }, null);

  const byKills  = [...players].sort((a,b)=>parseInt(b.kills||0)-parseInt(a.kills||0));
  const byDmg    = [...players].sort((a,b)=>parseInt(b.damage||0)-parseInt(a.damage||0));
  const byRating = [...players].filter(p=>p.rating!=null).sort((a,b)=>parseFloat(b.rating)-parseFloat(a.rating));

  const mvpHtml = mvp ? `
    <div class="mvp-card">
      <div class="mvp-avatar">${initials(mvp.name)}</div>
      <div class="mvp-info">
        <div class="mvp-name">${esc(mvp.name)}</div>
        <div class="mvp-stats">
          <div class="mvp-stat"><div class="mvp-val">${mvp.kills??0} / ${mvp.deaths??0} / ${mvp.assists??0}</div><div class="mvp-lbl">K / D / A</div></div>
          <div class="mvp-stat"><div class="mvp-val">${mvp.adr!=null?parseFloat(mvp.adr).toFixed(1):'—'}</div><div class="mvp-lbl">ADR</div></div>
          <div class="mvp-stat"><div class="mvp-val">${mvp.hs_pct!=null?parseFloat(mvp.hs_pct).toFixed(1)+'%':'—'}</div><div class="mvp-lbl">HS%</div></div>
          <div class="mvp-stat"><div class="mvp-val" style="color:var(--${kdf(mvp.kills&&mvp.deaths?(mvp.kills/mvp.deaths).toFixed(2):'0')})">${mvp.kills&&mvp.deaths?(mvp.kills/mvp.deaths).toFixed(2):'—'}</div><div class="mvp-lbl">K/D</div></div>
          ${mvp.rating!=null?`<div class="mvp-stat"><div class="mvp-val" style="color:#a78bfa">${parseFloat(mvp.rating).toFixed(2)}</div><div class="mvp-lbl">Rating</div></div>`:''}
          ${mvp.kast!=null?`<div class="mvp-stat"><div class="mvp-val">${parseFloat(mvp.kast).toFixed(1)}%</div><div class="mvp-lbl">KAST</div></div>`:''}
        </div>
      </div>
    </div>` : '';

  const awardsHtml = `<div class="awards-grid">
    ${byKills[0]?`<div class="award-card"><div class="award-avatar">${initials(byKills[0].name)}</div><div><div class="award-name">${esc(byKills[0].name)}</div><div style="font-size:10px;color:var(--muted2)">Most Kills</div></div><div style="margin-left:auto;text-align:right"><div class="award-val">${byKills[0].kills}</div></div></div>`:''}
    ${byDmg[0]?`<div class="award-card"><div class="award-avatar">${initials(byDmg[0].name)}</div><div><div class="award-name">${esc(byDmg[0].name)}</div><div style="font-size:10px;color:var(--muted2)">Most Damage</div></div><div style="margin-left:auto;text-align:right"><div class="award-val">${num(byDmg[0].damage)}</div></div></div>`:''}
    ${byRating[0]?`<div class="award-card"><div class="award-avatar">${initials(byRating[0].name)}</div><div><div class="award-name">${esc(byRating[0].name)}</div><div style="font-size:10px;color:var(--muted2)">Best Rating</div></div><div style="margin-left:auto;text-align:right"><div class="award-val" style="color:#a78bfa">${parseFloat(byRating[0].rating).toFixed(2)}</div></div></div>`:''}
  </div>`;

  const mapsHtml = maps.map(m=>{
    const mapPlayers = players.filter(p=>(p.mapnumber??1)===(m.mapnumber??1));
    const {t1, t2, n1, n2} = splitTeams(mapPlayers);
    return `
      <div style="margin-bottom:14px">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">
          <div style="font-family:'Rajdhani',sans-serif;font-weight:700;font-size:15px;letter-spacing:1px;text-transform:uppercase;color:var(--white)">${esc(m.mapname||'Map')}</div>
          <div style="font-size:14px;color:var(--white);font-family:'Rajdhani',sans-serif;font-weight:700">${m.team1_score??0} : ${m.team2_score??0}</div>
          ${m.total_rounds?`<div style="font-size:11px;color:var(--muted2)">${m.total_rounds} rounds</div>`:''}
        </div>
        <div class="card ovx">
          <table class="sb-table">
            <thead><tr>
              <th>Player</th><th>K</th><th>D</th><th>A</th>
              <th>K/D</th><th>ADR</th><th>HS%</th><th>Dmg</th><th>5K</th><th>Clutch</th>
              ${extraHdr}
            </tr></thead>
            <tbody>
              <tr class="team-divider ct-div"><td colspan="${colCount}">${esc(n1)}</td></tr>
              ${sbRows(t1, hasExtra)}
              <tr class="team-divider t-div"><td colspan="${colCount}">${esc(n2)}</td></tr>
              ${sbRows(t2, hasExtra)}
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
         📥 Demo${demo.size?' ('+demo.size+')':''}</a></div>`
    : '';

  document.getElementById('p-match').innerHTML = `
    <div class="back-btn" onclick="go(_back||'matches')">← Back</div>
    <div class="match-header-card" style="margin-bottom:12px">
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
      <div class="match-meta-row">
        <div class="meta-chip">Match <strong>#${meta.matchid||id}</strong></div>
        ${fmtDate(meta.end_time)?`<div class="meta-chip">${fmtDate(meta.end_time)}</div>`:''}
        ${won?`<div class="meta-chip"><span class="winner-tag">Winner: ${esc(won)}</span></div>`:''}
        ${demoBtnHtml}
      </div>
    </div>
    ${mvpHtml}
    ${awardsHtml}
    <div class="page-title" style="font-size:16px;margin-bottom:10px">Scoreboard</div>
    ${mapsHtml}`;
}

function sbRows(arr, hasExtra) {
  const cols = hasExtra ? 13 : 10;
  if (!arr.length) return `<tr><td colspan="${cols}" style="text-align:center;color:var(--muted);padding:12px">—</td></tr>`;
  return [...arr].sort((a,b)=>{
    const sa = a.rating!=null?parseFloat(a.rating)*100:parseInt(a.kills||0);
    const sb = b.rating!=null?parseFloat(b.rating)*100:parseInt(b.kills||0);
    return sb-sa;
  }).map(p=>{
    const kd     = p.deaths>0?(p.kills/p.deaths).toFixed(2):parseFloat(p.kills||0).toFixed(2);
    const adrVal = p.adr!=null?parseFloat(p.adr).toFixed(1):'—';
    const hsVal  = p.hs_pct!=null?parseFloat(p.hs_pct).toFixed(1)+'%':'—';
    const fiveK  = p.enemies5k ?? p['5k'] ?? 0;
    const clutch = p.v1_wins ?? 0;
    const ratingStyle = p.rating!=null?(parseFloat(p.rating)>=1.15?'style="color:#a78bfa"':parseFloat(p.rating)<0.85?'style="color:var(--loss)"':''):'';
    const extraCols = hasExtra ? `
      <td ${ratingStyle}>${p.rating!=null?parseFloat(p.rating).toFixed(2):'—'}</td>
      <td style="color:var(--muted2);font-size:12px">${p.kast!=null?parseFloat(p.kast).toFixed(1)+'%':'—'}</td>
      <td style="font-size:11px;color:var(--muted2)">${p.opening_kills??'—'}/${p.opening_deaths??'—'}</td>` : '';
    return `<tr>
      <td onclick="go('player',{name:'${esc(p.name)}'},'match')">${esc(p.name)}</td>
      <td class="kda-cell">${p.kills??0}</td>
      <td class="kda-cell">${p.deaths??0}</td>
      <td class="kda-cell">${p.assists??0}</td>
      <td class="kda-cell ${kdc(kd)}">${kd}</td>
      <td class="adr-highlight">${adrVal}</td>
      <td>${hsVal}</td>
      <td>${num(p.damage)}</td>
      <td>${fiveK}</td>
      <td>${clutch}</td>
      ${extraCols}
    </tr>`;
  }).join('');
}

// Check for ?match= URL param to deep-link
const urlParams = new URLSearchParams(location.search);
if(urlParams.get('match')) go('match',{id:urlParams.get('match')});
else go('matches');
</script>
</body>
</html>"""
async def start_http_server():
    app = web.Application()
    app.router.add_get('/api/player/{name}', handle_api_player)
    app.router.add_get('/api/matches',       handle_api_matches)
    app.router.add_get('/api/match/{matchid}', handle_api_match)
    app.router.add_get('/api/demos',          handle_api_demos)
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
    Build a mapping of matchid -> demo file by parsing .json metadata files.
    Uses caching to avoid repeated downloads of .json files.
    
    How it works:
    1. Fetches all files from fshost (both .dem and .json)
    2. For each .json file:
       - Downloads it
       - Parses the JSON to extract matchid
       - Maps matchid to the corresponding .dem file
    3. Returns dict: {matchid: {"name": demo_name, "download_url": url}}
    
    Example .json structure:
    {
      "matchid": "11",
      "team1_name": "Counter-Terrorists",
      "team2_name": "Team Nakai",
      "mapname": "de_mirage",
      ...
    }
    
    Returns dict: {"11": {"name": "2024-02-20_18-23-00_match11.dem", "download_url": "..."}}
    """
    global _MATCHID_DEMO_CACHE, _MATCHID_CACHE_TIME
    
    # Check cache
    if not force_refresh and _MATCHID_DEMO_CACHE is not None and _MATCHID_CACHE_TIME is not None:
        age = (datetime.now() - _MATCHID_CACHE_TIME).total_seconds()
        if age < _CACHE_TTL_SECONDS:
            return _MATCHID_DEMO_CACHE
    
    print("[Demo Map] Building matchid → demo mapping from .json files...")
    all_files = fetch_all_demos_raw()
    matchid_map = {}
    json_files_processed = 0
    json_files_with_matchid = 0
    
    # First pass: Parse all .json files to get match IDs
    json_to_dem = {}  # maps json filename to demo info
    for file_obj in all_files:
        name = file_obj.get("name", "")
        if not name.endswith(".json"):
            continue
            
        json_files_processed += 1
        download_url = file_obj.get("download_url", "")
        if not download_url:
            continue
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'https://fshost.me/'
            }
            resp = requests.get(download_url, headers=headers, timeout=10)
            resp.raise_for_status()
            metadata = resp.json()
            
            # Extract match ID from the JSON metadata (try different field names)
            matchid = metadata.get("matchid") or metadata.get("match_id") or metadata.get("id")
            if matchid:
                # The corresponding .dem file should have the same base name
                base_name = name.replace(".json", "")
                dem_name = f"{base_name}.dem"
                json_to_dem[name] = {
                    "matchid": str(matchid),
                    "dem_name": dem_name,
                    "metadata": metadata  # Store full metadata for debugging
                }
                json_files_with_matchid += 1
                print(f"[Demo Map] ✓ {name} → Match ID: {matchid}")
        except Exception as e:
            print(f"[Demo Map] ✗ Error parsing {name}: {e}")
            continue
    
    # Second pass: Find the actual .dem files and map them by match ID
    for file_obj in all_files:
        name = file_obj.get("name", "")
        if not name.endswith(".dem"):
            continue
            
        # Check if we have metadata for this demo
        json_name = name.replace(".dem", ".json")
        if json_name in json_to_dem:
            info = json_to_dem[json_name]
            matchid = info["matchid"]
            matchid_map[matchid] = {
                "name": name,
                "download_url": file_obj.get("download_url", "#"),
                "size_formatted": file_obj.get("size_formatted", ""),
                "modified_at": file_obj.get("modified_at", ""),
                "metadata": info.get("metadata", {})
            }
    
    print(f"[Demo Map] Processed {json_files_processed} .json files")
    print(f"[Demo Map] Found {json_files_with_matchid} with match IDs")
    print(f"[Demo Map] Mapped {len(matchid_map)} match IDs to demos")
    
    # Update cache
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