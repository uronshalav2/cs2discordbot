import os
import re
import json
import pytz
import a2s
import asyncio
import discord
import requests
# sqlite3 replaced by psycopg2
import io
from datetime import datetime, timedelta
from discord.ext import commands, tasks
from discord import app_commands
from typing import Literal, Optional
from mcrcon import MCRcon
from collections import defaultdict

# Discord UI components
from discord.ui import Button, View

# Try to import matplotlib for graphs
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

# Bot/GOTV filter list - exclude these from stats
BOT_FILTER = ["CSTV", "BOT", "GOTV", "SourceTV"]

# Track last read position in log file
log_file_position = 0

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
bot = commands.Bot(command_prefix="!", intents=intents, owner_id=OWNER_ID)

# ========== DATABASE SETUP (PostgreSQL via Supabase) ==========
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")

def get_db():
    """Get a database connection"""
    if not DATABASE_URL:
        raise Exception("DATABASE_URL not set in environment variables!")
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    return conn

def init_database():
    conn = get_db()
    c = conn.cursor()

    # Player sessions table
    c.execute('''CREATE TABLE IF NOT EXISTS player_sessions (
                 id SERIAL PRIMARY KEY,
                 player_name TEXT NOT NULL,
                 join_time TIMESTAMP,
                 leave_time TIMESTAMP,
                 duration_minutes INTEGER,
                 map_name TEXT)''')

    # Server snapshots table (for graphs)
    c.execute('''CREATE TABLE IF NOT EXISTS server_snapshots (
                 id SERIAL PRIMARY KEY,
                 timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 player_count INTEGER,
                 map_name TEXT)''')

    # Map statistics
    c.execute('''CREATE TABLE IF NOT EXISTS map_stats (
                 map_name TEXT PRIMARY KEY,
                 times_played INTEGER DEFAULT 0,
                 total_players INTEGER DEFAULT 0)''')

    # Player kills/deaths table
    c.execute('''CREATE TABLE IF NOT EXISTS player_stats (
                 player_name TEXT PRIMARY KEY,
                 kills INTEGER DEFAULT 0,
                 deaths INTEGER DEFAULT 0,
                 last_score INTEGER DEFAULT 0,
                 last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    conn.commit()
    conn.close()
    print("✓ Database initialized (Supabase PostgreSQL)")

# Initialize database on startup
try:
    init_database()
except Exception as e:
    print(f"⚠️ Database init error: {e}")

# ========== PAGINATION VIEW FOR DEMOS ==========
class DemosView(View):
    def __init__(self, offset=0):
        super().__init__(timeout=300)  # 5 minute timeout
        self.offset = offset
        self.update_buttons()
    
    def update_buttons(self):
        self.clear_items()
        
        # Previous button
        if self.offset > 0:
            prev_btn = Button(
                label="◀ Previous",
                style=discord.ButtonStyle.secondary,
                custom_id="prev"
            )
            prev_btn.callback = self.previous_page
            self.add_item(prev_btn)
        
        # Next button (will be enabled/disabled based on has_more)
        next_btn = Button(
            label="Next ▶",
            style=discord.ButtonStyle.primary,
            custom_id="next"
        )
        next_btn.callback = self.next_page
        self.add_item(next_btn)
        
        # Refresh button
        refresh_btn = Button(
            label="🔄 Refresh",
            style=discord.ButtonStyle.success,
            custom_id="refresh"
        )
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
            embed.set_footer(
                text=f"Showing {result['showing']} of {result['total']} demos"
            )
        
        # Update view buttons
        self.update_buttons()
        
        # Disable next button if no more demos
        if not result.get("has_more", False):
            for item in self.children:
                if item.custom_id == "next":
                    item.disabled = True
        
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=self
        )

# Track current players
current_players = {}

def owner_only():
    async def predicate(interaction: discord.Interaction):
        return interaction.user.id == OWNER_ID
    return app_commands.check(predicate)

def is_bot_player(player_name: str) -> bool:
    """Check if player name indicates a bot/GOTV"""
    player_upper = player_name.upper()
    return any(bot_keyword in player_upper for bot_keyword in BOT_FILTER)

def parse_server_logs():
    """
    Fetch CS2 server logs via RCON logaddress method.
    Uses mp_logdetail 3 which logs all kill events.
    Parses kill lines from the RCON log output.
    Returns list of events: [(killer, victim), ...]
    """
    global log_file_position
    events = []

    try:
        # Enable detailed logging via RCON if not already set
        send_rcon_silent("mp_logdetail 3")

        # Fetch recent log via RCON
        log_output = send_rcon("log")

        if not log_output or "Error" in log_output:
            return []

        # CS2 log line format with mp_logdetail 3:
        # L MM/DD/YYYY - HH:MM:SS: "Killer<id><steamid><team>" killed "Victim<id><steamid><team>" with "weapon"
        kill_pattern = re.compile(
            r'"([^"<]+)<\d+><[^>]*><[^>]*>" killed '
            r'"([^"<]+)<\d+><[^>]*><[^>]*>" with'
        )

        for line in log_output.splitlines():
            if ' killed ' not in line:
                continue
            match = kill_pattern.search(line)
            if match:
                killer = match.group(1).strip()
                victim = match.group(2).strip()
                if not is_bot_player(killer) and not is_bot_player(victim):
                    events.append((killer, victim))

    except Exception as e:
        print(f"Error fetching RCON logs: {e}")

    return events

def update_kd_stats(events):
    """Update kill/death stats in database from parsed events"""
    if not events:
        return
    
    conn = get_db()
    c = conn.cursor()
    
    for killer, victim in events:
        # Update killer's kills
        c.execute('''INSERT INTO player_stats (player_name, kills, deaths)
                     VALUES (%s, 1, 0)
                     ON CONFLICT (player_name) DO UPDATE SET
                     kills = kills + 1,
                     last_updated = CURRENT_TIMESTAMP''',
                  (killer,))
        
        # Update victim's deaths
        c.execute('''INSERT INTO player_stats (player_name, kills, deaths)
                     VALUES (%s, 0, 1)
                     ON CONFLICT (player_name) DO UPDATE SET
                     deaths = deaths + 1,
                     last_updated = CURRENT_TIMESTAMP''',
                  (victim,))
    
    conn.commit()
    conn.close()
    
    print(f"Updated K/D stats: {len(events)} kill events processed")

def fetch_player_stats_from_server():
    """
    Fetch current player kill scores via css_scores or status RCON
    Tries multiple commands to get scores
    """
    try:
        player_stats = {}

        # Try css_scores first (CounterStrikeSharp plugin command)
        resp = send_rcon("css_scores")
        if resp and "Error" not in resp and len(resp) > 10:
            # Parse format: "Name" Kills:X Deaths:Y
            for line in resp.splitlines():
                try:
                    kd_match = re.search(
                        r'"([^"]+)"\s+Kills?:(\d+)\s+Deaths?:(\d+)',
                        line, re.IGNORECASE
                    )
                    if kd_match:
                        name = kd_match.group(1).strip()
                        kills = int(kd_match.group(2))
                        deaths = int(kd_match.group(3))
                        if not is_bot_player(name):
                            player_stats[name] = (kills, deaths)
                except:
                    continue

        # If css_scores didn't work, try parsing status for scores
        if not player_stats:
            status = send_rcon("status")
            if status and "Error" not in status:
                for line in status.splitlines():
                    line = line.strip()
                    if not line.startswith('#'):
                        continue
                    try:
                        # status format has score in it
                        # # 2 "PlayerName" STEAM_1:0:xxx 00:00 50 0 active
                        parts = line.split('"')
                        if len(parts) >= 2:
                            name = parts[1].strip()
                            if is_bot_player(name):
                                continue
                            # Score is after the name+steamid block
                            after = parts[2].strip().split()
                            if len(after) >= 2:
                                score = int(after[1]) if after[1].isdigit() else 0
                                if score > 0:
                                    player_stats[name] = (score, 0)
                    except:
                        continue

        return player_stats

    except Exception as e:
        print(f"Error fetching player stats: {e}")
        return {}

def update_kd_from_scoreboard():
    """
    Update K/D stats from current server scoreboard every minute.
    Accumulates kills/deaths over time (not snapshot).
    Stores last known score to calculate deltas.
    """
    player_stats = fetch_player_stats_from_server()

    if not player_stats:
        return

    conn = get_db()
    c = conn.cursor()

    # Ensure last_score column exists for delta tracking
    try:
        c.execute('ALTER TABLE player_stats ADD COLUMN last_score INTEGER DEFAULT 0')
        conn.commit()
    except:
        pass  # Column already exists

    updated = 0
    for player_name, (kills, deaths) in player_stats.items():
        # Get previous score to calculate kill delta
        c.execute(
            'SELECT kills, deaths, last_score FROM player_stats WHERE player_name = %s',
            (player_name,)
        )
        row = c.fetchone()

        if row:
            prev_kills, prev_deaths, last_score = row
            last_score = last_score or 0
            kill_delta = max(0, kills - last_score)

            c.execute('''UPDATE player_stats SET
                         kills = kills + %s,
                         deaths = deaths + %s,
                         last_score = %s,
                         last_updated = CURRENT_TIMESTAMP
                         WHERE player_name = %s''',
                      (kill_delta,
                       deaths if deaths > 0 else 0,
                       kills,
                       player_name))
        else:
            c.execute('''INSERT INTO player_stats
                         (player_name, kills, deaths, last_score)
                         VALUES (%s, %s, %s, %s)''',
                      (player_name, kills, deaths, kills))
        updated += 1

    conn.commit()
    conn.close()

    if updated:
        print(f"K/D updated for {updated} players")

def send_rcon(command: str) -> str:
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            resp = rcon.command(command)
            
            # Handle empty or whitespace-only responses (CSS plugins often do this)
            if not resp or resp.strip() == "":
                return "✅ Command executed successfully"
            
            # Check for common CSS success indicators
            if any(indicator in resp.lower() for indicator in ["success", "completed", "done"]):
                return f"✅ {resp[:1000]}"
            
            # Truncate long responses
            response_text = resp[:2000] if len(resp) > 2000 else resp
            
            # If response seems like an error, mark it
            if any(error in resp.lower() for error in ["error", "failed", "invalid", "unknown"]):
                return f"⚠️ {response_text}"
            
            # Default: just return the response
            return response_text
            
    except Exception as e:
        return f"❌ RCON Connection Error: {e}"

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
        
        # Sort by modified_at (newest first)
        demos_sorted = sorted(
            demos,
            key=lambda x: x.get("modified_at", ""),
            reverse=True
        )
        
        # Paginate
        start_idx = offset
        end_idx = offset + limit
        page_demos = demos_sorted[start_idx:end_idx]
        has_more = end_idx < len(demos_sorted)
        
        # Format demos
        formatted_demos = []
        for demo in page_demos:
            name = demo.get("name", "Unknown")
            url = demo.get("download_url", "#")
            size = demo.get("size_formatted", "N/A")
            date_str = demo.get("modified_at", "")
            
            # Parse and format date
            try:
                date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                date_display = date_obj.strftime("%b %d, %Y %H:%M")
            except:
                date_display = "Unknown date"
            
            formatted_demos.append(
                f"🎬 [{name}](<{url}>)\n"
                f"    📅 {date_display} • 💾 {size}"
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
            players.append({
                "name": sanitize(css.group("name")),
                "ping": "-",
                "time": "-"
            })
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

# ========== ENHANCED STATUS EMBED WITH BETTER VISUALS ==========
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
        
        # Determine embed color based on player count
        player_count = info.player_count
        if player_count == 0:
            color = 0x95A5A6  # Gray
        elif player_count < info.max_players / 3:
            color = 0xE74C3C  # Red
        elif player_count < info.max_players * 2/3:
            color = 0xF39C12  # Orange
        else:
            color = 0x2ECC71  # Green
        
        embed = discord.Embed(
            title="🎮 CS2 Server Status",
            description=f"**{info.server_name}**",
            color=color,
            timestamp=datetime.now()
        )
        
        # Server details in organized fields
        embed.add_field(
            name="🗺️ Current Map",
            value=f"`{info.map_name}`",
            inline=True
        )
        embed.add_field(
            name="👥 Players",
            value=f"`{player_count}/{info.max_players}`",
            inline=True
        )
        embed.add_field(
            name="🌐 Connect",
            value=f"`connect {SERVER_IP}:{SERVER_PORT}`",
            inline=False
        )
        
        # Player list with better formatting
        if isinstance(players, list) and players and isinstance(players[0], dict):
            player_list = []
            for i, p in enumerate(players, 1):
                player_list.append(f"`{i}.` **{p['name']}**")
            listing = "\n".join(player_list)
        elif players:
            player_list = []
            for i, p in enumerate(players, 1):
                player_list.append(
                    f"`{i}.` **{sanitize(p.name)}** • `{p.score}` pts"
                )
            listing = "\n".join(player_list)
        else:
            listing = "*No players online*"
        
        embed.add_field(
            name=f"🎯 Players Online ({player_count})",
            value=listing if len(listing) < 1024 else listing[:1020] + "...",
            inline=False
        )
        
        # Footer with timestamp
        embed.set_footer(
            text=f"Last updated",
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

# ========== PLAYER COUNT GRAPH GENERATION ==========
def generate_player_graph():
    if not HAS_MATPLOTLIB:
        return None
    
    conn = get_db()
    c = conn.cursor()
    
    # Get last 24 hours of data
    c.execute('''SELECT timestamp, player_count
                 FROM server_snapshots
                 WHERE timestamp > datetime('now', '-24 hours')
                 ORDER BY timestamp''')
    data = c.fetchall()
    conn.close()
    
    if len(data) < 2:
        return None
    
    timestamps = [datetime.fromisoformat(row[0]) for row in data]
    counts = [row[1] for row in data]
    
    # Create graph
    plt.figure(figsize=(12, 6))
    plt.plot(timestamps, counts, linewidth=2, color='#5865F2', marker='o')
    plt.fill_between(timestamps, counts, alpha=0.3, color='#5865F2')
    
    plt.title('Server Player Count (Last 24 Hours)', fontsize=16, fontweight='bold')
    plt.xlabel('Time', fontsize=12)
    plt.ylabel('Players', fontsize=12)
    plt.grid(True, alpha=0.3)
    plt.gcf().autofmt_xdate()
    
    # Format x-axis
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=2))
    
    plt.tight_layout()
    
    # Save to bytes
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plt.close()
    
    return buf

# ========== PLAYER STATS FUNCTIONS ==========
def record_snapshot(player_count, map_name):
    conn = get_db()
    c = conn.cursor()
    c.execute('INSERT INTO server_snapshots (player_count, map_name) VALUES (%s, %s)',
              (player_count, map_name))
    conn.commit()
    conn.close()

def update_player_tracking(current_player_names, map_name):
    global current_players
    now = datetime.now()
    
    conn = get_db()
    c = conn.cursor()
    
    notifications = []
    
    # Check for players who left
    for player_name in list(current_players.keys()):
        if player_name not in current_player_names:
            join_time = current_players[player_name]
            leave_time = now
            duration = int((leave_time - join_time).total_seconds() / 60)
            
            c.execute('''INSERT INTO player_sessions
                         (player_name, join_time, leave_time, duration_minutes, map_name)
                         VALUES (%s, %s, %s, %s, %s)''',
                      (player_name, join_time, leave_time, duration, map_name))
            
            # Create leave notification
            hours = duration // 60
            mins = duration % 60
            
            if hours > 0:
                time_str = f"{hours}h {mins}m"
            else:
                time_str = f"{mins}m"
            
            notifications.append({
                'type': 'leave',
                'player': player_name,
                'duration': time_str
            })
            
            del current_players[player_name]
    
    # Check for new players
    for player_name in current_player_names:
        if player_name not in current_players:
            current_players[player_name] = now
            
            # Create join notification
            notifications.append({
                'type': 'join',
                'player': player_name,
                'map': map_name
            })
    
    conn.commit()
    conn.close()
    
    return notifications

def get_player_stats(player_name):
    conn = get_db()
    c = conn.cursor()
    
    # Total playtime
    c.execute('''SELECT SUM(duration_minutes), COUNT(*)
                 FROM player_sessions
                 WHERE player_name = %s''', (player_name,))
    result = c.fetchone()
    total_minutes = result[0] or 0
    total_sessions = result[1] or 0
    
    # Favorite map
    c.execute('''SELECT map_name, SUM(duration_minutes) as total
                 FROM player_sessions
                 WHERE player_name = %s
                 GROUP BY map_name
                 ORDER BY total DESC
                 LIMIT 1''', (player_name,))
    fav_map = c.fetchone()
    favorite_map = fav_map[0] if fav_map else "N/A"
    
    # Last seen
    c.execute('''SELECT leave_time
                 FROM player_sessions
                 WHERE player_name = %s
                 ORDER BY leave_time DESC
                 LIMIT 1''', (player_name,))
    last_seen = c.fetchone()
    last_seen_time = last_seen[0] if last_seen else None
    
    # K/D stats
    c.execute('''SELECT kills, deaths
                 FROM player_stats
                 WHERE player_name = %s''', (player_name,))
    kd_result = c.fetchone()
    
    if kd_result:
        kills, deaths = kd_result
        kd_ratio = kills / deaths if deaths > 0 else kills
    else:
        kills, deaths, kd_ratio = 0, 0, 0.0
    
    conn.close()
    
    return {
        'total_minutes': total_minutes,
        'total_sessions': total_sessions,
        'favorite_map': favorite_map,
        'last_seen': last_seen_time,
        'kills': kills,
        'deaths': deaths,
        'kd_ratio': kd_ratio
    }

def get_leaderboard(limit=10):
    conn = get_db()
    c = conn.cursor()
    
    # Get playtime and K/D stats
    c.execute('''SELECT 
                    ps.player_name,
                    SUM(ps.duration_minutes) as total_time,
                    COALESCE(pst.kills, 0) as kills,
                    COALESCE(pst.deaths, 0) as deaths
                 FROM player_sessions ps
                 LEFT JOIN player_stats pst ON ps.player_name = pst.player_name
                 GROUP BY ps.player_name
                 ORDER BY total_time DESC''')
    
    all_players = c.fetchall()
    conn.close()
    
    # Filter out bots and limit results
    real_players = [
        (name, time, kills, deaths) for name, time, kills, deaths in all_players 
        if not is_bot_player(name)
    ]
    
    return real_players[:limit]

# ========== BACKGROUND TASKS ==========
@tasks.loop(minutes=1)
async def update_server_stats():
    try:
        # Fetch kills via RCON logs (mp_logdetail 3 method)
        # No file access needed - works directly via RCON
        loop = asyncio.get_running_loop()
        events = await loop.run_in_executor(None, parse_server_logs)
        if events:
            update_kd_stats(events)
            print(f"K/D: {len(events)} kills tracked via RCON logs")
        else:
            # Fallback to scoreboard if no kill events found
            await loop.run_in_executor(None, update_kd_from_scoreboard)
        
        addr = (SERVER_IP, SERVER_PORT)
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, a2s.info, addr)
        
        # Get player list
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
        
        # Extract player names
        if player_list and isinstance(player_list[0], dict):
            player_names = [p['name'] for p in player_list]
        elif player_list:
            player_names = [sanitize(p.name) for p in player_list]
        else:
            player_names = []
        
        # Filter out bots from player tracking
        real_player_names = [name for name in player_names if not is_bot_player(name)]
        
        # Record snapshot (every check) - use real player count
        record_snapshot(len(real_player_names), info.map_name)
        
        # Update tracking and get notifications (only for real players)
        notifications = update_player_tracking(real_player_names, info.map_name)
        
        # Send notifications to Discord
        if notifications and NOTIFICATIONS_CHANNEL_ID:
            channel = bot.get_channel(NOTIFICATIONS_CHANNEL_ID)
            if channel:
                for notif in notifications:
                    if notif['type'] == 'join':
                        embed = discord.Embed(
                            description=f"👋 **{notif['player']}** joined the server",
                            color=0x2ECC71
                        )
                        embed.add_field(
                            name="Current Map",
                            value=f"`{notif['map']}`",
                            inline=True
                        )
                        embed.set_footer(text=f"Players online: {len(real_player_names)}")
                        await channel.send(embed=embed)
                    
                    elif notif['type'] == 'leave':
                        embed = discord.Embed(
                            description=f"👋 **{notif['player']}** left the server",
                            color=0x95A5A6
                        )
                        embed.add_field(
                            name="Session Duration",
                            value=f"`{notif['duration']}`",
                            inline=True
                        )
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
        
        # Try to find existing status message
        async for message in channel.history(limit=20):
            if message.author == bot.user and message.embeds:
                if "CS2 Server Status" in message.embeds[0].title:
                    await message.edit(embed=embed)
                    return
        
        # If no existing message, send new one
        await channel.send(embed=embed)
        
    except Exception as e:
        print(f"Error updating status message: {e}")

@bot.event
async def on_ready():
    print(f"Bot online as {bot.user.name}")
    print(f"Bot ID: {bot.user.id}")
    print(f"Owner ID from env: {OWNER_ID}")
    
    # Enable detailed kill logging on server
    try:
        send_rcon_silent("mp_logdetail 3")
        send_rcon_silent("log on")
        print("✓ Server kill logging enabled (mp_logdetail 3)")
    except Exception as e:
        print(f"⚠️ Could not enable kill logging: {e}")
    
    # Auto-sync slash commands
    print("Syncing slash commands...")
    try:
        synced = await bot.tree.sync()
        print(f"✓ Synced {len(synced)} commands globally")
    except Exception as e:
        print(f"✗ Failed to sync commands: {e}")
    
    print("Starting background tasks...")
    update_server_stats.start()
    update_status_message.start()

@bot.event
async def on_message(message):
    # Ignore bot's own messages
    if message.author == bot.user:
        return
    
    # Debug log for owner messages starting with !
    if message.content.startswith('!') and message.author.id == OWNER_ID:
        print(f"Owner command detected: {message.content}")
    
    # Process commands
    await bot.process_commands(message)

# ========== COMMANDS ==========
@bot.command()
@commands.guild_only()
@commands.is_owner()
async def sync(ctx, guilds: commands.Greedy[discord.Object] = None,
               spec: Optional[Literal["~", "*", "^"]] = None):
    print(f"Sync command called by {ctx.author.id}")
    await ctx.send("⏳ Syncing commands...", delete_after=5)
    
    if not guilds:
        if spec == "~":
            synced = await bot.tree.sync(guild=ctx.guild)
            await ctx.send(f"✅ Synced {len(synced)} commands to this server.")
        elif spec == "*":
            bot.tree.copy_global_to(guild=ctx.guild)
            synced = await bot.tree.sync(guild=ctx.guild)
            await ctx.send(f"✅ Copied and synced {len(synced)} commands to this server.")
        elif spec == "^":
            bot.tree.clear_commands(guild=ctx.guild)
            await bot.tree.sync(guild=ctx.guild)
            await ctx.send("✅ Cleared all commands from this server.")
            synced = []
        else:
            synced = await bot.tree.sync()
            await ctx.send(f"✅ Synced {len(synced)} commands globally (may take up to 1 hour).")
        return
    
    count = 0
    for guild in guilds:
        try:
            await bot.tree.sync(guild=guild)
            count += 1
        except:
            pass
    await ctx.send(f"✅ Synced to {count}/{len(guilds)} guilds.")

@bot.command()
async def ping(ctx):
    """Simple test command to verify bot is responding"""
    latency = round(bot.latency * 1000)
    await ctx.send(f"🏓 Pong! Latency: {latency}ms")

@bot.tree.command(name="status", description="View server status")
async def status_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    embed, _ = await get_enhanced_status_embed()
    await inter.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="graph", description="View player count graph (24h)")
async def graph_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    
    if not HAS_MATPLOTLIB:
        return await inter.followup.send(
            "❌ Graph feature requires matplotlib to be installed.",
            ephemeral=True
        )
    
    buf = generate_player_graph()
    
    if buf is None:
        return await inter.followup.send(
            "❌ Not enough data yet. Graphs require at least 2 data points.",
            ephemeral=True
        )
    
    file = discord.File(buf, filename="player_graph.png")
    embed = discord.Embed(
        title="📊 Player Count Analytics",
        description="Server activity over the last 24 hours",
        color=0x5865F2
    )
    embed.set_image(url="attachment://player_graph.png")
    
    await inter.followup.send(embed=embed, file=file, ephemeral=True)

@bot.tree.command(name="profile", description="View player statistics")
async def profile_cmd(inter: discord.Interaction, player_name: str):
    await inter.response.defer(ephemeral=True)
    
    stats = get_player_stats(player_name)
    
    if stats['total_sessions'] == 0:
        return await inter.followup.send(
            f"❌ No stats found for player **{player_name}**",
            ephemeral=True
        )
    
    hours = stats['total_minutes'] / 60
    avg_session = stats['total_minutes'] / stats['total_sessions']
    
    embed = discord.Embed(
        title=f"👤 Player Profile: {player_name}",
        color=0x2ECC71
    )
    
    embed.add_field(
        name="⏱️ Total Playtime",
        value=f"**{hours:.1f}** hours",
        inline=True
    )
    embed.add_field(
        name="🎮 Sessions",
        value=f"**{stats['total_sessions']}** total",
        inline=True
    )
    embed.add_field(
        name="⏰ Avg Session",
        value=f"**{avg_session:.0f}** min",
        inline=True
    )
    
    # K/D Stats
    embed.add_field(
        name="💀 Kills",
        value=f"**{stats['kills']}**",
        inline=True
    )
    embed.add_field(
        name="☠️ Deaths",
        value=f"**{stats['deaths']}**",
        inline=True
    )
    embed.add_field(
        name="📊 K/D Ratio",
        value=f"**{stats['kd_ratio']:.2f}**",
        inline=True
    )
    
    embed.add_field(
        name="🗺️ Favorite Map",
        value=f"`{stats['favorite_map']}`",
        inline=True
    )
    
    if stats['last_seen']:
        last_seen_dt = datetime.fromisoformat(stats['last_seen'])
        time_ago = datetime.now() - last_seen_dt
        days = time_ago.days
        hours_ago = time_ago.seconds // 3600
        
        if days > 0:
            last_seen_str = f"{days}d {hours_ago}h ago"
        else:
            last_seen_str = f"{hours_ago}h ago"
        
        embed.add_field(
            name="👁️ Last Seen",
            value=last_seen_str,
            inline=True
        )
    
    embed.set_footer(text=f"Stats tracked since bot installation")
    
    await inter.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="leaderboard", description="View top players")
async def leaderboard_cmd(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    
    leaderboard = get_leaderboard(10)
    
    if not leaderboard:
        return await inter.followup.send(
            "❌ No player data available yet.",
            ephemeral=True
        )
    
    embed = discord.Embed(
        title="🏆 Top Players Leaderboard",
        description="*Based on total playtime • Bots excluded*",
        color=0xF1C40F
    )
    
    medals = ["🥇", "🥈", "🥉"]
    
    for i, (player_name, total_minutes, kills, deaths) in enumerate(leaderboard, 1):
        hours = total_minutes / 60
        kd_ratio = kills / deaths if deaths > 0 else kills
        medal = medals[i-1] if i <= 3 else f"`{i}.`"
        
        # Format K/D display
        kd_display = f"K/D: {kd_ratio:.2f}" if (kills > 0 or deaths > 0) else "K/D: N/A"
        
        embed.add_field(
            name=f"{medal} {player_name}",
            value=f"**{hours:.1f}** hours • {kd_display}",
            inline=False
        )
    
    await inter.followup.send(embed=embed, ephemeral=True)

@bot.tree.command(name="demos", description="View server demos")
async def demos_cmd(inter: discord.Interaction):
    if SERVER_DEMOS_CHANNEL_ID and inter.channel_id != SERVER_DEMOS_CHANNEL_ID:
        return await inter.response.send_message(
            "Wrong channel!", ephemeral=True
        )
    
    await inter.response.defer(ephemeral=True)
    
    result = fetch_demos(0, 5)
    
    embed = discord.Embed(
        title="🎥 Server Demos",
        description="\n\n".join(result["demos"]),
        color=0x9B59B6
    )
    
    if result.get("total"):
        embed.set_footer(
            text=f"Showing {result['showing']} of {result['total']} demos"
        )
    
    view = DemosView(offset=0)
    
    # Disable next button if no more demos
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
        title=f"{stats['country_flag']} {stats['nickname']} — FACEIT CS2",
        color=0xFF8800
    )
    embed.set_thumbnail(url=stats["avatar"])
    embed.add_field(name="Level", value=stats["level"], inline=True)
    embed.add_field(name="ELO", value=stats["elo"], inline=True)
    embed.add_field(
        name="Win Rate",
        value=str(stats["win_rate"]) + "%",
        inline=True
    )
    embed.add_field(name="Matches", value=stats["matches"], inline=True)
    embed.add_field(name="K/D", value=stats["kd_ratio"], inline=True)
    embed.set_footer(text=f"Player ID: {stats['player_id']}")
    await inter.followup.send(embed=embed, ephemeral=True)

# Admin commands with enhanced feedback
@bot.tree.command(name="csssay", description="Send center-screen message to all players")
@owner_only()
async def csssay(inter: discord.Interaction, message: str):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f"css_cssay {message}")
    
    # Add context to the response
    await inter.followup.send(
        f"📢 **Message Sent**\n```{message}```\n{resp}",
        ephemeral=True
    )

@bot.tree.command(name="csshsay", description="Send hint message to all players")
@owner_only()
async def csshsay(inter: discord.Interaction, message: str):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f"css_hsay {message}")
    
    await inter.followup.send(
        f"💬 **Hint Sent**\n```{message}```\n{resp}",
        ephemeral=True
    )

@bot.tree.command(name="csskick", description="Kick a player from the server")
@owner_only()
async def csskick(inter: discord.Interaction, player: str):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f'css_kick "{player}"')
    
    await inter.followup.send(
        f"👢 **Kick Command**\nPlayer: `{player}`\n\n{resp}",
        ephemeral=True
    )

@bot.tree.command(name="cssban", description="Ban a player from the server")
@owner_only()
async def cssban(inter: discord.Interaction, 
                player: str, 
                minutes: int, 
                reason: str = "No reason"):
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f'css_ban "{player}" {minutes} "{reason}"')
    
    await inter.followup.send(
        f"🔨 **Ban Command**\n"
        f"Player: `{player}`\n"
        f"Duration: `{minutes} minutes`\n"
        f"Reason: `{reason}`\n\n{resp}",
        ephemeral=True
    )

@bot.tree.command(name="csschangemap", description="Change the server map")
@owner_only()
async def csschangemap(inter: discord.Interaction, map: str):
    if map not in MAP_WHITELIST:
        return await inter.response.send_message(
            f"❌ Map `{map}` not allowed.\n"
            f"Allowed maps: {', '.join(MAP_WHITELIST)}",
            ephemeral=True
        )
    
    await inter.response.defer(ephemeral=True)
    resp = send_rcon(f"css_changemap {map}")
    
    await inter.followup.send(
        f"🗺️ **Map Change**\nChanging to: `{map}`\n\n{resp}",
        ephemeral=True
    )

@csschangemap.autocomplete("map")
async def autocomplete_map(inter, current: str):
    return [
        app_commands.Choice(name=m, value=m)
        for m in MAP_WHITELIST if current.lower() in m.lower()
    ]

@bot.tree.command(name="cssreload")
@owner_only()
async def cssreload(inter):
    resp = send_rcon("css_reloadplugins")
    await inter.response.send_message(resp, ephemeral=True)

@bot.tree.command(name="setkd", description="Set kills/deaths for a player")
@owner_only()
async def setkd_cmd(inter: discord.Interaction, player_name: str, kills: int, deaths: int):
    """Manually set K/D stats for a player"""
    await inter.response.defer(ephemeral=True)
    
    conn = get_db()
    c = conn.cursor()
    
    c.execute('''INSERT INTO player_stats
                 (player_name, kills, deaths, last_updated)
                 VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                 ON CONFLICT (player_name) DO UPDATE SET
                 kills = %s,
                 deaths = %s,
                 last_updated = CURRENT_TIMESTAMP''',
              (player_name, kills, deaths, kills, deaths))
    
    conn.commit()
    conn.close()
    
    kd_ratio = kills / deaths if deaths > 0 else kills
    
    await inter.followup.send(
        f"✅ **K/D Stats Updated**\n"
        f"Player: `{player_name}`\n"
        f"Kills: **{kills}**\n"
        f"Deaths: **{deaths}**\n"
        f"K/D Ratio: **{kd_ratio:.2f}**",
        ephemeral=True
    )

@bot.tree.command(name="notifications", description="Toggle join/leave notifications")
@owner_only()
async def notifications_cmd(inter: discord.Interaction, 
                            enabled: bool,
                            channel: discord.TextChannel = None):
    """
    Enable or disable player join/leave notifications
    
    Parameters:
    -----------
    enabled: True to enable, False to disable
    channel: Channel where notifications should be posted (optional)
    """
    await inter.response.defer(ephemeral=True)
    
    if enabled:
        if channel:
            # User specified a channel
            channel_id = channel.id
            msg = f"✅ Notifications enabled in {channel.mention}"
        elif NOTIFICATIONS_CHANNEL_ID:
            # Already configured
            channel_id = NOTIFICATIONS_CHANNEL_ID
            msg = f"✅ Notifications are enabled in <#{NOTIFICATIONS_CHANNEL_ID}>"
        else:
            # No channel specified and none configured
            return await inter.followup.send(
                "❌ Please specify a channel: `/notifications enabled:True channel:#your-channel`",
                ephemeral=True
            )
        
        msg += f"\n\n**Note:** Set `NOTIFICATIONS_CHANNEL_ID={channel_id}` in Railway environment variables to persist this setting."
        
    else:
        msg = "✅ Notifications disabled. Remove `NOTIFICATIONS_CHANNEL_ID` from Railway to persist."
    
    await inter.followup.send(msg, ephemeral=True)

if not TOKEN:
    raise SystemExit("TOKEN missing.")

bot.run(TOKEN)