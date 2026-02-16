import os
import re
import json
import pytz
import a2s
import asyncio
import discord
import requests
import sqlite3
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
FACEIT_GAME_ID = "cs2"
MAP_WHITELIST = [
    "de_inferno", "de_mirage", "de_dust2", "de_overpass",
    "de_nuke", "de_ancient", "de_vertigo", "de_anubis"
]

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
bot = commands.Bot(command_prefix="!", intents=intents, owner_id=OWNER_ID)

# Database setup for player stats
DB_FILE = "player_stats.db"

def init_database():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Player sessions table
    c.execute('''CREATE TABLE IF NOT EXISTS player_sessions
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  player_name TEXT NOT NULL,
                  join_time TIMESTAMP,
                  leave_time TIMESTAMP,
                  duration_minutes INTEGER,
                  map_name TEXT)''')
    
    # Server snapshots table (for graphs)
    c.execute('''CREATE TABLE IF NOT EXISTS server_snapshots
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                  player_count INTEGER,
                  map_name TEXT)''')
    
    # Map statistics
    c.execute('''CREATE TABLE IF NOT EXISTS map_stats
                 (map_name TEXT PRIMARY KEY,
                  times_played INTEGER DEFAULT 0,
                  total_players INTEGER DEFAULT 0)''')
    
    conn.commit()
    conn.close()

# Initialize database on startup
init_database()

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

def send_rcon(command: str) -> str:
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            resp = rcon.command(command)
            return resp[:2000] if len(resp) > 2000 else resp
    except Exception as e:
        return f"RCON Error: {e}"

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
    
    conn = sqlite3.connect(DB_FILE)
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
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('INSERT INTO server_snapshots (player_count, map_name) VALUES (?, ?)',
              (player_count, map_name))
    conn.commit()
    conn.close()

def update_player_tracking(current_player_names, map_name):
    global current_players
    now = datetime.now()
    
    conn = sqlite3.connect(DB_FILE)
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
                         VALUES (?, ?, ?, ?, ?)''',
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
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Total playtime
    c.execute('''SELECT SUM(duration_minutes), COUNT(*)
                 FROM player_sessions
                 WHERE player_name = ?''', (player_name,))
    result = c.fetchone()
    total_minutes = result[0] or 0
    total_sessions = result[1] or 0
    
    # Favorite map
    c.execute('''SELECT map_name, SUM(duration_minutes) as total
                 FROM player_sessions
                 WHERE player_name = ?
                 GROUP BY map_name
                 ORDER BY total DESC
                 LIMIT 1''', (player_name,))
    fav_map = c.fetchone()
    favorite_map = fav_map[0] if fav_map else "N/A"
    
    # Last seen
    c.execute('''SELECT leave_time
                 FROM player_sessions
                 WHERE player_name = ?
                 ORDER BY leave_time DESC
                 LIMIT 1''', (player_name,))
    last_seen = c.fetchone()
    last_seen_time = last_seen[0] if last_seen else None
    
    conn.close()
    
    return {
        'total_minutes': total_minutes,
        'total_sessions': total_sessions,
        'favorite_map': favorite_map,
        'last_seen': last_seen_time
    }

def get_leaderboard(limit=10):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute('''SELECT player_name, SUM(duration_minutes) as total_time
                 FROM player_sessions
                 GROUP BY player_name
                 ORDER BY total_time DESC
                 LIMIT ?''', (limit,))
    
    leaderboard = c.fetchall()
    conn.close()
    
    return leaderboard

# ========== BACKGROUND TASKS ==========
@tasks.loop(minutes=1)  # Changed to 1 minute for faster notifications
async def update_server_stats():
    try:
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
        
        # Record snapshot (every check)
        record_snapshot(info.player_count, info.map_name)
        
        # Update tracking and get notifications
        notifications = update_player_tracking(player_names, info.map_name)
        
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
                        embed.set_footer(text=f"Players online: {len(player_names)}")
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
                        embed.set_footer(text=f"Players online: {len(player_names)}")
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
    await inter.response.defer()
    embed, _ = await get_enhanced_status_embed()
    await inter.followup.send(embed=embed)

@bot.tree.command(name="graph", description="View player count graph (24h)")
async def graph_cmd(inter: discord.Interaction):
    await inter.response.defer()
    
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
    
    await inter.followup.send(embed=embed, file=file)

@bot.tree.command(name="profile", description="View player statistics")
async def profile_cmd(inter: discord.Interaction, player_name: str):
    await inter.response.defer()
    
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
    
    await inter.followup.send(embed=embed)

@bot.tree.command(name="leaderboard", description="View top players")
async def leaderboard_cmd(inter: discord.Interaction):
    await inter.response.defer()
    
    leaderboard = get_leaderboard(10)
    
    if not leaderboard:
        return await inter.followup.send(
            "❌ No player data available yet.",
            ephemeral=True
        )
    
    embed = discord.Embed(
        title="🏆 Top Players Leaderboard",
        description="*Based on total playtime*",
        color=0xF1C40F
    )
    
    medals = ["🥇", "🥈", "🥉"]
    
    for i, (player_name, total_minutes) in enumerate(leaderboard, 1):
        hours = total_minutes / 60
        medal = medals[i-1] if i <= 3 else f"`{i}.`"
        
        embed.add_field(
            name=f"{medal} {player_name}",
            value=f"**{hours:.1f}** hours",
            inline=False
        )
    
    await inter.followup.send(embed=embed)

@bot.tree.command(name="demos", description="View server demos")
async def demos_cmd(inter: discord.Interaction):
    if SERVER_DEMOS_CHANNEL_ID and inter.channel_id != SERVER_DEMOS_CHANNEL_ID:
        return await inter.response.send_message(
            "Wrong channel!", ephemeral=True
        )
    
    await inter.response.defer()
    
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
    
    await inter.followup.send(embed=embed, view=view)

@bot.tree.command(name="elo", description="Get FACEIT stats for CS2")
async def faceit_cmd(inter: discord.Interaction, nickname: str):
    await inter.response.defer()
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
    await inter.followup.send(embed=embed)

# Admin commands (keeping them short for mobile)
@bot.tree.command(name="csssay")
@owner_only()
async def csssay(inter, message: str):
    resp = send_rcon(f"css_cssay {message}")
    await inter.response.send_message(resp, ephemeral=True)

@bot.tree.command(name="csshsay")
@owner_only()
async def csshsay(inter, message: str):
    resp = send_rcon(f"css_hsay {message}")
    await inter.response.send_message(resp, ephemeral=True)

@bot.tree.command(name="csskick")
@owner_only()
async def csskick(inter, player: str):
    resp = send_rcon(f'css_kick "{player}"')
    await inter.response.send_message(resp, ephemeral=True)

@bot.tree.command(name="cssban")
@owner_only()
async def cssban(inter, player: str, minutes: int, reason: str = "No reason"):
    resp = send_rcon(f'css_ban "{player}" {minutes} "{reason}"')
    await inter.response.send_message(resp, ephemeral=True)

@bot.tree.command(name="csschangemap")
@owner_only()
async def csschangemap(inter, map: str):
    if map not in MAP_WHITELIST:
        return await inter.response.send_message(
            "Map not allowed.", ephemeral=True
        )
    resp = send_rcon(f"css_changemap {map}")
    await inter.response.send_message(resp, ephemeral=True)

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