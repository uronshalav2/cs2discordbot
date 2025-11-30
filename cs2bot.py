import os
import re
import asyncio
import discord
import requests
from bs4 import BeautifulSoup
from discord.ext import tasks, commands
from discord import app_commands
import a2s
from mcrcon import MCRcon
from datetime import datetime
import pytz
from typing import Literal, Optional
import json # Added for handling API responses

# ====== BOT CONFIG ======
TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP", "127.0.0.1")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))
RCON_IP = os.getenv("RCON_IP", SERVER_IP)
RCON_PORT = int(os.getenv("RCON_PORT", 27015))
RCON_PASSWORD = os.getenv("RCON_PASSWORD", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
SERVER_DEMOS_CHANNEL_ID = int(os.getenv("SERVER_DEMOS_CHANNEL_ID", 0))
DEMOS_URL = os.getenv("DEMOS_URL")
GUILD_ID = int(os.getenv("GUILD_ID", "0") or "0")

# ====== API KEYS ======
# YOU MUST SET THIS IN YOUR ENVIRONMENT
FACEIT_API_KEY = os.getenv("FACEIT_API_KEY") 

# ====== OWNER ID (Used for both prefix and slash commands) ======
OWNER_ID = int(os.getenv("OWNER_ID", 0))

# ====== DISCORD CLIENT ======
intents = discord.Intents.default()
intents.message_content = True 
intents.messages = True 

bot = commands.Bot(command_prefix="!", intents=intents, owner_id=OWNER_ID)
tree = app_commands.CommandTree(bot)

# ====== MAP WHITELIST ======
MAP_WHITELIST = [
    "de_inferno", "de_mirage", "de_dust2", "de_overpass",
    "de_nuke", "de_ancient", "de_vertigo", "de_anubis"
]

# ====== HELPERS ======
def owner_only():
    async def predicate(interaction: discord.Interaction):
        return interaction.user.id == OWNER_ID
    return app_commands.check(predicate)

def send_rcon_command(command: str) -> str:
    """Send an RCON command to the CS2 server and return response."""
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            resp = rcon.command(command)
            return resp if len(resp) <= 2000 else resp[:2000] + "... (truncated)"
    except Exception as e:
        return f"⚠️ Error: {e}"

def country_code_to_flag(code: str) -> str:
    if not code or len(code) != 2:
        return "🏳️"
    # Unicode regional indicator symbols (works for most countries)
    return chr(ord(code[0].upper()) + 127397) + chr(ord(code[1].upper()) + 127397)

def fetch_demos():
    try:
        r = requests.get(DEMOS_URL, timeout=10)
        if r.status_code != 200:
            return ["⚠️ Could not fetch demos."]
        soup = BeautifulSoup(r.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".dem")]
        if not links:
            return ["⚠️ No demos found."]
        latest = links[-5:]
        return [f"[{d}](<{DEMOS_URL}{d}>)" for d in latest]
    except Exception as e:
        return [f"⚠️ Error fetching demos: {e}"]

# ---------- Blank-name fix: RCON parsing ----------
STATUS_NAME_RE = re.compile(r'^#\s*\d+\s+"(?P<name>.*?)"\s+')
CSS_LIST_RE    = re.compile(r'^\s*•\s*\[#\d+\]\s*"(?P<name>[^"]*)"')

def sanitize_name(s: str) -> str:
    if not s:
        return "—"
    s = s.replace('\x00', '').replace('\u200b', '')
    for ch in ['*', '_', '`', '~', '|', '>', '@']:
        s = s.replace(ch, f'\\{ch}')
    return s.strip()

def rcon_list_players():
    """Returns a list of unique, sanitized player names from RCON."""
    txt = send_rcon_command('css_players')
    used_css = txt and 'Unknown command' not in txt and 'Error' not in txt

    if not used_css:
        txt = send_rcon_command('status')

    players = []
    # ... (rcon_list_players logic remains the same) ...
    for raw in txt.splitlines():
        line = raw.strip()

        # Custom CSS format
        m_css = CSS_LIST_RE.match(line)
        if m_css:
            name = sanitize_name(m_css.group('name'))
            players.append({'name': name, 'time': '—', 'ping': '—'})
            continue

        # Vanilla status format
        m_std = STATUS_NAME_RE.match(line)
        if m_std:
            name = sanitize_name(m_std.group('name'))
            time_match = re.search(r'\b(\d{1,2}:\d{2})\b', line)
            ping_match = re.search(r'(\d+)\s*$', line.split('"')[-1].strip()) 
            
            players.append({
                'name': name,
                'time': time_match.group(1) if time_match else '—',
                'ping': ping_match.group(1) if ping_match else '—'
            })

    # De-dup + keep order
    seen = set()
    uniq = []
    for p in players:
        if p['name'] not in seen and p['name'] != '—':
            uniq.append(p)
            seen.add(p['name'])
    return uniq
# ---------------------------------------------------

async def get_server_status_embed() -> discord.Embed:
    # ... (get_server_status_embed logic remains the same) ...
    addr = (SERVER_IP, SERVER_PORT)
    try:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, a2s.info, addr)
        a2s_players = await asyncio.wait_for(loop.run_in_executor(None, a2s.players, addr), timeout=5)

        names_blank = (not a2s_players) or all(not getattr(p, 'name', '') for p in a2s_players)

        rcon_players = []
        if names_blank:
            rcon_players = rcon_list_players()
            
        berlin_tz = pytz.timezone("Europe/Berlin")
        last_updated = datetime.now(berlin_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

        embed = discord.Embed(title="🎮 CS2 Server Status - 🟢 Online", color=0x00FF00)
        embed.add_field(name="🖥️ Server Name", value=info.server_name, inline=False)
        embed.add_field(name="🗺️ Map", value=info.map_name, inline=True)
        embed.add_field(name="👥 Players", value=f"{info.player_count}/{info.max_players}", inline=True)

        stats = ""
        if rcon_players:
            stats = "\n".join(
                f"🎮 **{p['name']}**"
                for p in rcon_players
            )
        elif a2s_players:
            stats = "\n".join(
                f"🎮 **{sanitize_name(getattr(p, 'name', '') or '—')}** | 🏆 {getattr(p, 'score', 0)} | ⏳ {getattr(p, 'duration', 0)/60:.1f} mins"
                for p in sorted(a2s_players, key=lambda x: getattr(x, 'score', 0), reverse=True)
            )
        
        if not stats:
            stats = "No players online."

        embed.add_field(name="📊 Player Stats", value=stats, inline=False)
        embed.set_footer(text=f"Last updated: {last_updated}")
        return embed

    except asyncio.TimeoutError:
        embed = discord.Embed(title="⚠️ CS2 Server Status - 🟡 Partial Info", color=0xFFCC00)
        embed.add_field(name="❌ Player Info Timeout", value="Server is online, but player list could not be retrieved in time.", inline=False)
        return embed
    
    except Exception:
        embed = discord.Embed(title="⚠️ CS2 Server Status - 🔴 Offline", color=0xFF0000)
        embed.add_field(name="❌ Server Unreachable", value="The server is currently offline.", inline=False)
        return embed

async def fetch_faceit_player_stats(nickname: str) -> dict:
    """
    Fetches player data, including ELO, from the FACEIT API.
    Returns a dictionary or raises an exception on error.
    """
    if not FACEIT_API_KEY:
        raise ValueError("FACEIT_API_KEY is not configured.")

    headers = {"Authorization": f"Bearer {FACEIT_API_KEY}"}
    
    # 1. Get Player ID from Nickname
    player_id_url = f"https://open.faceit.com/data/v4/players?nickname={nickname}"
    id_resp = requests.get(player_id_url, headers=headers)
    
    if id_resp.status_code == 404:
        raise ValueError("Player not found on FACEIT.")
    elif id_resp.status_code != 200:
        id_resp.raise_for_status() # Raise for other HTTP errors (e.g., rate limit)
    
    player_data = id_resp.json()
    player_id = player_data.get('player_id')
    
    # 2. Get Game Stats (for ELO and other metrics)
    # Assuming CS:GO/CS2 is the desired game (GameID is 'csgo')
    stats_url = f"https://open.faceit.com/data/v4/players/{player_id}/stats/csgo"
    stats_resp = requests.get(stats_url, headers=headers)

    if stats_resp.status_code != 200:
        stats_resp.raise_for_status()
        
    stats_data = stats_resp.json()
    
    # Extract the necessary data
    lifetime_stats = stats_data.get('lifetime')
    
    return {
        'nickname': player_data.get('nickname'),
        'player_id': player_id,
        'country_flag': country_code_to_flag(player_data.get('country')),
        'avatar': player_data.get('avatar'),
        'level': player_data.get('games', {}).get('csgo', {}).get('skill_level'),
        'elo': player_data.get('games', {}).get('csgo', {}).get('faceit_elo'),
        'matches': lifetime_stats.get('Matches'),
        'win_rate': lifetime_stats.get('Win Rate %'),
        'kd_ratio': lifetime_stats.get('Average K/D Ratio')
    }

# ====== READY ======
@bot.event
async def on_ready():
    print(f"✅ Bot is running. Logged in as {bot.user.name}")
    print("Use the '!sync' prefix command to update application commands.")

# ====== PREFIX COMMANDS (for syncing) ======

@bot.command()
@commands.guild_only()
@commands.is_owner()
async def sync(ctx: commands.Context, guilds: commands.Greedy[discord.Object], spec: Optional[Literal["~", "*", "^"]] = None) -> None:
    """
    Manually syncs application commands. 
    Use: !sync | !sync ~ | !sync * | !sync ^
    """
    await ctx.send("Starting command synchronization...", delete_after=5)

    if not guilds:
        if spec == "~":
            synced = await ctx.bot.tree.sync(guild=ctx.guild)
        elif spec == "*":
            ctx.bot.tree.copy_global_to(guild=ctx.guild)
            synced = await ctx.bot.tree.sync(guild=ctx.guild)
        elif spec == "^":
            ctx.bot.tree.clear_commands(guild=ctx.guild)
            await ctx.bot.tree.sync(guild=ctx.guild)
            synced = []
        else:
            synced = await ctx.bot.tree.sync()

        await ctx.send(
            f"✅ Synced {len(synced)} commands {'globally' if spec is None else 'to the current guild'}. (Check the UI in a few minutes or hours)."
        )
        return

    ret = 0
    for guild in guilds:
        try:
            await ctx.bot.tree.sync(guild=guild)
        except discord.HTTPException:
            pass
        else:
            ret += 1

    await ctx.send(f"✅ Synced the tree to {ret}/{len(guilds)} specified guilds.")


# ====== SLASH COMMANDS ======

# Use this to force a full re-sync if the bot is only running on one main guild.
@tree.command(name="appsync", description="OWNER: Force sync and remove old commands.")
@owner_only()
async def appsync(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    
    if GUILD_ID:
        guild = discord.Object(id=GUILD_ID)
        await tree.sync(guild=guild)
        await interaction.followup.send(f"✅ Synced commands to Guild {GUILD_ID}.", ephemeral=True)
    else:
        await tree.sync()
        await interaction.followup.send("✅ Globally Synced commands.", ephemeral=True)
        
    print("Commands successfully re-synced.")


@tree.command(name="elo", description="Fetch FACEIT ELO and stats for a given nickname.")
async def elo(interaction: discord.Interaction, nickname: str):
    await interaction.response.defer()
    
    try:
        stats = await fetch_faceit_player_stats(nickname)
        
        embed = discord.Embed(
            title=f"⭐ FACEIT Stats for {stats['country_flag']} {stats['nickname']}", 
            color=0xFF5500 # Orange/Red typical of FACEIT
        )
        embed.set_thumbnail(url=stats['avatar'])

        embed.add_field(name="FACEIT Level", value=f"**{stats['level']}**", inline=True)
        embed.add_field(name="ELO", value=f"**{stats['elo']}**", inline=True)
        embed.add_field(name="Win Rate", value=f"**{stats['win_rate']}%**", inline=True)
        
        embed.add_field(name="Matches", value=f"{stats['matches']}", inline=True)
        embed.add_field(name="K/D Ratio", value=f"{stats['kd_ratio']}", inline=True)
        embed.add_field(name="\u200b", value="\u200b", inline=True) # Invisible field for spacing

        embed.set_footer(text=f"Player ID: {stats['player_id']}")
        await interaction.followup.send(embed=embed)

    except ValueError as e:
        await interaction.followup.send(f"❌ Error: {e}", ephemeral=True)
    except requests.HTTPError as e:
        # Catch rate limits (429) or other API issues
        await interaction.followup.send(f"❌ FACEIT API Error: Failed to fetch data. Check your API key or server status. (Status: {e.response.status_code})", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ An unexpected error occurred while fetching FACEIT data.", ephemeral=True)


# Regular Commands (Status and Demos remain the same)
@tree.command(name="whoami", description="Show your Discord user ID")
async def whoami(interaction: discord.Interaction):
    await interaction.response.send_message(f"👤 Your ID: `{interaction.user.id}`", ephemeral=True)

@tree.command(name="status", description="Get the current CS2 server status")
async def status(interaction: discord.Interaction):
    await interaction.response.defer()
    embed = await get_server_status_embed()
    await interaction.followup.send(embed=embed)

@tree.command(name="demos", description="Get latest CS2 demos")
async def demos(interaction: discord.Interaction):
    if SERVER_DEMOS_CHANNEL_ID and interaction.channel_id != SERVER_DEMOS_CHANNEL_ID:
        await interaction.response.send_message(
            f"❌ Only usable in <#{SERVER_DEMOS_CHANNEL_ID}>.", ephemeral=True
        )
        return
    await interaction.response.defer()
    demo_list = fetch_demos()
    embed = discord.Embed(title="🎥 Latest CS2 Demos", color=0x00FF00)
    embed.description = "\n".join(demo_list)
    await interaction.followup.send(embed=embed)

# ====== OWNER-ONLY CSS COMMANDS (remain the same) ======
@tree.command(name="csssay", description="Send a chat message via CSSSharp")
@owner_only()
async def csssay(interaction: discord.Interaction, message: str):
    resp = send_rcon_command(f'css_cssay {message}')
    await interaction.response.send_message(f"💬 Sent: {resp}", ephemeral=True)

@tree.command(name="csshsay", description="Display a HUD message via CSSSharp")
@owner_only()
async def csshsay(interaction: discord.Interaction, message: str):
    resp = send_rcon_command(f'css_hsay {message}')
    await interaction.response.send_message(f"🖥️ HUD: {resp}", ephemeral=True)

@tree.command(name="csskick", description="Kick a player via CSSSharp")
@owner_only()
async def csskick(interaction: discord.Interaction, player: str):
    resp = send_rcon_command(f'css_kick "{player}"')
    await interaction.response.send_message(f"👢 Kicked `{player}`.\n{resp}", ephemeral=True)

@tree.command(name="cssban", description="Ban a player via CSSSharp")
@owner_only()
async def cssban(interaction: discord.Interaction, player: str, minutes: int, reason: str = "No reason"):
    resp = send_rcon_command(f'css_ban "{player}" {minutes} "{reason}"')
    await interaction.response.send_message(f"🔨 Banned `{player}` for {minutes}m.\n{resp}", ephemeral=True)

@tree.command(name="csschangemap", description="Change map (whitelisted)")
@owner_only()
async def csschangemap(interaction: discord.Interaction, map: str):
    if map not in MAP_WHITELIST:
        await interaction.response.send_message("❌ Map not allowed.", ephemeral=True)
        return
    resp = send_rcon_command(f'css_changemap {map}')
    await interaction.response.send_message(f"🗺️ Changing to **{map}**\n{resp}", ephemeral=True)

@csschangemap.autocomplete('map')
async def map_autocomplete(interaction: discord.Interaction, current: str):
    current_lower = (current or "").lower()
    choices = [m for m in MAP_WHITELIST if current_lower in m.lower()]
    return [app_commands.Choice(name=m, value=m) for m in choices[:25]]

@tree.command(name="cssreload", description="Reload CounterStrikeSharp plugins")
@owner_only()
async def cssreload(interaction: discord.Interaction):
    resp = send_rcon_command('css_reloadplugins')
    await interaction.response.send_message(f"♻️ Reloaded plugins.\n{resp}", ephemeral=True)

# ====== RUN ======
if not TOKEN:
    raise SystemExit("❌ TOKEN not set in environment.")
bot.run(TOKEN)
