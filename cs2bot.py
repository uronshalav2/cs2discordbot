import os
import re
import asyncio
import discord
import requests
from bs4 import BeautifulSoup
from discord.ext import tasks
from discord import app_commands
import a2s
from mcrcon import MCRcon
from datetime import datetime
import pytz

# ====== BOT CONFIG ======
TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP", "127.0.0.1")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))
RCON_IP = os.getenv("RCON_IP", SERVER_IP)
RCON_PORT = int(os.getenv("RCON_PORT", 27015))
RCON_PASSWORD = os.getenv("RCON_PASSWORD", "")
FACEIT_API_KEY = os.getenv("FACEIT_API_KEY", "")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
SERVER_DEMOS_CHANNEL_ID = int(os.getenv("SERVER_DEMOS_CHANNEL_ID", 0))
DEMOS_URL = os.getenv("DEMOS_URL", "https://de34.fsho.st/demos/cs2/1842/")
GUILD_ID = int(os.getenv("GUILD_ID", "0") or "0")

# ====== OWNER ID ======
OWNER_ID = int(os.getenv("OWNER_ID", 0))

# ====== DISCORD CLIENT ======
intents = discord.Intents.default()
intents.messages = True
bot = discord.Client(intents=intents)
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
# Accepts both CSSharp list and vanilla `status` lines.
STATUS_NAME_RE = re.compile(r'^#\s*\d+\s+"(?P<name>.*?)"\s+')
CSS_LIST_RE    = re.compile(r'^\s*\d+\.\s+(?P<name>.+?)\s+\(.*\)$')

def sanitize_name(s: str) -> str:
    if not s:
        return "—"
    s = s.replace('\x00', '').replace('\u200b', '')
    for ch in ['*', '_', '`', '~', '|', '>', '@']:
        s = s.replace(ch, f'\\{ch}')
    return s.strip()

def rcon_list_players():
    """
    Try CounterStrikeSharp 'css_listplayers' first; fall back to 'status'.
    Return: list of dicts: [{'name': 'Player', 'time': 'mm:ss'|'—', 'ping': 'xx'|'—'}]
    """
    txt = send_rcon_command('css_listplayers')
    used_css = txt and 'Unknown command' not in txt and 'Error' not in txt

    if not used_css:
        txt = send_rcon_command('status')

    players = []

    for raw in txt.splitlines():
        line = raw.strip()

        # CSSharp format: "1. Name (SteamID64) ..."
        m_css = CSS_LIST_RE.match(line)
        if m_css:
            name = sanitize_name(m_css.group('name'))
            players.append({'name': name, 'time': '—', 'ping': '—'})
            continue

        # Vanilla status format: '# 2 "Name" ...'
        m_std = STATUS_NAME_RE.match(line)
        if m_std:
            name = sanitize_name(m_std.group('name'))

            # Try to guess time (mm:ss) and a ping number from the line
            time_match = re.search(r'\b(\d{1,2}:\d{2})\b', line)
            ping_match = re.search(r'\b(\d{1,3})\b(?!:)', line)  # crude, but avoids time
            players.append({
                'name': name,
                'time': time_match.group(1) if time_match else '—',
                'ping': ping_match.group(1) if ping_match else '—'
            })

    # De-dup + keep order
    seen = set()
    uniq = []
    for p in players:
        if p['name'] not in seen:
            uniq.append(p)
            seen.add(p['name'])
    return uniq
# ---------------------------------------------------

async def get_server_status_embed() -> discord.Embed:
    """Query A2S; if player names are blank/hidden, use RCON parsing as fallback."""
    addr = (SERVER_IP, SERVER_PORT)
    try:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, a2s.info, addr)
        a2s_players = await loop.run_in_executor(None, a2s.players, addr)

        # Are A2S names all blank or missing?
        names_blank = (not a2s_players) or all(not getattr(p, 'name', '') for p in a2s_players)

        # Pull names via RCON when A2S is useless
        rcon_players = rcon_list_players() if names_blank else None

        berlin_tz = pytz.timezone("Europe/Berlin")
        last_updated = datetime.now(berlin_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

        embed = discord.Embed(title="🎮 CS2 Server Status - 🟢 Online", color=0x00FF00)
        embed.add_field(name="🖥️ Server Name", value=info.server_name, inline=False)
        embed.add_field(name="🗺️ Map", value=info.map_name, inline=True)
        embed.add_field(name="👥 Players", value=f"{info.player_count}/{info.max_players}", inline=True)

        if rcon_players:
            stats = "\n".join(
                f"🎮 **{p['name']}** | ⏳ {p['time']} | 📶 {p['ping']} ms"
                for p in rcon_players
            ) or "No players online."
        elif a2s_players:
            stats = "\n".join(
                f"🎮 **{sanitize_name(getattr(p, 'name', '') or '—')}** | 🏆 {getattr(p, 'score', 0)} | ⏳ {getattr(p, 'duration', 0)/60:.1f} mins"
                for p in sorted(a2s_players, key=lambda x: getattr(x, 'score', 0), reverse=True)
            ) or "No players online."
        else:
            stats = "No players online."

        embed.add_field(name="📊 Player Stats", value=stats, inline=False)
        embed.set_footer(text=f"Last updated: {last_updated}")
        return embed

    except Exception:
        embed = discord.Embed(title="⚠️ CS2 Server Status - 🔴 Offline", color=0xFF0000)
        embed.add_field(name="❌ Server Unreachable", value="The server is currently offline.", inline=False)
        return embed

# ====== TASKS ======
@tasks.loop(minutes=15)
async def auto_say():
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        return
    async for m in channel.history(limit=20):
        if m.author == bot.user:
            try:
                await m.delete()
            except:
                pass
    send_rcon_command('say Server is owned by Reshtan Gaming Center')
    await channel.send("✅ **Server is owned by Reshtan Gaming Center** (Auto Message)")

@tasks.loop(minutes=2)
async def auto_advertise():
    ads = [
        "<___Join our Discord: discord.gg/reshtangamingcenter___>",
        "<___Invite your friends!___>",
        "<___Server powered by Reshtan Gaming Center___>",
    ]
    msg = ads[auto_advertise.current_loop % len(ads)]
    resp = send_rcon_command(f"css_cssay {msg}")
    print(f"✅ Auto-advertise: {msg} | RCON: {resp}")

# ====== READY ======
@bot.event
async def on_ready():
    if GUILD_ID:
        guild = discord.Object(id=GUILD_ID)
        await tree.sync(guild=guild)
        print(f"✅ Commands synced to guild {GUILD_ID} as {bot.user}")
    else:
        await tree.sync()
        print(f"✅ Global commands synced as {bot.user}")
    auto_say.start()
    auto_advertise.start()

# ====== COMMANDS ======
@tree.command(name="whoami", description="Show your Discord user ID")
async def whoami(interaction: discord.Interaction):
    await interaction.response.send_message(f"👤 Your ID: `{interaction.user.id}`", ephemeral=True)

@tree.command(name="status", description="Get the current CS2 server status")
async def status(interaction: discord.Interaction):
    await interaction.response.defer()
    embed = await get_server_status_embed()
    await interaction.followup.send(embed=embed)

@tree.command(name="elo", description="Check Faceit ELO")
@discord.app_commands.describe(nickname="The Faceit nickname")
async def elo(interaction: discord.Interaction, nickname: str):
    await interaction.response.defer()
    if not FACEIT_API_KEY:
        await interaction.followup.send("❌ FACEIT_API_KEY not set.")
        return
    try:
        url = f"https://open.faceit.com/data/v4/players?nickname={nickname}"
        r = requests.get(url, headers={"Authorization": f"Bearer {FACEIT_API_KEY}"}, timeout=10)
        data = r.json()
        if r.status_code != 200:
            raise Exception(data.get("message", "Unknown error"))
        games = data.get("games", {})
        cs_game = games.get("cs2") or games.get("csgo")
        if not cs_game:
            await interaction.followup.send("⚠️ No CS2/CSGO data found.")
            return
        elo_val = cs_game.get("faceit_elo", "N/A")
        level = cs_game.get("skill_level", "N/A")
        region = cs_game.get("region", "N/A")
        country_code = data.get("country", "N/A")
        flag = country_code_to_flag(country_code)
        profile_url = f"https://www.faceit.com/en/players/{nickname}"
        embed = discord.Embed(
            title=f"🎮 Faceit Profile: {nickname}",
            description=f"[🌐 View on Faceit]({profile_url})",
            color=0x0099FF
        )
        embed.add_field(name="📊 ELO", value=str(elo_val))
        embed.add_field(name="⭐ Level", value=str(level))
        embed.add_field(name="🌍 Region", value=region)
        embed.add_field(name="🌐 Country", value=f"{flag} {country_code}")
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: `{e}`")

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

# ====== OWNER-ONLY CSS COMMANDS ======
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