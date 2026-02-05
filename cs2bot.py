import os
import re
import json
import pytz
import a2s
import asyncio
import discord
import requests
from datetime import datetime
from discord.ext import commands
from discord import app_commands
from bs4 import BeautifulSoup
from typing import Literal, Optional
from mcrcon import MCRcon

# ===============================================================
# =============== ENVIRONMENT VARIABLES =========================
# ===============================================================

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
FACEIT_API_KEY = os.getenv("FACEIT_API_KEY")
OWNER_ID = int(os.getenv("OWNER_ID", 0))

FACEIT_GAME_ID = "cs2"   # ← UPDATED

MAP_WHITELIST = [
    "de_inferno", "de_mirage", "de_dust2", "de_overpass",
    "de_nuke", "de_ancient", "de_vertigo", "de_anubis"
]

# ===============================================================
# =============== DISCORD BOT SETUP =============================
# ===============================================================

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True

bot = commands.Bot(command_prefix="!", intents=intents, owner_id=OWNER_ID)

# ===============================================================
# ====================== UTILITIES ==============================
# ===============================================================

def owner_only():
    async def predicate(interaction: discord.Interaction):
        return interaction.user.id == OWNER_ID
    return app_commands.check(predicate)


# ---------- RCON Utility ----------
def send_rcon(command: str) -> str:
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            resp = rcon.command(command)
            return resp[:2000] if len(resp) > 2000 else resp
    except Exception as e:
        return f"⚠️ RCON Error: {e}"


# ---------- Demo Scraper ----------
def fetch_demos():
    # Setup
    USER = os.getenv("FSHO_USER").strip()
    PASS = os.getenv("FSHO_PASS").strip()
    URL = os.getenv("LIVEWIRE_URL").strip()
    
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0"})

    try:
        # 1. Login
        login_pg = s.get("https://fshost.me/login")
        token = BeautifulSoup(login_pg.text, "html.parser").find("input", {"name": "_token"})["value"]
        s.post("https://fshost.me/login", data={"_token": token, "email": USER, "password": PASS})

        # 2. Get Demos
        headers = {"X-CSRF-TOKEN": s.cookies.get("XSRF-TOKEN"), "Content-Type": "application/json"}
        payload = {
            "fingerprint": {"name": "pro.servers.files-table", "path": "pro/servers/1842/files", "method": "GET"},
            "serverMemo": {"data": {"server": 1842}},
            "updates": []
        }
        
        res = s.post(URL, headers=headers, json=payload).json()
        html = res.get('effects', {}).get('html', '')
        
        # 3. Clean Links
        soup = BeautifulSoup(html, "html.parser")
        links = [a['href'] for a in soup.find_all("a", href=True) if ".dem" in a['href']]

        if not links: return ["No demos found."]
        return [f"🎬 {l.split('/')[-1]}" for l in links[-5:]] # Shows last 5

    except Exception as e:
        return [f"Error: {e}"]
# ---------- Player Parsing ----------
STATUS_NAME_RE = re.compile(r'^#\s*\d+\s+"(?P<name>.*?)"\s+')
CSS_LIST_RE = re.compile(r'^\s*•\s*\[#\d+\]\s*"(?P<name>[^"]*)"')

def sanitize(s: str) -> str:
    if not s:
        return "—"
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

        # CSS player list
        css = CSS_LIST_RE.match(line)
        if css:
            players.append({"name": sanitize(css.group("name")), "ping": "—", "time": "—"})
            continue

        # Standard status output
        m = STATUS_NAME_RE.match(line)
        if m:
            name = sanitize(m.group("name"))
            time_match = re.search(r'\b(\d{1,2}:\d{2})\b', line)
            ping_match = re.search(r'(\d+)\s*$', line.split('"')[-1])

            players.append({
                "name": name,
                "time": time_match.group(1) if time_match else "—",
                "ping": ping_match.group(1) if ping_match else "—",
            })

    uniq = []
    seen = set()
    for p in players:
        if p["name"] not in seen:
            uniq.append(p)
            seen.add(p["name"])

    return uniq


# ===============================================================
# ================= FACEIT (CS2) UTILITIES ======================
# ===============================================================

def flag(cc):
    if not cc or len(cc) != 2:
        return "🏳️"
    return "".join(chr(ord(c.upper()) + 127397) for c in cc)


async def fetch_faceit_stats_cs2(nickname: str) -> dict:
    if not FACEIT_API_KEY:
        raise ValueError("FACEIT_API_KEY missing")

    headers = {"Authorization": f"Bearer {FACEIT_API_KEY}"}

    # 1. Lookup player ID
    info_url = f"https://open.faceit.com/data/v4/players?nickname={nickname}"
    r = requests.get(info_url, headers=headers)

    if r.status_code == 404:
        raise ValueError("Player not found on FACEIT.")
    r.raise_for_status()

    pdata = r.json()
    pid = pdata.get("player_id")

    # 2. Fetch CS2 stats
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


# ===============================================================
# ================== SERVER STATUS EMBED ========================
# ===============================================================

async def get_status_embed():
    addr = (SERVER_IP, SERVER_PORT)

    try:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, a2s.info, addr)
        a2s_players = await asyncio.wait_for(loop.run_in_executor(None, a2s.players, addr), 5)

        # fallback to RCON if A2S gives blank names
        if not a2s_players or all(not getattr(p, "name", "") for p in a2s_players):
            players = rcon_list_players()
        else:
            players = a2s_players

    except:
        embed = discord.Embed(title="❌ Server Offline", color=0xFF0000)
        return embed

    embed = discord.Embed(
        title="🟢 CS2 Server Online",
        color=0x00FF00
    )
    embed.add_field(name="Server", value=info.server_name, inline=False)
    embed.add_field(name="Map", value=info.map_name, inline=True)
    embed.add_field(name="Players", value=f"{info.player_count}/{info.max_players}", inline=True)

    # ✅ ADD THIS
    embed.add_field(
        name="🌐 Connect IP",
        value=f"`{SERVER_IP}:{SERVER_PORT}`",
        inline=False
    )

    # format player list
    if isinstance(players, list) and isinstance(players[0], dict):
        # RCON format
        listing = "\n".join(f"🎮 **{p['name']}**" for p in players)
    else:
        # A2S format
        listing = "\n".join(f"🎮 **{sanitize(p.name)}** | {p.score} pts" for p in players)

    embed.add_field(name="Players Online", value=listing or "No players", inline=False)
    embed.set_footer(text=f"Updated {datetime.now().strftime('%H:%M:%S')}")

    return embed


# ===============================================================
# ======================== DISCORD EVENTS ========================
# ===============================================================

@bot.event
async def on_ready():
    print(f"Bot online as {bot.user.name}")
    print("Use !sync to sync slash commands.")


# ===============================================================
# ===================== PREFIX COMMANDS ==========================
# ===============================================================

@bot.command()
@commands.guild_only()
@commands.is_owner()
async def sync(ctx, guilds: commands.Greedy[discord.Object] = None, spec: Optional[Literal["~", "*", "^"]] = None):
    await ctx.send("Syncing...", delete_after=5)

    if not guilds:
        if spec == "~":
            synced = await bot.tree.sync(guild=ctx.guild)
        elif spec == "*":
            bot.tree.copy_global_to(guild=ctx.guild)
            synced = await bot.tree.sync(guild=ctx.guild)
        elif spec == "^":
            bot.tree.clear_commands(guild=ctx.guild)
            synced = []
        else:
            synced = await bot.tree.sync()

        await ctx.send(f"Done. Synced {len(synced)} commands.")
        return

    count = 0
    for guild in guilds:
        try:
            await bot.tree.sync(guild=guild)
            count += 1
        except:
            pass

    await ctx.send(f"Synced to {count}/{len(guilds)} guilds.")


# ===============================================================
# ===================== SLASH COMMANDS ===========================
# ===============================================================

@bot.tree.command(name="status")
async def status_cmd(inter: discord.Interaction):
    await inter.response.defer()
    embed = await get_status_embed()
    await inter.followup.send(embed=embed)


@bot.tree.command(name="demos")
async def demos_cmd(inter: discord.Interaction):
    if SERVER_DEMOS_CHANNEL_ID and inter.channel_id != SERVER_DEMOS_CHANNEL_ID:
        return await inter.response.send_message("Wrong channel!", ephemeral=True)

    await inter.response.defer()
    lst = fetch_demos()
    embed = discord.Embed(title="🎥 Latest Demos", description="\n".join(lst), color=0x00FF00)
    await inter.followup.send(embed=embed)


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
    embed.add_field(name="Win Rate", value=str(stats["win_rate"]) + "%", inline=True)
    embed.add_field(name="Matches", value=stats["matches"], inline=True)
    embed.add_field(name="K/D", value=stats["kd_ratio"], inline=True)

    embed.set_footer(text=f"Player ID: {stats['player_id']}")

    await inter.followup.send(embed=embed)


# ===============================================================
# ===================== CSS ADMIN COMMANDS =======================
# ===============================================================

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
        return await inter.response.send_message("Map not allowed.", ephemeral=True)

    resp = send_rcon(f"css_changemap {map}")
    await inter.response.send_message(resp, ephemeral=True)


@csschangemap.autocomplete("map")
async def autocomplete_map(inter, current: str):
    return [app_commands.Choice(name=m, value=m)
            for m in MAP_WHITELIST if current.lower() in m.lower()]


@bot.tree.command(name="cssreload")
@owner_only()
async def cssreload(inter):
    resp = send_rcon("css_reloadplugins")
    await inter.response.send_message(resp, ephemeral=True)


# ===============================================================
# ========================== RUN ================================
# ===============================================================

if not TOKEN:
    raise SystemExit("TOKEN missing.")

bot.run(TOKEN)
