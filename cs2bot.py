import os
import asyncio
import discord
import requests
from bs4 import BeautifulSoup
from discord.ext import tasks
from discord import app_commands
import a2s  # pip install a2s
from mcrcon import MCRcon  # pip install mcrcon
from datetime import datetime
import pytz

# ===================== ENV =====================
TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP", "127.0.0.1")
SERVER_PORT = int(os.getenv("SERVER_PORT", "27015"))

RCON_IP = os.getenv("RCON_IP", SERVER_IP)
RCON_PORT = int(os.getenv("RCON_PORT", "27015"))
RCON_PASSWORD = os.getenv("RCON_PASSWORD", "")

FACEIT_API_KEY = os.getenv("FACEIT_API_KEY", "")

CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0") or "0")
SERVER_DEMOS_CHANNEL_ID = int(os.getenv("SERVER_DEMOS_CHANNEL_ID", "0") or "0")
DEMOS_URL = os.getenv("DEMOS_URL", "https://de34.fsho.st/demos/cs2/1842/")

# ================= DISCORD BOOT =================
intents = discord.Intents.default()
intents.messages = True
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

# ================= HELPERS =====================
MAP_WHITELIST = [
    "de_inferno", "de_mirage", "de_dust2", "de_overpass",
    "de_nuke", "de_ancient", "de_vertigo", "de_anubis",
]

def is_admin():
    async def predicate(interaction: discord.Interaction):
        return interaction.user.guild_permissions.administrator
    return app_commands.check(predicate)

def send_rcon_command(command: str) -> str:
    """Send an RCON command and return (truncated) response."""
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            resp = rcon.command(command)
            return resp if len(resp) <= 1000 else resp[:1000] + "... (truncated)"
    except Exception as e:
        return f"⚠️ Error: {e}"

def country_code_to_flag(code: str) -> str:
    if not code or len(code) != 2:
        return "🏳️"
    return chr(ord(code[0].upper()) + 127397) + chr(ord(code[1].upper()) + 127397)

def fetch_demos():
    """Scrape simple index for .dem links."""
    try:
        r = requests.get(DEMOS_URL, timeout=10)
        if r.status_code != 200:
            return ["⚠️ Could not fetch demos. Check the URL."]
        soup = BeautifulSoup(r.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".dem")]
        if not links:
            return ["⚠️ No demos found."]
        latest = links[-5:]
        return [f"[{d}](<{DEMOS_URL}{d}>)" for d in latest]
    except Exception as e:
        return [f"⚠️ Error fetching demos: {e}"]

async def get_server_status_embed() -> discord.Embed:
    """Query via A2S in executor to avoid blocking."""
    addr = (SERVER_IP, SERVER_PORT)
    try:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, a2s.info, addr)
        players = await loop.run_in_executor(None, a2s.players, addr)

        berlin_tz = pytz.timezone("Europe/Berlin")
        last_updated = datetime.now(berlin_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

        embed = discord.Embed(title="🎮 CS2 Server Status - 🟢 Online", color=0x00FF00)
        embed.add_field(name="🖥️ Server Name", value=info.server_name, inline=False)
        embed.add_field(name="🗺️ Map", value=info.map_name, inline=True)
        embed.add_field(name="👥 Players", value=f"{info.player_count}/{info.max_players}", inline=True)

        if players:
            stats = "\n".join(
                f"🎮 **{p.name}** | 🏆 **{p.score}** kills | ⏳ **{p.duration/60:.1f} mins**"
                for p in sorted(players, key=lambda x: x.score, reverse=True)
            )
        else:
            stats = "No players online."

        embed.add_field(name="📊 Live Player Stats", value=stats, inline=False)
        embed.set_footer(text=f"Last updated: {last_updated}")
        return embed

    except Exception:
        embed = discord.Embed(title="⚠️ CS2 Server Status - 🔴 Offline", color=0xFF0000)
        embed.add_field(name="❌ Server Unreachable", value="The server is currently offline.", inline=False)
        return embed

# ================= TASKS =======================
@tasks.loop(minutes=15)
async def auto_say():
    """Every 15 min: clear our last messages and post a note; say in-game."""
    if not CHANNEL_ID:
        return
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
    """Every 2 min: advertise via CSSSharp (fallback to say if needed)."""
    ads = [
        "<___Join our Discord: discord.gg/reshtangamingcenter___>",
        "<___Enjoying the server? Invite your friends!___>",
        "<___Server powered by Reshtan Gaming Center___>",
    ]
    msg = ads[auto_advertise.current_loop % len(ads)]
    resp = send_rcon_command(f"css_cssay {msg}")
    if "Unknown command" in resp or "⚠️ Error" in resp:
        resp = send_rcon_command(f"say {msg}")
    print(f"✅ Auto-advertise: {msg} | RCON: {resp}")

# ================= READY =======================
@bot.event
async def on_ready():
    try:
        synced = await tree.sync()  # GLOBAL sync to guarantee visibility
        print(f"✅ Synced {len(synced)} GLOBAL commands")
        cmds = await tree.fetch_commands()
        print("🧭 Commands:", ", ".join(f"/{c.name}" for c in cmds))
        print("🛡️ In guilds:", ", ".join(g.name for g in bot.guilds))
    except Exception as e:
        print("❌ on_ready sync error:", e)

    auto_say.start()
    auto_advertise.start()

# =============== PUBLIC COMMANDS ===============
@tree.command(name="sync", description="Force re-sync slash commands (temporary no-check)")
async def sync_cmd(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        cmds = await tree.sync()  # global
        await interaction.followup.send(f"✅ Synced {len(cmds)} GLOBAL commands.", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Sync error: {e}", ephemeral=True)

@tree.command(name="status", description="Get the current CS2 server status")
async def status(interaction: discord.Interaction):
    await interaction.response.defer()
    embed = await get_server_status_embed()
    await interaction.followup.send(embed=embed)

@tree.command(name="leaderboard", description="Show the top 5 players in the CS2 server")
async def leaderboard(interaction: discord.Interaction):
    await interaction.response.defer()
    try:
        players = a2s.players((SERVER_IP, SERVER_PORT), timeout=5)
        if not players:
            await interaction.followup.send("⚠️ No players online right now.")
            return
        top = sorted(players, key=lambda x: x.score, reverse=True)[:5]
        text = "\n".join(
            f"🥇 **{p.name}** | 🏆 **{p.score}** kills | ⏳ **{p.duration/60:.1f} mins**"
            for p in top
        )
        embed = discord.Embed(title="🏆 CS2 Leaderboard (Top 5)", color=0xFFD700)
        embed.add_field(name="🔹 Players", value=text, inline=False)
        embed.set_footer(text="Data updates every 6 hours.")
        await interaction.followup.send(embed=embed)
    except TimeoutError:
        await interaction.followup.send("⚠️ CS2 server is not responding. Try again later.")

@tree.command(name="demos", description="Get the latest CS2 demos")
async def demos(interaction: discord.Interaction):
    if SERVER_DEMOS_CHANNEL_ID and interaction.channel_id != SERVER_DEMOS_CHANNEL_ID:
        await interaction.response.send_message(
            f"❌ This command can only be used in <#{SERVER_DEMOS_CHANNEL_ID}>.", ephemeral=True
        )
        return
    await interaction.response.defer()
    demo_list = fetch_demos()
    embed = discord.Embed(title="🎥 Latest CS2 Demos", color=0x00FF00)
    embed.description = "\n".join(demo_list)
    await interaction.followup.send(embed=embed)

@tree.command(name="elo", description="Check Faceit ELO using nickname")
@app_commands.describe(nickname="The Faceit nickname")
async def elo(interaction: discord.Interaction, nickname: str):
    await interaction.response.defer()
    if not FACEIT_API_KEY:
        await interaction.followup.send("❌ FACEIT_API_KEY is missing in environment variables.")
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
            await interaction.followup.send("⚠️ No CS2 or CSGO data found for this player.")
            return

        elo_val = cs_game.get("faceit_elo", "N/A")
        level = cs_game.get("skill_level", "N/A")
        region = cs_game.get("region", "N/A")
        profile_url = f"https://www.faceit.com/en/players/{nickname}"
        country_code = data.get("country", "N/A")
        flag = country_code_to_flag(country_code)

        embed = discord.Embed(
            title=f"🎮 Faceit Profile: {nickname}",
            description=f"[🌐 View on Faceit]({profile_url})",
            color=0x0099FF
        )
        embed.add_field(name="📊 ELO", value=str(elo_val), inline=True)
        embed.add_field(name="⭐ Level", value=str(level), inline=True)
        embed.add_field(name="🌍 Region", value=region, inline=True)
        embed.add_field(name="🌐 Country", value=f"{flag} {country_code}", inline=True)
        embed.set_footer(text="Faceit stats via open.faceit.com API")
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error fetching Faceit data: `{e}`")

# ============ ADMIN / CSSSharp COMMANDS ============
def cooldown(rate: int, per: float):
    return app_commands.checks.cooldown(rate, per)

@tree.command(
    name="css",
    description="Run a CounterStrikeSharp command (e.g. css_cssay \"hello\")"
)
@app_commands.describe(command="The full command to run, starting with css_")
@is_admin()
@cooldown(1, 5.0)
async def css(interaction: discord.Interaction, command: str):
    await interaction.response.defer(ephemeral=True)
    if not command.startswith("css_"):
        await interaction.followup.send("❌ Command must start with `css_`.", ephemeral=True)
        return
    resp = send_rcon_command(command)
    await interaction.followup.send(f"▶️ `{command}`\n🧩 **RCON:** {resp}", ephemeral=True)

@css.error
async def css_error(interaction: discord.Interaction, error):
    if isinstance(error, app_commands.CommandOnCooldown):
        await interaction.response.send_message(
            f"⏳ Cooldown—try again in {error.retry_after:.1f}s", ephemeral=True
        )

@tree.command(name="csssay", description="Server chat via CSSSharp (css_cssay)")
@app_commands.describe(message="Message to broadcast")
@is_admin()
@cooldown(1, 5.0)
async def csssay(interaction: discord.Interaction, message: str):
    await interaction.response.defer(ephemeral=True)
    resp = send_rcon_command(f'css_cssay {message}')
    await interaction.followup.send(f"💬 Sent to chat.\n🧩 {resp}", ephemeral=True)

@tree.command(name="csshsay", description="Center/HUD message via CSSSharp (css_hsay)")
@app_commands.describe(message="HUD message to display")
@is_admin()
@cooldown(1, 5.0)
async def csshsay(interaction: discord.Interaction, message: str):
    await interaction.response.defer(ephemeral=True)
    resp = send_rcon_command(f'css_hsay {message}')
    await interaction.followup.send(f"🖥️ HUD shown.\n🧩 {resp}", ephemeral=True)

@tree.command(name="csskick", description="Kick a player (css_kick)")
@app_commands.describe(player="Exact player name")
@is_admin()
@cooldown(1, 5.0)
async def csskick(interaction: discord.Interaction, player: str):
    await interaction.response.defer(ephemeral=True)
    resp = send_rcon_command(f'css_kick "{player}"')
    await interaction.followup.send(f"👢 Kicked `{player}`.\n🧩 {resp}", ephemeral=True)

@tree.command(name="cssban", description="Ban a player (css_ban)")
@app_commands.describe(player="Exact player name", minutes="Duration in minutes", reason="Optional reason")
@is_admin()
@cooldown(1, 5.0)
async def cssban(interaction: discord.Interaction, player: str, minutes: int, reason: str = "No reason"):
    await interaction.response.defer(ephemeral=True)
    resp = send_rcon_command(f'css_ban "{player}" {minutes} "{reason}"')
    await interaction.followup.send(
        f"🔨 Banned `{player}` for **{minutes}m**. Reason: {reason}\n🧩 {resp}", ephemeral=True
    )

@tree.command(name="csschangemap", description="Change map (css_changemap) with whitelist")
@app_commands.describe(map="Map name (whitelisted)")
@is_admin()
@cooldown(1, 5.0)
async def csschangemap(interaction: discord.Interaction, map: str):
    await interaction.response.defer(ephemeral=True)
    if map not in MAP_WHITELIST:
        await interaction.followup.send("❌ Map not allowed. Ask an admin to whitelist it.", ephemeral=True)
        return
    resp = send_rcon_command(f'css_changemap {map}')
    await interaction.followup.send(f"🗺️ Changing map to **{map}**…\n🧩 {resp}", ephemeral=True)

@csschangemap.autocomplete("map")
async def map_autocomplete(interaction: discord.Interaction, current: str):
    q = (current or "").lower()
    choices = [m for m in MAP_WHITELIST if q in m.lower()]
    return [app_commands.Choice(name=m, value=m) for m in choices[:25]]

@tree.command(name="cssreload", description="Reload CSSSharp plugins (css_reloadplugins)")
@is_admin()
@cooldown(1, 10.0)
async def cssreload(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    resp = send_rcon_command("css_reloadplugins")
    await interaction.followup.send(f"♻️ Reloaded plugins.\n🧩 {resp}", ephemeral=True)

@tree.command(name="broadcast", description="Broadcast (tries css_cssay, fallback to say)")
@app_commands.describe(message="Message to broadcast")
@is_admin()
async def broadcast(interaction: discord.Interaction, message: str):
    await interaction.response.defer(ephemeral=True)
    resp = send_rcon_command(f'css_cssay {message}')
    if "Unknown command" in resp or "⚠️ Error" in resp:
        resp = send_rcon_command(f"say {message}")
    await interaction.followup.send(f"📢 Broadcasted.\n🧩 {resp}", ephemeral=True)

# ================= RUN =========================
if not TOKEN:
    raise SystemExit("❌ TOKEN not set in environment.")
bot.run(TOKEN)