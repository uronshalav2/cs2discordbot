import os
import discord
from discord import app_commands
from discord.ext import tasks
import a2s  # Source Server Query Protocol
from datetime import datetime, timedelta
import pytz  # Timezone support for Germany

# ✅ Load environment variables from Railway
TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))  # Set this in Railway variables

# ✅ Enable privileged intents
intents = discord.Intents.default()
intents.message_content = True  # Required for handling slash commands

# ✅ Initialize bot with command tree
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

# ✅ Track previous players & server uptime
previous_players = set()
previous_uptime = None

class ConnectButton(discord.ui.View):
    """A Discord button that lets players join the CS2 server."""
    def __init__(self):
        super().__init__()
        connect_url = f"steam://connect/{SERVER_IP}:{SERVER_PORT}"
        self.add_item(discord.ui.Button(label="🎮 Connect to CS2 Server", url=connect_url, style=discord.ButtonStyle.link))

@bot.event
async def on_ready():
    await tree.sync()  # Sync slash commands
    print(f'✅ Bot is online! Logged in as {bot.user}')
    cs2status_auto_update.start()  # Start automatic updates every 6 hours

@tasks.loop(hours=6)  # ✅ Auto-updates every 6 hours
async def cs2status_auto_update():
    """Automatically sends CS2 server updates, detects restarts, and welcomes new players."""
    global previous_players, previous_uptime
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        print(f"⚠️ Channel ID {CHANNEL_ID} not found! Make sure it's set correctly.")
        return

    # ✅ Delete old bot messages before sending a new one
    async for message in channel.history(limit=10):
        if message.author == bot.user:
            await message.delete()

    embed = await get_server_status_embed()
    if embed:
        await channel.send(embed=embed, view=ConnectButton())  # ✅ Added "Connect to Server" button

    try:
        info = a2s.info((SERVER_IP, SERVER_PORT))
        players = a2s.players((SERVER_IP, SERVER_PORT))

        # ✅ Detect if the server restarted
        current_uptime = info.duration  # Get current uptime
        if previous_uptime is not None and current_uptime < previous_uptime:
            await channel.send("🔄 **The CS2 server has restarted!** ⏳")

        previous_uptime = current_uptime  # Update last known uptime

        # ✅ Welcome new players who joined
        current_players = {p.name for p in players}
        new_players = current_players - previous_players  # Detect new players
        previous_players = current_players  # Update last known player list

        for player in new_players:
            await channel.send(f"👋 Welcome **{player}** to the CS2 server!")

    except Exception as e:
        print(f"⚠️ Error retrieving CS2 data: {e}")

@tree.command(name="status", description="Get the current CS2 server status")
async def status(interaction: discord.Interaction):
    """Slash command to get live CS2 server status"""

    # ✅ Defer the response to prevent timeout issues
    await interaction.response.defer()

    embed = await get_server_status_embed()

    # ✅ Follow-up response after data is fetched
    await interaction.followup.send(embed=embed, view=ConnectButton())

@tree.command(name="leaderboard", description="Show the top 5 players in the CS2 server")
async def leaderboard(interaction: discord.Interaction):
    """Show the top 5 players based on kills"""
    try:
        players = a2s.players((SERVER_IP, SERVER_PORT))

        if not players:
            await interaction.response.send_message("⚠️ No players online right now.")
            return

        # ✅ Sort players by kills and get top 5
        top_players = sorted(players, key=lambda x: x.score, reverse=True)[:5]
        leaderboard_text = "\n".join(
            [f"🥇 **{p.name}** | 🏆 **{p.score}** kills | ⏳ **{p.duration:.1f} mins**"
             for p in top_players]
        )

        embed = discord.Embed(title="🏆 CS2 Leaderboard (Top 5)", color=0xFFD700)
        embed.add_field(name="🔹 Players", value=leaderboard_text, inline=False)
        embed.set_footer(text="Data updates every 6 hours.")

        await interaction.response.send_message(embed=embed)

    except Exception:
        await interaction.response.send_message("⚠️ Could not retrieve player stats. Try again later.")

async def get_server_status_embed():
    """Fetch CS2 server status, show live player stats, and return an embed message."""
    server_address = (SERVER_IP, SERVER_PORT)

    try:
        info = a2s.info(server_address)  # ✅ Query server info
        players = a2s.players(server_address)  # ✅ Get live player stats

        # ✅ Server uptime
        server_uptime = str(timedelta(seconds=info.duration))

        # ✅ Server is online: Use GREEN color
        embed_color = 0x00ff00  # Green for online

        berlin_tz = pytz.timezone("Europe/Berlin")
        last_updated = datetime.now(berlin_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

        embed = discord.Embed(title="🎮 CS2 Server Status - 🟢 Online", color=embed_color)
        embed.add_field(name="🖥️ Server Name", value=info.server_name, inline=False)
        embed.add_field(name="🗺️ Map", value=info.map_name, inline=True)
        embed.add_field(name="⏳ Server Uptime", value=f"{server_uptime}", inline=True)
        embed.add_field(name="👥 Players", value=f"{info.player_count}/{info.max_players}", inline=True)

        # ✅ Show live player stats
        if players:
            player_stats = "\n".join(
                [f"🎮 **{p.name}** | 🏆 **{p.score}** kills | ⏳ **{p.duration:.1f} mins**"
                 for p in sorted(players, key=lambda x: x.score, reverse=True)]
            )
        else:
            player_stats = "No players online."

        embed.add_field(name="📊 Live Player Stats", value=player_stats, inline=False)
        embed.set_footer(text=f"Last updated: {last_updated}")

        return embed

    except Exception:
        embed_color = 0xff0000  # Red for offline
        berlin_tz = pytz.timezone("Europe/Berlin")
        last_updated = datetime.now(berlin_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

        embed = discord.Embed(title="🎮 CS2 Server Status - 🔴 Offline", color=embed_color)
        embed.add_field(name="⚠️ Server Status", value="The server is currently **offline** or unreachable.", inline=False)
        embed.set_footer(text=f"Last checked: {last_updated}")

        return embed

bot.run(TOKEN)