import os
import discord
from discord import app_commands
from discord.ext import tasks
import a2s  # Source Server Query Protocol
from datetime import datetime
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

@bot.event
async def on_ready():
    await tree.sync()  # Sync slash commands
    print(f'✅ Bot is online! Logged in as {bot.user}')
    cs2status_auto_update.start()  # Start automatic updates every 6 hours

@tasks.loop(hours=6)  # ✅ Auto-updates every 6 hours
async def cs2status_auto_update():
    """Automatically sends CS2 server updates."""
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        print(f"⚠️ Channel ID {CHANNEL_ID} not found!")
        return

    # ✅ Delete old messages before sending a new one
    async for message in channel.history(limit=5):
        if message.author == bot.user:
            await message.delete()

    embed = await get_server_status_embed()
    if embed:
        await channel.send(embed=embed)

@tree.command(name="status", description="Get the current CS2 server status")
async def status(interaction: discord.Interaction):
    """Slash command to get live CS2 server status"""
    await interaction.response.defer()
    embed = await get_server_status_embed()
    await interaction.followup.send(embed=embed)

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
            [f"🥇 **{p.name}** | 🏆 **{p.score}** kills | ⏳ **{p.duration / 60:.1f} mins**"
             for p in top_players]
        )

        embed = discord.Embed(title="🏆 CS2 Leaderboard (Top 5)", color=0xFFD700)
        embed.add_field(name="🔹 Players", value=leaderboard_text, inline=False)
        embed.set_footer(text="Data updates every 6 hours.")

        await interaction.response.send_message(embed=embed)

    except Exception:
        await interaction.response.send_message("⚠️ Could not retrieve player stats. Try again later.")

async def get_server_status_embed():
    """Fetch CS2 server status, show live player stats, and return an embed."""
    server_address = (SERVER_IP, SERVER_PORT)

    try:
        print(f"🔍 Checking CS2 server: {SERVER_IP}:{SERVER_PORT}")

        try:
            info = a2s.info(server_address)  # ✅ Get server info
            players = a2s.players(server_address)  # ✅ Get player list
            print(f"✅ Server is ONLINE! {info.server_name} | {info.map_name}")
        except Exception as e:
            print(f"⚠️ Server unreachable: {e}")
            return get_offline_embed()

        # ✅ Server is online
        embed_color = 0x00ff00  # Green for online

        berlin_tz = pytz.timezone("Europe/Berlin")
        last_updated = datetime.now(berlin_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

        embed = discord.Embed(title="🎮 CS2 Server Status - 🟢 Online", color=embed_color)
        embed.add_field(name="🖥️ Server Name", value=info.server_name, inline=False)
        embed.add_field(name="🗺️ Map", value=info.map_name, inline=True)
        embed.add_field(name="👥 Players", value=f"{info.player_count}/{info.max_players}", inline=True)

        # ✅ Show player list (even if empty)
        player_stats = "No players online."
        if players:
            player_stats = "\n".join(
                [f"🎮 **{p.name}** | 🏆 **{p.score}** kills | ⏳ **{p.duration / 60:.1f} mins**"
                 for p in sorted(players, key=lambda x: x.score, reverse=True)]
            )

        embed.add_field(name="📊 Live Player Stats", value=player_stats, inline=False)
        embed.set_footer(text=f"Last updated: {last_updated}")

        return embed

    except Exception as e:
        print(f"⚠️ Error retrieving CS2 server status: {e}")
        return get_offline_embed()

def get_offline_embed():
    """Returns an embed for when the server is offline."""
    embed_color = 0xff0000  # Red for offline
    berlin_tz = pytz.timezone("Europe/Berlin")
    last_updated = datetime.now(berlin_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

    embed = discord.Embed(title="⚠️ CS2 Server Status - 🔴 Offline", color=embed_color)
    embed.add_field(name="❌ Server Unreachable", value="The server is currently **offline** or experiencing issues.\n\
🔄 Try again later or contact support.", inline=False)
    embed.set_footer(text=f"Last checked: {last_updated}")

    return embed

bot.run(TOKEN)