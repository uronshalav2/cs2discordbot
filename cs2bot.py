import os
import discord
from discord.ext import commands, tasks
import a2s
from datetime import datetime

TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))  # Ensure this is set correctly

bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())

@bot.event
async def on_ready():
    print(f'✅ Bot is online! Logged in as {bot.user}')
    cs2status_auto_update.start()  # Start the 6-hour update loop

@tasks.loop(hours=6)  # ✅ Update every 6 hours
async def cs2status_auto_update():
    """Send CS2 server updates to the configured channel every 6 hours."""
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        print(f"⚠️ Channel ID {CHANNEL_ID} not found! Make sure it's set correctly.")
        return

    try:
        server_address = (SERVER_IP, SERVER_PORT)
        info = a2s.get_info(server_address)
        players = a2s.get_players(server_address)

        player_list = "\n".join([f"{p.name} - {p.score} kills" for p in players]) if players else "No players online."

        # ✅ Fix "Last Updated" issue by adding a proper timestamp
        last_updated = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        embed = discord.Embed(title="🎮 CS2 Server Status", color=0x00ff00)
        embed.add_field(name="🖥️ Server Name", value=info.server_name, inline=False)
        embed.add_field(name="🗺️ Map", value=info.map_name, inline=True)
        embed.add_field(name="👥 Players", value=f"{info.player_count}/{info.max_players}", inline=True)
        embed.add_field(name="🎯 Player List", value=player_list, inline=False)
        embed.set_footer(text=f"Last updated: {last_updated}")  # ✅ Timestamp added

        await channel.send(embed=embed)

    except Exception as e:
        print(f"⚠️ Error retrieving CS2 server status: {e}")

@bot.command()
async def set_channel(ctx):
    """Dynamically set the channel for updates."""
    global CHANNEL_ID
    CHANNEL_ID = ctx.channel.id
    await ctx.send(f"✅ This channel (`{ctx.channel.name}`) is now set for CS2 updates.")

bot.run(TOKEN)