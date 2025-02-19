import os
import discord
from discord.ext import commands, tasks
import a2s
from datetime import datetime

TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))  # Ensure this is set correctly

intents = discord.Intents.default()
intents.message_content = True  # ✅ Enables message content intent

bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f'✅ Bot is online! Logged in as {bot.user}')
    cs2status_auto_update.start()  # Auto-update every 6 hours

@tasks.loop(hours=6)  # ✅ Sends automatic updates every 6 hours
async def cs2status_auto_update():
    """Automatically sends CS2 server updates to a specific channel every 6 hours."""
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        print(f"⚠️ Channel ID {CHANNEL_ID} not found! Make sure it's set correctly.")
        return

    embed = await get_server_status_embed()
    if embed:
        await channel.send(embed=embed)

@bot.command()
async def server(ctx):
    """Manually fetches and sends CS2 server status when users type !server"""
    embed = await get_server_status_embed()
    if embed:
        await ctx.send(embed=embed)
    else:
        await ctx.send("⚠️ Could not retrieve server status. Please try again later.")

@bot.command()
async def set_channel(ctx):
    """Dynamically set the update channel for automatic messages."""
    global CHANNEL_ID
    CHANNEL_ID = ctx.channel.id
    await ctx.send(f"✅ This channel (`{ctx.channel.name}`) is now set for CS2 updates.")

async def get_server_status_embed():
    """Fetch CS2 server status and return an embed message"""
    try:
        server_address = (SERVER_IP, SERVER_PORT)
        info = a2s.info(server_address)
        players = a2s.players(server_address)

        player_list = "\n".join([f"{p.name} - {p.score} kills" for p in players]) if players else "No players online."

        last_updated = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        embed = discord.Embed(title="🎮 CS2 Server Status", color=0x00ff00)
        embed.add_field(name="🖥️ Server Name", value=info.server_name, inline=False)
        embed.add_field(name="🗺️ Map", value=info.map_name, inline=True)
        embed.add_field(name="👥 Players", value=f"{info.player_count}/{info.max_players}", inline=True)
        embed.add_field(name="🎯 Player List", value=player_list, inline=False)
        embed.set_footer(text=f"Last updated: {last_updated}")  # ✅ Timestamp added

        return embed
    except Exception as e:
        print(f"⚠️ Error retrieving CS2 server status: {e}")
        return None

bot.run(TOKEN)