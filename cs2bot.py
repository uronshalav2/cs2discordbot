import os
import discord
from discord.ext import commands, tasks
import a2s  # Source Server Query Protocol

# Load Environment Variables from Railway
TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))  # Default CS2 port
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))  # Discord channel for updates

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f'✅ Bot is online! Logged in as {bot.user}')
    cs2status_auto_update.start()  # Start auto-updating CS2 status

@tasks.loop(seconds=60)  # Updates every 60 seconds
async def cs2status_auto_update():
    try:
        server_address = (SERVER_IP, SERVER_PORT)
        info = a2s.info(server_address)
        players = a2s.players(server_address)

        # Player List
        player_list = "\n".join([f"{p.name} - {p.score} kills" for p in players]) if players else "No players online."

        # Embed Message
        embed = discord.Embed(title="CS2 Server Status", color=0x00ff00)
        embed.add_field(name="Server Name", value=info.server_name, inline=False)
        embed.add_field(name="Map", value=info.map_name, inline=True)
        embed.add_field(name="Players", value=f"{info.player_count}/{info.max_players}", inline=True)
        embed.add_field(name="Player List", value=player_list, inline=False)
        embed.set_footer(text="Last updated")

        # Send Update to Discord Channel
        channel = bot.get_channel(CHANNEL_ID)
        if channel:
            await channel.send(embed=embed)
        else:
            print(f"⚠️ Channel ID {CHANNEL_ID} not found!")

    except Exception as e:
        print(f"⚠️ Error retrieving CS2 server status: {e}")

@bot.command()
async def set_channel(ctx):
    """Command to set the update channel dynamically."""
    global CHANNEL_ID
    CHANNEL_ID = ctx.channel.id
    await ctx.send(f"✅ This channel (`{ctx.channel.name}`) is now set for CS2 updates.")

bot.run(TOKEN)