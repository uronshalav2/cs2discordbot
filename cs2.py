import discord
from discord.ext import commands, tasks
import a2s  # Source Server Query Protocol
import asyncio

TOKEN = "YOUR_DISCORD_BOT_TOKEN"
SERVER_IP = "YOUR_SERVER_IP"  # Example: "127.0.0.1"
SERVER_PORT = 27015  # Default CS2 server port
CHANNEL_ID = 123456789012345678  # Replace with your Discord channel ID

bot = commands.Bot(command_prefix="!", intents=discord.Intents.default())

@bot.event
async def on_ready():
    print(f'Bot is ready! Logged in as {bot.user}')
    
    # Start the server status update task
    cs2status_auto_update.start()

@tasks.loop(seconds=60)  # Updates every 60 seconds
async def cs2status_auto_update():
    """Automatically fetches and sends CS2 server status to a specific channel."""
    try:
        server_address = (SERVER_IP, SERVER_PORT)
        info = a2s.info(server_address)
        players = a2s.players(server_address)

        # Create player list
        player_list = "\n".join([f"{p.name} - {p.score} kills" for p in players]) if players else "No players online."

        # Create the embed message
        embed = discord.Embed(title="CS2 Server Status", color=0x00ff00)
        embed.add_field(name="Server Name", value=info.server_name, inline=False)
        embed.add_field(name="Map", value=info.map_name, inline=True)
        embed.add_field(name="Players", value=f"{info.player_count}/{info.max_players}", inline=True)
        embed.add_field(name="Player List", value=player_list, inline=False)
        embed.set_footer(text="Last updated")

        # Get the Discord channel and send the message
        channel = bot.get_channel(CHANNEL_ID)
        if channel:
            await channel.send(embed=embed)
        else:
            print(f"Error: Channel ID {CHANNEL_ID} not found!")

    except Exception as e:
        print(f"Error retrieving CS2 server status: {e}")

@bot.command()
async def set_channel(ctx):
    """Command to set the bot's update channel dynamically."""
    global CHANNEL_ID
    CHANNEL_ID = ctx.channel.id
    await ctx.send(f"✅ This channel (`{ctx.channel.name}`) is now set for CS2 updates.")

bot.run(TOKEN)