import os
import discord
import requests
from bs4 import BeautifulSoup
from discord.ext import tasks
import a2s
from mcrcon import MCRcon
from datetime import datetime
import pytz
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Load environment variables from Railway
TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))
RCON_IP = os.getenv("RCON_IP")
RCON_PORT = int(os.getenv("RCON_PORT", 27015))
RCON_PASSWORD = os.getenv("RCON_PASSWORD")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
SERVER_DEMOS_CHANNEL_ID = int(os.getenv("SERVER_DEMOS_CHANNEL_ID", 0))
DEMOS_URL = "http://de34.fshost.me/demos/cs2/1842/"

# Enable privileged intents
intents = discord.Intents.default()
intents.messages = True
bot = discord.Client(intents=intents)
tree = discord.app_commands.CommandTree(bot)

def send_rcon_command(command):
    """Send an RCON command to the CS2 server and return the response."""
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            response = rcon.command(command)
            return response if len(response) <= 1000 else response[:1000] + "... (truncated)"
    except Exception as e:
        logging.error(f"Error sending RCON command: {e}")
        return f"⚠️ Error: {e}"

def fetch_demos():
    """Scrapes the CS2 demos from the web directory."""
    try:
        response = requests.get(DEMOS_URL)
        if response.status_code != 200:
            logging.error(f"Failed to fetch demos, status code: {response.status_code}")
            return ["⚠️ Could not fetch demos. Check the URL."]

        soup = BeautifulSoup(response.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".dem")]

        if not links:
            logging.info("No demos found.")
            return ["⚠️ No demos found."]

        latest_demos = links[-5:]
        return [f"[{demo}](<{DEMOS_URL}{demo}>)" for demo in latest_demos]

    except Exception as e:
        logging.error(f"Error fetching demos: {e}")
        return [f"⚠️ Error fetching demos: {e}"]

async def get_server_status_embed():
    """Fetch CS2 server status and return an embed."""
    server_address = (SERVER_IP, SERVER_PORT)

    try:
        info = a2s.info(server_address)
        players = a2s.players(server_address)

        berlin_tz = pytz.timezone("Europe/Berlin")
        last_updated = datetime.now(berlin_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

        embed = discord.Embed(title="🎮 CS2 Server Status - 🟢 Online", color=0x00ff00)
        embed.add_field(name="🖥️ Server Name", value=info.server_name, inline=False)
        embed.add_field(name="🗺️ Map", value=info.map_name, inline=True)
        embed.add_field(name="👥 Players", value=f"{info.player_count}/{info.max_players}", inline=True)

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
        logging.error(f"Error fetching server status: {e}")
        embed = discord.Embed(title="⚠️ CS2 Server Status - 🔴 Offline", color=0xff0000)
        embed.add_field(name="❌ Server Unreachable", value="The server is currently offline.", inline=False)
        return embed

@tasks.loop(minutes=15)
async def auto_say():
    """Automatically sends a chat message to CS2 every 15 minutes and clears old messages."""
    channel = bot.get_channel(CHANNEL_ID)
    
    if channel:
        async for message in channel.history(limit=20):
            if message.author == bot.user:
                await message.delete()

        send_rcon_command("say Server is owned by Reshtan Gaming Center")
        logging.info("Auto message sent: Server is owned by Reshtan Gaming Center")
        await channel.send("✅ **Server is owned by Reshtan Gaming Center** (Auto Message)")

@bot.event
async def on_ready():
    await tree.sync()
    logging.info(f'Bot is online! Logged in as {bot.user}')
    auto_say.start()

@tree.command(name="status", description="Get the current CS2 server status")
async def status(interaction: discord.Interaction):
    """Slash command to get live CS2 server status"""
    await interaction.response.defer()
    embed = await get_server_status_embed()
    await interaction.followup.send(embed=embed)

@tree.command(name="leaderboard", description="Show the top 5 players in the CS2 server")
async def leaderboard(interaction: discord.Interaction):
    """Show the top 5 players based on kills"""
    await interaction.response.defer()
    
    try:
        players = a2s.players((SERVER_IP, SERVER_PORT), timeout=5)

        if not players:
            await interaction.followup.send("⚠️ No players online right now.")
            return

        top_players = sorted(players, key=lambda x: x.score, reverse=True)[:5]
        leaderboard_text = "\n".join(
            [f"🥇 **{p.name}** | 🏆 **{p.score}** kills | ⏳ **{p.duration / 60:.1f} mins**"
             for p in top_players]
        )

        embed = discord.Embed(title="🏆 CS2 Leaderboard (Top 5)", color=0xFFD700)
        embed.add_field(name="🔹 Players", value=leaderboard_text, inline=False)
        embed.set_footer(text="Data updates every 6 hours.")

        await interaction.followup.send(embed=embed)

    except TimeoutError:
        await interaction.followup.send("⚠️ CS2 server is not responding. Try again later.")

@tree.command(name="say", description="Send a message to CS2 chat")
@discord.app_commands.describe(message="The message to send")
async def say(interaction: discord.Interaction, message: str):
    """Sends a message to CS2 chat using `say`."""
    response = send_rcon_command(f"say {message}")
    await interaction.response.send_message(f"✅ Message sent to CS2 chat.\n📝 **RCON Response:** {response}")

@tree.command(name="demos", description="Get the latest CS2 demos")
async def demos(interaction: discord.Interaction):
    """Fetches and displays the latest 5 CS2 demos from the web directory."""
    
    if interaction.channel_id != SERVER_DEMOS_CHANNEL_ID:
        await interaction.response.send_message(
            f"❌ This command can only be used in <#{SERVER_DEMOS_CHANNEL_ID}>.", ephemeral=True
        )
        return

    await interaction.response.defer()
    demo_list = fetch_demos()
    demo_text = "\n".join(demo_list)

    embed = discord.Embed(title="🎥 Latest CS2 Demos", color=0x00ff00)
    embed.description = demo_text

    await interaction.followup.send(embed=embed)

bot.run(TOKEN)