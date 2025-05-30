import os
import discord
import requests
from bs4 import BeautifulSoup  # ✅ Used for scraping demo list
from discord.ext import tasks
import a2s  # Source Server Query Protocol
from mcrcon import MCRcon
from datetime import datetime
import pytz  

# ✅ Load environment variables from Railway
TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))
RCON_IP = os.getenv("RCON_IP")
RCON_PORT = int(os.getenv("RCON_PORT", 27015))
RCON_PASSWORD = os.getenv("RCON_PASSWORD")
FACEIT_API_KEY=os.getenv("FACEIT_API_KEY")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
SERVER_DEMOS_CHANNEL_ID = int(os.getenv("SERVER_DEMOS_CHANNEL_ID", 0))  # ✅ Restrict /demos to this channel
DEMOS_URL = "https://de34.fsho.st/demos/cs2/1842/"  # ✅ Your demo directory URL

# ✅ Enable privileged intents
intents = discord.Intents.default()
intents.messages = True  # ✅ Needed for deleting messages
bot = discord.Client(intents=intents)
tree = discord.app_commands.CommandTree(bot)

def send_rcon_command(command):
    """Send an RCON command to the CS2 server and return the response."""
    try:
        with MCRcon(RCON_IP, RCON_PASSWORD, port=RCON_PORT) as rcon:
            response = rcon.command(command)
            return response if len(response) <= 1000 else response[:1000] + "... (truncated)"
    except Exception as e:
        return f"⚠️ Error: {e}"

def country_code_to_flag(code):
    """Convert a 2-letter country code to a Discord-friendly emoji flag."""
    if not code or len(code) != 2:
        return "🏳️"
    return chr(ord(code[0].upper()) + 127397) + chr(ord(code[1].upper()) + 127397)


def fetch_demos():
    """Scrapes the CS2 demos from the web directory."""
    try:
        response = requests.get(DEMOS_URL)
        if response.status_code != 200:
            return ["⚠️ Could not fetch demos. Check the URL."]

        soup = BeautifulSoup(response.text, "html.parser")
        links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".dem")]

        if not links:
            return ["⚠️ No demos found."]

        # ✅ Get the latest 5 demos
        latest_demos = links[-5:]  

        return [f"[{demo}](<{DEMOS_URL}{demo}>)" for demo in latest_demos]

    except Exception as e:
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

    except Exception:
        embed = discord.Embed(title="⚠️ CS2 Server Status - 🔴 Offline", color=0xff0000)
        embed.add_field(name="❌ Server Unreachable", value="The server is currently offline.", inline=False)
        return embed

@tasks.loop(minutes=15)
async def auto_say():
    """Automatically sends a chat message to CS2 every 15 minutes and clears old messages."""
    channel = bot.get_channel(CHANNEL_ID)
    
    if channel:
        # ✅ Delete bot messages before sending a new one
        async for message in channel.history(limit=20):
            if message.author == bot.user:
                await message.delete()

        # ✅ Send the CS2 chat message
        send_rcon_command("say Server is owned by Reshtan Gaming Center")
        print("✅ Auto message sent: Server is owned by Reshtan Gaming Center")
        await channel.send("✅ **Server is owned by Reshtan Gaming Center** (Auto Message)")

@bot.event
async def on_ready():
    GUILD_ID = os.getenv("GUILD_ID") # 🔁 Replace this with your actual server ID
    guild = discord.Object(id=GUILD_ID)
    await tree.sync(guild=guild)  # 👈 Syncs slash commands immediately for that server only
    print(f'✅ Bot is online and commands synced to guild {GUILD_ID} as {bot.user}')
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

@tree.command(name="elo", description="Check Faceit ELO using nickname")
@discord.app_commands.describe(nickname="The Faceit nickname")
async def elo(interaction: discord.Interaction, nickname: str):
    """Fetch Faceit ELO, level, and region based on nickname."""
    await interaction.response.defer()

    api_key = os.getenv("FACEIT_API_KEY")
    if not api_key:
        await interaction.followup.send("❌ FACEIT_API_KEY is missing in environment variables.")
        return

    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    try:
        # 🔍 Get player info
        url = f"https://open.faceit.com/data/v4/players?nickname={nickname}"
        response = requests.get(url, headers=headers)
        data = response.json()

        if response.status_code != 200:
            msg = data.get("message", "Unknown error occurred.")
            raise Exception(msg)

        # ✅ Extract CS2 or CSGO data
        games = data.get("games", {})
        cs_game = games.get("cs2") or games.get("csgo")

        if not cs_game:
            await interaction.followup.send("⚠️ No CS2 or CSGO data found for this player.")
            return

        elo = cs_game.get("faceit_elo", "N/A")
        level = cs_game.get("skill_level", "N/A")
        region = cs_game.get("region", "N/A")
        profile_url = f"https://www.faceit.com/en/players/{nickname}"
        country_code = data.get("country", "N/A")
        flag = country_code_to_flag(country_code)


        embed = discord.Embed(
            title=f"🎮 Faceit Profile: {nickname}",
            description=f"[🌐 View on Faceit]({profile_url})",
            color=0x0099ff
        )
        embed.add_field(name="📊 ELO", value=str(elo), inline=True)
        embed.add_field(name="⭐ Level", value=str(level), inline=True)
        embed.add_field(name="🌍 Region", value=region, inline=True)
        embed.set_footer(text="Faceit stats via open.faceit.com API")
        embed.add_field(name="🌐 Country", value=f"{flag} {country_code}", inline=True)


        await interaction.followup.send(embed=embed)

    except Exception as e:
        await interaction.followup.send(f"❌ Error fetching Faceit data: `{e}`")


bot.run(TOKEN)
