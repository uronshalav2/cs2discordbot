import os
import discord
from discord import app_commands
from discord.ext import tasks
import a2s  # Source Server Query Protocol
import valve.rcon  # RCON for admin commands
from datetime import datetime
import pytz  # Timezone support for Germany

# ✅ Load environment variables from Railway
TOKEN = os.getenv("TOKEN")
SERVER_IP = os.getenv("SERVER_IP")
SERVER_PORT = int(os.getenv("SERVER_PORT", 27015))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", 0))
RCON_IP = os.getenv("SERVER_IP")
RCON_PORT = int(os.getenv("SERVER_PORT", 27015))
RCON_PASSWORD = os.getenv("RCON_PASSWORD")

# ✅ Enable privileged intents
intents = discord.Intents.default()
intents.message_content = True

# ✅ Initialize bot with command tree
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)

def send_rcon_command(command):
    """Send an RCON command to the CS2 server and return the response."""
    try:
        with valve.rcon.RCON((RCON_IP, RCON_PORT), RCON_PASSWORD) as rcon:
            response = rcon.execute(command)
            return response
    except Exception as e:
        return f"⚠️ Error: {e}"

class AdminMenu(discord.ui.View):
    """Interactive Admin Menu for CS2 Management"""
    def __init__(self):
        super().__init__()

    @discord.ui.button(label="Kick Player", style=discord.ButtonStyle.danger)
    async def kick_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("🔹 Use `/kick <player>` to remove a player.", ephemeral=True)

    @discord.ui.button(label="Ban Player", style=discord.ButtonStyle.danger)
    async def ban_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("🔹 Use `/ban <player>` to permanently ban a player.", ephemeral=True)

    @discord.ui.button(label="Mute Player", style=discord.ButtonStyle.secondary)
    async def mute_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("🔹 Use `/mute <player>` to mute a player in chat.", ephemeral=True)

    @discord.ui.button(label="Send Chat Message", style=discord.ButtonStyle.success)
    async def say_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("🔹 Use `/say <message>` to send a message to CS2 chat.", ephemeral=True)

@tree.command(name="admin", description="Open the CS2 Admin menu")
async def admin(interaction: discord.Interaction):
    """Displays an admin menu with buttons for quick actions."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ You don't have permission to use this.", ephemeral=True)
        return

    embed = discord.Embed(title="⚙️ CS2 Admin Menu", color=0x5865F2)
    embed.add_field(name="🚀 Available Actions", value="Click a button below to execute a command.")
    embed.set_footer(text="Use /kick, /ban, /mute, or /say for manual commands.")

    await interaction.response.send_message(embed=embed, view=AdminMenu())

@tree.command(name="say", description="Send a chat message to all players in the CS2 server")
@app_commands.describe(message="The message to send in chat")
async def say(interaction: discord.Interaction, message: str):
    """Sends a chat message to all players in CS2."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return

    response = send_rcon_command(f"sm_say [Discord] {message}")
    await interaction.response.send_message(f"✅ Message sent to CS2 chat:\n📝 **{message}**\n📝 **RCON Response:** {response}")

@tree.command(name="csay", description="Send a center message to all players in CS2")
@app_commands.describe(message="The message to display in the center")
async def csay(interaction: discord.Interaction, message: str):
    """Sends a center screen message in CS2."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return

    response = send_rcon_command(f"sm_csay {message}")
    await interaction.response.send_message(f"✅ Center message sent:\n📝 **{message}**\n📝 **RCON Response:** {response}")

@tree.command(name="kick", description="Kick a player from the CS2 server using CS2-Simple Admin")
@app_commands.describe(player="The player's name to kick")
async def kick(interaction: discord.Interaction, player: str):
    """Kick a player using CS2-Simple Admin"""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return

    response = send_rcon_command(f"sm_kick \"{player}\" \"Kicked by admin.\"")
    await interaction.response.send_message(f"✅ **{player}** has been kicked.\n📝 **RCON Response:** {response}")

@tree.command(name="ban", description="Ban a player from the CS2 server using CS2-Simple Admin")
@app_commands.describe(player="The player's name to ban")
async def ban(interaction: discord.Interaction, player: str):
    """Ban a player using CS2-Simple Admin"""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return

    response = send_rcon_command(f"sm_ban \"{player}\" 0 \"Banned by admin.\"")
    await interaction.response.send_message(f"🚫 **{player}** has been banned permanently.\n📝 **RCON Response:** {response}")

@tree.command(name="mute", description="Mute a player in CS2 chat using CS2-Simple Admin")
@app_commands.describe(player="The player's name to mute")
async def mute(interaction: discord.Interaction, player: str):
    """Mute a player using CS2-Simple Admin"""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ You don't have permission to use this command.", ephemeral=True)
        return

    response = send_rcon_command(f"sm_mute \"{player}\"")
    await interaction.response.send_message(f"🔇 **{player}** has been muted.\n📝 **RCON Response:** {response}")

bot.run(TOKEN)