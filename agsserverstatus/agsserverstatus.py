import discord
from redbot.core import commands
from discord.ext import tasks
import aiohttp
from datetime import datetime

class AGSServerStatus(commands.Cog):
    """Cog that monitors MMORPG server statuses using a REST health-check endpoint.
    
    Custom placeholders available in messages:
      • {name}        – Realm name
      • {ip}          – Server IP address
      • {port}        – Server port number
      • {status}      – New status ("online" or "offline")
      • {prev_status} – Previous status ("online", "offline", or "unknown")
      • {timestamp}   – UTC time (YYYY-MM-DD HH:MM:SS UTC)
    """

    def __init__(self, bot):
        self.bot = bot
        # Map server name to tuple: (ip, port, last_status)
        # last_status is True (online), False (offline) or None if unknown.
        self.servers = {}
        # The channel ID where notifications are posted.
        self.status_channel = None
        # Custom messages stored as { "online": message, "offline": message }
        self.status_messages = {}
        # Start the periodic status-check loop.
        self.check_status_task.start()

    def cog_unload(self):
        self.check_status_task.cancel()

    async def is_server_online(self, ip: str, port: int, timeout: int = 5) -> bool:
        """
        Check the health endpoint of the server.
        Expects the server to provide health info at http://<ip>:<port>/api/health.
        Returns True if the GET request returns a 200, else False.
        """
        health_url = f"http://{ip}:{port}/api/health"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(health_url, timeout=timeout) as response:
                    return response.status == 200
        except Exception:
            return False

    @commands.group(invoke_without_command=True)
    async def serverstatus(self, ctx):
        """
        Manage the monitoring of game servers.
        
        Subcommands:
          • add         - Add a server to monitor.
          • remove      - Remove a monitored server.
          • list        - List all monitored servers.
          • setchannel  - Define the channel for status updates.
          • setmessage  - Set a custom status update message.
          • instructions- Show setup instructions.
        
        Use [p]help serverstatus for more details.
        """
        await ctx.send_help(ctx.command)

    @serverstatus.command()
    async def add(self, ctx, name: str, ip: str, port: int):
        """
        Add a server to monitor.
        
        Examples:
          [p]serverstatus add Avalon 192.168.1.1 5757
          [p]serverstatus add "Public Test Realm" 192.168.1.2 5757
        
        If the realm name includes spaces or markdown, enclose it in quotes.
        """
        self.servers[name] = (ip, port, None)
        await ctx.send(f"Added server '{name}' at {ip}:{port}.")

    @serverstatus.command()
    async def remove(self, ctx, name: str):
        """Remove a monitored server."""
        if name in self.servers:
            del self.servers[name]
            await ctx.send(f"Removed server '{name}'.")
        else:
            await ctx.send("Server not found.")

    @serverstatus.command(name="list")
    async def list_servers(self, ctx):
        """List all monitored servers."""
        if not self.servers:
            await ctx.send("No servers are being monitored.")
            return

        lines = []
        for name, (ip, port, status) in self.servers.items():
            status_text = "Unknown"
            if status is True:
                status_text = "Online"
            elif status is False:
                status_text = "Offline"
            lines.append(f"`{name}` - {ip}:{port} - {status_text}")
        message = "**Monitored Servers:**\n" + "\n".join(lines)
        await ctx.send(message)

    @serverstatus.command()
    async def setchannel(self, ctx, channel: discord.TextChannel):
        """Set the channel where status update messages will be posted."""
        self.status_channel = channel.id
        await ctx.send(f"Status updates will be posted in {channel.mention}.")

    @serverstatus.command()
    async def setmessage(self, ctx, status: str):
        """
        Set a custom message for server status changes.
        
        When the realm's status changes, the saved message for "online" or "offline" is used.
        You can use the following placeholders in your message:
          {name}        - Realm name
          {ip}          - Server IP address
          {port}        - Server port
          {status}      - New status ("online" or "offline")
          {prev_status} - Previous status ("online", "offline", or "unknown")
          {timestamp}   - UTC timestamp (YYYY-MM-DD HH:MM:SS UTC)
        
        To set a custom message, reply to the desired message with:
          [p]serverstatus setmessage online
        or
          [p]serverstatus setmessage offline
        """
        if not ctx.message.reference:
            await ctx.send("Please reply to the message you want to save as the custom message.")
            return
        ref_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
        self.status_messages[status.lower()] = ref_msg.content
        await ctx.send(f"Custom message set for '{status}' status.")

    @serverstatus.group(name="instructions", invoke_without_command=True)
    async def instructions(self, ctx):
        """
        Provides instructions for setting up the health-check and integration.
        
        Subcommands:
          • generic - Show generic setup instructions.
          • mmo     - Show AEGIS Game Studios–specific instructions (bot owner only).
        """
        await ctx.send_help(ctx.command)

    @instructions.command(name="generic")
    async def instructions_generic(self, ctx):
        """
        Display generic setup instructions.
        
        Ensure that your REST endpoint at /api/health returns a HTTP 200 response when your server is healthy,
        and that your firewall is configured to allow incoming connections on your chosen port.
        """
        message = (
            "**Generic Setup Instructions**\n\n"
            "1. Confirm that your game server's REST endpoint `/api/health` returns an HTTP 200 status when healthy.\n"
            "2. Test the endpoint using your browser or a tool like curl: `curl http://<ip>:<port>/api/health`.\n"
            "3. Ensure that your firewall (or network security group) is configured to allow inbound connections on the specified port.\n"
            "4. If necessary, update your firewall rules (for example, using `sudo ufw allow <port>` on Ubuntu).\n"
        )
        await ctx.send(message)

    @instructions.command(name="mmo")
    @commands.is_owner()
    async def instructions_mmo(self, ctx):
        """
        Display AEGIS Game Studios–specific instructions.
        
        This MMO setup relies on additional code within the game server. It is connected to the 'MMOServerInstance -> RestDatabaseClient', 
        where the 'Rest Health Server' script has been added beneath 'Rest Database Client'. 
        (This is a reminder for Five, creator of AEGIS Kingdoms.)
        """
        message = (
            "**AEGIS Game Studios MMO Setup Instructions**\n\n"
            "1. The MMO route for AEGIS Game Studios relies on additional code within your game server.\n"
            "2. Integration is achieved via the 'MMOServerInstance -> RestDatabaseClient'.\n"
            "3. Ensure that the 'Rest Health Server' script is added beneath the 'Rest Database Client'.\n"
            "4. This setup is specifically tailored for AEGIS Game Studios. (Reminder for Five: check integration details.)\n"
        )
        await ctx.send(message)

    @tasks.loop(seconds=60)
    async def check_status_task(self):
        """
        Periodically checks each server's health endpoint.
        If a change is detected, sends a status update to the defined channel.
        
        The custom message (if defined) is processed to replace placeholders:
          {name}, {ip}, {port}, {status}, {prev_status}, {timestamp}
        """
        if not self.status_channel:
            return

        channel = self.bot.get_channel(self.status_channel)
        if not channel:
            return

        for name, (ip, port, last_status) in list(self.servers.items()):
            new_status = await self.is_server_online(ip, port)
            if last_status is None or new_status != last_status:
                current_status = "online" if new_status else "offline"
                previous_status = "unknown" if last_status is None else ("online" if last_status else "offline")
                timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                self.servers[name] = (ip, port, new_status)
                default_message = f"Server `{name}` is now {current_status}."
                message_template = self.status_messages.get(current_status, default_message)
                try:
                    # Format the custom message with defined placeholders.
                    message = message_template.format(
                        name=name,
                        ip=ip,
                        port=port,
                        status=current_status,
                        prev_status=previous_status,
                        timestamp=timestamp
                    )
                except Exception as e:
                    # Fallback to default message on formatting error.
                    message = default_message
                await channel.send(message)