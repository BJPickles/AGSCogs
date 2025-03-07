import discord
from redbot.core import commands
from discord.ext import tasks
import aiohttp

class AGSServerStatus(commands.Cog):
    """Cog that monitors MMORPG server statuses using a REST health-check endpoint."""

    def __init__(self, bot):
        self.bot = bot
        # Dictionary mapping server names to tuples: (ip, port, last_status)
        # last_status is True (online), False (offline) or None if unknown.
        self.servers = {}
        # Stores the channel ID where status update messages will be posted.
        self.status_channel = None
        # Custom status messages; keys are lowercase strings ("online", "offline").
        self.status_messages = {}
        # Start the periodic background task.
        self.check_status_task.start()

    def cog_unload(self):
        self.check_status_task.cancel()

    async def is_server_online(self, ip: str, port: int, timeout: int = 5) -> bool:
        """
        Checks the health endpoint of the server.
        The cog expects the server to serve health info at: http://<ip>:<port>/api/health.
        Returns True if a 200 status is received, otherwise False.
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
        Commands to manage monitoring of game servers.
        
        Subcommands include:
          • add
          • remove
          • list
          • setchannel
          • setmessage
        
        Use [p]help serverstatus for details.
        """
        await ctx.send_help(ctx.command)

    @serverstatus.command()
    async def add(self, ctx, name: str, ip: str, port: int):
        """
        Add a server to monitor.
        
        Examples:
        [p]serverstatus add Avalon 95.217.228.35 5757
        [p]serverstatus add "Public Test Realm" 95.217.228.35 5757
        
        Note: If the realm name contains spaces and/or markdown formatting, enclose it in quotes.
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
        
        To use this command, reply to the message that contains the text you want.
        For example, reply to your desired online message with:
          [p]serverstatus setmessage online
        """
        if not ctx.message.reference:
            await ctx.send("Please reply to the message you want to save as the custom message.")
            return
        ref_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
        self.status_messages[status.lower()] = ref_msg.content
        await ctx.send(f"Custom message set for '{status}' status.")

    @tasks.loop(seconds=60)
    async def check_status_task(self):
        """
        Periodically checks each server's health-check endpoint.
        If a change is detected (offline → online or vice-versa), posts a message to the designated channel.
        """
        if not self.status_channel:
            return

        channel = self.bot.get_channel(self.status_channel)
        if not channel:
            return

        for name, (ip, port, last_status) in list(self.servers.items()):
            is_online = await self.is_server_online(ip, port)
            if last_status is None or is_online != last_status:
                self.servers[name] = (ip, port, is_online)
                message_type = "online" if is_online else "offline"
                default_message = f"Server `{name}` is now {message_type}."
                # Use a custom message if defined, otherwise fallback to the default.
                message = self.status_messages.get(message_type, default_message)
                await channel.send(message)