import discord
from redbot.core import commands
from discord.ext import tasks
import aiohttp

class AGSServerStatus(commands.Cog):
    """Cog that monitors MMORPG server statuses using a REST health-check endpoint."""

    def __init__(self, bot):
        self.bot = bot
        # Stores servers as a dict with key = server name and value = (ip, port, last_status)
        # last_status is either True (online), False (offline) or None if unknown.
        self.servers = {}
        # Stores the channel ID where status update messages will be posted.
        self.status_channel = None
        # Custom status messages; keys are lowercase strings ("online", "offline").
        self.status_messages = {}
        # Start the background task.
        self.check_status_task.start()

    def cog_unload(self):
        self.check_status_task.cancel()

    async def is_server_online(self, ip: str, port: int, timeout: int = 5) -> bool:
        """
        Checks the health endpoint of the server.
        Expects your server to serve health info at: http://<ip>:<port>/api/health
        Returns True if a 200 status code is received, otherwise False.
        """
        health_url = f"http://{ip}:{port}/api/health"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(health_url, timeout=timeout) as response:
                    return response.status == 200
        except Exception:
            return False

    @commands.group()
    async def serverstatus(self, ctx):
        """Commands to manage the monitoring of game servers."""
        if ctx.invoked_subcommand is None:
            await ctx.send("Invalid serverstatus command. Use subcommands like add, remove, list, etc.")

    @serverstatus.command()
    async def add(self, ctx, name: str, ip: str, port: int):
        """
        Add a server to monitor.
        Example: [p]serverstatus add MyRealm 127.0.0.1 5757
        Note: The cog will query http://<ip>:<port>/api/health as its health-check endpoint.
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
            if status is True:
                status_text = "Online"
            elif status is False:
                status_text = "Offline"
            else:
                status_text = "Unknown"
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
        To use this command, reply to a message that contains the text you want to use.
        For example: reply to your desired status message with `[p]serverstatus setmessage online`
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
        This task periodically checks the health of each server by calling its REST health-check endpoint.
        When a change is detected (either from offline to online or vice versa), the cog posts an update
        to the designated channel.
        """
        if not self.status_channel:
            return

        channel = self.bot.get_channel(self.status_channel)
        if not channel:
            return

        for name, (ip, port, last_status) in list(self.servers.items()):
            is_online = await self.is_server_online(ip, port)

            # If status changes or has never been set, update and post a message.
            if last_status is None or is_online != last_status:
                self.servers[name] = (ip, port, is_online)
                message_type = "online" if is_online else "offline"
                default_message = f"Server `{name}` is now {message_type}."
                # Use a custom message if it exists.
                message = self.status_messages.get(message_type, default_message)
                await channel.send(message)