import discord
from redbot.core import commands
from discord.ext import tasks
import aiohttp

class AGSServerStatus(commands.Cog):
    """Cog that monitors MMORPG server statuses."""
    
    def __init__(self, bot):
        self.bot = bot
        self.servers = {}           # {name: (ip, port, status)}
        self.status_channel = None  # Channel ID where status updates will be sent.
        self.status_messages = {}   # {status: message}
        self.check_status_task.start()

    def cog_unload(self):
        self.check_status_task.cancel()

    @commands.group()
    async def serverstatus(self, ctx):
        """Commands to manage server status tracking."""
        if ctx.invoked_subcommand is None:
            await ctx.send("Invalid serverstatus command. Use subcommands like add, remove, list, etc.")

    @serverstatus.command()
    async def add(self, ctx, name: str, ip: str, port: int):
        """Add a realm to monitor."""
        self.servers[name] = (ip, port, None)
        await ctx.send(f"Added server `{name}` at `{ip}:{port}`.")

    @serverstatus.command()
    async def remove(self, ctx, name: str):
        """Remove a monitored realm."""
        if name in self.servers:
            del self.servers[name]
            await ctx.send(f"Removed server `{name}`.")
        else:
            await ctx.send("Server not found.")

    @serverstatus.command()
    async def list(self, ctx):
        """List all monitored realms."""
        if not self.servers:
            await ctx.send("No servers are being monitored.")
            return
        msg = "**Monitored Servers:**\n" + "\n".join(
            f"`{name}` - `{ip}:{port}`" for name, (ip, port, _) in self.servers.items()
        )
        await ctx.send(msg)

    @serverstatus.command()
    async def setchannel(self, ctx, channel: discord.TextChannel):
        """Set the channel for status updates."""
        self.status_channel = channel.id
        await ctx.send(f"Status updates will be posted in {channel.mention}.")

    @serverstatus.command()
    async def setmessage(self, ctx, status: str):
        """
        Set a custom message for server status changes.
        Reply to a message with this command to capture its content.
        """
        if not ctx.message.reference:
            await ctx.send("Please reply to a message you want to save.")
            return
        ref_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
        self.status_messages[status.lower()] = ref_msg.content
        await ctx.send(f"Saved message for `{status}` status.")

    @tasks.loop(seconds=60)
    async def check_status_task(self):
        """Periodically checks the status of the servers."""
        if not self.status_channel:
            return

        channel = self.bot.get_channel(self.status_channel)
        if not channel:
            return
        
        async with aiohttp.ClientSession() as session:
            for name, (ip, port, last_status) in self.servers.items():
                try:
                    async with session.get(f"http://{ip}:{port}", timeout=5) as resp:
                        is_online = resp.status == 200
                except Exception:
                    is_online = False
                
                if last_status is None or is_online != last_status:
                    self.servers[name] = (ip, port, is_online)
                    message_type = "online" if is_online else "offline"
                    message = self.status_messages.get(
                        message_type,
                        f"Server `{name}` is now {message_type}."
                    )
                    await channel.send(message)