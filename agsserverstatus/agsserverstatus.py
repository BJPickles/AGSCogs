import discord
import aiohttp
import asyncio
from datetime import datetime
import logging
from redbot.core import commands, Config
from discord.ext import tasks
from typing import Optional

logger = logging.getLogger(__name__)

class AGSServerStatus(commands.Cog):
    """Cog that monitors MMORPG server statuses using a REST health-check endpoint.
    
    Custom placeholders available in messages:
      • {name}        – Realm name
      • {ip}          – Server IP address
      • {port}        – Server port number
      • {status}      – New status ("online" or "offline")
      • {prev_status} – Previous status ("online", "offline", or "unknown")
      • {timestamp}   – UTC time (YYYY-MM-DD HH:MM:SS UTC)
      
    NOTE: All settings are saved persistently.
    """

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        # Set up persistent config with a unique identifier.
        self.config = Config.get_conf(self, identifier=123456789012345678, force_registration=True)
        default_global = {
            "servers": {},          # {name: {"ip": ip, "port": port, "enabled": bool, "last_status": bool or None}}
            "status_channel": None, # Stored as channel ID
            "status_messages": {},  # { "online": message, "offline": message }
            "active": True          # Overall toggle
        }
        self.config.register_global(**default_global)
        # In-memory storage: a server is represented as a tuple: (ip, port, last_status, enabled)
        self.servers: dict[str, tuple[str, int, Optional[bool], bool]] = {}
        self.status_channel: Optional[int] = None
        self.status_messages: dict[str, str] = {}
        self.active: bool = True
        # To track the last update message (per realm) so it can be deleted.
        self.last_messages: dict[str, int] = {}
        # Create a persistent aiohttp session
        self.session = aiohttp.ClientSession()
        # Load persistent settings
        self.bot.loop.create_task(self.initialize_settings())
        # Start the periodic status-check task
        self.check_status_task.start()

    async def initialize_settings(self) -> None:
        data = await self.config.all()
        servers = data.get("servers", {})
        for name, details in servers.items():
            ip = details.get("ip")
            port = details.get("port")
            last_status = details.get("last_status", None)
            enabled = details.get("enabled", True)
            self.servers[name] = (ip, port, last_status, enabled)
        self.status_channel = data.get("status_channel")
        self.status_messages = data.get("status_messages", {})
        self.active = data.get("active", True)
        logger.info("Initialized settings for AGSServerStatus.")

    def cog_unload(self) -> None:
        self.check_status_task.cancel()
        if not self.session.closed:
            # Schedule session close on unload.
            self.bot.loop.create_task(self.session.close())
        logger.info("Cog unloaded: session closed and task cancelled.")

    @tasks.loop(minutes=1)
    async def check_status_task(self) -> None:
        """Periodic task that performs a status update for all enabled servers."""
        if self.active:
            logger.debug("Running periodic status update.")
            await self.run_status_update()

    @check_status_task.error
    async def check_status_task_error(self, error: Exception) -> None:
        logger.error("Error in check_status_task: %s", error)

    async def is_server_online(self, ip: str, port: int, timeout: int = 5) -> bool:
        """
        Check the health endpoint of the server.
        Expects a healthy server to return status 200 at http://<ip>:<port>/api/health.
        Returns True if healthy, else False.
        """
        health_url = f"http://{ip}:{port}/api/health"
        try:
            async with self.session.get(health_url, timeout=timeout) as response:
                return response.status == 200
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug("Error checking server %s:%s - %s", ip, port, e)
            return False

    async def _send_status_update(self, realm: str, channel: discord.TextChannel, content: str) -> None:
        """
        Sends a status update message while deleting the previous one (if any) for the same realm.
        """
        if realm in self.last_messages:
            try:
                old_msg = await channel.fetch_message(self.last_messages[realm])
                await old_msg.delete()
            except Exception as e:
                logger.debug("Failed to delete previous message for %s: %s", realm, e)
        try:
            new_msg = await channel.send(content)
            self.last_messages[realm] = new_msg.id
        except Exception as e:
            logger.error("Failed to send status update for %s: %s", realm, e)

    def _validate_format(self, message_text: str) -> bool:
        """
        Validates the custom message using dummy placeholder values.
        """
        try:
            message_text.format(
                name="TestServer",
                ip="127.0.0.1",
                port=1234,
                status="online",
                prev_status="offline",
                timestamp="2023-01-01 00:00:00 UTC"
            )
            return True
        except Exception as e:
            logger.error("Format validation error: %s", e)
            return False

    async def _set_custom_message(self, kind: str, message_text: Optional[str], ctx: commands.Context) -> None:
        """
        Helper function to set or view a custom message for the given kind ("online" or "offline").
        If a message_text is provided, its formatting is validated before saving.
        """
        if message_text:
            # Validate formatting.
            if not self._validate_format(message_text):
                await ctx.send("The provided message formatting is invalid. Please check your placeholders.")
                return
            self.status_messages[kind] = message_text
            current_messages = await self.config.status_messages()
            current_messages[kind] = message_text
            await self.config.status_messages.set(current_messages)
            await ctx.send(f"Custom message for {kind} status updated.")
        elif ctx.message.reference:
            try:
                ref_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
            except Exception as e:
                logger.error("Failed to fetch referenced message: %s", e)
                return await ctx.send("Failed to fetch the referenced message.")
            if not self._validate_format(ref_msg.content):
                await ctx.send("The referenced message has invalid formatting placeholders.")
                return
            self.status_messages[kind] = ref_msg.content
            current_messages = await self.config.status_messages()
            current_messages[kind] = ref_msg.content
            await self.config.status_messages.set(current_messages)
            await ctx.send(f"Custom message for {kind} status updated from referenced message.")
        else:
            current = self.status_messages.get(kind)
            if current:
                await ctx.send(f"Current custom message for {kind}: {current}")
            else:
                await ctx.send(f"No custom message set for {kind} status.")

    @commands.group(invoke_without_command=True)
    async def serverstatus(self, ctx: commands.Context) -> None:
        """
        Manage the monitoring of game servers.
        
        Subcommands include: add, remove, list, togglerealm, setchannel, setmessage, toggle, view, formatting,
        instructions, and reset.
        """
        await ctx.send_help(ctx.command)

    @serverstatus.command()
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def add(self, ctx: commands.Context, name: str, ip: str, port: int) -> None:
        """
        Add a server to monitor.
        
        Examples:
          [p]serverstatus add Avalon 192.168.1.1 5757
          [p]serverstatus add "Public Test Realm" 192.168.1.2 5757
        """
        self.servers[name] = (ip, port, None, True)
        current_servers = await self.config.servers()
        current_servers[name] = {"ip": ip, "port": port, "enabled": True, "last_status": None}
        await self.config.servers.set(current_servers)
        await ctx.send(f"Added server {name} at {ip}:{port}.")
        logger.info("Added server %s at %s:%d", name, ip, port)

    @serverstatus.command()
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def remove(self, ctx: commands.Context, name: str) -> None:
        """Remove a monitored server."""
        if name in self.servers:
            del self.servers[name]
            if name in self.last_messages:
                del self.last_messages[name]
            current_servers = await self.config.servers()
            if name in current_servers:
                del current_servers[name]
                await self.config.servers.set(current_servers)
            await ctx.send(f"Removed server {name}.")
            logger.info("Removed server %s", name)
        else:
            await ctx.send("Server not found.")

    @serverstatus.command(name="list")
    async def list_servers(self, ctx: commands.Context) -> None:
        """List all monitored servers."""
        if not self.servers:
            await ctx.send("No servers are being monitored.")
            return
        lines = []
        for name, (ip, port, status, enabled) in self.servers.items():
            status_text = "Unknown"
            if status is True:
                status_text = "Online"
            elif status is False:
                status_text = "Offline"
            lines.append(f"{name} - {ip}:{port} - {status_text} - {'Enabled' if enabled else 'Disabled'}")
        message = "**Monitored Servers:**\n" + "\n".join(lines)
        await ctx.send(message)

    @serverstatus.command()
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def togglerealm(self, ctx: commands.Context, name: str, state: Optional[str] = None) -> None:
        """
        Toggle monitoring on or off for an individual realm.
        
        Usage: [p]serverstatus togglerealm <realm name> [on/off]
        Without an on/off argument, the current state will be toggled.
        """
        if name not in self.servers:
            return await ctx.send("Server not found.")
        ip, port, last_status, enabled = self.servers[name]
        if state:
            state_lower = state.lower()
            if state_lower in ["on", "enable", "enabled"]:
                new_enabled = True
            elif state_lower in ["off", "disable", "disabled"]:
                new_enabled = False
            else:
                return await ctx.send("Invalid state. Use on/off.")
        else:
            new_enabled = not enabled
        self.servers[name] = (ip, port, last_status, new_enabled)
        current_servers = await self.config.servers()
        if name in current_servers:
            current_servers[name]["enabled"] = new_enabled
            await self.config.servers.set(current_servers)
        await ctx.send(f"Monitoring for server {name} is now {'enabled' if new_enabled else 'disabled'}.")
        logger.info("Toggled realm %s to %s", name, "enabled" if new_enabled else "disabled")
        
        if new_enabled and self.status_channel:
            new_status_bool = await self.is_server_online(ip, port)
            if last_status is not None and new_status_bool == last_status:
                return
            current_status = "online" if new_status_bool else "offline"
            prev_status = "unknown" if last_status is None else ("online" if last_status else "offline")
            timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            default_message = f"Server {name} is now {current_status}."
            message_template = self.status_messages.get(current_status, default_message)
            try:
                message = message_template.format(
                    name=name,
                    ip=ip,
                    port=port,
                    status=current_status,
                    prev_status=prev_status,
                    timestamp=timestamp
                )
            except Exception as e:
                logger.error("Error formatting message for %s: %s", name, e)
                message = default_message
            channel = self.bot.get_channel(self.status_channel)
            if channel:
                await self._send_status_update(name, channel, message)
            self.servers[name] = (ip, port, new_status_bool, new_enabled)
            current_servers = await self.config.servers()
            if name in current_servers:
                current_servers[name]["last_status"] = new_status_bool
                await self.config.servers.set(current_servers)

    @serverstatus.command()
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def setchannel(self, ctx: commands.Context, channel: discord.TextChannel) -> None:
        """Set the channel where status update messages will be posted."""
        self.status_channel = channel.id
        await self.config.status_channel.set(channel.id)
        await ctx.send(f"Status updates will be posted in {channel.mention}.")
        logger.info("Set status channel to %s", channel.id)

    @serverstatus.group(name="setmessage", invoke_without_command=True)
    async def setmessage(self, ctx: commands.Context) -> None:
        """
        Set or view a custom message for status changes.
        
        Custom messages can include these placeholders:
          • {name}        - Realm name
          • {ip}          - Server IP address
          • {port}        - Server port number
          • {status}      - New status ("online" or "offline")
          • {prev_status} - Previous status ("online", "offline", or "unknown")
          • {timestamp}   - UTC timestamp (YYYY-MM-DD HH:MM:SS UTC)
        """
        await ctx.send_help(ctx.command)

    @setmessage.command(name="online")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def setmessage_online(self, ctx: commands.Context, *, message_text: Optional[str] = None) -> None:
        """
        Set or view the custom message for when a realm comes online.
        """
        await self._set_custom_message("online", message_text, ctx)

    @setmessage.command(name="offline")
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def setmessage_offline(self, ctx: commands.Context, *, message_text: Optional[str] = None) -> None:
        """
        Set or view the custom message for when a realm goes offline.
        """
        await self._set_custom_message("offline", message_text, ctx)

    @serverstatus.command()
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def toggle(self, ctx: commands.Context) -> None:
        """
        Toggle the overall server status monitoring on or off.
        When toggled on, an immediate update is sent.
        """
        self.active = not self.active
        await self.config.active.set(self.active)
        state_str = "enabled" if self.active else "disabled"
        await ctx.send(f"Overall server status monitoring has been {state_str}.")
        logger.info("Overall monitoring toggled to %s", state_str)
        if self.active:
            await self.run_status_update()

    async def run_status_update(self) -> None:
        """
        Immediately checks the status for all enabled servers and sends updates
        if the state has changed.
        """
        if not self.status_channel:
            return
        channel = self.bot.get_channel(self.status_channel)
        if not channel:
            return
        for name, (ip, port, last_status, enabled) in list(self.servers.items()):
            if not enabled:
                continue
            new_status = await self.is_server_online(ip, port)
            if last_status is None or new_status != last_status:
                current_status = "online" if new_status else "offline"
                previous_status = "unknown" if last_status is None else ("online" if last_status else "offline")
                timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
                default_message = f"Server {name} is now {current_status}."
                message_template = self.status_messages.get(current_status, default_message)
                try:
                    message = message_template.format(
                        name=name,
                        ip=ip,
                        port=port,
                        status=current_status,
                        prev_status=previous_status,
                        timestamp=timestamp
                    )
                except Exception as e:
                    logger.error("Error formatting message for %s: %s", name, e)
                    message = default_message
                await self._send_status_update(name, channel, message)
                self.servers[name] = (ip, port, new_status, enabled)
                current_servers = await self.config.servers()
                if name in current_servers:
                    current_servers[name]["last_status"] = new_status
                    await self.config.servers.set(current_servers)
                logger.info("Status update for %s: %s", name, current_status)

    @serverstatus.command()
    async def view(self, ctx: commands.Context) -> None:
        """
        View all current settings, including overall monitoring state, the designated
        status channel, monitored servers, and custom messages.
        """
        embed = discord.Embed(title="AGSServerStatus Settings", color=discord.Color.blue())
        embed.add_field(name="Overall Monitoring", value="Enabled" if self.active else "Disabled", inline=False)
        if self.status_channel:
            channel = self.bot.get_channel(self.status_channel)
            embed.add_field(name="Status Channel", value=channel.mention if channel else f"ID: {self.status_channel}", inline=False)
        else:
            embed.add_field(name="Status Channel", value="Not set", inline=False)
        if self.servers:
            server_lines = []
            for name, (ip, port, status, enabled) in self.servers.items():
                status_text = "Unknown"
                if status is True:
                    status_text = "Online"
                elif status is False:
                    status_text = "Offline"
                server_lines.append(f"**{name}** - {ip}:{port} - {status_text} - {'Enabled' if enabled else 'Disabled'}")
            embed.add_field(name="Monitored Servers", value="\n".join(server_lines), inline=False)
        else:
            embed.add_field(name="Monitored Servers", value="No servers have been added.", inline=False)
        if self.status_messages:
            message_lines = []
            for key, msg in self.status_messages.items():
                message_lines.append(f"**{key.title()}**: {msg}")
            embed.add_field(name="Custom Status Messages", value="\n".join(message_lines), inline=False)
        else:
            embed.add_field(name="Custom Status Messages", value="No custom messages set.", inline=False)
        await ctx.send(embed=embed)

    @serverstatus.command()
    async def formatting(self, ctx: commands.Context) -> None:
        """
        Display a list of available placeholders for custom message formatting.
        """
        message = (
            "**Available Placeholders for Custom Messages:**\n\n"
            "**{name}** - Realm name\n"
            "**{ip}** - Server IP address\n"
            "**{port}** - Server port number\n"
            "**{status}** - New status (\"online\" or \"offline\")\n"
            "**{prev_status}** - Previous status (\"online\", \"offline\", or \"unknown\")\n"
            "**{timestamp}** - UTC timestamp (YYYY-MM-DD HH:MM:SS UTC)"
        )
        await ctx.send(message)

    @serverstatus.group(name="instructions", invoke_without_command=True)
    async def instructions(self, ctx: commands.Context) -> None:
        """
        Provides instructions for setting up the health-check and integration.
        """
        await ctx.send_help(ctx.command)

    @instructions.command(name="generic")
    async def instructions_generic(self, ctx: commands.Context) -> None:
        """
        Display generic setup instructions.
        """
        message = (
            "**Generic Setup Instructions**\n\n"
            "1. Confirm that your game server's REST endpoint `/api/health` returns an HTTP 200 status when healthy.\n"
            "2. Test the endpoint using your browser or a tool like curl:\n"
            "   `curl http://<ip>:<port>/api/health`\n"
            "3. Ensure that your firewall (or network security group) is configured to allow inbound connections on the specified port.\n"
            "4. If necessary, update your firewall rules (for example, on Ubuntu use: `sudo ufw allow <port>`).\n"
        )
        await ctx.send(message)

    @instructions.command(name="mmo")
    @commands.is_owner()
    async def instructions_mmo(self, ctx: commands.Context) -> None:
        """
        Display AEGIS Game Studios–specific instructions.
        """
        message = (
            "**AEGIS Game Studios MMO Setup Instructions**\n\n"
            "1. The MMO route for AEGIS Game Studios relies on additional code within your game server.\n"
            "2. Integration is achieved via the 'MMOServerInstance -> RestDatabaseClient'.\n"
            "3. Ensure that the 'Rest Health Server' script is added beneath the 'Rest Database Client'.\n"
            "4. This setup is specifically tailored for AEGIS Game Studios. (Reminder for Five: check integration details.)\n"
        )
        await ctx.send(message)

    @serverstatus.command()
    @commands.is_owner()
    @commands.cooldown(1, 5, commands.BucketType.user)
    async def reset(self, ctx: commands.Context) -> None:
        """
        Reset (wipe) all settings. This will delete all servers, custom messages, and the set channel.
        To confirm, you must type "I agree" within 60 seconds.
        """
        await ctx.send("WARNING: This will wipe ALL settings including servers, custom messages, and the set channel. "
                       "Type 'I agree' within 60 seconds to confirm.")
        try:
            def check(m: discord.Message) -> bool:
                return m.author == ctx.author and m.channel == ctx.channel and m.content.strip().lower() == "i agree"
            await self.bot.wait_for("message", check=check, timeout=60)
        except asyncio.TimeoutError:
            return await ctx.send("Reset cancelled due to timeout.")
        self.servers = {}
        self.last_messages = {}
        self.status_channel = None
        self.status_messages = {}
        self.active = True
        await self.config.servers.set({})
        await self.config.status_channel.set(None)
        await self.config.status_messages.set({})
        await self.config.active.set(True)
        await ctx.send("All settings have been reset.")
        logger.info("All settings have been reset by %s", ctx.author)

def setup(bot: commands.Bot) -> None:
    bot.add_cog(AGSServerStatus(bot))