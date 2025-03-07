import discord
import aiohttp
import asyncio
from datetime import datetime
from redbot.core import commands, Config
from discord.ext import tasks

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

    def __init__(self, bot):
        self.bot = bot
        # Set up persistent config with a unique identifier.
        self.config = Config.get_conf(self, identifier=123456789012345678, force_registration=True)
        default_global = {
            "servers": {},          # Stored as {name: {"ip": ip, "port": port}}
            "status_channel": None, # Stored as channel ID
            "status_messages": {}   # Stored as { "online": message, "offline": message }
        }
        self.config.register_global(**default_global)
        # In-memory storage (last_status is not persisted)
        self.servers = {}        # mapping: realm name -> (ip, port, last_status)
        self.status_channel = None
        self.status_messages = {}
        # Load persistent settings.
        self.bot.loop.create_task(self.initialize_settings())
        # Start the periodic status-check loop.
        self.check_status_task.start()

    async def initialize_settings(self):
        data = await self.config.all()
        servers = data.get("servers", {})
        for name, details in servers.items():
            # Persisted servers do not store last_status; initialize it as None.
            self.servers[name] = (details.get("ip"), details.get("port"), None)
        self.status_channel = data.get("status_channel")
        self.status_messages = data.get("status_messages", {})

    def cog_unload(self):
        self.check_status_task.cancel()

    async def is_server_online(self, ip: str, port: int, timeout: int = 5) -> bool:
        """
        Check the health endpoint of the server.
        Expects the server to provide health info at http://<ip>:<port>/api/health.
        Returns True if the GET request returns status 200, else False.
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
          • setmessage  - Set or view a custom status update message.
          • view        - View all current settings.
          • formatting  - Display available placeholders.
          • instructions- Show setup instructions.
          • reset       - Reset (wipe) all settings (bot owner only).
        
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
        # Update persistent config
        current_servers = await self.config.servers()
        current_servers[name] = {"ip": ip, "port": port}
        await self.config.servers.set(current_servers)
        await ctx.send(f"Added server {name} at {ip}:{port}.")

    @serverstatus.command()
    async def remove(self, ctx, name: str):
        """Remove a monitored server."""
        if name in self.servers:
            del self.servers[name]
            # Update persistent config
            current_servers = await self.config.servers()
            if name in current_servers:
                del current_servers[name]
                await self.config.servers.set(current_servers)
            await ctx.send(f"Removed server {name}.")
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
            lines.append(f"{name} - {ip}:{port} - {status_text}")
        message = "**Monitored Servers:**\n" + "\n".join(lines)
        await ctx.send(message)

    @serverstatus.command()
    async def setchannel(self, ctx, channel: discord.TextChannel):
        """Set the channel where status update messages will be posted."""
        self.status_channel = channel.id
        await self.config.status_channel.set(channel.id)
        await ctx.send(f"Status updates will be posted in {channel.mention}.")

    @serverstatus.group(name="setmessage", invoke_without_command=True)
    async def setmessage(self, ctx):
        """
        Set or view a custom message for status changes.
        
        When the realm's status changes, the saved message for "online" or "offline" is used.
        You can use the following placeholders in your message:
          • {name}        - Realm name
          • {ip}          - Server IP address
          • {port}        - Server port
          • {status}      - New status ("online" or "offline")
          • {prev_status} - Previous status ("online", "offline", or "unknown")
          • {timestamp}   - UTC timestamp (YYYY-MM-DD HH:MM:SS UTC)
        
        Formatting examples:
          • "Server {name} is now {status}." 
          • "Alert: {name} (IP: {ip}) switched to {status} at {timestamp} (was {prev_status})."
        
        To set a custom message, reply to the desired message with one of:
          [p]serverstatus setmessage online
          [p]serverstatus setmessage offline
        
        If you do not reply, the current message (if any) will be displayed.
        """
        await ctx.send_help(ctx.command)

    @setmessage.command(name="online")
    async def setmessage_online(self, ctx):
        """
        Set or view the custom message for when a realm comes online.
        
        Example formatting: "Server {name} is now {status}!"
        
        If you reply to a message with this command, its contents will be saved as the custom online message.
        If no reply is provided, the current custom message for online status is displayed.
        """
        if ctx.message.reference:
            ref_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
            self.status_messages["online"] = ref_msg.content
            current_messages = await self.config.status_messages()
            current_messages["online"] = ref_msg.content
            await self.config.status_messages.set(current_messages)
            await ctx.send("Custom message for online status updated.")
        else:
            current = self.status_messages.get("online")
            if current:
                await ctx.send(f"Current custom message for online: {current}")
            else:
                await ctx.send("No custom message set for online status.")

    @setmessage.command(name="offline")
    async def setmessage_offline(self, ctx):
        """
        Set or view the custom message for when a realm goes offline.
        
        Example formatting: "Alert: {name} is now {status} (was {prev_status})."
        
        If you reply to a message with this command, its contents will be saved as the custom offline message.
        If no reply is provided, the current custom message for offline status is displayed.
        """
        if ctx.message.reference:
            ref_msg = await ctx.channel.fetch_message(ctx.message.reference.message_id)
            self.status_messages["offline"] = ref_msg.content
            current_messages = await self.config.status_messages()
            current_messages["offline"] = ref_msg.content
            await self.config.status_messages.set(current_messages)
            await ctx.send("Custom message for offline status updated.")
        else:
            current = self.status_messages.get("offline")
            if current:
                await ctx.send(f"Current custom message for offline: {current}")
            else:
                await ctx.send("No custom message set for offline status.")

    @serverstatus.command()
    async def view(self, ctx):
        """
        View all current settings.
        
        This displays:
          • The designated channel for status updates.
          • The list of monitored servers (with IP, port, and current status).
          • Any custom messages that have been set.
        """
        embed = discord.Embed(title="AGSServerStatus Settings", color=discord.Color.blue())
        # Display the status channel
        if self.status_channel:
            channel = self.bot.get_channel(self.status_channel)
            embed.add_field(name="Status Channel", value=channel.mention if channel else f"ID: {self.status_channel}", inline=False)
        else:
            embed.add_field(name="Status Channel", value="Not set", inline=False)
        # Display monitored servers
        if self.servers:
            server_lines = []
            for name, (ip, port, status) in self.servers.items():
                status_text = "Unknown"
                if status is True:
                    status_text = "Online"
                elif status is False:
                    status_text = "Offline"
                server_lines.append(f"**{name}** - {ip}:{port} - {status_text}")
            embed.add_field(name="Monitored Servers", value="\n".join(server_lines), inline=False)
        else:
            embed.add_field(name="Monitored Servers", value="No servers have been added.", inline=False)
        # Display custom status messages
        if self.status_messages:
            message_lines = []
            for key, msg in self.status_messages.items():
                message_lines.append(f"**{key.title()}**: {msg}")
            embed.add_field(name="Custom Status Messages", value="\n".join(message_lines), inline=False)
        else:
            embed.add_field(name="Custom Status Messages", value="No custom messages set.", inline=False)
        await ctx.send(embed=embed)

    @serverstatus.command()
    async def formatting(self, ctx):
        """
        Display a list of available placeholders for custom message formatting.
        
        Placeholders:
          • {name}        - Realm name
          • {ip}          - Server IP address
          • {port}        - Server port number
          • {status}      - New status ("online" or "offline")
          • {prev_status} - Previous status ("online", "offline", or "unknown")
          • {timestamp}   - UTC timestamp (YYYY-MM-DD HH:MM:SS UTC)
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
        
        Ensure that your REST endpoint at /api/health returns an HTTP 200 response when your server is healthy,
        and that your firewall is configured to allow incoming connections on your chosen port.
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
    async def instructions_mmo(self, ctx):
        """
        Display AEGIS Game Studios–specific instructions.
        
        This MMO setup relies on additional code within your game server. It is connected to the 
        'MMOServerInstance -> RestDatabaseClient' where the 'Rest Health Server' script has been added 
        beneath the 'Rest Database Client'. (Reminder for Five, creator of AEGIS Kingdoms.)
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
    async def reset(self, ctx):
        """
        Reset (wipe) all settings. This will delete all servers, custom messages, and the set channel.
        
        To confirm, you must type "I agree" within 60 seconds.
        """
        await ctx.send("WARNING: This will wipe ALL settings including servers, custom messages, and the set channel. "
                       "Type 'I agree' within 60 seconds to confirm.")
        try:
            def check(m):
                return (m.author == ctx.author and m.channel == ctx.channel 
                        and m.content.strip().lower() == "i agree")
            confirmation = await self.bot.wait_for("message", check=check, timeout=60)
        except asyncio.TimeoutError:
            return await ctx.send("Reset cancelled due to timeout.")
        # Reset all in-memory values.
        self.servers = {}
        self.status_channel = None
        self.status_messages = {}
        # Reset persistent config values.
        await self.config.servers.set({})
        await self.config.status_channel.set(None)
        await self.config.status_messages.set({})
        await ctx.send("All settings have been reset.")

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
                    message = default_message
                await channel.send(message)
