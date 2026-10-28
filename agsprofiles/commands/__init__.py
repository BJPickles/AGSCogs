# commands/__init__.py
from .snapshot import SnapshotCommands
from .leaderboard import LeaderboardCommands
from .icons import IconCommands

class Commands(
    SnapshotCommands,
    LeaderboardCommands,
    IconCommands,
):
    """Command mixins collected by the single AGSProfiles cog.

    These classes are not separate Cogs and should never be instantiated
    manually. Red/discord.py discovers inherited commands during Cog creation.
    """