# commands/__init__.py
from redbot.core.i18n import Translator, cog_i18n
from .snapshot import SnapshotCommands
from .leaderboard import LeaderboardCommands
from .icons import IconCommands

_ = Translator("AGSProfiles", __file__)

@cog_i18n(_)
class Commands(
    SnapshotCommands,
    LeaderboardCommands,
    IconCommands,
):
    """Aggregate all AGSProfiles sub-command groups."""
    pass