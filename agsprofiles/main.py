# main.py
import logging
from pathlib import Path
from redbot.core import commands
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path

from .models import AGSConfig
from .commands.snapshot import SnapshotCommands
from .commands.leaderboard import LeaderboardCommands
from .commands.icons import IconCommands
from .tasks import daily_snapshot_loop, weekly_snapshot_loop

log = logging.getLogger("red.agsprofiles")

class AGSProfiles(
    SnapshotCommands,
    LeaderboardCommands,
    IconCommands,
    commands.Cog
):
    """
    AGSProfiles cog: captures daily, weekly & manual snapshots of LevelUp stats
    and provides per-year/master leaderboards and role→icon mappings.
    """

    def __init__(self, bot: Red):
        super().__init__()
        self.bot: Red = bot
        # Load or create config file
        self.cog_path: Path = cog_data_path(self)
        self.cog_path.mkdir(exist_ok=True)
        self.db_file: Path = self.cog_path / "AGSProfiles.json"
        self.config: AGSConfig = AGSConfig()
        self.load()

    async def cog_load(self):
        # start periodic snapshot loops
        daily_snapshot_loop.start(self)
        weekly_snapshot_loop.start(self)
        log.info("AGSProfiles loaded and snapshot loops started")

    async def cog_unload(self):
        # stop periodic snapshot loops
        daily_snapshot_loop.cancel()
        weekly_snapshot_loop.cancel()
        log.info("AGSProfiles unloaded and snapshot loops stopped")

    def load(self) -> None:
        """Load AGSProfiles config from disk."""
        if not self.db_file.exists():
            return
        try:
            text = self.db_file.read_text(encoding="utf-8")
            self.config = AGSConfig.model_validate_json(text)
            log.info("AGSProfiles config loaded")
        except Exception as e:
            log.error("Failed to load AGSProfiles config", exc_info=e)

    def save(self) -> None:
        """Save AGSProfiles config to disk."""
        try:
            json_data = self.config.model_dump_json(indent=2)
            # Atomic write
            tmp = self.db_file.with_suffix(".tmp")
            tmp.write_text(json_data, encoding="utf-8")
            tmp.replace(self.db_file)
            log.debug("AGSProfiles config saved")
        except Exception as e:
            log.error("Failed to save AGSProfiles config", exc_info=e)