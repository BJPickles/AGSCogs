# AGS Profiles — installation and dependency requirements

AGS Profiles stores LevelUp year-end archives, manual snapshots, leaderboards
and optional role icons. It **depends on Vertyco's LevelUp cog** and on Python
libraries Pydantic 2 and Pillow.

## Installation order

1. Enable Red's Downloader cog, if not already enabled: `[p]load downloader`.
2. Add Vertyco's official repository (if not already installed):
   `[p]repo add vrt-cogs https://github.com/vertyco/vrt-cogs`.
3. Install and load LevelUp:
   `[p]cog install vrt-cogs levelup` then `[p]load levelup`.
4. Add this repository if needed:
   `[p]repo add AGSCogs https://github.com/BJPickles/AGSCogs`.
5. Install AGS Profiles: `[p]cog install AGSCogs agsprofiles`.
6. **After LevelUp is loaded**, load AGS Profiles:
   `[p]load agsprofiles`.

Red Downloader installs the Python packages listed in AGS Profiles'
`info.json` (`pydantic>=2.11,<2.12` and `Pillow>=9.5`). These are deliberately
aligned to the Pydantic 2.11 constraint advertised by the upstream LevelUp
cog at the time of this fix. The `required_cogs` field documents the
dependency on LevelUp, but **does not install or load LevelUp automatically**.
Do not `pip install levelup` from PyPI as a substitute for installing the
Red cog from Vertyco's repository.

## Missing or incompatible LevelUp

AGS Profiles now fails setup **with an actionable error** if LevelUp is not
loaded, the required formatter cannot be imported, or that formatter has a
different call signature. It no longer crashes on the LevelUp import simply
because the AGS Profiles package was imported before LevelUp.

The verified upstream `levelup.common.formatter.get_user_position` API takes
`guild`, `conf`, `lbtype`, `target_user` and `key`. Rank collection
now passes these arguments to the installed LevelUp formatter rather than
using the outdated keyword set that caused ranks to fall back to zero.

## Operational notes

- AGS Profiles requires Red 3.5+ and its metadata advertises Python 3.9+.
- The Pydantic 2 model schema and existing snapshot disk format are unchanged
  by this dependency fix.
- If LevelUp is unloaded while AGS Profiles is running, live snapshot/stat
  operations may return no data. Reload LevelUp first, then AGS Profiles.
- LevelUp is a separate project with its own API and update cycle. Upstream
  API changes beyond the checked formatter signature can still affect this
  integration, so test against your installed LevelUp version.
- Tests for this fix:
  `python -m unittest discover -s tests -p 'test_agsprofiles_dependencies.py' -v`.
  The isolated contract suite mocks LevelUp and **does not replace** a
  live Red/LevelUp integration smoke test.
