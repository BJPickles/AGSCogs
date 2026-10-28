"""AUD-012 regression tests for Discord-safe AGS Activity listing.

Execute the production pagination helpers and list_activities command method
from its AST with fake Discord embeds and guild configuration. No Red/Discord
bot or network access is needed.
"""

import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "agsactivity" / "agsactivities.py"
TREE = ast.parse(SOURCE.read_text(encoding="utf-8"), filename=str(SOURCE))
COG = next(n for n in TREE.body if isinstance(n, ast.ClassDef) and n.name == "Activities")
METHOD = next(
    n for n in COG.body
    if isinstance(n, ast.AsyncFunctionDef) and n.name == "list_activities"
)
METHOD.decorator_list = []
HELPERS = [
    n for n in TREE.body
    if isinstance(n, ast.FunctionDef)
    and n.name in {"_truncate_embed_text", "paginate_activity_fields"}
]
assert len(HELPERS) == 2


class FakeEmbed:
    def __init__(self, *, title, color):
        self.title = title
        self.color = color
        self.fields = []
        self.footer = ""

    def add_field(self, *, name, value, inline):
        assert len(self.fields) < 25, "Discord rejects more than 25 fields"
        assert len(name) <= 256
        assert len(value) <= 1024
        self.fields.append((name, value, inline))

    def set_footer(self, *, text):
        self.footer = text


namespace = {
    "ACTIVITY_LIST_PAGE_FIELDS": 20,
    "ACTIVITY_LIST_PAGE_CHAR_BUDGET": 5500,
    "discord": SimpleNamespace(
        Embed=FakeEmbed, Color=SimpleNamespace(green=lambda: 2)
    ),
}
test_module = ast.fix_missing_locations(ast.Module(
    body=[
        *HELPERS,
        ast.ClassDef(
            name="ListHarness", bases=[], keywords=[], decorator_list=[],
            body=[METHOD],
        ),
    ],
    type_ignores=[],
))
exec(compile(test_module, str(SOURCE), "exec"), namespace)
paginate_activity_fields = namespace["paginate_activity_fields"]
ListHarness = namespace["ListHarness"]


def sample_instances(count, *, title="Game night"):
    return {
        f"activity-{i:04d}": {
            "title": title,
            "owner_id": 9000 + i,
            "status": "OPEN",
            "scheduled_time": None,
        }
        for i in range(count)
    }


class PageHelperTests(unittest.TestCase):
    def test_20_items_fit_one_page_and_21_requires_two(self):
        for count, expected in ((1, 1), (20, 1), (21, 2), (25, 2), (50, 3), (100, 5)):
            with self.subTest(count=count):
                fields = [(f"id-{i}", "Owner: @user\nStatus: OPEN") for i in range(count)]
                pages = paginate_activity_fields(fields)
                self.assertEqual(len(pages), expected)
                self.assertEqual(sum(map(len, pages)), count)
                self.assertTrue(all(1 <= len(page) <= 20 for page in pages))

    def test_empty_items_have_no_pages(self):
        self.assertEqual(paginate_activity_fields([]), [])

    def test_oversized_titles_and_values_are_clamped(self):
        fields = [("n" * 2000, "v" * 2000)]
        page = paginate_activity_fields(fields)[0][0]
        self.assertEqual(len(page[0]), 256)
        self.assertEqual(len(page[1]), 1024)
        self.assertTrue(page[0].endswith("…"))
        self.assertTrue(page[1].endswith("…"))

    def test_character_budget_splits_even_if_field_count_is_small(self):
        fields = [("A" * 256, "B" * 1024) for _ in range(13)]
        pages = paginate_activity_fields(fields)
        self.assertGreater(len(pages), 1)
        self.assertEqual(sum(map(len, pages)), 13)
        self.assertTrue(all(
            sum(len(name) + len(value) for name, value in page) <= 5500
            for page in pages
        ))


class ListCommandTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cog = ListHarness()
        self.config = SimpleNamespace(instances=AsyncMock(return_value=sample_instances(5)))
        self.cog.config = SimpleNamespace(guild=Mock(return_value=self.config))
        self.guild = SimpleNamespace(
            id=1,
            get_member=lambda uid: SimpleNamespace(mention=f"<@{uid}>"),
        )
        self.ctx = SimpleNamespace(guild=self.guild, send=AsyncMock())

    async def render(self, count, page=1, title="Game night"):
        self.config.instances.return_value = sample_instances(count, title=title)
        self.ctx.send.reset_mock()
        await self.cog.list_activities(self.ctx, page=page)
        self.ctx.send.assert_awaited_once()
        return self.ctx.send.await_args.kwargs.get("embed")

    async def test_default_page_and_footer(self):
        embed = await self.render(52)
        self.assertEqual(len(embed.fields), 20)
        self.assertIn("Page 1/3", embed.footer)
        self.assertIn("52 activities", embed.footer)
        self.assertEqual(embed.fields[0][0], "activity-0000: Game night")
        self.assertEqual(embed.fields[-1][0], "activity-0019: Game night")

    async def test_page_navigation_keeps_all_activities_without_repeats(self):
        names = []
        for page, expected_count in [(1, 20), (2, 20), (3, 12)]:
            embed = await self.render(52, page=page)
            self.assertEqual(len(embed.fields), expected_count)
            names.extend(name for name, _, _ in embed.fields)
        self.assertEqual(len(names), 52)
        self.assertEqual(len(set(names)), 52)
        self.assertEqual(names[20], "activity-0020: Game night")

    async def test_empty_list_replies_without_embed(self):
        embed = await self.render(0)
        self.assertIsNone(embed)
        self.assertEqual(self.ctx.send.await_args.args[0], "No activities found.")

    async def test_rejects_negative_zero_and_too_large_pages(self):
        for page in (-3, 0, 3):
            with self.subTest(page=page):
                embed = await self.render(25, page=page)
                self.assertIsNone(embed)
                self.assertIn("between 1 and 2", self.ctx.send.await_args.args[0])

    async def test_exact_25_field_boundary_never_breaks_embed(self):
        first = await self.render(25, page=1)
        second = await self.render(25, page=2)
        self.assertEqual((len(first.fields), len(second.fields)), (20, 5))

    async def test_large_titles_and_status_do_not_break_discord_limits(self):
        instances = sample_instances(47, title="X" * 2500)
        for item in instances.values():
            item["status"] = "S" * 1800
        self.config.instances.return_value = instances
        all_names = []
        page_count = len(paginate_activity_fields([
            (f"{iid}: {inst['title']}",
             f"Owner: <@{inst['owner_id']}>\nStatus: {inst['status']}")
            for iid, inst in instances.items()
        ]))
        for page in range(1, page_count + 1):
            await self.cog.list_activities(self.ctx, page=page)
            embed = self.ctx.send.await_args.kwargs["embed"]
            self.assertTrue(len(embed.fields) <= 20)
            self.assertLessEqual(
                len(embed.title) + len(embed.footer)
                + sum(len(name) + len(value) for name, value, _ in embed.fields),
                6000,
            )
            for name, value, _ in embed.fields:
                self.assertLessEqual(len(name), 256)
                self.assertLessEqual(len(value), 1024)
                all_names.append(name)
        self.assertEqual(len(all_names), 47)

    async def test_scheduled_start_indicator_still_present(self):
        entries = sample_instances(1)
        entries["activity-0000"]["status"] = "SCHEDULED"
        entries["activity-0000"]["scheduled_time"] = 1700000000
        self.config.instances.return_value = entries
        await self.cog.list_activities(self.ctx)
        embed = self.ctx.send.await_args.kwargs["embed"]
        self.assertIn("starts <t:1700000000:R>", embed.fields[0][1])
        self.assertIn("Owner: <@9000>", embed.fields[0][1])


if __name__ == "__main__":
    unittest.main()
