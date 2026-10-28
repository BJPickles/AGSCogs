"""AUD-005: AGSProfiles Python 3.9+ import and persisted-schema compatibility.

Run in a Python version matrix with Pydantic 2 installed:
    python -m unittest discover -s tests -p test_agsprofiles_python_compat.py -v

The model module is loaded directly, avoiding AUD-006's undeclared external
LevelUp/Red dependencies. No production Discord integration is simulated.
"""
import ast
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sys
import typing
import unittest

ROOT = Path(__file__).resolve().parents[1]
MODELS_PATH = ROOT / "agsprofiles" / "models.py"
COMMON_PATH = ROOT / "agsprofiles" / "common.py"

spec = importlib.util.spec_from_file_location("agsprofiles_compat_models", MODELS_PATH)
models = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = models
spec.loader.exec_module(models)


class PythonCompatibilityTests(unittest.TestCase):
    def test_snapshot_enum_has_original_string_values(self):
        self.assertEqual(
            [kind.value for kind in models.SnapshotType],
            ["MANUAL", "DAILY", "WEEKLY", "YEAR_END"],
        )
        self.assertEqual(str(models.SnapshotType.DAILY), "DAILY")
        self.assertEqual(models.SnapshotType("YEAR_END"), models.SnapshotType.YEAR_END)
        self.assertIsInstance(models.SnapshotType.MANUAL, str)
        self.assertEqual(json.dumps({"type": models.SnapshotType.DAILY}), '{"type": "DAILY"}')

    def test_snapshot_round_trip_preserves_existing_wire_format(self):
        stats = models.Stats(
            xp=123.5, level=5, messages=25, voicetime=3.5,
            stars=2, balance=100, rank=4,
        )
        snap = models.Snapshot(
            id="id-1",
            name="2026-10-28_DAILY",
            created=datetime(2026, 10, 28, tzinfo=timezone.utc),
            type=models.SnapshotType.DAILY,
            data={42: stats},
        )
        payload = json.loads(snap.model_dump_json())
        self.assertEqual(payload["type"], "DAILY")
        self.assertEqual(payload["data"]["42"]["xp"], 123.5)
        loaded = models.Snapshot.model_validate_json(json.dumps(payload))
        self.assertEqual(loaded.type, models.SnapshotType.DAILY)
        self.assertEqual(loaded.data[42].level, 5)
        # Existing persistence writes through model_dump() + json.dumps(default=str).
        legacy = json.loads(json.dumps(snap.model_dump(), default=str))
        self.assertEqual(legacy["type"], "DAILY")
        self.assertEqual(models.Snapshot.model_validate(legacy).type, models.SnapshotType.DAILY)

    def test_year_data_optional_datetime_and_config_serialization(self):
        old = models.YearData.model_validate(
            {"finalized": False, "finalized_at": None, "users": {}}
        )
        self.assertIsNone(old.finalized_at)
        now = datetime(2026, 10, 28, 12, 30, tzinfo=timezone.utc)
        archived = models.YearData(finalized=True, finalized_at=now)
        self.assertEqual(
            models.YearData.model_validate_json(archived.model_dump_json()).finalized_at,
            now,
        )
        cfg = models.AGSConfig(guilds={123: models.GuildConfig(years={2026: archived})})
        decoded = models.AGSConfig.model_validate_json(cfg.model_dump_json())
        self.assertEqual(decoded.guilds[123].years[2026].finalized_at, now)

    def test_no_unsupported_runtime_union_annotations(self):
        # 'T | None' can be parsed by Python 3.9 but fails when executed as a
        # runtime annotation. Inspect both impacted modules, including common.py
        # which cannot yet be imported without the optional LevelUp package.
        for path in (MODELS_PATH, COMMON_PATH):
            with self.subTest(path=path.name):
                module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
                for node in ast.walk(module):
                    annotations = []
                    if isinstance(node, (ast.AnnAssign,)):
                        annotations.append(node.annotation)
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        annotations.extend(
                            arg.annotation for arg in (
                                node.args.posonlyargs + node.args.args
                                + node.args.kwonlyargs + ([node.args.vararg] if node.args.vararg else [])
                                + ([node.args.kwarg] if node.args.kwarg else [])
                            ) if arg.annotation is not None
                        )
                        if node.returns is not None:
                            annotations.append(node.returns)
                    for annotation in annotations:
                        self.assertFalse(
                            any(isinstance(n, ast.BinOp) and isinstance(n.op, ast.BitOr)
                                for n in ast.walk(annotation)),
                            f"PEP 604 runtime union in {path.name}:{node.lineno}",
                        )


if __name__ == "__main__":
    unittest.main()
