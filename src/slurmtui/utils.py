import json
import os
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import List, Optional

from rich.console import Console
from textual.theme import BUILTIN_THEMES

console = Console(stderr=True)

_default_config_dir = Path.home() / ".config" / "slurmtui"
SETTINGS_FILE = Path(
    os.environ.get("SLURMTUI_SETTINGS", _default_config_dir / "settings.json")
)


@dataclass
class SETTINGS:
    MOCK: bool = field(default=False, metadata="Use mock data for testing")
    THEME: str = field(
        default="textual-dark",
        metadata=f"Theme name. One of {list(BUILTIN_THEMES.keys())}",
    )
    UPDATE_INTERVAL: int = field(
        default=10,
        metadata="Update interval in seconds (multiplied by 5 when CHECK_ALL_JOBS is enabled)",
    )
    CHECK_ALL_JOBS: bool = field(default=False, metadata="Show all jobs in the queue")
    SQUEUE_ARGS: Optional[List[str]] = field(
        default=None, metadata="Additional squeue arguments (space-separated on input)"
    )
    ACCOUNTS: Optional[List[str]] = field(
        default=None,
        metadata="Account filter list (comma-separated on input). Workaround for squeue --json bug < 24.05.1",
    )
    TAIL_LINES: int = field(
        default=10000,
        metadata="Number of lines to show when tailing logs",
    )
    OLD_JOBS_END_TIME: str = field(
        default="now",
        metadata="End time for old jobs query (sacct time format)",
    )
    OLD_JOBS_START_TIME: str = field(
        default="now-7days",
        metadata="Start time for old jobs query (sacct time format)",
    )
    DEBUG_SQUEUE_JSON_PATH: Optional[str] = field(
        default=None, metadata="JSON file to substitute for squeue output"
    )
    DEBUG_SACCT_JSON_PATH: Optional[str] = field(
        default=None, metadata="JSON file to substitute for sacct output"
    )

    def save(self) -> None:
        SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(SETTINGS_FILE, "w") as f:
            json.dump(asdict(self), f, indent=4)

    def __hash__(self) -> int:
        def _hashable(v):
            return tuple(v) if isinstance(v, list) else v

        return hash(tuple(_hashable(getattr(self, f.name)) for f in fields(self)))

    @classmethod
    def validate(cls, data: dict) -> dict:
        """Strip unknown keys, fill missing with defaults, and coerce types."""
        _defaults = {f.name: f.default for f in fields(cls)}
        known = set(_defaults.keys())

        # Strip unknown keys
        for key in set(data.keys()) - known:
            data.pop(key)

        # Fill missing keys with defaults
        for key in known - set(data.keys()):
            data[key] = _defaults[key]

        # Booleans
        for key in ("MOCK", "CHECK_ALL_JOBS"):
            if not isinstance(data.get(key), bool):
                data[key] = bool(data.get(key, _defaults[key]))

        # Integer with minimum bound
        try:
            data["UPDATE_INTERVAL"] = max(1, int(data["UPDATE_INTERVAL"]))
        except (TypeError, ValueError):
            data["UPDATE_INTERVAL"] = _defaults["UPDATE_INTERVAL"]

        # Theme validity
        if data.get("THEME") not in BUILTIN_THEMES:
            console.print(
                f"Invalid theme '{data.get('THEME')}', reverting to 'textual-dark'",
                style="yellow",
            )
            data["THEME"] = _defaults["THEME"]

        for key in ("TAIL_LINES",):
            try:
                data[key] = max(1, int(data[key]))
            except (TypeError, ValueError):
                data[key] = _defaults[key]

        # Non-empty strings with fallback to defaults
        for key in ("OLD_JOBS_START_TIME", "OLD_JOBS_END_TIME"):
            if not isinstance(data.get(key), str) or not data[key].strip():
                data[key] = _defaults[key]

        # Optional List[str]: keep as list or None, never other types
        for key in ("SQUEUE_ARGS", "ACCOUNTS"):
            v = data.get(key)
            if v is None:
                pass
            elif isinstance(v, list):
                data[key] = [str(x) for x in v] if v else None
            else:
                data[key] = None

        # Optional str
        for key in ("DEBUG_SQUEUE_JSON_PATH", "DEBUG_SACCT_JSON_PATH"):
            v = data.get(key)
            if v is not None and not isinstance(v, str):
                data[key] = None

        return data

    @staticmethod
    def load() -> "SETTINGS":
        if SETTINGS_FILE.exists():
            try:
                with open(SETTINGS_FILE) as f:
                    data = json.load(f)
                data = SETTINGS.validate(data)
                instance = SETTINGS(**data)
                instance.save()
                return instance
            except (json.JSONDecodeError, TypeError) as e:
                console.print(
                    f"Failed to load settings ({e}). Using defaults.", style="red"
                )
        else:
            console.print(
                f"No settings file at {SETTINGS_FILE}. Creating with defaults.",
                style="yellow",
            )
        instance = SETTINGS()
        instance.save()
        return instance

    @staticmethod
    def get_fields_descriptions() -> dict:
        return {f.name: f.metadata for f in fields(SETTINGS)}


settings = SETTINGS.load()
