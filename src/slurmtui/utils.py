import datetime
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
_UPDATE_STATE_FILE = _default_config_dir / "update_check.json"


def get_last_update_check() -> Optional[datetime.date]:
    try:
        with open(_UPDATE_STATE_FILE) as f:
            data = json.load(f)
        return datetime.date.fromisoformat(data["last_check"])
    except Exception:
        return None


def set_last_update_check() -> None:
    _default_config_dir.mkdir(parents=True, exist_ok=True)
    with open(_UPDATE_STATE_FILE, "w") as f:
        json.dump({"last_check": datetime.date.today().isoformat()}, f)


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
    PRIMARY_TEXT_UTIL_CMD: str = field(
        default="tail",
        metadata="Command to use to open the logs file. Should have a placeholder for the file path, e.g. 'less +F {log_path}' or 'tail -f {log_path}'. ",
    )
    SECONDARY_TEXT_UTIL_CMD: str = field(
        default="less",
        metadata="Command to use to open the secondary logs file (e.g. STDERR). Should have a placeholder for the file path, e.g. 'less +F {log_path}' or 'tail -f {log_path}'. ",
    )
    TAIL_LINES: int = field(
        default=10000,
        metadata="Number of lines to show when tailing logs",
    )
    PEEK_LINES: int = field(
        default=100,
        metadata="Number of lines to show in the log peek popup (Space / Ctrl+Space)",
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
    DEBUG_SINFO_JSON_PATH: Optional[str] = field(
        default=None, metadata="JSON file to substitute for sinfo output"
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

        if data.get("PRIMARY_TEXT_UTIL_CMD") is not None and not isinstance(
            data["PRIMARY_TEXT_UTIL_CMD"], str
        ):
            console.print(
                f"Invalid PRIMARY_TEXT_UTIL_CMD '{data.get('PRIMARY_TEXT_UTIL_CMD')}', reverting to default",
                style="yellow",
            )
            data["PRIMARY_TEXT_UTIL_CMD"] = _defaults["PRIMARY_TEXT_UTIL_CMD"]

        if data.get("SECONDARY_TEXT_UTIL_CMD") is not None and not isinstance(
            data["SECONDARY_TEXT_UTIL_CMD"], str
        ):
            console.print(
                f"Invalid SECONDARY_TEXT_UTIL_CMD '{data.get('SECONDARY_TEXT_UTIL_CMD')}', reverting to default",
                style="yellow",
            )
            data["SECONDARY_TEXT_UTIL_CMD"] = _defaults["SECONDARY_TEXT_UTIL_CMD"]

        if (
            data.get("PRIMARY_TEXT_UTIL_CMD") is not None
            and data["PRIMARY_TEXT_UTIL_CMD"] not in ("tail", "less")
            and "{log_path}" not in data["PRIMARY_TEXT_UTIL_CMD"]
        ):
            data["PRIMARY_TEXT_UTIL_CMD"] = (
                f"{data['PRIMARY_TEXT_UTIL_CMD']} {{log_path}}"
            )

        if (
            data.get("SECONDARY_TEXT_UTIL_CMD") is not None
            and data["SECONDARY_TEXT_UTIL_CMD"] not in ("tail", "less")
            and "{log_path}" not in data["SECONDARY_TEXT_UTIL_CMD"]
        ):
            data["SECONDARY_TEXT_UTIL_CMD"] = (
                f"{data['SECONDARY_TEXT_UTIL_CMD']} {{log_path}}"
            )

        for key in ("TAIL_LINES", "PEEK_LINES"):
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
        for key in (
            "DEBUG_SQUEUE_JSON_PATH",
            "DEBUG_SACCT_JSON_PATH",
            "DEBUG_SINFO_JSON_PATH",
        ):
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
