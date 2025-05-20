import json
import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import List

from rich.console import Console

console = Console(stderr=True)


SETTINGS_FILE = Path.home() / ".slurmtui_settings.json"
if os.environ.get("SLURMTUI_SETTINGS"):
    SETTINGS_FILE = Path(os.environ["SLURMTUI_SETTINGS"])


@dataclass
class SETTINGS:
    MOCK: bool = field(default=False, metadata="Use mock data for testing")
    UPDATE_INTERVAL: int = field(
        default=10,
        metadata="Update interval in seconds. Will be multiplied by 5 if CHECK_ALL_JOBS is True",
    )
    CHECK_ALL_JOBS: bool = field(default=False, metadata="Check all jobs in the queue")
    SQUEUE_ARGS: List[str] = field(
        default=None, metadata="List of additional arguments to pass to squeue"
    )
    ACCOUNTS: List[str] = field(
        default=None,
        metadata="List of accounts to filter the jobs by since squeue --json has a bug on version < 24.05.1.",
    )
    OLD_JOBS_END_TIME: str = field(
        default="now",
        metadata="End time for the old jobs query. now[{+|-}count[seconds(default)|minutes|hours|days|weeks]] or check on https://slurm.schedmd.com/sacct.html for more info",
    )
    OLD_JOBS_START_TIME: str = field(
        default="now-7days",
        metadata="Start time for the old jobs query. now[{+|-}count[seconds(default)|minutes|hours|days|weeks]] or check on https://slurm.schedmd.com/sacct.html for more info",
    )
    DEBUG_SQUEUE_JSON_PATH: str = field(
        default=None, metadata="Path to the debug squeue json file"
    )
    DEBUG_SACCT_JSON_PATH: str = field(
        default=None, metadata="Path to the debug sacct json file"
    )

    def __post_init__(self):
        if self.CHECK_ALL_JOBS:
            self.UPDATE_INTERVAL = self.UPDATE_INTERVAL * 5

    def save(self):
        with open(SETTINGS_FILE, "w") as f:
            json.dump(self.__dict__, f, indent=4)

    def __hash__(self):
        return hash(tuple(getattr(self, field.name) for field in fields(self)))

    @classmethod
    def validate(cls, settings):
        # check if all the keys are present
        missing_keys = set(cls.__dataclass_fields__.keys()) - set(settings.keys())
        if missing_keys:
            console.print(
                f"Settings file is missing the following keys: {missing_keys}",
                style="red",
            )
            # replace missing keys with default values
            for key in missing_keys:
                settings[key] = getattr(cls, key)
                console.print(
                    f"Replacing missing key {key} with default value: {settings[key]}",
                )
            # save the updated settings
            cls(**settings).save()
        return settings

    @staticmethod
    def load():
        if SETTINGS_FILE.exists():
            with open(SETTINGS_FILE, "r") as f:
                settings = json.load(f)
                if "DEBUG_SQUEUE_JSON_PATH" not in settings:
                    settings["DEBUG_SQUEUE_JSON_PATH"] = settings.get(
                        "FAKE_QUEUE_JSON_PATH", None
                    )
                    _ = settings.pop("FAKE_QUEUE_JSON_PATH", None)
                    settings = SETTINGS.validate(settings)
                    settings = SETTINGS(**settings)
                    settings.save()
                else:
                    settings = SETTINGS.validate(settings)
                    settings = SETTINGS(**settings)
                return settings
        else:
            console.print(
                "Settings file not found. Using default settingsuration."
                f"\nCreating a settingsuration file at {SETTINGS_FILE}",
                style="yellow",
            )
            SETTINGS().save()
            return SETTINGS()

    @staticmethod
    def get_fields_descriptions():
        return {field.name: field.metadata for field in fields(SETTINGS)}


global settings

settings = SETTINGS().load()