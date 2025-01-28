import os

from rich.console import Console

console = Console()

MOCK = os.getenv("MOCK", "False").lower() == "true"
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL", "10"))
CHECK_ALL_JOBS = os.getenv("ALL_JOBS", "False").lower() == "true"
if CHECK_ALL_JOBS:
    UPDATE_INTERVAL = UPDATE_INTERVAL * 5
