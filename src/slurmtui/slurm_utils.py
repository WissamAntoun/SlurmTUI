import datetime
import json
import os
import subprocess
import sys
from ast import literal_eval
from functools import lru_cache
from typing import Any, Dict

from .utils import SETTINGS, console


def get_fake_squeue(debug_squeue_json_path: str = None):
    if debug_squeue_json_path:
        with open(debug_squeue_json_path, "r") as f:
            return f.read()
    else:
        return json.dumps({"jobs": []})


def get_fake_sacct(debug_sacct_json_path: str = None):
    if debug_sacct_json_path:
        with open(debug_sacct_json_path, "r") as f:
            return f.read()
    else:
        return json.dumps({"jobs": []})


def get_time(time_field) -> str:
    if isinstance(time_field, int):
        return time_field
    elif isinstance(time_field, dict):
        return time_field["number"]
    else:
        return 0


@lru_cache
def get_fake_latest_time(settings: SETTINGS):
    all_jobs = json.loads(get_fake_squeue(settings.DEBUG_SQUEUE_JSON_PATH))["jobs"]
    latest_job = sorted(all_jobs, key=lambda k: get_time(k["submit_time"]))[-1]
    latest_time = get_time(latest_job["submit_time"])
    return latest_time


def get_datetime_now(settings: SETTINGS):
    if settings.MOCK:
        # get the latest time from the fake squeue
        return datetime.datetime.fromtimestamp(get_fake_latest_time(settings))
    else:
        return datetime.datetime.now()


@lru_cache
def get_user():
    return os.getenv("USER", os.getenv("USERNAME", "unknown"))


class CommandNotFoundError(Exception):
    """Exception raised when a command is not found."""

    def __init__(self, message="Command not found."):
        super().__init__()
        self.message = message


def get_running_jobs(
    settings: SETTINGS,
    no_jobs_msg: str = "[yellow]No Jobs are running![/yellow]",
) -> Dict[int, Dict]:
    if settings.MOCK:
        running_jobs = get_fake_squeue(settings.DEBUG_SQUEUE_JSON_PATH)
    else:
        try:
            if settings.CHECK_ALL_JOBS:
                cmd = ["squeue", "--json"]
                if settings.SQUEUE_ARGS:
                    cmd.extend(settings.SQUEUE_ARGS)
                running_jobs = subprocess.check_output(
                    cmd, stderr=subprocess.DEVNULL
                ).decode("utf-8")
            else:
                cmd = ["squeue", "-u", get_user(), "--json"]
                if settings.SQUEUE_ARGS:
                    cmd.extend(settings.SQUEUE_ARGS)
                running_jobs = subprocess.check_output(
                    cmd,
                    stderr=subprocess.DEVNULL,
                ).decode("utf-8")
        except subprocess.CalledProcessError as e:
            console.print(no_jobs_msg)
            return None
        except FileNotFoundError as e:
            console.print(
                "squeue command not found. Please make sure Slurm is installed and configured correctly."
            )
            return CommandNotFoundError("`squeue` command not found")

    squeue_load = json.loads(running_jobs)

    if settings.ACCOUNTS:
        squeue_load["jobs"] = [
            job for job in squeue_load["jobs"] if job["account"] in settings.ACCOUNTS
        ]
    running_jobs = squeue_load["jobs"]
    # sort by job id
    running_jobs = sorted(running_jobs, key=lambda k: k["job_id"])
    running_jobs_dict = {item["job_id"]: item for item in running_jobs}
    return running_jobs_dict


def get_old_jobs(
    settings: SETTINGS,
    start_time: datetime.datetime = None,
    end_time: datetime.datetime = None,
    no_jobs_msg: str = "[yellow]No Jobs are running![/yellow]",
) -> Dict[int, Dict]:
    if settings.MOCK:
        old_jobs = get_fake_sacct(settings.DEBUG_SACCT_JSON_PATH)
    else:
        try:
            start_time = start_time or settings.OLD_JOB_START_TIME or "now-7days"
            end_time = end_time or settings.OLD_JOB_END_TIME or "now"

            cmd = [
                "sacct",
                "--json",
                "--starttime",
                start_time,
                "--endtime",
                end_time,
            ]
            if settings.SQUEUE_ARGS:
                cmd.extend(settings.SQUEUE_ARGS)
            old_jobs = subprocess.check_output(
                cmd,
                stderr=subprocess.DEVNULL,
            ).decode("utf-8")
        except subprocess.CalledProcessError as e:
            console.print(no_jobs_msg)
            return None
        except FileNotFoundError as e:
            console.print(
                "sacct command not found. Please make sure Slurm is installed and configured correctly."
            )
            return CommandNotFoundError("`sacct` command not found")

    sacct_load = json.loads(old_jobs)
    if settings.ACCOUNTS:
        sacct_load["jobs"] = [
            job for job in sacct_load["jobs"] if job["account"] in settings.ACCOUNTS
        ]
    old_jobs = sacct_load["jobs"]
    # sort inversely by job id
    old_jobs = sorted(old_jobs, key=lambda k: k["job_id"], reverse=True)

    old_jobs = {item["job_id"]: item for item in old_jobs}
    return old_jobs


def get_rich_state(state: str):
    if "To be Deleted" in state:
        actual_state = get_rich_state(state.replace("(To be Deleted)", "").strip())
        return f"{actual_state} [red](To be Deleted)[/red]"

    if isinstance(state, str) and state.startswith("["):
        # transform the string into a list of states
        state = literal_eval(state)
        return " ".join([get_rich_state(s) for s in state])
    elif isinstance(state, list):
        return " ".join([get_rich_state(s) for s in state])
    else:
        if state == "RUNNING":
            return "[green]Running[/green]"
        elif state == "PENDING":
            return "[cyan]Pending[cyan]"
        elif state == "COMPLETED":
            return "[blue]Completed[/blue]"
        elif state == "FAILED":
            return "[bright_red]Failed[/bright_red]"
        elif state == "TIMEOUT":
            return "[bright_red]Timeout[/bright_red]"
        elif state == "CANCELLED":
            return "[bright_red]Cancelled[/bright_red]"
        elif "To be Deleted" in state:
            actual_state = get_rich_state(state.replace("(To be Deleted)", "").strip())
            return f"{actual_state} [red](To be Deleted)[/red]"
        else:
            return f"[yellow]{state}[/yellow]"


def check_for_state(job_state: str, state_to_check: str):
    if isinstance(job_state, list):
        return any([check_for_state(s, state_to_check) for s in job_state])
    else:
        return job_state == state_to_check


def format_time_string(time_delta: datetime.timedelta) -> str:
    days = time_delta.days
    hours, remainder = divmod(time_delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    time_string = ""
    if days > 0:
        fraction_of_day = days + round(time_delta.seconds / 86400, 1)
        time_string += f"{fraction_of_day} days"
        return time_string
    if hours > 0:
        fraction_of_hour = round(time_delta.seconds / 3600, 1)
        time_string += f"{fraction_of_hour} hrs"
        return time_string
    if minutes > 0:
        fraction_of_minute = round(time_delta.seconds / 60, 1)
        time_string += f"{fraction_of_minute} mins"
        return time_string
    if seconds > 0:
        if seconds < 1:
            seconds = round(seconds, 1)
        time_string += f"{seconds} secs"
        return time_string
    return time_string


def get_start_and_end_time_string(
    submit_time, start_time, end_time, job_state, settings: SETTINGS
) -> str:
    submit_time = get_time(submit_time)
    start_time = get_time(start_time)
    end_time = get_time(end_time)

    submit_time_string = ""
    start_time_string = ""
    end_time_string = ""

    if submit_time:
        # submit_time_string = str(datetime.datetime.fromtimestamp(submit_time))
        submit_time_string = datetime.datetime.fromtimestamp(submit_time).strftime(
            "%y-%m-%d %H:%M:%S"
        )
    if start_time:
        start_time_string = datetime.datetime.fromtimestamp(start_time).strftime(
            "%y-%m-%d %H:%M:%S"
        )
    if end_time:
        end_time_string = datetime.datetime.fromtimestamp(end_time).strftime(
            "%y-%m-%d %H:%M:%S"
        )

    if not check_for_state(job_state, "PENDING") and end_time:
        time_remaining = datetime.datetime.fromtimestamp(end_time) - get_datetime_now(
            settings
        )
        # check if time remaining is positive
        # TypeError: '>' not supported between instances of 'datetime.timedelta' and 'int'
        if time_remaining.days >= 0:
            formatted_time_remaining = format_time_string(time_remaining)
            end_time_string += (
                " (in " + formatted_time_remaining + ")"
                if formatted_time_remaining
                else " (Instant)"
            )

    if check_for_state(job_state, "PENDING"):
        if start_time:
            time_till_start = datetime.datetime.fromtimestamp(
                start_time
            ) - get_datetime_now(settings)
            if time_till_start.days >= 0:
                formatted_time_till_start = format_time_string(time_till_start)
                start_time_string += (
                    " (in " + formatted_time_till_start + ")"
                    if formatted_time_till_start
                    else " (Instant)"
                )
        elif submit_time:
            time_since_submit = get_datetime_now(
                settings
            ) - datetime.datetime.fromtimestamp(submit_time)
            if time_since_submit.days >= 0:
                formatted_time_since_submit = format_time_string(time_since_submit)
                submit_time_string += (
                    (" (sub. " + formatted_time_since_submit + " ago)")
                    if formatted_time_since_submit
                    else " (just now)"
                )
                start_time_string = submit_time_string
        else:
            pass

    return start_time_string, end_time_string


def check_for_any_job_array(jobs_dict):
    if not jobs_dict or jobs_dict is None:
        return False
    return any(
        [
            (job["array_job_id"]["set"] and job["array_job_id"]["number"] != 0)
            or job["array_task_id"]["set"]
            for job in jobs_dict.values()
        ]
    )


def check_for_any_old_job_array(jobs_dict):
    if not jobs_dict or jobs_dict is None:
        return False
    return any(
        [
            (job["array"]["task_id"]["set"] and job["array"]["task_id"]["number"] != 0)
            for job in jobs_dict.values()
        ]
    )


def check_for_job_state_reason(jobs_dict):
    if not jobs_dict or jobs_dict is None:
        return False
    return any([job["state_reason"] != "None" for job in jobs_dict.values()])


def get_job_resources(job_dict):
    if not job_dict or job_dict is None:
        return {}
    return job_dict.get("job_resources", {}) or {}


class SlurmTUIReturn:
    """Return value for SlurmTUI."""

    def __init__(self, action: str, extra: Dict[str, Any]) -> None:
        self.action = action
        self.extra = extra
