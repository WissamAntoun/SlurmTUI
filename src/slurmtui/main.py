import argparse
import datetime
import json
import os
import socket
import subprocess
import sys
from collections import OrderedDict
from typing import Any, Coroutine, Dict

from packaging.version import Version
from rich import print_json
from rich.syntax import Syntax
from rich.text import Text
from textual import log
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid, Horizontal, Vertical
from textual.coordinate import Coordinate
from textual.css.query import NoMatches
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    RichLog,
    Static,
)

from .slurm_utils import (
    check_for_any_job_array,
    check_for_job_state_reason,
    check_for_state,
    get_rich_state,
    get_running_jobs,
    get_start_and_end_time_string,
)
from .utils import SETTINGS, console

global settings

settings = SETTINGS().load()


class SettingsScreen(Screen):
    BINDINGS = [
        # I couldn't disable the default bindings, so I just overrode them
        Binding("c", "do_nothing", "", False),
        Binding("l", "do_nothing", "", False),
        Binding("e", "do_nothing", "", False),
        Binding("d", "do_nothing", "", False),
        Binding("i", "do_nothing", "", False),
        Binding("ctrl+s", "save_settings", "Save Settings", key_display="Ctrl+S"),
        Binding("escape", "app.pop_screen", "Go Back", key_display="Esc"),
        Binding("b", "app.pop_screen", "Go Back", False),
        Binding("backspace", "app.pop_screen", "Go Back", False),
        Binding("q", "app.quit", "Quit", key_display="Q"),
    ]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app.title = "SlurmTUI Settings"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True, name="SlurmTUI Settings", id="settings_header")
        settings_dict = settings.__dict__
        with Vertical(classes="settings_row"):
            for key, value in settings_dict.items():
                _value = str(value)
                # add a Label and an input field for each settings variable
                with Vertical():
                    key_str = f"{key} ({SETTINGS.get_fields_descriptions()[key]})"
                    yield Static(key_str, id=f"label_{key}")
                    yield Input(_value, id=f"input_{key}")
        yield Footer()

    def action_save_settings(self) -> None:
        settings_dict = settings.__dict__
        for key, value in settings_dict.items():
            try:
                new_value = self.query_one(f"#input_{key}").value
                if new_value == "None" or new_value == "" or new_value == "null":
                    new_value = None
                elif isinstance(value, int) and new_value.isdigit():
                    new_value = int(new_value)
                elif isinstance(value, bool):
                    new_value = new_value.lower() == "true"
                setattr(settings, key, new_value)
            except NoMatches:
                pass
        settings.save()
        self.notify("Settings saved")
        self.dismiss()

    def action_do_nothing(self) -> None:
        pass


class InfoScreen(Screen[str]):
    BINDINGS = [
        # I couldn't disable the default bindings, so I just overrode them
        Binding("c", "do_nothing", "", False),
        Binding("l", "do_nothing", "", False),
        Binding("e", "do_nothing", "", False),
        Binding("d", "do_nothing", "", False),
        Binding("i", "do_nothing", "", False),
        Binding("s", "print_cli", "Print in CLI", key_display="S"),
        Binding("escape", "app.pop_screen", "Go Back", key_display="Esc"),
        Binding("b", "app.pop_screen", "Go Back", False),
        Binding("backspace", "app.pop_screen", "Go Back", False),
        Binding("q", "app.quit", "Quit", key_display="Q"),
    ]

    def __init__(self, info: Dict[str, Any], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.info = info
        self.app.title = f"slurm Job Info: {self.info['job_id']}"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True, name="slurm Job Info", id="info_header")
        yield RichLog(
            highlight=True, markup=False, auto_scroll=False, wrap=True, id="info_text"
        )
        yield Footer()

    def on_mount(self) -> None:
        rich_text = self.query_one(RichLog)
        rich_text.write(
            Syntax(json.dumps(self.info, indent=4), "json", word_wrap=True),
            shrink=False,
        )

    def action_print_cli(self) -> None:
        self.dismiss(json.dumps(self.info, indent=4))

    def action_do_nothing(self) -> None:
        pass


class ConfirmScreen(Screen[bool]):
    """Screen with confirm a dialog."""

    BINDINGS = [
        Binding("y", "yes", "Yes", key_display="Y"),
        Binding("n", "no", "No", key_display="N"),
        Binding("c", "do_nothing", "", False),
        Binding("l", "do_nothing", "", False),
        Binding("e", "do_nothing", "", False),
        Binding("d", "do_nothing", "", False),
        Binding("i", "do_nothing", "", False),
        Binding("escape", "app.pop_screen", "Go Back", key_display="Esc"),
        Binding("s", "do_nothing", "", False),
        Binding("b", "app.pop_screen", "Go Back", False),
        Binding("backspace", "app.pop_screen", "Go Back", False),
        Binding("q", "app.quit", "Quit", key_display="Q"),
        Binding("left", "app.focus_next", "Focus Next", show=False),
        Binding("right", "app.focus_previous", "Focus Previous", show=False),
    ]

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.message = message

    def compose(self) -> ComposeResult:
        yield Grid(
            Label(self.message, id="question"),
            Button("Yes", variant="error", id="yes"),
            Button("No", variant="primary", id="no"),
            id="confirm_dialog",
        )
        yield Footer()

    def action_do_nothing(self) -> None:
        pass

    def action_yes(self) -> None:
        self.dismiss(True)

    def action_no(self) -> None:
        self.dismiss(False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "yes")


class SlurmTUIReturn:
    """Return value for SlurmTUI."""

    def __init__(self, action: str, extra: Dict[str, Any]) -> None:
        self.action = action
        self.extra = extra


class ColumnManager:
    def __init__(self, initial_columns: Dict[str, bool]):
        # OrderedDict to store column names and their enabled status
        self.columns = initial_columns.copy()

    def enable_column(self, column_name):
        """Enable a column by name."""
        if column_name in self.columns:
            self.columns[column_name] = True

    def disable_column(self, column_name):
        """Disable a column by name."""
        if column_name in self.columns:
            self.columns[column_name] = False

    def get_enabled_columns(self):
        """Return a list of enabled column names in order."""
        return [col for col, enabled in self.columns.items() if enabled]

    def get_all_columns(self):
        """Return a list of all column names in order."""
        return list(self.columns.keys())


DEFAULT_COLUMNS = {
    "Job id": True,
    "Arr. ID": False,
    "Arr. Idx": False,
    "Name": True,
    "Node Name": True,
    "Partition": True,
    "Start/Sub. Time": True,
    "End Time": True,
    "State": True,
    "State Reason": True,
    "Account": True,
    "User": False,
}


class SlurmTUI(App[SlurmTUIReturn]):
    """A Textual UI for slurm jobs."""

    DEFAULT_CSS = """
        DataTable {
            scrollbar-gutter: stable;
        }
    """

    CSS_PATH = "css/slurmtui.css"

    BINDINGS = [
        Binding("l", "logs_out", "Logs (STDOUT)", key_display="L"),
        Binding("e", "logs_err", "Logs (STDERR)", key_display="E"),
        Binding("c", "connect", "Connect to Node (ssh)", key_display="C"),
        Binding("i", "info", "Info", key_display="I"),
        Binding("d", "delete", "Delete", key_display="D"),
        Binding("s", "settings", "Settings", key_display="S"),
        Binding("q", "quit", "Quit", key_display="Q"),
    ]

    job_table = None
    jobs_to_be_deleted = []

    def _display_job_table(self) -> None:
        try:
            job_table = self.query_one(DataTable)
            self.job_table = job_table
        except NoMatches:
            job_table = self.job_table

        old_cursor = job_table.cursor_coordinate
        self.running_jobs_dict = get_running_jobs(settings=settings)
        job_array_exists = check_for_any_job_array(self.running_jobs_dict)
        job_reason_exists = check_for_job_state_reason(self.running_jobs_dict)

        job_table.clear(columns=True)
        column_manager = ColumnManager(DEFAULT_COLUMNS)
        job_table.cursor_type = "row"
        if settings.CHECK_ALL_JOBS:
            column_manager.enable_column("User")
        else:
            column_manager.disable_column("User")
        if job_array_exists:
            column_manager.enable_column("Arr. ID")
            column_manager.enable_column("Arr. Idx")
        else:
            column_manager.disable_column("Arr. ID")
            column_manager.disable_column("Arr. Idx")
        if job_reason_exists:
            column_manager.enable_column("State Reason")
        else:
            column_manager.disable_column("State Reason")

        job_table.add_columns(*column_manager.get_enabled_columns())

        # if a job has been deleted, remove it from jobs_to_be_deleted
        if (
            self.jobs_to_be_deleted is not None
            and self.running_jobs_dict is not None
            and len(self.jobs_to_be_deleted) > 0
            and len(self.running_jobs_dict) > 0
        ):
            for job in self.jobs_to_be_deleted.copy():
                if job not in self.running_jobs_dict:
                    self.jobs_to_be_deleted.remove(job)

        if self.running_jobs_dict is None or len(self.running_jobs_dict) == 0:
            _columns = ["No jobs running"] + (
                len(column_manager.get_enabled_columns()) - 1
            ) * [""]
            job_table.add_row(*_columns)
            return

        for idx, (k, v) in enumerate(self.running_jobs_dict.items()):
            # if any(x["type"] == "FRAG_JOB_REQUEST" for x in v["events"]):
            #     if k not in self.jobs_to_be_deleted:
            #         self.jobs_to_be_deleted.append(k)
            job_state = (
                str(v["job_state"]) + " (To be Deleted)"
                if k in self.jobs_to_be_deleted
                else str(v["job_state"])
            )
            start_time_string, end_time_string = get_start_and_end_time_string(
                v["submit_time"],
                v["start_time"],
                v["end_time"],
                v["job_state"],
                settings,
            )

            _columns = [str(v["job_id"])]
            if job_array_exists:
                _columns.extend(
                    [
                        str(
                            v["array_job_id"]["number"]
                            if v["array_job_id"]["set"]
                            and v["array_job_id"]["number"] != 0
                            else ""
                        ),
                        str(
                            v["array_task_id"]["number"]
                            if v["array_task_id"]["set"]
                            else ""
                        ),
                    ]
                )
            _columns.extend(
                [
                    str(v["name"])[0:50],
                    str(v["job_resources"].get("nodes", ""))[0:25],
                    str(v["partition"]),
                    start_time_string,
                    end_time_string,
                    get_rich_state(job_state),
                ]
            )
            if job_reason_exists:
                _columns.append(
                    str(v["state_reason"]) if v["state_reason"] != "None" else ""
                )
            _columns.append(str(v["account"]))

            if settings.CHECK_ALL_JOBS:
                _columns.append(str(v["user_name"]))
            job_table.add_row(*_columns, key=str(k))

        total_jobs = len(self.running_jobs_dict)
        running_jobs = len(
            [
                x
                for x in self.running_jobs_dict.values()
                if check_for_state(x["job_state"], "RUNNING")
            ]
        )
        to_be_deleted_jobs = len(self.jobs_to_be_deleted)

        self.title = f"SlurmTUI: {total_jobs} jobs ({running_jobs} running"

        if to_be_deleted_jobs > 0:
            self.title += f", {to_be_deleted_jobs} to be deleted"
        self.title += ")"

        job_table.cursor_coordinate = (
            old_cursor
            if old_cursor.row < len(self.running_jobs_dict)
            else Coordinate(row=len(self.running_jobs_dict) - 1, column=0)
        )

    def _update_job_table(self) -> None:
        self._display_job_table()
        self.set_timer(settings.UPDATE_INTERVAL, self._update_job_table)

    def on_mount(self) -> None:
        self._display_job_table()
        self.set_timer(settings.UPDATE_INTERVAL, self._update_job_table)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield DataTable(zebra_stripes=True, name="job_table", id="job_table")
        yield Footer()

    def _check_no_jobs(self) -> bool:
        if self.running_jobs_dict is None or len(self.running_jobs_dict) == 0:
            self.notify("No jobs running", severity="warning")
            return True
        return False

    def _get_log_screen(self, is_std_out: bool) -> None:
        """Show the logs (STDOUT)."""
        # get the id of the selected job
        try:
            job_table = self.query_one(DataTable)
            self.job_table = job_table
        except NoMatches:
            job_table = self.job_table

        if self._check_no_jobs():
            return

        selected_job = list(self.running_jobs_dict.values())[
            job_table.cursor_coordinate.row
        ]

        if check_for_state(selected_job["job_state"], "PENDING"):
            self.notify(
                f"Job {selected_job['job_id']} is in Pending state, no logs available!",
                severity="warning",
            )
            return

        log_path = os.path.join(
            selected_job["standard_output" if is_std_out else "standard_error"],
        )

        # check if the log file exists
        if not os.path.isfile(log_path):
            self.notify(
                "Log file not created yet or not found!" f"\n{log_path}",
                severity="error",
            )
            return
        self.exit(SlurmTUIReturn("logs", {"log_path": log_path}))

    def action_logs_out(self) -> None:
        """Show the logs (STDOUT)."""
        # get the id of the selected job
        self._get_log_screen(is_std_out=True)

    def action_logs_err(self) -> None:
        """Show the logs (STDERR)."""
        # get the id of the selected job
        self._get_log_screen(is_std_out=False)

    def action_connect(self) -> None:
        """Connect to the node via SSH."""
        if self._check_no_jobs():
            return

        try:
            job_table = self.query_one(DataTable)
            self.job_table = job_table
        except NoMatches:
            job_table = self.job_table

        selected_job = list(self.running_jobs_dict.values())[
            job_table.cursor_coordinate.row
        ]

        if check_for_state(selected_job["job_state"], "PENDING"):
            self.notify(
                f"Job {selected_job['job_id']} is in Waiting state, you cannot connect to it!!",
                severity="warning",
            )
            return

        self.exit(
            SlurmTUIReturn("connect", extra={"batch_host": selected_job["batch_host"]})
        )

    def _delete_job(self, selected_job: Dict[str, Any], delete_array=False) -> None:
        if delete_array:
            self.jobs_to_be_deleted.extend(
                [
                    job["job_id"]
                    for job in self.running_jobs_dict.values()
                    if job["array_job_id"]["number"]
                    == selected_job["array_job_id"]["number"]
                ]
            )
        else:
            self.jobs_to_be_deleted.append(selected_job["job_id"])
        if not settings.MOCK:
            if delete_array:
                os.system(f"scancel {selected_job['array_job_id']['number']}")
            else:
                os.system(f"scancel {selected_job['job_id']}")

    def _check_job_is_array(self, selected_job: Dict[str, Any]) -> bool:
        """Check if the selected job is an array job."""

        if selected_job["array_job_id"]["number"] == 0:
            return False
        elif (
            selected_job["array_task_id"]["number"] > 1
            or selected_job["array_job_id"]["number"] != selected_job["job_id"]
        ):
            return True

        return False

    def action_delete(self) -> None:
        """Delete the job."""
        if self._check_no_jobs():
            return

        try:
            job_table = self.query_one(DataTable)
            self.job_table = job_table
        except NoMatches:
            job_table = self.job_table

        selected_job = list(self.running_jobs_dict.values())[
            job_table.cursor_coordinate.row
        ]

        if selected_job["job_id"] in self.jobs_to_be_deleted:
            self.notify(
                f"Job {selected_job['job_id']} is already in the queue to be deleted!!",
                severity="warning",
            )
            return

        def check_confirm_nslurmray(confirm: bool) -> None:
            """Called when ConfirmScreen is dismissed."""
            if confirm:
                self._delete_job(selected_job, delete_array=True)
            else:
                self._delete_job(selected_job, delete_array=False)

        def check_confirm(confirm: bool) -> None:
            """Called when ConfirmScreen is dismissed."""
            if confirm:
                if self._check_job_is_array(selected_job):
                    self.push_screen(
                        ConfirmScreen(
                            "This is an array job, do you want to delete all the jobs in the array?"
                        ),
                        check_confirm_nslurmray,
                    )
                else:
                    self._delete_job(selected_job)

        delete_message = "\nAre you sure you want to delete this job?\n\n"
        # add the id and the name of the job to the message
        delete_message += f"Job ID: {selected_job['job_id']}\n"
        delete_message += f"Job Name: {selected_job['name']}\n"
        node_name = selected_job["job_resources"].get("nodes", "")[0:25]
        if node_name:
            delete_message += f"Node Name: {node_name}\n"
        self.push_screen(ConfirmScreen(delete_message), check_confirm)

    def action_settings(self) -> None:
        """Show the settings."""
        self.push_screen(SettingsScreen())

    def action_info(self) -> None:
        """Show the job info."""
        if self._check_no_jobs():
            return

        try:
            job_table = self.query_one(DataTable)
            self.job_table = job_table
        except NoMatches:
            job_table = self.job_table

        selected_job = list(self.running_jobs_dict.values())[
            job_table.cursor_coordinate.row
        ]

        def print_cli(string_to_print: str) -> None:
            """Print the string to the CLI."""
            self.exit(SlurmTUIReturn("print", {"string_to_print": string_to_print}))

        self.push_screen(InfoScreen(selected_job), print_cli)

    def action_quit(self) -> None:
        """Quit the application."""
        self.exit(SlurmTUIReturn("quit", {}))


def slurmcommand_executor(slurm_return: SlurmTUIReturn, mock=settings.MOCK) -> None:
    if slurm_return.action == "connect":
        if mock:
            print(f"ssh -o StrictHostKeyChecking=no {slurm_return.extra['batch_host']}")
        else:
            os.system(
                f"ssh -o StrictHostKeyChecking=no {slurm_return.extra['batch_host']}"
            )
    elif slurm_return.action == "logs":
        if mock:
            print(f"tail -n 10000 -f {slurm_return.extra['log_path']}")
        else:
            os.system(f"tail -n 10000 -f {slurm_return.extra['log_path']}")
    elif slurm_return.action == "print":
        print_json(slurm_return.extra["string_to_print"])
    elif slurm_return.action == "quit":
        sys.exit(0)
    else:
        raise Exception("Not implemented yet")


def main():
    parser = argparse.ArgumentParser(description="SlurmTUI")
    parser.add_argument(
        "--update_interval", type=int, help="Update interval", default=None
    )
    parser.add_argument("--mock", action="store_true", help="Mock mode", default=None)
    parser.add_argument(
        "--check_all_jobs", action="store_true", help="Check all jobs", default=None
    )
    parser.add_argument(
        "--fake_queue_json_path", help="Fake queue JSON path", default=None
    )
    parser.add_argument(
        "--acc",
        help="comma-seperated account list to filter by since squeue --json has a bug on version < 24.05.1.",
        default=None,
    )
    args, remaining_args = parser.parse_known_args()

    if args.update_interval is not None:
        settings.UPDATE_INTERVAL = args.update_interval
    if args.mock is not None:
        settings.MOCK = args.mock
    if args.check_all_jobs is not None:
        settings.CHECK_ALL_JOBS = args.check_all_jobs
    if args.fake_queue_json_path:
        settings.FAKE_QUEUE_JSON_PATH = args.fake_queue_json_path
    if remaining_args:
        settings.SQUEUE_ARGS = remaining_args
    if args.acc:
        settings.ACCOUNTS = args.acc.split(",")

    while True:
        app = SlurmTUI()
        reply = app.run()
        if reply:
            slurmcommand_executor(reply)


def entry_point():
    main()


if __name__ == "__main__":
    main()
