import datetime
import json
import os
import socket
import subprocess
import sys
from typing import Any, Coroutine, Dict

from rich import print_json
from rich.syntax import Syntax
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Grid
from textual.coordinate import Coordinate
from textual.css.query import NoMatches
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Label,
    Log,
    RichLog,
    Static,
)

from .slurm_utils import MOCK, check_for_state, get_rich_state, get_running_jobs

MOCK = os.getenv("MOCK", "False").lower() == "true"
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL", "10"))
TAIL_LINES = int(os.getenv("TAIL_LINES", "-1"))
CHECK_ALL_JOBS = os.getenv("ALL_JOBS", "False").lower() == "true"
if CHECK_ALL_JOBS:
    UPDATE_INTERVAL = 30


# class LogScreen(Screen):
#     BINDINGS = [
#         # I couldn't disable the default bindings, so I just overrode them
#         Binding("c", "do_nothing", "", False),
#         Binding("l", "do_nothing", "", False),
#         Binding("e", "do_nothing", "", False),
#         Binding("d", "do_nothing", "", False),
#         ("escape", "app.pop_screen", "Go Back"),
#         Binding("b", "app.pop_screen", "Go Back", False),
#         Binding("backspace", "app.pop_screen", "Go Back", False),
#         ("q", "app.quit", "Quit"),
#     ]

#     def __init__(self, file_path: str, **kwargs: Any) -> None:
#         super().__init__(**kwargs)
#         self.file_path = file_path
#         self.last_position = 0
#         self.file_warning_shown = False

#     def compose(self) -> ComposeResult:
#         yield Header(show_clock=True, name="slurm Job Logs", id="log_header")
#         yield RichLog(highlight=True, markup=False, id="logs_text")
#         yield Footer()

#     def on_mount(self) -> None:
#         self.set_timer(1, self.update_log)

#     async def _text_reader(self) -> None:
#         logs = self.query_one(RichLog)
#         if os.path.isfile(self.file_path):
#             with open(self.file_path, "r") as f:
#                 f.seek(self.last_position)
#                 text = f.read()
#                 self.last_position = f.tell()
#             if text:
#                 if TAIL_LINES > 0:
#                     text = "\n".join(text.split("\n")[-TAIL_LINES:])
#                 logs.write(Text(text, no_wrap=False, end=""))
#         else:
#             if not self.file_warning_shown:
#                 logs.write(
#                     Text(
#                         "Log file not created yet or not found! Waiting...",
#                         style="yellow",
#                     )
#                 )
#                 self.file_warning_shown = True

#     async def update_log(self) -> None:
#         await self._text_reader()
#         self.set_timer(1, self.update_log)

#     def action_do_nothing(self) -> None:
#         pass


def get_time(time_field) -> str:
    if isinstance(time_field, int):
        return time_field
    elif isinstance(time_field, dict):
        return time_field["number"]
    else:
        return 0


def get_start_and_end_time_string(submit_time, start_time, end_time, job_state) -> str:
    submit_time = get_time(submit_time)
    start_time = get_time(start_time)
    end_time = get_time(end_time)

    submit_time_string = ""
    start_time_string = ""
    end_time_string = ""

    if submit_time:
        submit_time_string = str(datetime.datetime.fromtimestamp(submit_time))
    if start_time:
        start_time_string = str(datetime.datetime.fromtimestamp(start_time))
    if end_time:
        end_time_string = str(datetime.datetime.fromtimestamp(end_time))

    if not check_for_state(job_state, "PENDING") and end_time:
        time_remaining = (
            datetime.datetime.fromtimestamp(end_time) - datetime.datetime.now()
        ) / datetime.timedelta(hours=1)
        if time_remaining > 0:
            end_time_string += " (in " + str(round(time_remaining, 1)) + " hrs)"

    if check_for_state(job_state, "PENDING"):
        if start_time:
            time_till_start = (
                datetime.datetime.fromtimestamp(start_time) - datetime.datetime.now()
            ) / datetime.timedelta(hours=1)
            if time_till_start > 0:
                start_time_string += " (in " + str(round(time_till_start, 1)) + " hrs)"
        elif submit_time:
            time_since_submit = (
                datetime.datetime.now() - datetime.datetime.fromtimestamp(submit_time)
            ) / datetime.timedelta(hours=1)
            if time_since_submit > 0:
                submit_time_string += (
                    " (submitted " + str(round(time_since_submit, 1)) + " hrs ago)"
                )
                start_time_string = submit_time_string
        else:
            pass

    return start_time_string, end_time_string


class InfoScreen(Screen[str]):
    BINDINGS = [
        # I couldn't disable the default bindings, so I just overrode them
        Binding("c", "do_nothing", "", False),
        Binding("l", "do_nothing", "", False),
        Binding("e", "do_nothing", "", False),
        Binding("d", "do_nothing", "", False),
        Binding("i", "do_nothing", "", False),
        ("s", "print_cli", "Print in CLI"),
        ("escape", "app.pop_screen", "Go Back"),
        Binding("b", "app.pop_screen", "Go Back", False),
        Binding("backspace", "app.pop_screen", "Go Back", False),
        ("q", "app.quit", "Quit"),
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


class MessageScreen(ModalScreen):
    BINDINGS = [
        # I couldn't disable the default bindings, so I just overrode them
        Binding("c", "do_nothing", "", False),
        Binding("l", "do_nothing", "", False),
        Binding("e", "do_nothing", "", False),
        Binding("d", "do_nothing", "", False),
        Binding("i", "do_nothing", "", False),
        ("escape", "app.pop_screen", "Go Back"),
        Binding("b", "app.pop_screen", "Go Back", False),
        Binding("backspace", "app.pop_screen", "Go Back", False),
        ("q", "app.quit", "Quit"),
    ]

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.message = message

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True, name="slurm Job Logs", id="message_header")
        yield Grid(
            Label(
                Text(self.message, style="yellow"),
                id="message_label",
            ),
            id="message_dialog",
        )
        yield Footer()

    def action_do_nothing(self) -> None:
        pass


class ConfirmScreen(ModalScreen[bool]):
    """Screen with confirm a dialog."""

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

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "yes":
            self.dismiss(True)
        else:
            self.dismiss(False)


class SlurmTUIReturn:
    """Return value for SlurmTUI."""

    def __init__(self, action: str, extra: Dict[str, Any]) -> None:
        self.action = action
        self.extra = extra


class SlurmTUI(App[SlurmTUIReturn]):
    """A Textual UI for slurm jobs."""

    DEFAULT_CSS = """
        DataTable {
            scrollbar-gutter: stable;
        }
    """

    CSS_PATH = "css/slurmtui.css"

    BINDINGS = [
        ("l", "logs_out", "Logs (STDOUT)"),
        ("e", "logs_err", "Logs (STDERR)"),
        ("c", "connect", "Connect to Node (ssh)"),
        ("i", "info", "Info"),
        ("d", "delete", "Delete"),
        ("q", "quit", "Quit"),
    ]

    first_display = True
    job_table = None
    jobs_to_be_deleted = []

    def __init__(self, mock=MOCK, check_all_jobs=CHECK_ALL_JOBS, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.mock = mock
        self.check_all_jobs = check_all_jobs

    def _display_job_table(self) -> None:
        try:
            job_table = self.query_one(DataTable)
            self.job_table = job_table
        except NoMatches:
            job_table = self.job_table

        old_cursor = job_table.cursor_coordinate

        job_table.clear()
        if self.first_display:
            job_table.cursor_type = "row"
            columns = [
                "Job id",
                "Arr. ID",
                "Arr. Idx",
                "Name",
                "Node Name",
                "Partition",
                "Start/Sub. Time",
                "End Time",
                "State",
                "State Reason",
                "Account",
            ]
            if self.check_all_jobs:
                columns.append("User")
            job_table.add_columns(*columns)
            self.first_display = False

        self.running_jobs_dict = get_running_jobs(
            mock=self.mock, check_all_jobs=self.check_all_jobs
        )

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
            _columns = [
                "No jobs running",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
            if self.check_all_jobs:
                _columns.append("")
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
                v["submit_time"], v["start_time"], v["end_time"], v["job_state"]
            )

            _columns = [
                str(v["job_id"]),
                str(
                    v["array_job_id"]["number"]
                    if v["array_job_id"]["set"] and v["array_job_id"]["number"] != 0
                    else ""
                ),
                str(v["array_task_id"]["number"] if v["array_task_id"]["set"] else ""),
                str(v["name"])[0:50],
                str(v["job_resources"].get("nodes", ""))[0:25],
                str(v["partition"]),
                start_time_string,
                end_time_string,
                get_rich_state(job_state),
                str(v["state_reason"]) if v["state_reason"] != "None" else "",
                str(v["account"]),
            ]
            if self.check_all_jobs:
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
        self.set_timer(UPDATE_INTERVAL, self._update_job_table)

    def on_mount(self) -> None:
        self._display_job_table()
        self.set_timer(UPDATE_INTERVAL, self._update_job_table)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield DataTable(zebra_stripes=True, name="job_table", id="job_table")
        yield Footer()

    def _check_no_jobs(self) -> bool:
        if self.running_jobs_dict is None or len(self.running_jobs_dict) == 0:
            self.push_screen(MessageScreen("No jobs running"))
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
            self.push_screen(
                MessageScreen("Job is in Pending state, no logs available!!")
            )
            return

        log_path = os.path.join(
            selected_job["standard_output" if is_std_out else "standard_error"],
        )

        # check if the log file exists
        if not os.path.isfile(log_path):
            self.push_screen(MessageScreen("Log file not created yet or not found!"))
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
            self.push_screen(
                MessageScreen("Job is in Waiting state, you cannot connect to it!!")
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
        if not self.mock:
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
            self.push_screen(
                MessageScreen("Job is already in the queue to be deleted!!")
            )
            return

        def check_confirm_nslurmray(confirm: bool) -> None:
            """Called when ConfirmScreen is dismissed."""
            if confirm:
                self._delete_job(selected_job, delete_array=True)

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

        self.push_screen(
            ConfirmScreen("Are you sure you want to delete this job?"), check_confirm
        )

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


def slurmcommand_executor(slurm_return: SlurmTUIReturn, mock=MOCK) -> None:
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
    while True:
        app = SlurmTUI()
        reply = app.run()
        if reply:
            slurmcommand_executor(reply)


def entry_point():
    main()


if __name__ == "__main__":
    main()
