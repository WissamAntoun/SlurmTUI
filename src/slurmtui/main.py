import argparse
import datetime
import os
import sys
from typing import Any, Callable, Dict
from weakref import WeakSet

from rich import print_json
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.coordinate import Coordinate
from textual.css.query import NoMatches
from textual.timer import Timer
from textual.widgets import DataTable, Footer, Header

from .screens import (
    get_confirm_screen,
    get_info_screen,
    get_old_jobs_screen,
    get_settings_screen,
)
from .screens.utils import ColumnManager
from .slurm_utils import (
    SlurmTUIReturn,
    check_for_any_job_array,
    check_for_job_state_reason,
    check_for_state,
    get_rich_state,
    get_running_jobs,
    get_start_and_end_time_string,
)
from .utils import settings

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
        Binding("o", "old_jobs", "Old Jobs", key_display="O"),
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
                    str(v.get("job_resources", {}).get("nodes", ""))[0:25],
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

        with self.suspend():
            os.system(f"tail -n 10000 -f {log_path}")

        self.refresh()


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
                    confirm_screen = get_confirm_screen(self.BINDINGS)
                    self.push_screen(
                        confirm_screen(
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
        node_name = str(selected_job.get("job_resources", {}).get("nodes", ""))[0:25]
        if node_name:
            delete_message += f"Node Name: {node_name}\n"
        confirm_screen = get_confirm_screen(self.BINDINGS)
        self.push_screen(confirm_screen(delete_message), check_confirm)

    def action_settings(self) -> None:
        """Show the settings."""
        settings_screen = get_settings_screen(self.BINDINGS)
        self.push_screen(settings_screen())

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

        info_screen = get_info_screen(self.BINDINGS)
        self.push_screen(info_screen(selected_job), print_cli)

    @work
    async def action_old_jobs(self) -> None:
        """Show the old jobs."""
        for timer in self._timers:
            timer.stop()
        self._timers.clear()
        self._timers = WeakSet()
        old_jobs_screen = get_old_jobs_screen(self.BINDINGS)
        await self.push_screen_wait(old_jobs_screen(settings=settings))
        self.set_timer(
            settings.UPDATE_INTERVAL, self._update_job_table
        )


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
        "--debug_squeue_json_path", help="Fake queue JSON path", default=None
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
    if args.debug_squeue_json_path:
        settings.DEBUG_SQUEUE_JSON_PATH = args.debug_squeue_json_path
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
