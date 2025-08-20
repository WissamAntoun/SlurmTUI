import datetime
import os
from typing import Any, Dict, List, Tuple

from textual.app import ComposeResult
from textual.binding import Binding
from textual.coordinate import Coordinate
from textual.css.query import NoMatches
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header

from slurmtui.screens.info import get_info_screen

from ..slurm_utils import (
    SlurmTUIReturn,
    check_for_any_old_job_array,
    check_for_state,
    format_time_string,
    get_old_jobs,
    get_rich_state,
)
from ..utils import SETTINGS
from .settings import get_settings_screen
from .utils import ColumnManager

OLD_SCREEN_BINDINGS = [
    Binding("escape", "screen.dismiss", "Go Back", key_display="Esc"),
    Binding("l", "logs_out", "Logs (STDOUT)", key_display="L"),
    Binding("e", "logs_err", "Logs (STDERR)", key_display="E"),
    Binding("i", "info", "Info", key_display="I"),
    Binding("s", "settings", "Settings", key_display="S"),
    Binding("q", "quit", "Quit", key_display="Q"),
]


DEFAULT_COLUMNS = {
    "Job id": True,
    "Arr. ID": False,
    "Arr. Idx": False,
    "Name": True,
    "Node Name": True,
    "Partition": True,
    "Submit Time": True,
    "Start Time": True,
    "End Time": True,
    "State": True,
    "Account": True,
}


def get_time_strings(job: Dict[str, Any]) -> Tuple[str, str, str]:

    submit_time = job["time"]["submission"]
    start_time = job["time"]["start"]
    end_time = job["time"]["end"]

    submit_time_string = ""
    start_time_string = ""
    end_time_string = ""

    if submit_time:
        submit_time_string = datetime.datetime.fromtimestamp(submit_time).strftime(
            "%y-%m-%d %H:%M:%S"
        )

    if start_time:
        start_time_string = datetime.datetime.fromtimestamp(start_time).strftime(
            "%y-%m-%d %H:%M:%S"
        )
        started_after = datetime.datetime.fromtimestamp(
            start_time
        ) - datetime.datetime.fromtimestamp(submit_time)
        start_time_string += f" +{format_time_string(started_after)}"

    if end_time:
        end_time_string = datetime.datetime.fromtimestamp(end_time).strftime(
            "%y-%m-%d %H:%M:%S"
        )
        ended_after = datetime.datetime.fromtimestamp(
            end_time
        ) - datetime.datetime.fromtimestamp(start_time)
        if end_time > start_time:
            end_time_string += f" +{format_time_string(ended_after)}"
        else:
            end_time_string += f" (Instant)"

    return (
        submit_time_string,
        start_time_string,
        end_time_string,
    )


def get_old_jobs_screen(OLD_BINDINGS: List[Binding]):
    """
    Returns the OldJobsScreen with the old bindings removed.
    """
    # Disable or replace the old bindings
    bindings = OLD_SCREEN_BINDINGS.copy()
    for old_binding in OLD_BINDINGS:
        for binding in OLD_SCREEN_BINDINGS:
            if old_binding.key != binding.key:
                bindings.append(
                    Binding(
                        old_binding.key,
                        "do_nothing",
                        "",
                        False,
                    )
                )

    class OldJobsScreen(Screen):

        BINDINGS = bindings

        DEFAULT_CSS = """
            DataTable {
                scrollbar-gutter: stable;
            }
        """

        CSS_PATH = "../css/slurmtui.css"

        job_table = None

        def __init__(self, settings: SETTINGS, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.app.title = "SlurmTUI Old Jobs"
            self.settings = settings
            # to be used later for when the user wants to change the time window in the view
            self.start_time = settings.OLD_JOBS_START_TIME
            self.end_time = settings.OLD_JOBS_END_TIME

        def on_mount(self) -> None:
            try:
                job_table = self.query_one(DataTable)
                self.job_table = job_table
            except NoMatches:
                job_table = self.job_table

            old_cursor = job_table.cursor_coordinate

            self.old_jobs = get_old_jobs(self.settings, self.start_time, self.end_time)
            job_array_exists = check_for_any_old_job_array(self.old_jobs)

            job_table.clear()
            column_manager = ColumnManager(DEFAULT_COLUMNS)
            job_table.cursor_type = "row"
            if job_array_exists:
                column_manager.enable_column("Arr. ID")
                column_manager.enable_column("Arr. Idx")
            else:
                column_manager.disable_column("Arr. ID")
                column_manager.disable_column("Arr. Idx")

            job_table.add_columns(*column_manager.get_enabled_columns())

            if len(self.old_jobs) == 0:
                _columns = ["No jobs in the past window"] + (
                    len(column_manager.get_enabled_columns()) - 1
                ) * [""]
                job_table.add_row(*_columns)
                return

            for idx, (k, v) in enumerate(self.old_jobs.items()):
                submit_time_string, start_time_string, end_time_string = (
                    get_time_strings(v)
                )

                _columns = [str(v["job_id"])]
                if job_array_exists:
                    _columns.extend(
                        [
                            str(v["array"]["job_id"]) if v["array"]["job_id"] else "",
                            (
                                str(v["array"]["task_id"]["number"])
                                if v["array"]["task_id"]["set"]
                                else ""
                            ),
                        ]
                    )
                _columns.extend(
                    [
                        str(v["name"])[0:50],
                        str(v.get("nodes", ""))[0:25],
                        str(v["partition"]),
                        submit_time_string,
                        start_time_string,
                        end_time_string,
                        get_rich_state(v["state"]["current"]),
                        str(v["account"]),
                    ]
                )

                job_table.add_row(*_columns, key=str(k))

            total_jobs = len(self.old_jobs)

            self.title = f"SlurmTUI: {total_jobs} jobs"

            job_table.cursor_coordinate = (
                old_cursor
                if old_cursor.row < len(self.old_jobs)
                else Coordinate(row=len(self.old_jobs) - 1, column=0)
            )

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            yield DataTable(
                zebra_stripes=True, name="old_job_table", id="old_job_table"
            )
            yield Footer()

        def _check_no_jobs(self) -> bool:
            if self.old_jobs is None or len(self.old_jobs) == 0:
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

            selected_job = list(self.old_jobs.values())[job_table.cursor_coordinate.row]

            if check_for_state(selected_job["state"]["current"], "PENDING"):
                self.notify(
                    f"Job {selected_job['job_id']} is in Pending state, no logs available!",
                    severity="warning",
                )
                return

            # check if standard output or standard error in the selected job
            if is_std_out:
                if not selected_job.get("stdout_expanded", ""):
                    self.notify(
                        f"Job {selected_job['job_id']} has no standard output!. This may be due to slurm version being < 24.05",
                        severity="warning",
                    )
                    return
            else:
                if not selected_job.get("stderr_expanded", ""):
                    self.notify(
                        f"Job {selected_job['job_id']} has no standard error!. This may be due to slurm version being < 24.05",
                        severity="warning",
                    )
                    return

            log_path = os.path.join(
                selected_job["stdout_expanded" if is_std_out else "stderr_expanded"],
            )

            # check if the log file exists
            if not os.path.isfile(log_path):
                self.notify(
                    "Log file not created yet or not found!" f"\n{log_path}",
                    severity="error",
                )
                return

            with self.app.suspend():
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

        def action_settings(self) -> None:
            """Show the settings."""
            settings_screen = get_settings_screen(self.BINDINGS)
            self.app.push_screen(settings_screen())

        def action_info(self) -> None:
            """Show the job info."""
            if self._check_no_jobs():
                return

            try:
                job_table = self.query_one(DataTable)
                self.job_table = job_table
            except NoMatches:
                job_table = self.job_table

            selected_job = list(self.old_jobs.values())[job_table.cursor_coordinate.row]

            def print_cli(string_to_print: str) -> None:
                """Print the string to the CLI."""
                self.exit(SlurmTUIReturn("print", {"string_to_print": string_to_print}))

            info_screen = get_info_screen(self.BINDINGS)
            self.app.push_screen(info_screen(selected_job), print_cli)

        # def action_quit(self) -> None:
        #     """Quit the application."""
        #     self.exit(SlurmTUIReturn("quit", {}))

    return OldJobsScreen
