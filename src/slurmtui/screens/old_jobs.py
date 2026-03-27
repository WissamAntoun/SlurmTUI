import datetime
import os
from typing import Any, Dict, List, Tuple

from textual.app import ComposeResult
from textual.binding import Binding
from textual.coordinate import Coordinate
from textual.css.query import NoMatches
from textual.screen import ModalScreen
from textual.widgets import Footer, Header

from slurmtui.screens.info import InfoScreen
from slurmtui.screens.log_peek import LogPeekScreen

from ..slurm_utils import (
    SlurmTUIReturn,
    check_for_any_old_job_array,
    check_for_state,
    format_time_string,
    get_old_jobs,
    get_rich_state,
)
from ..utils import SETTINGS, settings
from .settings import SettingsScreen
from .sortable_data_table import SortableDataTable
from .utils import ColumnManager

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


class OldJobsScreen(ModalScreen):

    BINDINGS = [
        # fmt: off
        Binding("escape", "screen.dismiss", "Go Back", key_display="Esc"),
        Binding("l", "logs_out_tail", "Logs", key_display="L"),
        Binding("e", "logs_err_tail", "STDERR", key_display="E"),
        Binding("ctrl+l", "logs_out_less", "Less of Logs (STDOUT)", key_display="Ctrl+L", show=False),
        Binding("ctrl+e", "logs_err_less", "Less of Logs (STDERR)", key_display="Ctrl+E", show=False),
        Binding("space", "peek_stdout", "Peek", key_display="Space"),
        Binding("ctrl+space", "peek_stderr", "Peek STDERR", key_display="Ctrl+Space", show=False),
        Binding("i", "info", "Info", key_display="I"),
        Binding("s", "settings", "Settings", key_display="S"),
        Binding("q", "quit", "Quit", key_display="Q"),
        # fmt: on
    ]

    DEFAULT_CSS = """
        DataTable {
            scrollbar-gutter: stable;
        }
    """

    CSS_PATH = "../css/slurmtui.css"

    job_table = None

    def _get_selected_job(self, job_table: SortableDataTable) -> Dict[str, Any] | None:
        """Get the selected job using the row key, which is stable across sorts."""
        coord = job_table.cursor_coordinate
        cell_key = job_table.coordinate_to_cell_key(coord)
        row_key = cell_key.row_key.value
        return self.old_jobs.get(int(row_key))

    def __init__(self, settings: SETTINGS, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app.title = "SlurmTUI Old Jobs"
        self.settings = settings
        # to be used later for when the user wants to change the time window in the view
        self.start_time = settings.OLD_JOBS_START_TIME
        self.end_time = settings.OLD_JOBS_END_TIME

    def on_mount(self) -> None:
        try:
            job_table = self.query_one(SortableDataTable)
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

        if self.old_jobs is None or len(self.old_jobs) == 0:
            _columns = ["No jobs in the past window"] + (
                len(column_manager.get_enabled_columns()) - 1
            ) * [""]
            job_table.add_row(*_columns)
            return

        for idx, (k, v) in enumerate(self.old_jobs.items()):
            submit_time_string, start_time_string, end_time_string = get_time_strings(v)

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
        yield SortableDataTable(
            zebra_stripes=True, name="old_job_table", id="old_job_table"
        )
        yield Footer()

    def _check_no_jobs(self) -> bool:
        if self.old_jobs is None or len(self.old_jobs) == 0:
            self.notify("No jobs running", severity="warning")
            return True
        return False

    def _get_log_screen(self, is_primary: bool, is_std_out: bool) -> None:
        """Show the logs (STDOUT)."""
        # get the id of the selected job
        try:
            job_table = self.query_one(SortableDataTable)
            self.job_table = job_table
        except NoMatches:
            job_table = self.job_table

        if self._check_no_jobs():
            return

        selected_job = self._get_selected_job(job_table)
        if selected_job is None:
            return

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
            cmd = ""

            if is_primary:
                text_util_cmd = settings.PRIMARY_TEXT_UTIL_CMD
            else:
                text_util_cmd = settings.SECONDARY_TEXT_UTIL_CMD

            if text_util_cmd.lower() == "tail":
                cmd = f"tail -n {settings.TAIL_LINES} -f {log_path}"
            elif text_util_cmd.lower() == "less":
                cmd = f"less {log_path}"
            else:
                cmd = text_util_cmd.format(log_path=log_path)

            os.system(cmd)
            if not any(
                x in text_util_cmd.lower()
                for x in ["tail", "less", "nano", "vim", "vi", "micro"]
            ):
                input("Press Enter to continue...")

        self.refresh()

    def action_logs_out_tail(self) -> None:
        """Show the logs (STDOUT)."""
        # get the id of the selected job
        self._get_log_screen(is_primary=True, is_std_out=True)

    def action_logs_err_tail(self) -> None:
        """Show the logs (STDERR)."""
        # get the id of the selected job
        self._get_log_screen(is_primary=True, is_std_out=False)

    def action_logs_out_less(self) -> None:
        """Show the logs (STDOUT) with less."""
        # get the id of the selected job
        self._get_log_screen(is_primary=False, is_std_out=True)

    def action_logs_err_less(self) -> None:
        """Show the logs (STDERR) with less."""
        # get the id of the selected job
        self._get_log_screen(is_primary=False, is_std_out=False)

    def _peek_log(self, is_std_out: bool) -> None:
        """Show the last N lines of a log file in a popup."""
        try:
            job_table = self.query_one(SortableDataTable)
            self.job_table = job_table
        except NoMatches:
            job_table = self.job_table

        if self._check_no_jobs():
            return

        selected_job = self._get_selected_job(job_table)
        if selected_job is None:
            return

        if check_for_state(selected_job["state"]["current"], "PENDING"):
            self.notify(
                f"Job {selected_job['job_id']} is in Pending state, no logs available!",
                severity="warning",
            )
            return

        key = "stdout_expanded" if is_std_out else "stderr_expanded"
        if not selected_job.get(key, ""):
            stream = "standard output" if is_std_out else "standard error"
            self.notify(
                f"Job {selected_job['job_id']} has no {stream}!. This may be due to slurm version being < 24.05",
                severity="warning",
            )
            return

        log_path = selected_job[key]

        if not os.path.isfile(log_path):
            self.notify(
                "Log file not created yet or not found!" f"\n{log_path}",
                severity="error",
            )
            return

        stream = "STDOUT" if is_std_out else "STDERR"
        title = f"Peek {stream}: {selected_job['name']} ({selected_job['job_id']})"
        self.app.push_screen(LogPeekScreen(log_path, settings.PEEK_LINES, title))

    def action_peek_stdout(self) -> None:
        """Peek at the last N lines of STDOUT."""
        self._peek_log(is_std_out=True)

    def action_peek_stderr(self) -> None:
        """Peek at the last N lines of STDERR."""
        self._peek_log(is_std_out=False)

    def action_settings(self) -> None:
        """Show the settings."""

        def apply_theme(saved: bool) -> None:
            if saved:
                self.app.theme = settings.THEME

        self.app.push_screen(SettingsScreen(), apply_theme)

    def action_info(self) -> None:
        """Show the job info."""
        if self._check_no_jobs():
            return

        try:
            job_table = self.query_one(SortableDataTable)
            self.job_table = job_table
        except NoMatches:
            job_table = self.job_table

        selected_job = self._get_selected_job(job_table)
        if selected_job is None:
            return

        def print_cli(string_to_print: str) -> None:
            """Print the string to the CLI."""
            self.app.exit(SlurmTUIReturn("print", {"string_to_print": string_to_print}))

        self.app.push_screen(InfoScreen(selected_job), print_cli)

    def action_quit(self) -> None:
        """Quit the application."""
        self.app.exit(SlurmTUIReturn("quit", {}))
