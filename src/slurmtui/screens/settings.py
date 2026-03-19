from typing import Any, List

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.theme import BUILTIN_THEMES
from textual.widgets import Button, Checkbox, Footer, Header, Input, Label, OptionList

from ..utils import settings

SCREEN_BINDINGS = [
    Binding("ctrl+s", "save_settings", "Save Settings", key_display="Ctrl+S"),
    Binding("escape", "dismiss_screen", "Go Back", key_display="Esc"),
    Binding("b", "dismiss_screen", "Go Back", False),
    Binding("backspace", "dismiss_screen", "Go Back", False),
    Binding("q", "app.quit", "Quit", key_display="Q"),
]

THEME_SELECTION_BINDINGS = [
    Binding("enter", "select_theme", "Select Theme", key_display="Enter"),
    Binding("escape", "app.pop_screen", "Go Back", key_display="Esc"),
    Binding("b", "app.pop_screen", "Go Back", False),
    Binding("backspace", "app.pop_screen", "Go Back", False),
    Binding("q", "app.quit", "Quit", key_display="Q"),
]


class ThemeSelectionScreen(ModalScreen[str]):
    BINDINGS = THEME_SELECTION_BINDINGS

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True, name="Select Theme", id="theme_selection_header")
        yield Label(
            "Select a theme and press Enter to apply:",
            id="theme_selection_instructions",
        )
        yield OptionList(
            *list(BUILTIN_THEMES.keys()),
            id="theme_list",
            classes="theme_selection_list",
        )
        yield Footer()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        self.app.theme = event.option.prompt
        self.dismiss(str(event.option.prompt))


class SettingsScreen(ModalScreen[bool]):
    BINDINGS = SCREEN_BINDINGS

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._selected_theme: str = settings.THEME

    def on_mount(self) -> None:
        self.app.title = "SlurmTUI Settings"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True, name="SlurmTUI Settings", id="settings_header")
        with VerticalScroll():
            with Horizontal(classes="settings_row"):
                yield Label("Theme", classes="settings_label")
                yield Label(self._selected_theme, id="label_THEME_VALUE")
                yield Button(
                    "Select Theme",
                    id="button_THEME",
                    variant="primary",
                    classes="settings_button",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Update Interval (seconds)", classes="settings_label")
                yield Input(
                    str(settings.UPDATE_INTERVAL),
                    id="input_UPDATE_INTERVAL",
                    placeholder="10",
                    tooltip="Seconds between job list refreshes (×5 when Check All Jobs is on)",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Check All Jobs", classes="settings_label")
                yield Checkbox(
                    id="input_CHECK_ALL_JOBS",
                    value=settings.CHECK_ALL_JOBS,
                    button_first=False,
                    tooltip="Show all jobs in the queue, not just yours",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Mock Mode", classes="settings_label")
                yield Checkbox(
                    id="input_MOCK",
                    value=settings.MOCK,
                    button_first=False,
                    tooltip="Use mock data instead of connecting to a real Slurm cluster",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Squeue Arguments", classes="settings_label")
                yield Input(
                    " ".join(settings.SQUEUE_ARGS) if settings.SQUEUE_ARGS else "",
                    id="input_SQUEUE_ARGS",
                    tooltip="Extra squeue arguments (space-separated)",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Accounts", classes="settings_label")
                yield Input(
                    ", ".join(settings.ACCOUNTS) if settings.ACCOUNTS else "",
                    id="input_ACCOUNTS",
                    tooltip="Accounts to filter by (comma-separated). Workaround for squeue --json bug on versions < 24.05.1",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Text Util Command", classes="settings_label")
                yield Input(
                    settings.PRIMARY_TEXT_UTIL_CMD,
                    id="input_PRIMARY_TEXT_UTIL_CMD",
                    placeholder="tail -f {log_path}",
                    tooltip="Command to use to open log files. Should include '{log_path}' as a placeholder for the log file path.",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Secondary Text Util Command", classes="settings_label")
                yield Input(
                    settings.SECONDARY_TEXT_UTIL_CMD,
                    id="input_SECONDARY_TEXT_UTIL_CMD",
                    placeholder="less +F {log_path}",
                    tooltip="Command to use to open secondary log files (e.g. STDERR). Should include '{log_path}' as a placeholder for the log file path.",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Tail Lines", classes="settings_label")
                yield Input(
                    str(settings.TAIL_LINES),
                    id="input_TAIL_LINES",
                    placeholder="10000",
                    tooltip="Number of lines to show when tailing logs",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Old Jobs Start Time", classes="settings_label")
                yield Input(
                    settings.OLD_JOBS_START_TIME,
                    id="input_OLD_JOBS_START_TIME",
                    placeholder="now-7days",
                    tooltip="Start time for old jobs (sacct format, e.g. now-7days)",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Old Jobs End Time", classes="settings_label")
                yield Input(
                    settings.OLD_JOBS_END_TIME,
                    id="input_OLD_JOBS_END_TIME",
                    placeholder="now",
                    tooltip="End time for old jobs (sacct format, e.g. now)",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Debug Squeue JSON Path", classes="settings_label")
                yield Input(
                    settings.DEBUG_SQUEUE_JSON_PATH or "",
                    id="input_DEBUG_SQUEUE_JSON_PATH",
                    tooltip="JSON file path to substitute for squeue output (debug/testing)",
                )

            with Horizontal(classes="settings_row"):
                yield Label("Debug Sacct JSON Path", classes="settings_label")
                yield Input(
                    settings.DEBUG_SACCT_JSON_PATH or "",
                    id="input_DEBUG_SACCT_JSON_PATH",
                    tooltip="JSON file path to substitute for sacct output (debug/testing)",
                )

        yield Footer()

    def action_save_settings(self) -> None:
        # Theme — stored in instance var, updated by ThemeSelectionScreen callback
        settings.THEME = self._selected_theme

        # Integer with minimum bound
        try:
            settings.UPDATE_INTERVAL = max(
                1,
                int(self.query_one("#input_UPDATE_INTERVAL", Input).value.strip()),
            )
        except (ValueError, TypeError):
            settings.UPDATE_INTERVAL = 10

        # Booleans — read directly from Checkbox widgets, never via __dict__ iteration
        settings.CHECK_ALL_JOBS = self.query_one(
            "#input_CHECK_ALL_JOBS", Checkbox
        ).value
        settings.MOCK = self.query_one("#input_MOCK", Checkbox).value

        # List[str] space-separated → None if blank
        squeue_str = self.query_one("#input_SQUEUE_ARGS", Input).value.strip()
        settings.SQUEUE_ARGS = squeue_str.split() if squeue_str else None

        # List[str] comma-separated → None if blank
        accounts_str = self.query_one("#input_ACCOUNTS", Input).value.strip()
        settings.ACCOUNTS = (
            [a.strip() for a in accounts_str.split(",") if a.strip()]
            if accounts_str
            else None
        )

        tail_cmd_str = self.query_one(
            "#input_PRIMARY_TEXT_UTIL_CMD", Input
        ).value.strip()
        settings.PRIMARY_TEXT_UTIL_CMD = tail_cmd_str or "tail"

        tail_cmd_str = self.query_one(
            "#input_SECONDARY_TEXT_UTIL_CMD", Input
        ).value.strip()
        settings.SECONDARY_TEXT_UTIL_CMD = tail_cmd_str or "less"

        tail_lines_str = self.query_one("#input_TAIL_LINES", Input).value.strip()
        try:
            settings.TAIL_LINES = max(1, int(tail_lines_str))
        except (ValueError, TypeError):
            settings.TAIL_LINES = 10000

        # Strings with fallback to defaults
        start = self.query_one("#input_OLD_JOBS_START_TIME", Input).value.strip()
        settings.OLD_JOBS_START_TIME = start or "now-7days"

        end = self.query_one("#input_OLD_JOBS_END_TIME", Input).value.strip()
        settings.OLD_JOBS_END_TIME = end or "now"

        # Optional strings — None if blank
        squeue_path = self.query_one(
            "#input_DEBUG_SQUEUE_JSON_PATH", Input
        ).value.strip()
        settings.DEBUG_SQUEUE_JSON_PATH = squeue_path or None

        sacct_path = self.query_one("#input_DEBUG_SACCT_JSON_PATH", Input).value.strip()
        settings.DEBUG_SACCT_JSON_PATH = sacct_path or None

        settings.save()
        self.notify("Settings saved")
        self.dismiss(True)

    def action_dismiss_screen(self) -> None:
        self.dismiss(False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "button_THEME":

            def set_theme(theme_name: str) -> None:
                if theme_name:
                    self._selected_theme = theme_name
                    self.query_one("#label_THEME_VALUE", Label).update(theme_name)

            self.app.push_screen(ThemeSelectionScreen(), set_theme)
