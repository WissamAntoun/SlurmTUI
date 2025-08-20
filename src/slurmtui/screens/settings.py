from typing import Any, List

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.screen import ModalScreen, Screen
from textual.theme import BUILTIN_THEMES
from textual.widgets import (
    Button,
    Checkbox,
    Footer,
    Header,
    Input,
    Label,
    OptionList,
    Static,
)

from ..utils import SETTINGS, settings

SCREEN_BINDINGS = [
    Binding("ctrl+s", "save_settings", "Save Settings", key_display="Ctrl+S"),
    Binding("escape", "app.pop_screen", "Go Back", key_display="Esc"),
    Binding("b", "app.pop_screen", "Go Back", False),
    Binding("backspace", "app.pop_screen", "Go Back", False),
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

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app.title = "Select Theme"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True, name="Select Theme (Press Enter to Select)", id="theme_selection_header")
        yield Label(
            "Select a theme from the list below: (Press Enter to apply the selected theme)",
            id="theme_selection_instructions",
        )
        yield OptionList(
            *list(BUILTIN_THEMES.keys()),
            id="theme_list",
            classes="theme_selection_list",
        )
        yield Footer()

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        """Handle the selection of a theme."""
        self.app.theme = event.option.prompt
        self.dismiss(event.option.prompt)




def get_settings_screen(OLD_BINDINGS: List[Binding]):
    """
    Returns the SettingsScreen with the old bindings removed.
    """
    # Disable or replace the old bindings
    bindings = SCREEN_BINDINGS.copy()
    for old_binding in OLD_BINDINGS:
        for binding in SCREEN_BINDINGS:
            if old_binding.key != binding.key:
                bindings.append(
                    Binding(
                        old_binding.key,
                        "do_nothing",
                        "",
                        False,
                    )
                )

    class SettingsScreen(Screen):
        BINDINGS = bindings

        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.app.title = "SlurmTUI Settings"

        def compose(self) -> ComposeResult:
            yield Header(
                show_clock=True, name="SlurmTUI Settings", id="settings_header"
            )
            with Vertical():
                with Horizontal(classes="settings_row"):
                    yield Label("Theme", id="label_THEME", classes="settings_label")
                    yield Label(settings.THEME, id="label_THEME_VALUE")
                    yield Button("Select Theme", id="button_THEME", variant="primary", classes="settings_button")

                with Horizontal(classes="settings_row"):
                    yield Label(f"Update Interval (seconds)", id="label_UPDATE_INTERVAL", classes="settings_label")
                    yield Input(
                        str(settings.UPDATE_INTERVAL),
                        id="input_UPDATE_INTERVAL",
                        placeholder="10",
                        tooltip="Update interval in seconds. Will be multiplied by 5 if CHECK_ALL_JOBS is True",
                    )

                with Horizontal(classes="settings_row"):
                    yield Label("Squeue Arguments", id="label_SQUEUE_ARGS", classes="settings_label")
                    yield Input(
                        str(settings.SQUEUE_ARGS) if settings.SQUEUE_ARGS else "",
                        id="input_SQUEUE_ARGS",
                        tooltip="List of additional arguments to pass to squeue",
                    )
                with Horizontal(classes="settings_row"):
                    yield Label("Accounts", id="label_ACCOUNTS", classes="settings_label")
                    yield Input(
                        str(settings.ACCOUNTS) if settings.ACCOUNTS else "",
                        id="input_ACCOUNTS",
                        tooltip="List of accounts to filter the jobs by since squeue --json has a bug on version < 24.05.1",
                    )

                with Horizontal(classes="settings_row"):
                    yield Label("Old Jobs Start Time", id="label_OLD_JOBS_START_TIME", classes="settings_label")
                    yield Input(
                        settings.OLD_JOBS_START_TIME,
                        id="input_OLD_JOBS_START_TIME",
                        placeholder="now-7days",
                        tooltip="Start time for the old jobs query. now[{+|-}count[seconds(default)|minutes|hours|days|weeks]] or check on https://slurm.schedmd.com/sacct.html for more info",
                    )

                with Horizontal(classes="settings_row"):
                    yield Label("Old Jobs End Time", id="label_OLD_JOBS_END_TIME", classes="settings_label")
                    yield Input(
                        settings.OLD_JOBS_END_TIME,
                        id="input_OLD_JOBS_END_TIME",
                        placeholder="now",
                        tooltip="End time for the old jobs query. now[{+|-}count[seconds(default)|minutes|hours|days|weeks]] or check on https://slurm.schedmd.com/sacct.html for more info",
                    )

                with Horizontal(classes="settings_row"):
                    yield Label("Debug Squeue JSON Path", id="label_DEBUG_SQUEUE_JSON_PATH", classes="settings_label")
                    yield Input(
                        settings.DEBUG_SQUEUE_JSON_PATH if settings.DEBUG_SQUEUE_JSON_PATH else "",
                        id="input_DEBUG_SQUEUE_JSON_PATH",
                        placeholder="",
                        tooltip="Path to the debug squeue json file",
                    )

                with Horizontal(classes="settings_row"):
                    yield Label("Debug Sacct JSON Path", id="label_DEBUG_SACCT_JSON_PATH", classes="settings_label")
                    yield Input(
                        settings.DEBUG_SACCT_JSON_PATH if settings.DEBUG_SACCT_JSON_PATH else "",
                        id="input_DEBUG_SACCT_JSON_PATH",
                        placeholder="",
                        tooltip="Path to the debug sacct json file",
                    )


                with Horizontal(classes="settings_row"):
                    yield Label("Mock Mode", id="label_MOCK", classes="settings_label")
                    yield Checkbox(
                        id="input_MOCK",
                        value=settings.MOCK,
                        button_first=False,
                        tooltip="Enable or disable mock mode. If enabled, the app will not connect to a real Slurm cluster.",
                    )
            yield Footer()

        def action_save_settings(self) -> None:
            settings_dict = settings.__dict__
            for key, value in settings_dict.items():
                try:
                    new_value = self.query_one(f"#input_{key}").value
                    if new_value == "None" or new_value == "" or new_value == "null":
                        new_value = None
                    elif isinstance(value, bool):
                        pass  # this is to catch bool before int
                    elif isinstance(value, int) and new_value.isdigit():
                        new_value = int(new_value)
                    setattr(settings, key, new_value)
                except NoMatches:
                    pass
            settings.THEME = self.query_one("#label_THEME_VALUE").renderable
            settings.save()
            self.notify("Settings saved")
            self.dismiss()

        def action_do_nothing(self) -> None:
            pass

        def on_button_pressed(self, event: Button.Pressed) -> None:
            if event.button.id == "button_THEME":
                def set_theme(theme_name: str) -> None:
                    self.query_one("#label_THEME_VALUE").update(theme_name)

                self.app.push_screen(ThemeSelectionScreen(), set_theme)


    return SettingsScreen
