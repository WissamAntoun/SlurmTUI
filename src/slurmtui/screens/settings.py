
from typing import Any, List

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.css.query import NoMatches
from textual.screen import Screen
from textual.widgets import Footer, Header, Input, Static

from ..utils import SETTINGS, settings

SCREEN_BINDINGS = [
    Binding("ctrl+s", "save_settings", "Save Settings", key_display="Ctrl+S"),
    Binding("escape", "app.pop_screen", "Go Back", key_display="Esc"),
    Binding("b", "app.pop_screen", "Go Back", False),
    Binding("backspace", "app.pop_screen", "Go Back", False),
    Binding("q", "app.quit", "Quit", key_display="Q"),
]


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

    return SettingsScreen