
from typing import Any, List

from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Grid
from textual.screen import Screen
from textual.widgets import Button, Footer, Label

CONFITM_SCREEN_BINDINGS = [
    Binding("y", "yes", "Yes", key_display="Y"),
    Binding("n", "no", "No", key_display="N"),
    Binding("escape", "app.pop_screen", "Go Back", key_display="Esc"),
    Binding("b", "app.pop_screen", "Go Back", False),
    Binding("backspace", "app.pop_screen", "Go Back", False),
    Binding("q", "app.quit", "Quit", key_display="Q"),
    Binding("left", "app.focus_next", "Focus Next", show=False),
    Binding("right", "app.focus_previous", "Focus Previous", show=False),
]


def get_confirm_screen(OLD_BINDINGS: List[Binding]):
    """
    Returns the ConfirmScreen with the old bindings removed.
    """
    # Disable or replace the old bindings
    bindings = CONFITM_SCREEN_BINDINGS.copy()
    for old_binding in OLD_BINDINGS:
        for binding in CONFITM_SCREEN_BINDINGS:
            if old_binding.key != binding.key:
                bindings.append(
                    Binding(
                        old_binding.key,
                        "do_nothing",
                        "",
                        False,
                    )
                )


    class ConfirmScreen(Screen[bool]):
        """Screen with confirm a dialog."""

        BINDINGS = bindings

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

    return ConfirmScreen