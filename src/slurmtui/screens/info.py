import json
from typing import Any, Dict, List

from rich.syntax import Syntax
from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer, Header, RichLog

INFO_SCREEN_BINDINGS = [
    Binding("s", "print_cli", "Print in CLI", key_display="S"),
    Binding("escape", "app.pop_screen", "Go Back", key_display="Esc"),
    Binding("b", "app.pop_screen", "Go Back", False),
    Binding("backspace", "app.pop_screen", "Go Back", False),
    Binding("q", "app.quit", "Quit", key_display="Q"),
]


def get_info_screen(OLD_BINDINGS: List[Binding]):
    """
    Returns the InfoScreen with the old bindings removed.
    """
    # Disable or replace the old bindings
    bindings = INFO_SCREEN_BINDINGS.copy()
    for old_binding in OLD_BINDINGS:
        for binding in INFO_SCREEN_BINDINGS:
            if old_binding.key != binding.key:
                bindings.append(
                    Binding(
                        old_binding.key,
                        "do_nothing",
                        "",
                        False,
                    )
                )

    class InfoScreen(Screen[str]):
        BINDINGS = bindings

        def __init__(self, info: Dict[str, Any], **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.info = info
            self.app.title = f"slurm Job Info: {self.info['job_id']}"

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True, name="slurm Job Info", id="info_header")
            yield RichLog(
                highlight=True,
                markup=False,
                auto_scroll=False,
                wrap=True,
                id="info_text",
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

    return InfoScreen
