from collections import deque

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import ModalScreen
from textual.widgets import Footer, Header, RichLog


class LogPeekScreen(ModalScreen[None]):
    """Quick popup that shows the last N lines of a log file."""

    DEFAULT_CSS = """
    LogPeekScreen {
        align: center middle;
        background: $background 80%;
    }

    #log_peek_container {
        width: 90%;
        height: 85%;
        border: thick $background 80%;
        background: $surface;
    }
    """

    BINDINGS = [
        Binding("escape", "app.pop_screen", "Close", key_display="Esc"),
        Binding("space", "app.pop_screen", "Close", key_display="Space"),
        Binding("q", "app.pop_screen", "Close", key_display="Q"),
    ]

    def __init__(self, log_path: str, num_lines: int, title: str = "Log Peek") -> None:
        super().__init__()
        self.log_path = log_path
        self.num_lines = num_lines
        self._title = title

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield RichLog(
            highlight=True,
            markup=False,
            auto_scroll=True,
            wrap=True,
            id="log_peek_container",
        )
        yield Footer()

    def on_mount(self) -> None:
        self.app.title = self._title
        rich_log = self.query_one(RichLog)
        try:
            with open(self.log_path, "r") as f:
                lines = deque(f, maxlen=self.num_lines)
            for line in lines:
                rich_log.write(line.rstrip("\n"))
        except Exception as e:
            rich_log.write(f"Error reading log file: {e}")
