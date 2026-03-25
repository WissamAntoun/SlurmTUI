"""Live utilization monitoring screen with nvtop-style area charts."""

from __future__ import annotations

from collections import deque
from typing import Any, List, Optional, Sequence, Tuple

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import Footer, Header, Static

from ..monitor import (
    MonitorCapabilities,
    NodeMonitor,
    UtilSample,
    detect_capabilities,
    extract_gpu_indices,
)

# How many data points to keep in history
HISTORY_LENGTH = 120
# Chart defaults
DEFAULT_CHART_HEIGHT = 8
SAMPLE_INTERVAL = 2  # seconds between samples

# Block characters for filled area (index 0 = empty, 8 = full)
_BLOCKS = [" ", "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"]


def _format_bytes(b: float) -> str:
    gb = b / (1024**3)
    if gb >= 1:
        return f"{gb:.1f}G"
    mb = b / (1024**2)
    return f"{mb:.0f}M"


def _format_mb(mb: float) -> str:
    if mb >= 1024:
        return f"{mb / 1024:.1f}G"
    return f"{mb:.0f}M"


# ── Area chart rendering ──────────────────────────────────────────


def _render_area_chart(
    chart_width: int,
    chart_height: int,
    series: Sequence[Tuple[str, str, Sequence[float]]],
    max_val: float = 100.0,
    interval: int = SAMPLE_INTERVAL,
) -> Text:
    """Render an nvtop-style filled area chart.

    Args:
        chart_width: width of the chart area (excluding Y-axis).
        chart_height: height in rows.
        series: list of (label, rich_color, data_points).
            Drawn back to front — last series renders on top.
        max_val: the value that maps to 100% height.
        interval: seconds between data points (for X-axis labels).

    Returns:
        Rich Text renderable.
    """
    if chart_width < 10:
        chart_width = 10
    if chart_height < 4:
        chart_height = 4

    # Grid of (character, color) — row 0 is top
    grid: list[list[tuple[str, str]]] = [
        [(" ", "dim")] * chart_width for _ in range(chart_height)
    ]

    # Draw each series (first series is background, last is foreground)
    for _label, color, data in series:
        values = list(data)
        # Right-align: most recent data on the right
        if len(values) < chart_width:
            values = [0.0] * (chart_width - len(values)) + values
        else:
            values = values[-chart_width:]

        for col in range(chart_width):
            v = max(0.0, min(values[col], max_val))
            # How many sub-rows to fill (each row = 8 sub-levels)
            fill = v / max_val * chart_height * 8 if max_val > 0 else 0

            for row in range(chart_height):
                # row 0 = top row, row (chart_height-1) = bottom row
                row_bottom_sublevel = (chart_height - 1 - row) * 8
                row_top_sublevel = row_bottom_sublevel + 8

                if fill >= row_top_sublevel:
                    grid[row][col] = ("█", color)
                elif fill > row_bottom_sublevel:
                    level = int(fill - row_bottom_sublevel)
                    level = max(0, min(level, 8))
                    grid[row][col] = (_BLOCKS[level], color)
                # else: leave whatever was drawn by a previous series

    # Build Rich Text output
    text = Text()

    # ── Legend ──
    for i, (label, color, _data) in enumerate(series):
        if i > 0:
            text.append("  ")
        text.append("█ ", style=color)
        text.append(label, style=color + " bold")
    text.append("\n")

    # ── Y-axis + chart rows ──
    y_positions = {}
    if chart_height >= 4:
        y_positions[0] = "100"
        y_positions[chart_height - 1] = "  0"
    if chart_height >= 6:
        y_positions[chart_height // 2] = " 50"
    if chart_height >= 8:
        y_positions[chart_height // 4] = " 75"
        y_positions[3 * chart_height // 4] = " 25"

    for row in range(chart_height):
        if row in y_positions:
            text.append(f"{y_positions[row]:>3}", style="dim")
        else:
            text.append("   ", style="dim")
        text.append("│", style="dim")

        for col in range(chart_width):
            char, color = grid[row][col]
            text.append(char, style=color)
        text.append("\n")

    # ── X-axis line ──
    text.append("   └", style="dim")
    text.append("─" * chart_width, style="dim")
    text.append("\n")

    # ── Time labels ──
    total_secs = chart_width * interval
    time_line = list(" " * chart_width)

    # Place ~5 labels evenly across the axis
    n_labels = 5
    for i in range(n_labels):
        frac = i / (n_labels - 1)
        secs = int(total_secs * (1 - frac))
        label = f"{secs}s"
        pos = int(frac * (chart_width - len(label)))
        pos = max(0, min(pos, chart_width - len(label)))
        for j, c in enumerate(label):
            if pos + j < chart_width:
                time_line[pos + j] = c

    text.append("    ", style="dim")
    text.append("".join(time_line), style="dim")

    return text


# ── Area chart widget ─────────────────────────────────────────────


class AreaChart(Widget):
    """A widget that displays an nvtop-style filled area chart."""

    DEFAULT_CSS = """
    AreaChart {
        width: 1fr;
        height: auto;
        min-height: 12;
        padding: 0 1;
    }
    """

    def __init__(
        self,
        chart_height: int = DEFAULT_CHART_HEIGHT,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.chart_height = chart_height
        # List of (label, color, deque) — set via update_series()
        self._series: list[tuple[str, str, deque[float]]] = []

    def update_series(
        self, series: list[tuple[str, str, deque[float]]]
    ) -> None:
        self._series = series
        self.refresh()

    def render(self) -> Text:
        if not self._series:
            return Text("No data yet...")
        chart_width = self.size.width - 6  # 3 for Y-axis + "│" + padding
        return _render_area_chart(
            chart_width=max(chart_width, 10),
            chart_height=self.chart_height,
            series=[(l, c, list(d)) for l, c, d in self._series],
        )


# ── Utilization screen ────────────────────────────────────────────


class UtilizationScreen(ModalScreen[None]):
    """Live-updating utilization monitor with nvtop-style area charts."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close", key_display="Esc"),
        Binding("q", "quit_app", "Quit", key_display="Q"),
    ]

    DEFAULT_CSS = """
    UtilizationScreen {
        background: $background 100%;
    }

    #util_container {
        padding: 1 2;
    }

    .util-status {
        margin: 0 0 1 0;
        text-style: italic;
        color: $text-muted;
    }

    .chart-label {
        text-style: bold;
        margin: 1 0 0 0;
        height: 1;
    }

    .chart-detail {
        height: 1;
        margin: 0 0 0 2;
        color: $text-muted;
    }

    AreaChart {
        margin: 0 0 0 0;
    }

    .gpu-charts-row {
        height: auto;
    }
    """

    def __init__(
        self,
        node: str,
        job_id: Optional[int] = None,
        job_name: Optional[str] = None,
        gpu_indices: Optional[List[int]] = None,
        num_cpus: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.node = node
        self.job_id = job_id
        self.job_name = job_name
        self.gpu_indices = gpu_indices
        self.num_cpus = num_cpus
        self._monitor: Optional[NodeMonitor] = None
        self._caps: Optional[MonitorCapabilities] = None

        # History deques
        self._cpu_history: deque[float] = deque(maxlen=HISTORY_LENGTH)
        self._mem_history: deque[float] = deque(maxlen=HISTORY_LENGTH)
        self._gpu_util_history: dict[int, deque[float]] = {}
        self._gpu_mem_history: dict[int, deque[float]] = {}
        self._sample_count = 0
        self._last_sample: Optional[UtilSample] = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with VerticalScroll(id="util_container"):
            yield Static("Connecting...", id="status_label", classes="util-status")

            # CPU + Memory chart
            yield Static("[bold]CPU / Memory[/bold]", classes="chart-label")
            yield Static("", id="cpu_mem_detail", classes="chart-detail")
            yield AreaChart(chart_height=DEFAULT_CHART_HEIGHT, id="cpu_mem_chart")

            # GPU charts container — populated dynamically
            yield Vertical(id="gpu_container")

        yield Footer()

    def on_mount(self) -> None:
        title = f"Utilization: {self.node}"
        if self.job_id:
            title = f"Utilization: Job {self.job_id}"
            if self.job_name:
                title += f" ({self.job_name})"
            title += f" on {self.node}"
        self.app.title = title
        self._start_monitoring()

    def on_unmount(self) -> None:
        if self._monitor:
            self._monitor.stop()

    def action_dismiss(self) -> None:
        if self._monitor:
            self._monitor.stop()
        self.dismiss()

    def action_quit_app(self) -> None:
        if self._monitor:
            self._monitor.stop()
        from ..slurm_utils import SlurmTUIReturn

        self.app.exit(SlurmTUIReturn("quit", {}))

    @work(thread=True)
    def _start_monitoring(self) -> None:
        """Detect capabilities and start the monitor (runs in worker thread)."""
        self.app.call_from_thread(self._update_status, "Probing node capabilities...")

        caps = detect_capabilities(self.node, self.job_id)
        self._caps = caps

        if not caps.has_ssh:
            self.app.call_from_thread(
                self._update_status,
                f"[red]Cannot connect: {caps.error}[/red]\n\n"
                "SSH access to compute nodes is required for live monitoring.",
            )
            return

        status_parts = []
        if caps.has_cgroup:
            status_parts.append("[green]cgroup[/green] (job-scoped CPU/RAM)")
        else:
            status_parts.append("[yellow]/proc[/yellow] (node-level CPU/RAM)")
        if caps.has_nvidia_smi:
            gpu_note = ""
            if self.gpu_indices:
                gpu_note = f" GPUs: {','.join(str(i) for i in self.gpu_indices)}"
            status_parts.append(f"[green]nvidia-smi[/green]{gpu_note}")
        else:
            status_parts.append("[dim]no GPU monitoring (nvidia-smi not found)[/dim]")

        status = "Sources: " + " · ".join(status_parts) + "  |  Refresh: 2s"
        self.app.call_from_thread(self._update_status, status)

        self._monitor = NodeMonitor(
            node=self.node,
            caps=caps,
            on_sample=self._on_sample,
            gpu_indices=self.gpu_indices,
            interval=2,
            num_cpus=self.num_cpus,
        )
        self._monitor.start()

    def _on_sample(self, sample: UtilSample) -> None:
        """Called from the monitor thread with each new data sample."""
        self._sample_count += 1
        self._last_sample = sample
        self._cpu_history.append(sample.cpu.usage_pct)
        self._mem_history.append(sample.mem.pct)

        for gpu in sample.gpus:
            if gpu.index not in self._gpu_util_history:
                self._gpu_util_history[gpu.index] = deque(maxlen=HISTORY_LENGTH)
                self._gpu_mem_history[gpu.index] = deque(maxlen=HISTORY_LENGTH)
            self._gpu_util_history[gpu.index].append(gpu.utilization_pct)
            self._gpu_mem_history[gpu.index].append(gpu.mem_pct)

        self.app.call_from_thread(self._refresh_display, sample)

    def _update_status(self, text: str) -> None:
        try:
            self.query_one("#status_label", Static).update(text)
        except Exception:
            pass

    def _refresh_display(self, sample: UtilSample) -> None:
        """Update all chart widgets with the latest data."""
        # CPU/Memory detail line
        try:
            detail = self.query_one("#cpu_mem_detail", Static)
            parts = [f"CPU: {sample.cpu.usage_pct:.1f}%"]
            if self.num_cpus:
                parts[0] += f" ({self.num_cpus} cores)"
            parts.append(
                f"RAM: {sample.mem.pct:.1f}%"
                f" ({_format_bytes(sample.mem.used_bytes)}"
                f" / {_format_bytes(sample.mem.total_bytes)})"
            )
            detail.update("  ·  ".join(parts))
        except Exception:
            pass

        # CPU/Memory chart
        try:
            chart = self.query_one("#cpu_mem_chart", AreaChart)
            chart.update_series([
                ("CPU %", "cyan", self._cpu_history),
                ("RAM %", "dark_orange", self._mem_history),
            ])
        except Exception:
            pass

        # GPU charts
        if sample.gpus:
            self._refresh_gpus(sample)

    def _refresh_gpus(self, sample: UtilSample) -> None:
        """Update or create GPU chart widgets."""
        try:
            container = self.query_one("#gpu_container", Vertical)
        except Exception:
            return

        # Check how many GPUs we have to decide layout
        n_gpus = len(sample.gpus)
        use_pairs = n_gpus >= 2

        for i, gpu in enumerate(sample.gpus):
            chart_id = f"gpu_{gpu.index}_chart"
            detail_id = f"gpu_{gpu.index}_detail"
            row_id = f"gpu_row_{i // 2}"

            try:
                # Update existing chart
                chart = self.query_one(f"#{chart_id}", AreaChart)
                chart.update_series([
                    (f"GPU{gpu.index} %", "cyan", self._gpu_util_history[gpu.index]),
                    (f"GPU{gpu.index} mem%", "dark_orange", self._gpu_mem_history[gpu.index]),
                ])
                detail = self.query_one(f"#{detail_id}", Static)
                detail.update(
                    f"Util: {gpu.utilization_pct:.1f}%  ·  "
                    f"VRAM: {gpu.mem_pct:.1f}%"
                    f" ({_format_mb(gpu.mem_used_mb)} / {_format_mb(gpu.mem_total_mb)})"
                )
            except Exception:
                # Create chart for this GPU
                label_widget = Static(
                    f"[bold]GPU {gpu.index}[/bold]",
                    classes="chart-label",
                )
                detail_widget = Static(
                    f"Util: {gpu.utilization_pct:.1f}%  ·  "
                    f"VRAM: {gpu.mem_pct:.1f}%"
                    f" ({_format_mb(gpu.mem_used_mb)} / {_format_mb(gpu.mem_total_mb)})",
                    id=detail_id,
                    classes="chart-detail",
                )
                new_chart = AreaChart(
                    chart_height=DEFAULT_CHART_HEIGHT,
                    id=chart_id,
                )

                if use_pairs and i % 2 == 0:
                    # Start a new Horizontal row for pairs
                    row = Horizontal(id=row_id, classes="gpu-charts-row")
                    container.mount(row)
                    inner = Vertical()
                    row.mount(inner)
                    inner.mount(label_widget)
                    inner.mount(detail_widget)
                    inner.mount(new_chart)
                elif use_pairs and i % 2 == 1:
                    # Add to existing row
                    try:
                        row = self.query_one(f"#{row_id}", Horizontal)
                        inner = Vertical()
                        row.mount(inner)
                        inner.mount(label_widget)
                        inner.mount(detail_widget)
                        inner.mount(new_chart)
                    except Exception:
                        container.mount(label_widget)
                        container.mount(detail_widget)
                        container.mount(new_chart)
                else:
                    # Single GPU per row
                    container.mount(label_widget)
                    container.mount(detail_widget)
                    container.mount(new_chart)

                # Trigger initial render
                new_chart.update_series([
                    (f"GPU{gpu.index} %", "cyan", self._gpu_util_history.get(gpu.index, deque())),
                    (f"GPU{gpu.index} mem%", "dark_orange", self._gpu_mem_history.get(gpu.index, deque())),
                ])
