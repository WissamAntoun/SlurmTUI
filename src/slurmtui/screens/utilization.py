"""Live utilization monitoring screen using textual-plot for charts."""

from __future__ import annotations

import numpy as np
from collections import deque
from typing import Any, List, Optional

from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.events import MouseScrollDown, MouseScrollUp
from textual.screen import ModalScreen
from textual.widgets import Footer, Header, Static
from textual_plot import HiResMode, PlotWidget

from ..monitor import (
    MonitorCapabilities,
    NodeMonitor,
    UtilSample,
    detect_capabilities,
    extract_gpu_indices,
)

# How many data points to keep in history
HISTORY_LENGTH = 120
SAMPLE_INTERVAL = 2  # seconds between samples


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


# ── Utilization chart wrapper ─────────────────────────────────────


class UtilChart(PlotWidget):
    """PlotWidget pre-configured for utilization time series."""

    DEFAULT_CSS = """
    UtilChart {
        width: 1fr;
        height: 14;
    }
    """

    def __init__(self, max_val: float = 100.0, **kwargs: Any) -> None:
        super().__init__(allow_pan_and_zoom=False, **kwargs)
        self.max_val = max_val
        self._series_data: list[tuple[str, str, deque[float]]] = []

    def zoom_in(self, event: MouseScrollDown) -> None:
        # Don't stop event — let it bubble to VerticalScroll for scrolling
        pass

    def zoom_out(self, event: MouseScrollUp) -> None:
        # Don't stop event — let it bubble to VerticalScroll for scrolling
        pass

    def on_mount(self) -> None:
        # Configure fixed axes
        self.set_ylimits(0, self.max_val)
        self.set_xlimits(-HISTORY_LENGTH * SAMPLE_INTERVAL, 0)
        self.set_xlabel("Time (s)")

    def update_series(
        self, series: list[tuple[str, str, deque[float]]]
    ) -> None:
        """Update chart data. series: list of (label, color, data_deque)."""
        self._series_data = series
        self.clear()

        for label, color, data in series:
            values = list(data)
            n = len(values)
            if n == 0:
                continue
            # X-axis: time in seconds ago (negative = past, 0 = now)
            x = np.arange(-n * SAMPLE_INTERVAL, 0, SAMPLE_INTERVAL)
            if len(x) > len(values):
                x = x[-len(values):]
            elif len(values) > len(x):
                values = values[-len(x):]
            y = np.array(values, dtype=float)
            self.plot(
                x=x,
                y=y,
                line_style=color,
                hires_mode=HiResMode.BRAILLE,
                label=label,
            )

        self.set_ylimits(0, self.max_val)
        self.set_xlimits(-HISTORY_LENGTH * SAMPLE_INTERVAL, 0)
        self.show_legend()


# ── Utilization screen ────────────────────────────────────────────


class UtilizationScreen(ModalScreen[None]):
    """Live-updating utilization monitor with textual-plot charts."""

    BINDINGS = [
        Binding("escape", "dismiss", "Close", key_display="Esc"),
        Binding("q", "quit_app", "Quit", key_display="Q"),
    ]

    DEFAULT_CSS = """
    UtilizationScreen {
        background: $background 100%;
    }

    #util_container {
        padding: 1 4 1 2;
    }

    .util-status {
        margin: 0 0 1 0;
        text-style: italic;
        color: $text-muted;
    }

    .chart-detail {
        height: 1;
        margin: 0 0 0 2;
        color: $text-muted;
    }

    #gpu_container {
        height: auto;
    }

    .charts-row {
        height: 14;
    }

    .chart-col {
        width: 1fr;
        height: 14;
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
        self._gpu_power_history: dict[int, deque[float]] = {}
        self._gpu_power_limits: dict[int, float] = {}
        self._gpu_initialized: set[int] = set()
        self._sample_count = 0
        self._last_sample: Optional[UtilSample] = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with VerticalScroll(id="util_container"):
            yield Static("Connecting...", id="status_label", classes="util-status")

            # CPU and RAM side by side
            with Horizontal(classes="charts-row"):
                with Vertical(classes="chart-col"):
                    yield Static("", id="cpu_detail", classes="chart-detail")
                    yield UtilChart(max_val=100.0, id="cpu_chart")
                with Vertical(classes="chart-col"):
                    yield Static("", id="mem_detail", classes="chart-detail")
                    yield UtilChart(max_val=100.0, id="mem_chart")

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
                self._gpu_power_history[gpu.index] = deque(maxlen=HISTORY_LENGTH)
            self._gpu_util_history[gpu.index].append(gpu.utilization_pct)
            self._gpu_mem_history[gpu.index].append(gpu.mem_pct)
            self._gpu_power_history[gpu.index].append(gpu.power_draw_w)
            if gpu.power_limit_w > 0:
                self._gpu_power_limits[gpu.index] = gpu.power_limit_w

        self.app.call_from_thread(self._refresh_display, sample)

    def _update_status(self, text: str) -> None:
        try:
            self.query_one("#status_label", Static).update(text)
        except Exception:
            pass

    def _refresh_display(self, sample: UtilSample) -> None:
        """Update all chart widgets with the latest data."""
        # CPU chart
        try:
            cpu_detail = self.query_one("#cpu_detail", Static)
            cpu_label = f"CPU: {sample.cpu.usage_pct:.1f}%"
            if self.num_cpus:
                cpu_label += f" ({self.num_cpus} cores)"
            cpu_detail.update(cpu_label)
        except Exception:
            pass

        try:
            cpu_chart = self.query_one("#cpu_chart", UtilChart)
            cpu_chart.update_series([
                ("CPU %", "cyan", self._cpu_history),
            ])
        except Exception:
            pass

        # RAM chart
        try:
            mem_detail = self.query_one("#mem_detail", Static)
            mem_detail.update(
                f"RAM: {sample.mem.pct:.1f}%"
                f" ({_format_bytes(sample.mem.used_bytes)}"
                f" / {_format_bytes(sample.mem.total_bytes)})"
            )
        except Exception:
            pass

        try:
            mem_chart = self.query_one("#mem_chart", UtilChart)
            mem_chart.update_series([
                ("RAM %", "yellow", self._mem_history),
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

        # First pass: create widgets for any new GPUs
        new_gpus = [g for g in sample.gpus if g.index not in self._gpu_initialized]
        if new_gpus:
            self._create_gpu_widgets(container, new_gpus)

        # Second pass: update all initialized GPUs
        for gpu in sample.gpus:
            if gpu.index not in self._gpu_initialized:
                continue
            self._update_gpu_widgets(gpu)

    def _create_gpu_widgets(self, container: Vertical, gpus: list) -> None:
        """Mount chart widgets for newly discovered GPUs."""
        for gpu in gpus:
            util_id = f"gpu_{gpu.index}_util_chart"
            mem_id = f"gpu_{gpu.index}_mem_chart"
            power_id = f"gpu_{gpu.index}_power_chart"
            detail_id = f"gpu_{gpu.index}_detail"

            power_limit = self._gpu_power_limits.get(gpu.index, 0)
            has_power = power_limit > 0

            detail_text = (
                f"GPU {gpu.index}:  "
                f"Util {gpu.utilization_pct:.0f}%  ·  "
                f"VRAM {gpu.mem_pct:.0f}%"
                f" ({_format_mb(gpu.mem_used_mb)}/{_format_mb(gpu.mem_total_mb)})"
            )
            if has_power:
                detail_text += f"  ·  Power {gpu.power_draw_w:.0f}W/{power_limit:.0f}W"

            detail_widget = Static(detail_text, id=detail_id, classes="chart-detail")
            util_chart = UtilChart(max_val=100.0, id=util_id)
            mem_chart = UtilChart(max_val=100.0, id=mem_id)

            row = Horizontal(classes="charts-row")
            col_util = Vertical(classes="chart-col")
            col_mem = Vertical(classes="chart-col")

            container.mount(detail_widget)
            container.mount(row)
            row.mount(col_util)
            row.mount(col_mem)
            col_util.mount(util_chart)
            col_mem.mount(mem_chart)

            if has_power:
                power_chart = UtilChart(
                    max_val=power_limit,
                    id=power_id,
                )
                container.mount(power_chart)

            self._gpu_initialized.add(gpu.index)

    def _update_gpu_widgets(self, gpu) -> None:
        """Update existing chart widgets for a GPU."""
        util_id = f"gpu_{gpu.index}_util_chart"
        mem_id = f"gpu_{gpu.index}_mem_chart"
        power_id = f"gpu_{gpu.index}_power_chart"
        detail_id = f"gpu_{gpu.index}_detail"

        power_limit = self._gpu_power_limits.get(gpu.index, 0)
        has_power = power_limit > 0

        try:
            self.query_one(f"#{util_id}", UtilChart).update_series([
                (f"GPU{gpu.index} util", "cyan", self._gpu_util_history[gpu.index]),
            ])
        except Exception:
            pass

        try:
            self.query_one(f"#{mem_id}", UtilChart).update_series([
                (f"GPU{gpu.index} VRAM", "yellow", self._gpu_mem_history[gpu.index]),
            ])
        except Exception:
            pass

        if has_power:
            try:
                self.query_one(f"#{power_id}", UtilChart).update_series([
                    (f"GPU{gpu.index} power", "magenta", self._gpu_power_history[gpu.index]),
                ])
            except Exception:
                pass

        try:
            detail = self.query_one(f"#{detail_id}", Static)
            detail_text = (
                f"GPU {gpu.index}:  "
                f"Util {gpu.utilization_pct:.0f}%  ·  "
                f"VRAM {gpu.mem_pct:.0f}%"
                f" ({_format_mb(gpu.mem_used_mb)}/{_format_mb(gpu.mem_total_mb)})"
            )
            if has_power:
                detail_text += f"  ·  Power {gpu.power_draw_w:.0f}W/{power_limit:.0f}W"
            detail.update(detail_text)
        except Exception:
            pass
