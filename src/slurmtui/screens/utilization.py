"""Live utilization monitoring screen with sparkline graphs."""

from __future__ import annotations

from collections import deque
from typing import Any, List, Optional

from rich.text import Text
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Footer, Header, Sparkline, Static

from ..monitor import (
    MonitorCapabilities,
    NodeMonitor,
    UtilSample,
    detect_capabilities,
    extract_gpu_indices,
)

# How many data points to keep in the sparkline history
HISTORY_LENGTH = 60


def _make_bar(pct: float, width: int = 30) -> str:
    """Create a Rich-markup progress bar."""
    pct = max(0.0, min(pct, 100.0))
    filled = int(pct / 100 * width)
    empty = width - filled
    if pct >= 90:
        color = "red"
    elif pct >= 70:
        color = "yellow"
    else:
        color = "green"
    bar = f"[{color}]{'━' * filled}[/{color}][dim]{'─' * empty}[/dim]"
    return f"{bar}  [bold]{pct:5.1f}%[/bold]"


def _format_bytes(b: float) -> str:
    """Format bytes to human-readable."""
    gb = b / (1024**3)
    if gb >= 1:
        return f"{gb:.1f}G"
    mb = b / (1024**2)
    return f"{mb:.0f}M"


def _format_mb(mb: float) -> str:
    if mb >= 1024:
        return f"{mb / 1024:.1f}G"
    return f"{mb:.0f}M"


class UtilizationScreen(ModalScreen[None]):
    """Live-updating utilization monitor for a node/job."""

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

    .util-section-title {
        text-style: bold;
        margin: 1 0 0 0;
    }

    .util-bar-label {
        height: 1;
        margin: 0 0 0 2;
    }

    .util-sparkline {
        height: 2;
        margin: 0 0 0 4;
    }

    .util-status {
        margin: 1 0;
        text-style: italic;
        color: $text-muted;
    }

    .gpu-section {
        margin: 0 0 0 2;
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

        # History for sparklines
        self._cpu_history: deque[float] = deque(maxlen=HISTORY_LENGTH)
        self._mem_history: deque[float] = deque(maxlen=HISTORY_LENGTH)
        # gpu index -> deque of util%
        self._gpu_util_history: dict[int, deque[float]] = {}
        self._gpu_mem_history: dict[int, deque[float]] = {}
        self._sample_count = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with VerticalScroll(id="util_container"):
            yield Static("Connecting...", id="status_label", classes="util-status")

            # CPU section
            yield Static("[bold]CPU[/bold]", classes="util-section-title")
            yield Static("", id="cpu_bar", classes="util-bar-label")
            yield Sparkline([], id="cpu_sparkline", classes="util-sparkline")

            # Memory section
            yield Static("[bold]Memory[/bold]", classes="util-section-title")
            yield Static("", id="mem_bar", classes="util-bar-label")
            yield Sparkline([], id="mem_sparkline", classes="util-sparkline")

            # GPU section - dynamically populated
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
        self._cpu_history.append(sample.cpu.usage_pct)
        self._mem_history.append(sample.mem.pct)

        for gpu in sample.gpus:
            if gpu.index not in self._gpu_util_history:
                self._gpu_util_history[gpu.index] = deque(maxlen=HISTORY_LENGTH)
                self._gpu_mem_history[gpu.index] = deque(maxlen=HISTORY_LENGTH)
            self._gpu_util_history[gpu.index].append(gpu.utilization_pct)
            self._gpu_mem_history[gpu.index].append(gpu.mem_pct)

        # Update UI from main thread
        self.app.call_from_thread(self._refresh_display, sample)

    def _update_status(self, text: str) -> None:
        try:
            self.query_one("#status_label", Static).update(text)
        except Exception:
            pass

    def _refresh_display(self, sample: UtilSample) -> None:
        """Update all UI widgets with the latest sample."""
        # CPU
        try:
            cpu_bar = self.query_one("#cpu_bar", Static)
            cpu_label = _make_bar(sample.cpu.usage_pct)
            if self.num_cpus:
                cpu_label += f"  [dim]({self.num_cpus} cores)[/dim]"
            elif self._caps and not self._caps.has_cgroup:
                cpu_label += "  [dim](node-level)[/dim]"
            cpu_bar.update(cpu_label)
        except Exception:
            pass

        try:
            cpu_spark = self.query_one("#cpu_sparkline", Sparkline)
            cpu_spark.data = list(self._cpu_history)
        except Exception:
            pass

        # Memory
        try:
            mem_bar = self.query_one("#mem_bar", Static)
            mem_label = _make_bar(sample.mem.pct)
            mem_label += f"  [dim]({_format_bytes(sample.mem.used_bytes)} / {_format_bytes(sample.mem.total_bytes)})[/dim]"
            mem_bar.update(mem_label)
        except Exception:
            pass

        try:
            mem_spark = self.query_one("#mem_sparkline", Sparkline)
            mem_spark.data = list(self._mem_history)
        except Exception:
            pass

        # GPUs
        if sample.gpus:
            self._refresh_gpus(sample)

    def _refresh_gpus(self, sample: UtilSample) -> None:
        """Update or create GPU widgets."""
        try:
            container = self.query_one("#gpu_container", Vertical)
        except Exception:
            return

        for gpu in sample.gpus:
            bar_id = f"gpu_{gpu.index}_bar"
            spark_id = f"gpu_{gpu.index}_spark"
            mem_bar_id = f"gpu_{gpu.index}_mem_bar"
            mem_spark_id = f"gpu_{gpu.index}_mem_spark"

            try:
                # Try to update existing widgets
                gpu_bar = self.query_one(f"#{bar_id}", Static)
                gpu_bar.update(
                    f"Util  {_make_bar(gpu.utilization_pct)}"
                )
                gpu_spark = self.query_one(f"#{spark_id}", Sparkline)
                gpu_spark.data = list(self._gpu_util_history.get(gpu.index, []))

                mem_bar = self.query_one(f"#{mem_bar_id}", Static)
                mem_bar.update(
                    f"VRAM  {_make_bar(gpu.mem_pct)}"
                    f"  [dim]({_format_mb(gpu.mem_used_mb)} / {_format_mb(gpu.mem_total_mb)})[/dim]"
                )
                mem_spark = self.query_one(f"#{mem_spark_id}", Sparkline)
                mem_spark.data = list(self._gpu_mem_history.get(gpu.index, []))
            except Exception:
                # Create new GPU widgets on first sample
                container.mount(
                    Static(
                        f"[bold]GPU {gpu.index}[/bold]",
                        classes="util-section-title",
                    )
                )
                container.mount(
                    Static(
                        f"Util  {_make_bar(gpu.utilization_pct)}",
                        id=bar_id,
                        classes="util-bar-label gpu-section",
                    )
                )
                container.mount(
                    Sparkline(
                        list(self._gpu_util_history.get(gpu.index, [])),
                        id=spark_id,
                        classes="util-sparkline",
                    )
                )
                container.mount(
                    Static(
                        f"VRAM  {_make_bar(gpu.mem_pct)}"
                        f"  [dim]({_format_mb(gpu.mem_used_mb)} / {_format_mb(gpu.mem_total_mb)})[/dim]",
                        id=mem_bar_id,
                        classes="util-bar-label gpu-section",
                    )
                )
                container.mount(
                    Sparkline(
                        list(self._gpu_mem_history.get(gpu.index, [])),
                        id=mem_spark_id,
                        classes="util-sparkline",
                    )
                )
