from copy import deepcopy
from typing import Any

from rich.panel import Panel
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import VerticalScroll
from textual.css.query import NoMatches
from textual.screen import ModalScreen
from textual.widgets import Footer, Header, Static

from ..slurm_utils import (
    CommandNotFoundError,
    build_node_to_jobs,
    get_resources,
    get_running_jobs,
)
from ..utils import SETTINGS
from .sortable_data_table import SortableDataTable

BAR_WIDTH = 20


def _make_bar(used: int, total: int, width: int = BAR_WIDTH) -> str:
    """Create a Unicode progress bar with percentage."""
    if total == 0:
        return f"[dim]{'─' * width}[/dim]  [dim]N/A[/dim]"
    pct = used / total
    filled = int(pct * width)
    empty = width - filled
    if pct >= 0.9:
        color = "red"
    elif pct >= 0.7:
        color = "yellow"
    else:
        color = "green"
    bar = f"[{color}]{'━' * filled}[/{color}][dim]{'─' * empty}[/dim]"
    return f"{bar} [bold]{pct:>4.0%}[/bold]"


def _stat_items(items: list[tuple[str, int, str]]) -> str:
    """Build a dot-separated stat string, skipping zero values."""
    parts = []
    for label, value, color in items:
        if value:
            parts.append(f"[{color}]{value}[/{color}] {label}")
    return " · ".join(parts) if parts else "[dim]none[/dim]"


def _format_mem(mb: int) -> str:
    """Format memory in MB to a human-readable string."""
    if mb >= 1024:
        return f"{mb / 1024:.0f}G"
    return f"{mb}M"


def _state_color(state: str) -> str:
    """Return a Rich color name for a node state string."""
    s = state.upper()
    if "DOWN" in s or "DRAIN" in s or "NOT_RESPONDING" in s:
        return "red"
    if "IDLE" in s:
        return "cyan"
    if "MIXED" in s:
        return "yellow"
    if "ALLOCATED" in s:
        return "green"
    return "white"


# ── Partition card (clickable) ──────────────────────────────────────


class PartitionCard(Static):
    """A card widget displaying partition resource info with progress bars."""

    DEFAULT_CSS = """
        PartitionCard {
            height: auto;
            margin: 0 1 0 1;
        }
    """

    def __init__(
        self,
        name: str,
        data: dict,
        has_gpus: bool,
        node_to_jobs: dict,
        settings: SETTINGS,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.partition_name = name
        self.data = data
        self.has_gpus = has_gpus
        self.node_to_jobs = node_to_jobs
        self.settings = settings

    def render(self) -> Panel:
        d = self.data
        nodes_used = d["nodes_allocated"] + d["nodes_mixed"]
        cpus_used = d["cpus_allocated"]

        lines = []

        # --- Nodes ---
        node_stats = _stat_items(
            [
                ("allocated", d["nodes_allocated"], "green"),
                ("idle", d["nodes_idle"], "cyan"),
                ("mixed", d["nodes_mixed"], "yellow"),
                ("other", d["nodes_other"], "red"),
            ]
        )
        lines.append(
            f"  [bold]Nodes[/bold]  {_make_bar(nodes_used, d['nodes_total'])}"
            f"   {node_stats}  [dim]({d['nodes_total']} total)[/dim]"
        )

        # --- CPUs ---
        cpu_stats = _stat_items(
            [
                ("used", cpus_used, "green"),
                ("idle", d["cpus_idle"], "cyan"),
                ("other", d["cpus_other"], "red"),
            ]
        )
        lines.append(
            f"  [bold]CPUs [/bold]  {_make_bar(cpus_used, d['cpus_total'])}"
            f"   {cpu_stats}  [dim]({d['cpus_total']} total)[/dim]"
        )

        # --- GPUs / Features ---
        if d["gpus_total"] > 0:
            if d["gpu_type"] == "UNK":
                gpu_type_str = f"[dim]{d['features']}[/dim]"
            else:
                gpu_type_str = f"[bright_magenta]{d['gpu_type']}[/bright_magenta]"
            lines.append(
                f"  [bold]GPUs [/bold]  {gpu_type_str}"
                f"  ·  {d['gpus_per_node']}/node"
                f"  ·  [bold]{d['gpus_total']}[/bold] total"
            )
        elif d.get("features"):
            lines.append(f"  [bold]Info [/bold]  [dim]{d['features']}[/dim]")

        content = "\n".join(lines)
        return Panel(
            content,
            title=f"[bold]{self.partition_name}[/bold]",
            subtitle="[dim]click to expand[/dim]",
            border_style="cyan",
            expand=True,
            title_align="left",
            subtitle_align="left",
        )

    def on_click(self) -> None:
        self.app.push_screen(
            PartitionDetailScreen(
                self.partition_name, self.data, self.node_to_jobs, self.settings
            )
        )


# ── Partition detail screen ─────────────────────────────────────────


class PartitionDetailScreen(ModalScreen):
    """Detail view showing all nodes in a partition with job info."""

    BINDINGS = [
        Binding("i", "info", "Job Info", key_display="I"),
        Binding("escape", "screen.dismiss", "Go Back", key_display="Esc"),
        Binding("q", "quit", "Quit", key_display="Q"),
    ]

    CSS_PATH = "../css/slurmtui.css"

    def __init__(
        self,
        name: str,
        data: dict,
        node_to_jobs: dict,
        settings: SETTINGS,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.partition_name = name
        self.data = data
        self.node_to_jobs = node_to_jobs
        self.settings = settings
        # Ordered list of node names matching table rows
        self._node_names: list[str] = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield SortableDataTable(zebra_stripes=True, id="partition_detail_table")
        yield Footer()

    def on_mount(self) -> None:
        self.app.title = f"SlurmTUI: {self.partition_name}"

        table = self.query_one(SortableDataTable)
        table.cursor_type = "row"

        has_gres = any(ng["gres"] for ng in self.data["node_groups"])

        columns = [
            "Node",
            "State",
            "CPUs Used",
            "CPUs Total",
            "CPU %",
            "Memory",
            "Mem Used",
        ]
        if has_gres:
            columns.extend(["GRES", "GRES Used"])
        columns.append("Features")
        columns.extend(["Job ID", "User", "Job Name"])

        table.add_columns(*columns)

        self._node_names = []
        for ng in self.data["node_groups"]:
            node_name = ng["node"]
            self._node_names.append(node_name)
            cpu_pct = (
                f"{ng['cpus_allocated'] / ng['cpus_total'] * 100:.0f}%"
                if ng["cpus_total"] > 0
                else "N/A"
            )
            state_col = _state_color(ng["state"])

            # Cross-reference with running jobs
            jobs_on_node = self.node_to_jobs.get(node_name, [])
            if jobs_on_node:
                job_ids = ", ".join(str(j["job_id"]) for j in jobs_on_node)
                users = ", ".join(sorted(set(j["user"] for j in jobs_on_node)))
                names = ", ".join(sorted(set(j["name"] for j in jobs_on_node)))
            else:
                job_ids = ""
                users = ""
                names = ""

            row = [
                node_name,
                f"[{state_col}]{ng['state']}[/{state_col}]",
                str(ng["cpus_allocated"]),
                str(ng["cpus_total"]),
                cpu_pct,
                _format_mem(ng["mem_total_mb"]),
                _format_mem(ng["mem_alloc_mb"]),
            ]
            if has_gres:
                row.extend([ng["gres"], ng["gres_used"]])
            row.append(ng["features"])
            row.extend([job_ids, users, names])

            table.add_row(*row, key=node_name)

    def action_info(self) -> None:
        """Show full job info for the job running on the selected node."""
        try:
            table = self.query_one(SortableDataTable)
        except NoMatches:
            return

        if not self._node_names:
            return

        row_idx = table.cursor_coordinate.row
        if row_idx >= len(self._node_names):
            return

        node_name = self._node_names[row_idx]
        jobs_on_node = self.node_to_jobs.get(node_name, [])
        if not jobs_on_node:
            self.notify(f"No running jobs on {node_name}", severity="warning")
            return

        # Fetch the full job dict to pass to InfoScreen
        all_jobs = get_running_jobs(settings=self._get_all_jobs_settings())
        if not all_jobs or isinstance(all_jobs, CommandNotFoundError):
            self.notify("Could not fetch job details", severity="error")
            return

        job_id = jobs_on_node[0]["job_id"]
        job_info = all_jobs.get(job_id)
        if not job_info:
            self.notify(f"Job {job_id} no longer in queue", severity="warning")
            return

        from .info import InfoScreen

        self.app.push_screen(InfoScreen(job_info))

    def _get_all_jobs_settings(self) -> SETTINGS:
        """Return a copy of settings with CHECK_ALL_JOBS enabled."""
        s = deepcopy(self.settings)
        s.CHECK_ALL_JOBS = True
        return s

    def action_quit(self) -> None:
        from ..slurm_utils import SlurmTUIReturn

        self.app.exit(SlurmTUIReturn("quit", {}))


# ── Resources overview screen ───────────────────────────────────────


class ResourcesScreen(ModalScreen):

    BINDINGS = [
        Binding("escape", "screen.dismiss", "Go Back", key_display="Esc"),
        Binding("q", "quit", "Quit", key_display="Q"),
    ]

    CSS_PATH = "../css/slurmtui.css"

    def __init__(self, settings: SETTINGS, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.settings = settings

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        resources = get_resources(self.settings)

        if isinstance(resources, CommandNotFoundError):
            yield Static(f"[red]Error: {resources.message}[/red]")
        elif not resources:
            yield Static("[yellow]No resource information available[/yellow]")
        else:
            # Fetch all running jobs to cross-reference nodes
            all_jobs_settings = deepcopy(self.settings)
            all_jobs_settings.CHECK_ALL_JOBS = True
            all_jobs = get_running_jobs(settings=all_jobs_settings)
            if isinstance(all_jobs, CommandNotFoundError):
                all_jobs = None
            node_to_jobs = build_node_to_jobs(all_jobs)

            has_gpus = any(p["gpus_total"] > 0 for p in resources.values())
            with VerticalScroll():
                for name in sorted(resources):
                    yield PartitionCard(
                        name,
                        resources[name],
                        has_gpus,
                        node_to_jobs,
                        self.settings,
                    )

        yield Footer()

    def on_mount(self) -> None:
        resources = get_resources(self.settings)
        if resources and not isinstance(resources, CommandNotFoundError):
            self.app.title = f"SlurmTUI Resources: {len(resources)} partitions"
        else:
            self.app.title = "SlurmTUI Resources"

    def action_quit(self) -> None:
        from ..slurm_utils import SlurmTUIReturn

        self.app.exit(SlurmTUIReturn("quit", {}))
