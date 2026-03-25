"""Live utilization monitoring via SSH.

Provides backends for streaming CPU, RAM, and GPU utilization data from
compute nodes.  Supports job-scoped monitoring via cgroup v2 and
node-level fallback via /proc.

Architecture
------------
1. **Capability detection** (`detect_capabilities`):
   SSHes into the target node once and probes what data sources exist:
   - SSH reachable?
   - nvidia-smi present? (needed for GPU monitoring)
   - cgroup v2 job directory present? (needed for job-scoped CPU/RAM)

2. **Script generation** (`_build_monitor_script`):
   Builds a small shell loop that runs on the remote node via a single
   SSH connection.  Every `interval` seconds it prints tagged lines:
       cpu <fields from /proc/stat>      -- node-level CPU ticks
       MEM_TOTAL:<kB>  MEM_AVAIL:<kB>    -- node-level RAM from /proc/meminfo
       CGCPU:<usec>                       -- job CPU time from cgroup cpu.stat
       CGMEM:<bytes>  CGMEMMAX:<bytes>    -- job RAM from cgroup memory.*
       GPU:<idx>,<util%>,<mem_used>,<mem_total>  -- per-GPU from nvidia-smi
       ---END---                          -- marks end of one sample

3. **Streaming monitor** (`NodeMonitor`):
   Opens one long-lived `ssh <node> '<script>'` subprocess.  A daemon
   thread reads stdout line by line, accumulates lines between ---END---
   markers, parses each batch into a `UtilSample`, and calls `on_sample`.

4. **Data flow**:
   - CPU%: computed from the delta of two consecutive /proc/stat reads
     (or cgroup cpu.stat usage_usec deltas for job-scoped monitoring).
   - RAM: read directly from /proc/meminfo or cgroup memory.current/max.
   - GPU: parsed from nvidia-smi CSV output.  The `-i` flag filters to
     only the GPUs allocated to the job (indices from Slurm gres_detail).

5. **Fallback chain** (handled by the caller in utilization.py):
   cgroup v2 (job-scoped) -> /proc (node-level) -> error message.
   GPU monitoring is independent: present if nvidia-smi exists, absent
   otherwise.  CPU/RAM always work if SSH works.

6. **GPU index extraction** (`extract_gpu_indices`):
   Reads the Slurm job dict to find which GPU indices were allocated,
   so nvidia-smi only queries the job's GPUs (not the whole node).
   Tries gres_detail first, falls back to tres_alloc_str.

Future: ROCm support can be added as an alternative to nvidia-smi,
using `rocm-smi --showuse --json` with the same streaming pattern.
"""

from __future__ import annotations

import re
import subprocess
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional


# ── Data containers ───────────────────────────────────────────────


@dataclass
class CpuSample:
    usage_pct: float = 0.0


@dataclass
class MemSample:
    used_bytes: int = 0
    total_bytes: int = 0

    @property
    def pct(self) -> float:
        return (self.used_bytes / self.total_bytes * 100) if self.total_bytes else 0.0

    @property
    def used_gb(self) -> float:
        return self.used_bytes / (1024**3)

    @property
    def total_gb(self) -> float:
        return self.total_bytes / (1024**3)


@dataclass
class GpuSample:
    index: int = 0
    utilization_pct: float = 0.0
    mem_used_mb: float = 0.0
    mem_total_mb: float = 0.0
    power_draw_w: float = 0.0
    power_limit_w: float = 0.0

    @property
    def mem_pct(self) -> float:
        return (self.mem_used_mb / self.mem_total_mb * 100) if self.mem_total_mb else 0.0

    @property
    def power_pct(self) -> float:
        return (self.power_draw_w / self.power_limit_w * 100) if self.power_limit_w else 0.0


@dataclass
class UtilSample:
    cpu: CpuSample = field(default_factory=CpuSample)
    mem: MemSample = field(default_factory=MemSample)
    gpus: List[GpuSample] = field(default_factory=list)


@dataclass
class MonitorCapabilities:
    """What data sources are available on this node."""

    has_ssh: bool = False
    has_nvidia_smi: bool = False
    has_cgroup: bool = False
    cgroup_path: str = ""
    error: str = ""


# ── SSH helper ────────────────────────────────────────────────────


def _ssh_cmd(node: str) -> list[str]:
    return ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=5", node]


def _ssh_run(node: str, command: str, timeout: int = 10) -> Optional[str]:
    """Run a command via SSH and return stdout, or None on failure."""
    try:
        result = subprocess.run(
            _ssh_cmd(node) + [command],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode == 0:
            return result.stdout
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None


# ── Capability detection ──────────────────────────────────────────


def detect_capabilities(
    node: str, job_id: Optional[int] = None
) -> MonitorCapabilities:
    """Probe a node to determine what monitoring is available."""
    caps = MonitorCapabilities()

    # Test SSH connectivity
    test = _ssh_run(node, "echo ok", timeout=5)
    if test is None or "ok" not in test:
        caps.error = f"Cannot SSH into {node}"
        return caps
    caps.has_ssh = True

    # Test nvidia-smi
    nv_test = _ssh_run(node, "which nvidia-smi", timeout=5)
    caps.has_nvidia_smi = nv_test is not None and "nvidia-smi" in nv_test

    # Test cgroup v2 for this job
    if job_id is not None:
        cgroup_base = "/sys/fs/cgroup/system.slice/slurmstepd.scope"
        cgroup_path = f"{cgroup_base}/job_{job_id}"
        cg_test = _ssh_run(node, f"test -d {cgroup_path} && echo yes", timeout=5)
        if cg_test and "yes" in cg_test:
            caps.has_cgroup = True
            caps.cgroup_path = cgroup_path

    return caps


# ── Monitoring script builder ─────────────────────────────────────


def _build_monitor_script(
    caps: MonitorCapabilities,
    gpu_indices: Optional[List[int]] = None,
    interval: int = 2,
) -> str:
    """Build a shell script that streams utilization data.

    The script outputs tagged lines in a loop:
        STAT:<cpu line from /proc/stat>
        MEM_TOTAL:<kB>
        MEM_AVAIL:<kB>
        CGCPU:<usage_usec value>
        CGMEM:<bytes>
        CGMEMMAX:<bytes>
        GPU:<index>,<util%>,<mem_used_mb>,<mem_total_mb>
        ---END---
    """
    parts = []

    # CPU - always available via /proc/stat
    parts.append("head -1 /proc/stat")

    # Memory - always available via /proc/meminfo
    parts.append(
        r"awk '/^MemTotal:/{print \"MEM_TOTAL:\" $2} /^MemAvailable:/{print \"MEM_AVAIL:\" $2}' /proc/meminfo"
    )

    # Cgroup CPU/Mem (job-scoped)
    if caps.has_cgroup:
        cg = caps.cgroup_path
        parts.append(
            f'echo "CGCPU:$(grep usage_usec {cg}/cpu.stat 2>/dev/null | head -1 | awk \'{{print $2}}\')"'
        )
        parts.append(f'echo "CGMEM:$(cat {cg}/memory.current 2>/dev/null)"')
        parts.append(f'echo "CGMEMMAX:$(cat {cg}/memory.max 2>/dev/null)"')

    # GPU via nvidia-smi
    if caps.has_nvidia_smi:
        idx_flag = ""
        if gpu_indices:
            idx_flag = f"-i {','.join(str(i) for i in gpu_indices)}"
        parts.append(
            f"nvidia-smi {idx_flag} --query-gpu=index,utilization.gpu,memory.used,memory.total,power.draw,power.limit"
            " --format=csv,noheader,nounits 2>/dev/null"
            " | while IFS= read -r line; do echo \"GPU:$line\"; done"
        )

    body = "\n".join(parts)
    return f'while true; do\n{body}\necho "---END---"\nsleep {interval}\ndone'


# ── Streaming monitor ─────────────────────────────────────────────


class NodeMonitor:
    """Streams utilization data from a node via SSH.

    Usage:
        monitor = NodeMonitor(node, caps, on_sample=callback)
        monitor.start()
        ...
        monitor.stop()
    """

    def __init__(
        self,
        node: str,
        caps: MonitorCapabilities,
        on_sample: Callable[[UtilSample], None],
        gpu_indices: Optional[List[int]] = None,
        interval: int = 2,
        num_cpus: Optional[int] = None,
    ) -> None:
        self.node = node
        self.caps = caps
        self.on_sample = on_sample
        self.gpu_indices = gpu_indices
        self.interval = interval
        self.num_cpus = num_cpus
        self._process: Optional[subprocess.Popen] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._prev_cpu_total: int = 0
        self._prev_cpu_idle: int = 0
        self._prev_cg_cpu_usec: int = 0

    def start(self) -> None:
        script = _build_monitor_script(self.caps, self.gpu_indices, self.interval)
        try:
            self._process = subprocess.Popen(
                _ssh_cmd(self.node) + [script],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
            )
        except (FileNotFoundError, OSError):
            return

        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._process:
            try:
                self._process.terminate()
                self._process.wait(timeout=3)
            except Exception:
                try:
                    self._process.kill()
                except Exception:
                    pass
            self._process = None

    def _read_loop(self) -> None:
        """Read stdout from the SSH process and emit samples."""
        if not self._process or not self._process.stdout:
            return

        current_lines: List[str] = []

        for raw_line in self._process.stdout:
            if self._stop_event.is_set():
                break
            line = raw_line.strip()
            if line == "---END---":
                sample = self._parse_sample(current_lines)
                self.on_sample(sample)
                current_lines = []
            else:
                current_lines.append(line)

    def _parse_sample(self, lines: List[str]) -> UtilSample:
        sample = UtilSample()
        mem_total_kb = 0
        mem_avail_kb = 0
        cg_cpu_usec = 0
        cg_mem_bytes = 0
        cg_mem_max = 0

        for line in lines:
            if line.startswith("cpu "):
                # /proc/stat aggregate CPU line
                sample.cpu = self._parse_proc_cpu(line)

            elif line.startswith("MEM_TOTAL:"):
                try:
                    mem_total_kb = int(line.split(":")[1])
                except (ValueError, IndexError):
                    pass

            elif line.startswith("MEM_AVAIL:"):
                try:
                    mem_avail_kb = int(line.split(":")[1])
                except (ValueError, IndexError):
                    pass

            elif line.startswith("CGCPU:"):
                try:
                    cg_cpu_usec = int(line.split(":")[1])
                except (ValueError, IndexError):
                    pass

            elif line.startswith("CGMEM:"):
                try:
                    cg_mem_bytes = int(line.split(":")[1])
                except (ValueError, IndexError):
                    pass

            elif line.startswith("CGMEMMAX:"):
                val = line.split(":")[1]
                if val.strip() == "max":
                    cg_mem_max = 0
                else:
                    try:
                        cg_mem_max = int(val)
                    except (ValueError, IndexError):
                        pass

            elif line.startswith("GPU:"):
                gpu = self._parse_gpu_line(line[4:])
                if gpu:
                    sample.gpus.append(gpu)

        # Decide CPU/mem source: prefer cgroup (job-scoped) over /proc (node-level)
        if self.caps.has_cgroup and cg_cpu_usec > 0:
            sample.cpu = self._parse_cgroup_cpu(cg_cpu_usec)

        if self.caps.has_cgroup and cg_mem_bytes > 0:
            sample.mem = MemSample(
                used_bytes=cg_mem_bytes,
                total_bytes=cg_mem_max if cg_mem_max > 0 else mem_total_kb * 1024,
            )
        elif mem_total_kb > 0:
            used_kb = mem_total_kb - mem_avail_kb
            sample.mem = MemSample(
                used_bytes=used_kb * 1024,
                total_bytes=mem_total_kb * 1024,
            )

        return sample

    def _parse_proc_cpu(self, line: str) -> CpuSample:
        """Compute CPU% from /proc/stat delta."""
        parts = line.split()
        if len(parts) < 5:
            return CpuSample()

        # user, nice, system, idle, iowait, irq, softirq, steal
        values = [int(x) for x in parts[1:]]
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        total = sum(values)

        if self._prev_cpu_total == 0:
            # First sample, no delta yet
            self._prev_cpu_total = total
            self._prev_cpu_idle = idle
            return CpuSample(usage_pct=0.0)

        d_total = total - self._prev_cpu_total
        d_idle = idle - self._prev_cpu_idle
        self._prev_cpu_total = total
        self._prev_cpu_idle = idle

        if d_total == 0:
            return CpuSample(usage_pct=0.0)

        return CpuSample(usage_pct=(1.0 - d_idle / d_total) * 100)

    def _parse_cgroup_cpu(self, usage_usec: int) -> CpuSample:
        """Compute CPU% from cgroup cpu.stat usage_usec delta."""
        if self._prev_cg_cpu_usec == 0:
            self._prev_cg_cpu_usec = usage_usec
            return CpuSample(usage_pct=0.0)

        d_usec = usage_usec - self._prev_cg_cpu_usec
        self._prev_cg_cpu_usec = usage_usec

        # Convert to percentage: d_usec is wall-clock microseconds * cores used
        # Over self.interval seconds, 100% of 1 core = interval * 1e6 usec
        d_seconds = self.interval
        max_usec = d_seconds * 1_000_000
        if self.num_cpus:
            max_usec *= self.num_cpus

        if max_usec == 0:
            return CpuSample(usage_pct=0.0)

        return CpuSample(usage_pct=min(d_usec / max_usec * 100, 100.0))

    def _parse_gpu_line(self, line: str) -> Optional[GpuSample]:
        """Parse: index, util%, mem_used_mb, mem_total_mb, power_draw_w, power_limit_w"""
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 4:
            return None
        try:
            sample = GpuSample(
                index=int(parts[0]),
                utilization_pct=float(parts[1]),
                mem_used_mb=float(parts[2]),
                mem_total_mb=float(parts[3]),
            )
            # Power fields may not be available on all GPUs
            if len(parts) >= 6:
                try:
                    sample.power_draw_w = float(parts[4])
                    sample.power_limit_w = float(parts[5])
                except ValueError:
                    pass  # "[Not Supported]" on some GPUs
            return sample
        except (ValueError, IndexError):
            return None


# ── GPU index extraction from Slurm job data ─────────────────────


def extract_gpu_indices(job_dict: dict) -> Optional[List[int]]:
    """Extract allocated GPU indices from a Slurm job dictionary.

    Tries gres_detail first, then falls back to tres_alloc_str.
    Returns None if no GPU info is found.
    """
    # Try gres_detail: list of strings like "gpu:0", "gpu:h100:0-3", "gpu(IDX:0-1)"
    gres_detail = job_dict.get("gres_detail", [])
    if gres_detail:
        indices = []
        for entry in gres_detail:
            if not isinstance(entry, str):
                continue
            # Match patterns like IDX:0-3 or gpu:0 or gpu:h100:0,1
            idx_match = re.search(r"IDX:([0-9,\-]+)", entry)
            if idx_match:
                indices.extend(_expand_range(idx_match.group(1)))
            else:
                # Try trailing numbers: gpu:0 or gpu:h100:0
                num_match = re.search(r":(\d+(?:[,\-]\d+)*)$", entry)
                if num_match:
                    indices.extend(_expand_range(num_match.group(1)))
        if indices:
            return sorted(set(indices))

    # Try tres_alloc_str: "cpu=32,mem=64G,gres/gpu=4"
    # NOTE: tres_alloc_str only gives the count, not the actual indices.
    # We cannot guess which GPU indices were assigned, so return None
    # to let nvidia-smi query all GPUs on the node.
    return None


def _expand_range(s: str) -> List[int]:
    """Expand '0-3,5' into [0,1,2,3,5]."""
    result = []
    for part in s.split(","):
        if "-" in part:
            lo, hi = part.split("-", 1)
            result.extend(range(int(lo), int(hi) + 1))
        else:
            result.append(int(part))
    return result
