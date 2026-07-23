"""Definition of a single unit of work (job) handled by the simulator."""

from __future__ import annotations

from dataclasses import dataclass

from .enums import FlowTag


@dataclass(slots=True)
class Job:
    """One computational job flowing through the simulated system.

    Attributes:
        runtime: Actual processing time (units of simulated time).
        span: Number of service nodes the job occupies while running.
        walltime: Requested processing time; defaults to ``runtime``.
        flow: Name of the workload flow (stream) the job came from.
        label: Optional label (e.g., project name).
        arrived_at: Timestamp of arrival into the system.
        priority: Current priority value (grows with aging).
        tier: Site-policy group the job belongs to (if any).
        started_at: Timestamp when execution started (set by the deck).
    """

    runtime: float
    span: int
    walltime: float | None = None
    flow: str = FlowTag.DEFAULT
    label: str | None = None
    arrived_at: float | None = None
    priority: float = 0.0
    tier: int | None = None
    started_at: float | None = None

    def __post_init__(self):
        """Default the requested walltime to the actual runtime."""

        if self.walltime is None:
            self.walltime = self.runtime

    @property
    def finished_at(self) -> float | None:
        """Timestamp when the job releases its nodes (actual)."""

        if self.started_at is not None:
            return self.started_at + self.runtime
        return None

    @property
    def planned_finished_at(self) -> float | None:
        """Timestamp of the planned release (based on the walltime)."""

        if self.started_at is not None and self.walltime is not None:
            return self.started_at + self.walltime
        return None

    @property
    def waited(self) -> float | None:
        """Time the job spent waiting before execution started."""

        if self.started_at is not None and self.arrived_at is not None:
            return self.started_at - self.arrived_at
        return None

    @property
    def flow_time(self) -> float | None:
        """Total time in the system (waiting plus execution)."""

        waited = self.waited
        if waited is not None:
            return waited + self.runtime
        return None

    @property
    def tag(self) -> str:
        """Compact ``flow[:label]`` identifier used in reports."""

        return f'{self.flow}:{self.label}' if self.label else self.flow

    def boost(self, amount: float):
        """Age the job by increasing its priority.

        Args:
            amount: Priority increment (usually a time delta).
        """

        self.priority += amount
