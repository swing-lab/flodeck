"""Waiting backlog: admission limits, ordering and the holding buffer."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import islice
from typing import TYPE_CHECKING

from .enums import BacklogScope, OrderingRule

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator, Mapping

    from .job import Job


@dataclass(frozen=True)
class BacklogRules:
    """Admission and ordering policy for the backlog.

    Attributes:
        ordering: Ordering rule (FIFO or priority with aging).
        total_limit: Maximum number of jobs in the backlog overall.
        per_flow_limit: Default per-flow limit (applied to any flow
            without an explicit entry in ``flow_limits``).
        flow_limits: Explicit per-flow limits, keyed by flow name.
        on_admit: Hook invoked for every admitted job (e.g., to seed
            its priority according to a site policy).
    """

    ordering: OrderingRule = OrderingRule.FIFO
    total_limit: int | None = None
    per_flow_limit: int | None = None
    flow_limits: Mapping[str, int] = field(default_factory=dict)
    on_admit: Callable[[Job], None] | None = None


class Backlog:
    """Jobs waiting for execution, plus an optional holding buffer.

    Jobs that do not pass the admission limits are either held in a
    per-flow buffer (promoted back as soon as a slot frees up) or
    dropped and counted, depending on ``hold_overflow``.
    """

    def __init__(self, rules: BacklogRules | None = None,
                 total_limit: int | None = None,
                 hold_overflow: bool = False):
        """Initialize the backlog.

        Args:
            rules: Admission and ordering policy (defaults to plain
                FIFO without limits).
            total_limit: Overall limit override; takes precedence over
                ``rules.total_limit`` when provided.
            hold_overflow: Hold rejected jobs in a buffer instead of
                dropping them.
        """

        self._rules = rules or BacklogRules()
        self._total_limit = (total_limit if total_limit is not None
                             else self._rules.total_limit)

        self._line: list[Job] = []
        self._flow_counts: Counter[str] = Counter()
        self._last_offer_at = 0.

        self._held: dict[str, list[Job]] | None = (
            defaultdict(list) if hold_overflow else None)
        self._drops: Counter[str] | None = (
            None if hold_overflow else Counter())

    def clear(self):
        """Forget all waiting/held jobs and reset the counters."""

        self._line.clear()
        self._flow_counts.clear()
        self._last_offer_at = 0.

        if self._held is not None:
            self._held.clear()
        if self._drops is not None:
            self._drops.clear()

    def __len__(self) -> int:
        """Number of jobs waiting in the backlog (buffer excluded)."""

        return len(self._line)

    @property
    def is_empty(self) -> bool:
        """Whether the backlog has no waiting jobs."""

        return not self._line

    @property
    def held_count(self) -> int:
        """Number of jobs kept in the holding buffer."""

        if self._held is None:
            return 0
        return sum(len(jobs) for jobs in self._held.values())

    @property
    def total_count(self) -> int:
        """Number of jobs waiting or held."""

        return len(self._line) + self.held_count

    @property
    def dropped_count(self) -> int:
        """Total number of dropped jobs."""

        return sum(self._drops.values()) if self._drops else 0

    def count_for_flow(
        self, flow: str, scope: BacklogScope = BacklogScope.LINE,
    ) -> int:
        """Get the number of jobs of one flow.

        Args:
            flow: Flow name.
            scope: Which part of the backlog to count.

        Returns:
            Number of jobs.
        """

        if scope is BacklogScope.HELD:
            if self._held is None:
                return 0
            return len(self._held.get(flow, ()))
        if scope is BacklogScope.ALL:
            held = 0
            if self._held is not None:
                held = len(self._held.get(flow, ()))
            return self._flow_counts[flow] + held
        return self._flow_counts[flow]

    def counts_by_flow(
        self, scope: BacklogScope = BacklogScope.LINE,
    ) -> list[tuple[str, int]]:
        """Get per-flow job counts.

        Args:
            scope: Which part of the backlog to report.

        Returns:
            Pairs of flow name and job count.
        """

        if scope is BacklogScope.HELD:
            if self._held is None:
                return []
            return [(flow, len(jobs)) for flow, jobs in self._held.items()]
        if scope is BacklogScope.ALL:
            merged: Counter[str] = Counter(self._flow_counts)
            if self._held is not None:
                for flow, jobs in self._held.items():
                    merged[flow] += len(jobs)
            return list(merged.items())
        return list(self._flow_counts.items())

    def dropped_by_flow(self) -> list[tuple[str, int]]:
        """Get per-flow counts of dropped jobs.

        Returns:
            Pairs of flow name and dropped-job count.
        """

        return list(self._drops.items()) if self._drops else []

    def dropped_for_flow(self, flow: str) -> int:
        """Get the number of dropped jobs of one flow.

        Args:
            flow: Flow name.

        Returns:
            Number of dropped jobs.
        """

        return self._drops[flow] if self._drops else 0

    def _flow_limit_for(self, flow: str) -> int | None:
        """Resolve the applicable per-flow limit for a flow name."""

        if flow in self._rules.flow_limits:
            return self._rules.flow_limits[flow]
        return self._rules.per_flow_limit

    def _admit(self, job: Job, now: float):
        """Insert an approved job into the waiting line."""

        if self._rules.ordering == OrderingRule.PRIORITY:
            aging = now - self._last_offer_at
            for waiting in self._line:
                waiting.boost(aging)

        if self._rules.on_admit is not None:
            self._rules.on_admit(job)

        if self._rules.ordering == OrderingRule.PRIORITY:
            position = 0
            for idx in range(len(self._line) - 1, -1, -1):
                if self._line[idx].priority >= job.priority:
                    position = idx + 1
                    break
            self._line.insert(position, job)
        else:
            self._line.append(job)

        self._flow_counts[job.flow] += 1
        self._last_offer_at = now

    def _turn_away(self, job: Job):
        """Hold or drop a job that failed the admission limits."""

        if self._held is not None:
            self._held[job.flow].append(job)
        elif self._drops is not None:
            self._drops[job.flow] += 1

    def offer(self, job: Job, now: float) -> bool:
        """Offer a job for admission into the backlog.

        Args:
            job: Job object.
            now: Current simulated time.

        Returns:
            True if the job was admitted, False if it was held/dropped.
        """

        limited, fits = False, True

        if self._total_limit is not None:
            limited = True
            fits = len(self._line) < self._total_limit

        flow_limit = self._flow_limit_for(job.flow)
        if fits and flow_limit is not None:
            limited = True
            fits = self._flow_counts[job.flow] < flow_limit

        if not limited or fits:
            self._admit(job, now)
            return True

        self._turn_away(job)
        return False

    def peek(self) -> Job:
        """Show the next job without removing it from the backlog."""

        return self._line[0]

    def peek_last(self) -> Job:
        """Show the last job without removing it from the backlog."""

        return self._line[-1]

    def scan(self, limit: int | None = None) -> Iterator[Job]:
        """Iterate over the waiting jobs (in queue order).

        Args:
            limit: Number of jobs to go through (all when None).

        Returns:
            Iterator over jobs.
        """

        return islice(self._line, limit)

    def _after_take(self, flow: str, now: float):
        """Update counters and promote a held job of the same flow."""

        self._flow_counts[flow] -= 1
        if not self._flow_counts[flow]:
            del self._flow_counts[flow]

        if self._held is not None and self._held.get(flow):
            promoted = self._held[flow].pop(0)
            if not self._held[flow]:
                del self._held[flow]
            self.offer(promoted, now)

    def take_next(self, now: float) -> Job:
        """Remove and return the next job from the backlog.

        Args:
            now: Current simulated time.

        Returns:
            Job object.
        """

        job = self._line.pop(0)
        self._after_take(job.flow, now)
        return job

    def take_by_key(self, key: int, now: float) -> Job:
        """Remove and return a particular job identified by its key.

        Args:
            key: Identity key of the job (as returned by ``id()``).
            now: Current simulated time.

        Returns:
            Job object.

        Raises:
            LookupError: If no waiting job matches the key.
        """

        for idx, job in enumerate(self._line):
            if id(job) == key:
                self._line.pop(idx)
                self._after_take(job.flow, now)
                return job

        raise LookupError('Job is not found in the backlog.')
