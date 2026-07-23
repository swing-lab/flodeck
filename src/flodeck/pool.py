"""Pool of service nodes that execute jobs."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from .job import Job


class NodePool:
    """Pool of identical service nodes with job placements.

    Running jobs are kept ordered by their (actual) finish time, so the
    next completion event is always at the head of the list.
    """

    def __init__(self, size: int):
        """Initialize the node pool.

        Args:
            size: Number of service nodes.
        """

        self._size = size
        self._busy: list[bool] = [False] * size
        self._placements: list[tuple[Job, tuple[int, ...]]] = []
        self._flow_counts: Counter[str] = Counter()

    def clear(self):
        """Release all nodes and forget current placements."""

        self._busy = [False] * self._size
        self._placements.clear()
        self._flow_counts.clear()

    @property
    def size(self) -> int:
        """Total number of service nodes."""

        return self._size

    @property
    def busy_count(self) -> int:
        """Number of busy service nodes."""

        return sum(1 for busy in self._busy if busy)

    @property
    def idle_count(self) -> int:
        """Number of idle service nodes."""

        return self._size - self.busy_count

    @property
    def running_count(self) -> int:
        """Number of jobs currently executing."""

        return len(self._placements)

    @property
    def next_finished_at(self) -> float | None:
        """Timestamp of the closest job completion (None when idle)."""

        if self._placements:
            return self._placements[0][0].finished_at
        return None

    def counts_by_flow(self) -> list[tuple[str, int]]:
        """Get per-flow counts of executing jobs.

        Returns:
            Pairs of flow name and job count.
        """

        return list(self._flow_counts.items())

    def busy_until(self) -> dict[int, float]:
        """Get planned release timestamps for every busy node.

        The planned release is based on the requested walltime, i.e.,
        what a scheduler is allowed to assume; a job overrunning its
        walltime keeps its nodes busy until the actual finish.

        Returns:
            Map of node id to its planned release timestamp.
        """

        horizon = {}
        for job, node_ids in self._placements:
            planned = max(job.planned_finished_at, job.finished_at)
            for node_id in node_ids:
                horizon[node_id] = planned
        return horizon

    def fits(self, job: Job) -> bool:
        """Check whether enough idle nodes exist to launch a job.

        Args:
            job: Job object.

        Returns:
            True if the job can be launched now.
        """

        return self.idle_count >= job.span

    def _register(self, job: Job, node_ids: tuple[int, ...], now: float):
        """Record a placement and keep it ordered by finish time."""

        job.started_at = now
        self._placements.append((job, node_ids))
        self._placements.sort(key=lambda p: p[0].finished_at)
        self._flow_counts[job.flow] += 1

    def launch(self, job: Job, now: float):
        """Launch a job on the first available idle nodes.

        Args:
            job: Job object.
            now: Current simulated time.

        Raises:
            RuntimeError: If there are not enough idle nodes.
        """

        if not self.fits(job):
            raise RuntimeError('Job spans more nodes than are idle.')

        node_ids = []
        for node_id, busy in enumerate(self._busy):
            if not busy:
                self._busy[node_id] = True
                node_ids.append(node_id)
                if len(node_ids) == job.span:
                    break

        self._register(job, tuple(node_ids), now)

    def launch_on(self, job: Job, node_ids: Sequence[int], now: float):
        """Launch a job on explicitly chosen nodes.

        Args:
            job: Job object.
            node_ids: Ids of the nodes to occupy.
            now: Current simulated time.

        Raises:
            ValueError: If the node count does not match the job span.
            RuntimeError: If any requested node is already busy.
        """

        if len(node_ids) != job.span:
            raise ValueError('Provided nodes do not match the job span.')

        taken = []
        for node_id in node_ids:
            if self._busy[node_id]:
                for reverted in taken:
                    self._busy[reverted] = False
                raise RuntimeError('Requested node is already busy.')
            self._busy[node_id] = True
            taken.append(node_id)

        self._register(job, tuple(node_ids), now)

    def harvest(self, now: float) -> list[Job]:
        """Release nodes of jobs finishing exactly at this time.

        Args:
            now: Current simulated time.

        Returns:
            Finished jobs.
        """

        finished = []
        while self._placements and now == self.next_finished_at:

            job, node_ids = self._placements.pop(0)
            finished.append(job)

            self._flow_counts[job.flow] -= 1
            if not self._flow_counts[job.flow]:
                del self._flow_counts[job.flow]

            for node_id in node_ids:
                self._busy[node_id] = False

        return finished
