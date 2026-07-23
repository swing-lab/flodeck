"""Backfill scheduler that plans job start times over node timelines."""

from __future__ import annotations

import math

from bisect import insort_left
from heapq import heappop, heappush
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from .job import Job


class NodeTimeline:
    """Reserved busy windows of a single node, ordered in time."""

    def __init__(self):
        """Initialize an empty timeline."""

        self._windows: list[tuple[float, float]] = []

    def clear(self):
        """Drop all reservations."""

        self._windows.clear()

    def open_slots(self, span_time: float,
                   now: float) -> Iterator[tuple[float, int]]:
        """Yield start-window markers where a job of a given length fits.

        Each idle gap long enough for ``span_time`` produces an opening
        marker ``(gap_start, 1)`` and a closing marker
        ``(latest_feasible_start, -1)``; the tail of the timeline
        produces a final opening marker.

        Args:
            span_time: Required processing time (walltime).
            now: Current simulated time.

        Yields:
            Tuples of timestamp and +1/-1 marker value.
        """

        cursor = now
        for start, end in self._windows:

            if cursor < start and start - cursor >= span_time:
                yield (cursor, 1)
                yield (start - span_time, -1)
            elif end < cursor:
                continue

            cursor = end

        yield (cursor, 1)

    def book(self, start: float, end: float):
        """Reserve a busy window, merging with adjacent reservations.

        Args:
            start: Reservation start timestamp.
            end: Reservation end timestamp.

        Raises:
            ValueError: If the window overlaps an existing reservation.
        """

        prev_end = -math.inf
        idx = 0
        while True:

            window = (self._windows[idx]
                      if idx < len(self._windows) else None)

            if prev_end > start or (window is not None
                                    and start <= window[0] < end):
                raise ValueError('Window overlaps a reservation.')

            if prev_end < start:
                if window is None or end < window[0]:
                    self._windows.insert(idx, (start, end))
                    return
                if end == window[0]:
                    self._windows[idx] = (start, window[1])
                    return
            else:  # prev_end == start: extend the previous window
                if window is None or end < window[0]:
                    self._windows[idx - 1] = (self._windows[idx - 1][0],
                                              end)
                    return
                if end == window[0]:
                    self._windows[idx - 1] = (
                        self._windows[idx - 1][0],
                        self._windows.pop(idx)[1])
                    return

            prev_end = window[1]
            idx += 1


class Scheduler:
    """Cumulative schedule of planned job starts across all nodes."""

    def __init__(self, num_nodes: int):
        """Initialize the scheduler.

        Args:
            num_nodes: Number of service nodes.
        """

        self._now = 0.
        self._timelines = [NodeTimeline() for _ in range(num_nodes)]
        # entries: (start_timestamp, job_key, node_ids)
        self._planned: list[tuple[float, int, tuple[int, ...]]] = []

    @property
    def next_start(self) -> float | None:
        """Timestamp of the earliest planned start (None when empty)."""

        if self._planned:
            return self._planned[0][0]
        return None

    def has_due(self, now: float) -> bool:
        """Check whether any job is planned to start at this time.

        Args:
            now: Current simulated time.

        Returns:
            True if at least one planned start is due.
        """

        return now == self.next_start

    def is_fast_tracked(self, job_key: int) -> bool:
        """Check whether a job is planned to start right now (backfill).

        Args:
            job_key: Identity key of the job (as returned by ``id()``).

        Returns:
            True if the job is among the starts due at current time.
        """

        for start, key, _ in self._planned:
            if start != self._now:
                break
            if key == job_key:
                return True
        return False

    def _pick_slot(self, job: Job) -> tuple[float, list[int]]:
        """Find the earliest start time and nodes fitting a job.

        Args:
            job: Job object.

        Returns:
            Start timestamp and the list of chosen node ids.

        Raises:
            ValueError: If the job parameters cannot be scheduled.
        """

        if not job.walltime or not job.span:
            raise ValueError('Job walltime or span is not defined.')
        if job.span > len(self._timelines):
            raise ValueError('Job span exceeds the number of nodes.')

        slot_feeds = [timeline.open_slots(job.walltime, self._now)
                      for timeline in self._timelines]

        markers: list[tuple[float, int, int]] = []
        for node_id, feed in enumerate(slot_feeds):
            timestamp, value = next(feed)
            heappush(markers, (timestamp, node_id, value))

        chosen: list[int] = []
        remaining = job.span
        while markers:

            timestamp, node_id, value = heappop(markers)
            if value > 0:
                chosen.append(node_id)
            else:
                chosen.remove(node_id)

            remaining -= value
            if not remaining:
                return timestamp, chosen

            follow_up = next(slot_feeds[node_id], None)
            if follow_up is not None:
                heappush(markers,
                         (follow_up[0], node_id, follow_up[1]))

        raise ValueError('No feasible slot was found for the job.')

    def plan(self, job: Job, now: float | None = None):
        """Add one job to the schedule.

        Args:
            job: Job object.
            now: Current simulated time (kept as is when None).
        """

        if now is not None:
            self._now = now

        if job.walltime == 0.:
            return

        start, node_ids = self._pick_slot(job)
        end = start + job.walltime

        for node_id in node_ids:
            self._timelines[node_id].book(start, end)

        insort_left(self._planned, (start, id(job), tuple(node_ids)),
                    key=lambda entry: entry[0])

    def plan_backlog(self, jobs: Iterable[Job]):
        """Plan every waiting job, in backlog order.

        Args:
            jobs: Iterable over waiting jobs.
        """

        for job in jobs:
            self.plan(job)

    def rebase(self, busy_until: dict[int, float], now: float):
        """Rebuild the schedule base from currently busy nodes.

        Args:
            busy_until: Planned release timestamp per busy node id.
            now: Current simulated time.
        """

        self._now = now

        for node_id, timeline in enumerate(self._timelines):
            timeline.clear()
            if node_id in busy_until:
                timeline.book(now, busy_until[node_id])

        self._planned.clear()

    def take_due(self, now: float) -> list[tuple[int, tuple[int, ...]]]:
        """Pop all planned starts that are due at this time.

        Args:
            now: Current simulated time.

        Returns:
            Pairs of job identity key and the planned node ids.
        """

        due = []
        if now == self.next_start:
            self._now = now
            while now == self.next_start:
                _, key, node_ids = self._planned.pop(0)
                due.append((key, node_ids))
        return due
