"""FLoDeck simulation engine: the discrete-event loop and statistics."""

from __future__ import annotations

import heapq

from dataclasses import dataclass
from itertools import count
from typing import TYPE_CHECKING

from .backlog import Backlog
from .pool import NodePool
from .enums import EventType
from .scheduler import Scheduler

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from .backlog import BacklogRules
    from .job import Job


@dataclass(frozen=True, slots=True)
class TraceEntry:
    """Snapshot of the system state at one simulated event.

    Attributes:
        at: Event timestamp.
        event: Kind of the event that produced this snapshot.
        running: Number of jobs executing on the node pool.
        queued: Number of jobs waiting in the backlog.
        held: Number of jobs in the holding buffer.
    """

    at: float
    event: EventType
    running: int
    queued: int
    held: int

    def as_line(self) -> str:
        """Render the entry as one comma-separated trace line."""

        return (f'{self.at},{self.event.value},{self.running},'
                f'{self.queued},{self.held}')


class _ArrivalFeed:
    """Merged, time-ordered feed of jobs from several workload flows."""

    def __init__(self, flows: Sequence[Iterator[Job]]):
        """Prime the feed by pulling the first job of every flow."""

        self._order = count()
        self._merged: list[tuple[float, int, Job, Iterator[Job]]] = []
        for flow in flows:
            self._push(flow)

    def _push(self, flow: Iterator[Job]):
        """Fetch the next job of one flow into the merge heap."""

        job = next(flow, None)
        if job is not None:
            heapq.heappush(
                self._merged,
                (job.arrived_at, next(self._order), job, flow))

    @property
    def next_at(self) -> float | None:
        """Timestamp of the next arrival (None when exhausted)."""

        if self._merged:
            return self._merged[0][0]
        return None

    def pop(self) -> Job:
        """Remove and return the next arriving job."""

        _, _, job, flow = heapq.heappop(self._merged)
        self._push(flow)
        return job


class FloDeck:
    """Discrete-event simulator of load flowing through a machine.

    Jobs arrive from workload flows, wait in the backlog and execute
    on the pool of service nodes; an optional backfill scheduler plans
    starts using per-node timelines (walltime based).
    """

    def __init__(self, num_nodes: int,
                 backlog_limit: int | None = None,
                 backlog_rules: BacklogRules | None = None,
                 hold_overflow: bool = False,
                 backfill: bool = False,
                 time_limit: float | None = None,
                 output_path: str | None = None,
                 trace_path: str | None = None):
        """Initialize the simulator.

        Args:
            num_nodes: Number of service (computing) nodes.
            backlog_limit: Total limit of the waiting backlog.
            backlog_rules: Admission/ordering policy of the backlog.
            hold_overflow: Hold rejected jobs in a buffer instead of
                dropping them.
            backfill: Use the backfill scheduler for job placement.
            time_limit: Timestamp when the processing must stop; when
                None, the run lasts while the flows produce jobs.
            output_path: File to store per-job records (arrival, start
                and end timestamps, span, flow, label).
            trace_path: File to store the system-state trace.
        """

        self._pool = NodePool(num_nodes)
        self._backlog = Backlog(rules=backlog_rules,
                                total_limit=backlog_limit,
                                hold_overflow=hold_overflow)
        self._scheduler = Scheduler(num_nodes) if backfill else None

        self._time_limit = time_limit
        self._output_path = output_path
        self._trace_path = trace_path

        self._trace: list[TraceEntry] = []
        self._completed: list[Job] = []

        self._elapsed = 0.
        self._job_area = 0.       # integral of jobs-in-system over time
        self._busy_area = 0.      # integral of busy nodes over time

    @property
    def trace(self) -> list[TraceEntry]:
        """System-state trace collected during the last run."""

        return self._trace

    @property
    def completed(self) -> list[Job]:
        """Jobs that finished execution during the last run."""

        return self._completed

    @property
    def dropped_count(self) -> int:
        """Number of jobs dropped by the backlog during the last run."""

        return self._backlog.dropped_count

    def mean_job_count(self) -> float:
        """Time-averaged number of jobs in the system.

        Returns:
            Average count of waiting, held and executing jobs.
        """

        return (self._job_area / self._elapsed) if self._elapsed else 0.

    def mean_flow_time(self) -> float:
        """Average time in the system per completed job.

        Returns:
            Mean of waiting plus execution time.
        """

        return (
            sum(job.flow_time for job in self._completed)
            / len(self._completed)
        ) if self._completed else 0.

    def utilization(self) -> float:
        """Fraction of the total node-time that was busy.

        Returns:
            Value between 0 and 1.
        """

        return (
            self._busy_area
            / (self._pool.size * self._elapsed)
        ) if self._elapsed else 0.

    def report(self):
        """Print summary statistics of the last run."""

        print(f'Simulated time       : {self._elapsed}')
        print(f'Completed jobs       : {len(self._completed)}')
        print(f'Dropped jobs         : {self.dropped_count}')
        print(f'AVG jobs in system   : {self.mean_job_count()}')
        print(f'AVG flow time (delay): {self.mean_flow_time()}')
        print(f'Utilization          : {self.utilization()}')

    def _reset(self):
        """Clear all state left from a previous run."""

        self._pool.clear()
        self._backlog.clear()
        if self._scheduler is not None:
            self._scheduler.rebase({}, 0.)

        self._trace.clear()
        self._completed.clear()

        self._elapsed = 0.
        self._job_area = 0.
        self._busy_area = 0.

    def _accrue(self, delta: float):
        """Accumulate time-weighted statistics over a time interval."""

        if delta <= 0.:
            return
        in_system = (len(self._backlog) + self._backlog.held_count
                     + self._pool.running_count)
        self._job_area += in_system * delta
        self._busy_area += self._pool.busy_count * delta

    def _record(self, at: float, event: EventType, verbose: bool):
        """Append one trace entry (and print it when verbose)."""

        entry = TraceEntry(at=at,
                           event=event,
                           running=self._pool.running_count,
                           queued=len(self._backlog),
                           held=self._backlog.held_count)
        self._trace.append(entry)
        if verbose:
            print(entry.as_line())

    def _next_event_at(self, arrivals: _ArrivalFeed) -> float | None:
        """Find the timestamp of the next discrete event."""

        candidates = [at for at in (arrivals.next_at,
                                    self._pool.next_finished_at)
                      if at is not None]

        if self._scheduler is not None:
            planned = self._scheduler.next_start
            if planned is not None:
                candidates.append(planned)

        return min(candidates) if candidates else None

    def _dispatch(self, now: float, verbose: bool):
        """Start every waiting job that can begin execution now."""

        if self._scheduler is None:
            while (not self._backlog.is_empty
                    and self._pool.fits(self._backlog.peek())):
                job = self._backlog.take_next(now)
                self._pool.launch(job, now)
                self._record(now, EventType.LAUNCH, verbose)
            return

        self._scheduler.rebase(self._pool.busy_until(), now)
        self._scheduler.plan_backlog(self._backlog.scan())

        for key, node_ids in self._scheduler.take_due(now):
            job = self._backlog.take_by_key(key, now)
            self._pool.launch_on(job, node_ids, now)
            self._record(now, EventType.LAUNCH, verbose)

    def _finish_job(self, job: Job, sink):
        """Account for one finished job (record and output line)."""

        self._completed.append(job)
        if sink is not None:
            label = job.label if job.label is not None else ''
            sink.write(f'{job.arrived_at},{job.started_at},'
                       f'{job.finished_at},{job.span},'
                       f'{job.flow},{label}\n')

    def run(self, flows: Sequence[Iterator[Job]],
            verbose: bool = False):
        """Process the given workload flows until they are drained.

        Args:
            flows: Workload flows (job generators) to simulate.
            verbose: Print trace entries while processing.
        """

        self._reset()

        arrivals = _ArrivalFeed(flows)
        sink = open(self._output_path, 'w') if self._output_path else None

        try:
            now = 0.
            while True:

                event_at = self._next_event_at(arrivals)
                if event_at is None:
                    break

                if (self._time_limit is not None
                        and event_at > self._time_limit):
                    self._accrue(self._time_limit - now)
                    now = self._time_limit
                    self._record(now, EventType.HALT, verbose)
                    break

                self._accrue(event_at - now)
                now = event_at

                for job in self._pool.harvest(now):
                    self._finish_job(job, sink)
                    self._record(now, EventType.FINISH, verbose)

                while arrivals.next_at == now:
                    self._backlog.offer(arrivals.pop(), now)
                    self._record(now, EventType.ARRIVAL, verbose)

                self._dispatch(now, verbose)

            self._elapsed = now

        finally:
            if sink is not None:
                sink.close()

        if self._trace_path:
            with open(self._trace_path, 'w') as trace_sink:
                for entry in self._trace:
                    trace_sink.write(entry.as_line() + '\n')
