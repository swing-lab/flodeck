"""Workload flows: generators that produce jobs for the simulator.

Rates are parameters of exponential distributions, expressed in events
per unit of simulated time: on average, jobs arrive every
``1 / arrival_rate`` and run for ``1 / execution_rate`` time units.
Time is unit-free — the unit chosen for the rates is also the unit of
time limits, walltimes and the reported statistics.
"""

from __future__ import annotations

import os
import random

from typing import TYPE_CHECKING

from .job import Job
from .enums import FlowTag

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from .policy import PriorityTier

DEFAULT_SPAN = 1
DEFAULT_FLOW = FlowTag.DEFAULT


def poisson_flow(arrival_rate: float, execution_rate: float,
                 span: int | None = None, flow: str | None = None,
                 label: str | None = None,
                 first_arrival_at: float | None = None,
                 num_jobs: int | None = None,
                 time_limit: float | None = None) -> Iterator[Job]:
    """Yield jobs with exponential inter-arrival and execution times.

    Args:
        arrival_rate: Arrival rate for jobs (Poisson process); the
            mean inter-arrival time is ``1 / arrival_rate``.
        execution_rate: Execution (service) rate for jobs; the mean
            runtime is ``1 / execution_rate``.
        span: Number of nodes per job (defaults to 1).
        flow: Name of the workload flow.
        label: Label (project) name.
        first_arrival_at: Initial (first) arrival timestamp.
        num_jobs: Number of generated jobs (None means unlimited).
        time_limit: Maximum timestamp until generation is done.

    Yields:
        Job objects in arrival order.

    Raises:
        ValueError: If neither ``num_jobs`` nor ``time_limit`` is set.
    """

    if not num_jobs and not time_limit:
        raise ValueError('Neither the job count nor time limit is set.')

    span = span or DEFAULT_SPAN
    flow = flow or DEFAULT_FLOW

    arrives_at = first_arrival_at or random.expovariate(arrival_rate)
    while (num_jobs is None or num_jobs > 0) and (
            time_limit is None or arrives_at < time_limit):

        yield Job(arrived_at=arrives_at,
                  runtime=random.expovariate(execution_rate),
                  span=span,
                  flow=flow,
                  label=label)

        arrives_at += random.expovariate(arrival_rate)

        if num_jobs:
            num_jobs -= 1


def tiered_flow(arrival_rate: float, tiers: Sequence[PriorityTier],
                flow: str | None = None, label: str | None = None,
                first_arrival_at: float | None = None,
                num_jobs: int | None = None,
                time_limit: float | None = None) -> Iterator[Job]:
    """Yield jobs with random parameters drawn from priority tiers.

    Every job picks a random tier; its walltime is a random number of
    whole time units within the tier limit, its runtime is random but
    capped at the walltime (jobs are killed when the requested time is
    exhausted), and its span falls into the tier node range.

    Args:
        arrival_rate: Arrival rate for jobs (Poisson process); the
            mean inter-arrival time is ``1 / arrival_rate``.
        tiers: Priority tiers to draw job parameters from (e.g.,
            ``list(FRONTIER.tiers.values())``).
        flow: Name of the workload flow.
        label: Label (project) name.
        first_arrival_at: Initial (first) arrival timestamp.
        num_jobs: Number of generated jobs (None means unlimited).
        time_limit: Maximum timestamp until generation is done.

    Yields:
        Job objects in arrival order.

    Raises:
        ValueError: If neither ``num_jobs`` nor ``time_limit`` is set.
    """

    if not num_jobs and not time_limit:
        raise ValueError('Neither the job count nor time limit is set.')

    tiers = list(tiers)
    flow = flow or DEFAULT_FLOW

    arrives_at = first_arrival_at or random.expovariate(arrival_rate)
    while (num_jobs is None or num_jobs > 0) and (
            time_limit is None or arrives_at < time_limit):

        tier = random.choice(tiers)
        walltime = float(
            int(random.uniform(1., tier.max_walltime / 60.)) * 60.)

        yield Job(arrived_at=arrives_at,
                  walltime=walltime,
                  runtime=min(walltime,
                              random.uniform(1., walltime)
                              * random.expovariate(1.)),
                  span=int(random.uniform(*tier.span_range)),
                  flow=flow,
                  label=label)

        arrives_at += random.expovariate(arrival_rate)

        if num_jobs:
            num_jobs -= 1


def _parse_record(fields: list[str], flow: str | None) -> dict | None:
    """Build job options from one input record (or None to skip it).

    Supported record layouts:
    1. ``arrivalDelta,runtime,span``
    2. ``arrivalDelta,walltime,runtime,span``
    3. ``arrivalDelta,walltime,runtime,span,flowName,label``
    """

    options: dict = {}
    try:
        if len(fields) == 3:
            options.update({
                'runtime': float(fields[1]),
                'span': int(float(fields[2])),
                'flow': flow or DEFAULT_FLOW})

        elif len(fields) > 3:
            options.update({
                'walltime': float(fields[1]),
                'runtime': float(fields[2]),
                'span': int(float(fields[3]))})

            if flow:
                options['flow'] = flow
            elif len(fields) > 4:
                options['flow'] = fields[4]
            else:
                options['flow'] = DEFAULT_FLOW

            if len(fields) > 5:
                options['label'] = fields[5]

    except ValueError:
        return None

    if not options or options['runtime'] == 0.:
        return None

    return options


def file_flow(path: str, flow: str | None = None,
              time_limit: float | None = None) -> Iterator[Job]:
    """Yield jobs read from an input file.

    The first field of every record is the arrival timestamp relative
    to the end of the previous file pass; with a time limit set, the
    file is replayed repeatedly until the limit is reached.

    Args:
        path: Input file name (see record layouts in ``_parse_record``).
        flow: Name of the workload flow (overrides the file field).
        time_limit: Maximum timestamp until generation is done.

    Yields:
        Job objects in arrival order.

    Raises:
        ValueError: If the input file path is not valid.
    """

    if not path or not os.path.exists(path):
        raise ValueError('Input data is not defined (wrong file path).')

    pass_offset = arrives_at = 0.
    while not time_limit or pass_offset < time_limit:

        with open(path) as records:
            for record in records:
                fields = record.rstrip('\n').split(',')

                try:
                    arrives_at = pass_offset + float(fields[0])
                except ValueError:
                    continue

                if time_limit and time_limit < arrives_at:
                    break

                options = _parse_record(fields, flow)
                if options is None:
                    continue

                yield Job(arrived_at=arrives_at, **options)

        if not time_limit or not arrives_at:
            break

        pass_offset = arrives_at
