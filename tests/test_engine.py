import math
import random

from decimal import Decimal

import pytest

from flodeck import (BacklogRules, EventType, FloDeck, Job,
                     OrderingRule, poisson_flow)


def burst(specs):
    """Yield jobs from (arrived_at, runtime, span) tuples."""
    for arrived_at, runtime, span in specs:
        yield Job(runtime=runtime, span=span, arrived_at=arrived_at,
                  flow='t')


def test_single_job_lifecycle():
    simulator = FloDeck(num_nodes=2)
    simulator.run(flows=[burst([(1., 5., 2)])])

    (job,) = simulator.completed
    assert job.started_at == 1.
    assert job.finished_at == 6.
    events = [entry.event for entry in simulator.trace]
    assert events == [EventType.ARRIVAL, EventType.LAUNCH,
                      EventType.FINISH]


def test_fifo_waits_for_free_nodes():
    simulator = FloDeck(num_nodes=2)
    simulator.run(flows=[burst([(0., 10., 2), (1., 1., 2)])])

    starts = sorted(job.started_at for job in simulator.completed)
    assert starts == [0., 10.]
    assert simulator.mean_flow_time() == (10. + 10.) / 2


def test_fifo_head_of_line_blocking():
    # a wide job blocks a narrow one behind it (no backfill)
    simulator = FloDeck(num_nodes=2)
    simulator.run(flows=[burst([(0., 10., 2), (1., 1., 2),
                                (2., 1., 1)])])

    by_span_runtime = {(j.span, j.runtime): j.started_at
                       for j in simulator.completed}
    assert by_span_runtime[(1, 1.)] == 11.


def test_backfill_lets_small_job_jump_ahead():
    def jobs():
        yield Job(runtime=10., span=4, arrived_at=1., flow='t')
        yield Job(runtime=20., span=4, arrived_at=2., flow='t')
        yield Job(runtime=5., span=1, arrived_at=3., flow='t')

    simulator = FloDeck(num_nodes=5, backfill=True)
    simulator.run(flows=[jobs()])

    starts = {job.runtime: job.started_at
              for job in simulator.completed}
    assert starts[5.] == 3.    # backfilled immediately
    assert starts[20.] == 11.  # waited for the wide release


def test_backfill_survives_jobs_overrunning_their_walltime():
    def jobs():
        # runtime exceeds walltime: the node stays busy past the
        # planned release and the scheduler must keep replanning
        yield Job(runtime=10., walltime=2., span=1, arrived_at=0.,
                  flow='t')
        yield Job(runtime=1., walltime=1., span=1, arrived_at=1.,
                  flow='t')

    simulator = FloDeck(num_nodes=1, backfill=True)
    simulator.run(flows=[jobs()])

    starts = sorted(job.started_at for job in simulator.completed)
    assert starts == [0., 10.]


def test_multiple_flows_are_merged_in_time_order():
    simulator = FloDeck(num_nodes=10)
    simulator.run(flows=[burst([(1., 1., 1), (5., 1., 1)]),
                         burst([(2., 1., 1), (3., 1., 1)])])

    arrivals = [entry.at for entry in simulator.trace
                if entry.event == EventType.ARRIVAL]
    assert arrivals == [1., 2., 3., 5.]


def test_time_limit_halts_processing():
    simulator = FloDeck(num_nodes=1, time_limit=5.)
    simulator.run(flows=[burst([(1., 2., 1), (10., 2., 1)])])

    assert len(simulator.completed) == 1
    assert simulator.trace[-1].event == EventType.HALT
    assert simulator.trace[-1].at == 5.


def test_dropped_jobs_are_counted():
    simulator = FloDeck(num_nodes=1, backlog_limit=1)
    simulator.run(flows=[burst([(0., 100., 1), (1., 1., 1),
                                (2., 1., 1), (3., 1., 1)])])

    assert simulator.dropped_count == 2


def test_hold_overflow_keeps_jobs_instead_of_dropping():
    rules = BacklogRules(per_flow_limit=1)
    simulator = FloDeck(num_nodes=1, backlog_rules=rules,
                        hold_overflow=True)
    simulator.run(flows=[burst([(0., 5., 1), (1., 5., 1),
                                (2., 5., 1)])])

    assert simulator.dropped_count == 0
    assert len(simulator.completed) == 3
    held = [entry.held for entry in simulator.trace]
    assert max(held) == 1  # third job was held for a while


def test_priority_ordering_changes_launch_order():
    def seed(job):
        job.priority = job.span * 100.  # wider jobs outrank aging

    rules = BacklogRules(ordering=OrderingRule.PRIORITY,
                         on_admit=seed)
    simulator = FloDeck(num_nodes=2, backlog_rules=rules)
    # all queued behind the first; then span=2 outranks span=1
    simulator.run(flows=[burst([(0., 10., 2), (1., 1., 1),
                                (2., 1., 2)])])

    by_span = {job.span: job.started_at for job in simulator.completed
               if job.arrived_at > 0.}
    assert by_span[2] < by_span[1]


def test_output_and_trace_files(tmp_path):
    out_path = tmp_path / 'out.txt'
    trace_path = tmp_path / 'trace.txt'

    simulator = FloDeck(num_nodes=2, output_path=str(out_path),
                        trace_path=str(trace_path))
    simulator.run(flows=[burst([(1., 2., 1), (2., 2., 1)])])

    records = out_path.read_text().splitlines()
    assert len(records) == 2
    arrived, started, finished, span, flow, label = \
        records[0].split(',')
    assert (float(arrived), float(started), float(finished)) \
        == (1., 1., 3.)
    assert (int(span), flow, label) == (1, 't', '')

    trace_lines = trace_path.read_text().splitlines()
    assert len(trace_lines) == len(simulator.trace)
    assert trace_lines[0].split(',')[1] == 'a'


def test_stats_reset_between_runs():
    simulator = FloDeck(num_nodes=1)
    simulator.run(flows=[burst([(0., 2., 1)])])
    first_count = len(simulator.completed)
    simulator.run(flows=[burst([(0., 2., 1)])])

    assert len(simulator.completed) == first_count == 1


def test_report_prints_summary(capsys):
    simulator = FloDeck(num_nodes=1)
    simulator.run(flows=[burst([(0., 2., 1)])])
    simulator.report()

    printed = capsys.readouterr().out
    assert 'Completed jobs' in printed
    assert 'Utilization' in printed


@pytest.mark.parametrize('num_nodes,arrival_rate,service_rate',
                         [(10, 2.5, 1. / 3)])  # rho = 0.75
def test_mmc_statistics_match_erlang_c(num_nodes, arrival_rate,
                                       service_rate):
    """Simulated M/M/c averages must match queueing theory."""

    theory_jobs, theory_delay = _erlang_c(
        num_nodes, Decimal(f'{arrival_rate}'),
        Decimal(f'{service_rate}'))

    random.seed(42)
    simulator = FloDeck(num_nodes=num_nodes)
    jobs_acc = delay_acc = 0.
    runs = 5
    for _ in range(runs):
        simulator.run(flows=[poisson_flow(arrival_rate=arrival_rate,
                                          execution_rate=service_rate,
                                          time_limit=20000.)])
        jobs_acc += simulator.mean_job_count()
        delay_acc += simulator.mean_flow_time()

    assert math.isclose(jobs_acc / runs, theory_jobs, rel_tol=0.1)
    assert math.isclose(delay_acc / runs, theory_delay, rel_tol=0.1)
    assert 0. < simulator.utilization() < 1.


def _erlang_c(n, a_rate, s_rate):
    rho = a_rate / (n * s_rate)
    erlangs = a_rate / s_rate

    p_zero = Decimal(1) / (
        ((erlangs ** n) / (math.factorial(n) * (Decimal(1) - rho)))
        + sum((erlangs ** i) / math.factorial(i) for i in range(n)))
    p_queued = p_zero * ((erlangs ** n)
                         / (math.factorial(n) * (Decimal(1) - rho)))

    mean_jobs = ((rho * p_queued) / (Decimal(1) - rho)) + erlangs
    mean_delay = (p_queued / ((n * s_rate) - a_rate)
                  + (Decimal(1) / s_rate))
    return float(mean_jobs), float(mean_delay)
