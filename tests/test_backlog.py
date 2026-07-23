import pytest

from flodeck import Backlog, BacklogRules, BacklogScope, Job, OrderingRule


def make_job(flow='x', priority=0., arrived_at=0.):
    return Job(runtime=1., span=1, flow=flow, priority=priority,
               arrived_at=arrived_at)


def test_fifo_order():
    backlog = Backlog()
    jobs = [make_job(arrived_at=float(i)) for i in range(3)]
    for job in jobs:
        assert backlog.offer(job, job.arrived_at)

    assert len(backlog) == 3
    assert backlog.peek() is jobs[0]
    assert backlog.peek_last() is jobs[2]
    assert backlog.take_next(3.) is jobs[0]


def test_priority_order_with_stable_ties():
    rules = BacklogRules(ordering=OrderingRule.PRIORITY)
    backlog = Backlog(rules=rules)

    low = make_job(priority=1.)
    high = make_job(priority=10.)
    tied = make_job(priority=10.)

    for job in (low, high, tied):
        backlog.offer(job, 0.)

    assert backlog.take_next(0.) is high  # first of the tied pair
    assert backlog.take_next(0.) is tied
    assert backlog.take_next(0.) is low


def test_priority_aging_boosts_waiting_jobs():
    rules = BacklogRules(ordering=OrderingRule.PRIORITY)
    backlog = Backlog(rules=rules)

    early = make_job(priority=0.)
    backlog.offer(early, 0.)
    late = make_job(priority=3.)
    backlog.offer(late, 5.)  # early aged by 5 while waiting

    assert early.priority == 5.
    assert backlog.take_next(5.) is early


def test_on_admit_hook_is_applied():
    seen = []
    rules = BacklogRules(on_admit=seen.append)
    backlog = Backlog(rules=rules)

    job = make_job()
    backlog.offer(job, 0.)
    assert seen == [job]


def test_total_limit_drops_extra_jobs():
    backlog = Backlog(total_limit=2)

    admitted = [backlog.offer(make_job(), 0.) for _ in range(4)]
    assert admitted == [True, True, False, False]
    assert len(backlog) == 2
    assert backlog.dropped_count == 2
    assert backlog.dropped_for_flow('x') == 2
    assert backlog.dropped_by_flow() == [('x', 2)]


def test_per_flow_limit():
    rules = BacklogRules(per_flow_limit=1)
    backlog = Backlog(rules=rules)

    assert backlog.offer(make_job(flow='a'), 0.)
    assert not backlog.offer(make_job(flow='a'), 0.)
    assert backlog.offer(make_job(flow='b'), 0.)

    assert backlog.count_for_flow('a') == 1
    assert sorted(backlog.counts_by_flow()) == [('a', 1), ('b', 1)]


def test_explicit_flow_limit_beats_default():
    rules = BacklogRules(per_flow_limit=1, flow_limits={'a': 2})
    backlog = Backlog(rules=rules)

    assert backlog.offer(make_job(flow='a'), 0.)
    assert backlog.offer(make_job(flow='a'), 0.)
    assert not backlog.offer(make_job(flow='a'), 0.)


def test_hold_overflow_buffers_and_promotes():
    rules = BacklogRules(per_flow_limit=2)
    backlog = Backlog(rules=rules, hold_overflow=True)

    jobs = [make_job(arrived_at=float(i)) for i in range(5)]
    for job in jobs:
        backlog.offer(job, job.arrived_at)

    assert len(backlog) == 2
    assert backlog.held_count == 3
    assert backlog.total_count == 5
    assert backlog.dropped_count == 0
    assert backlog.count_for_flow('x', BacklogScope.HELD) == 3

    taken = backlog.take_next(10.)
    assert taken is jobs[0]
    assert len(backlog) == 2  # jobs[2] promoted from the buffer
    assert backlog.held_count == 2
    assert list(backlog.scan())[-1] is jobs[2]


def test_take_by_key():
    backlog = Backlog()
    jobs = [make_job() for _ in range(3)]
    for job in jobs:
        backlog.offer(job, 0.)

    assert backlog.take_by_key(id(jobs[1]), 0.) is jobs[1]
    assert len(backlog) == 2

    with pytest.raises(LookupError):
        backlog.take_by_key(id(jobs[1]), 0.)


def test_scan_and_clear():
    backlog = Backlog(total_limit=5)
    for _ in range(4):
        backlog.offer(make_job(), 0.)

    assert len(list(backlog.scan())) == 4
    assert len(list(backlog.scan(limit=2))) == 2

    backlog.clear()
    assert backlog.is_empty
    assert backlog.total_count == 0
