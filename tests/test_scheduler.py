import pytest

from flodeck import Job, Scheduler
from flodeck.scheduler import NodeTimeline


def make_job(walltime=10., span=1):
    return Job(runtime=walltime, span=span, walltime=walltime,
               arrived_at=0.)


class TestNodeTimeline:

    def test_open_slots_on_empty_timeline(self):
        timeline = NodeTimeline()
        assert list(timeline.open_slots(5., 2.)) == [(2., 1)]

    def test_open_slots_skips_short_gaps(self):
        timeline = NodeTimeline()
        timeline.book(0., 10.)
        timeline.book(12., 20.)  # gap of 2 is too short for 5
        timeline.book(40., 50.)  # gap of 20 fits 5

        slots = list(timeline.open_slots(5., 0.))
        assert slots == [(20., 1), (35., -1), (50., 1)]

    def test_book_merges_touching_windows(self):
        timeline = NodeTimeline()
        timeline.book(0., 5.)
        timeline.book(10., 15.)
        timeline.book(5., 10.)  # bridges both

        assert list(timeline.open_slots(1., 0.)) == [(15., 1)]

    def test_book_rejects_overlap(self):
        timeline = NodeTimeline()
        timeline.book(0., 10.)
        with pytest.raises(ValueError):
            timeline.book(5., 15.)
        with pytest.raises(ValueError):
            timeline.book(0., 5.)


class TestScheduler:

    def test_plan_immediate_start_on_idle_nodes(self):
        scheduler = Scheduler(num_nodes=4)
        scheduler.rebase({}, 5.)

        job = make_job(walltime=10., span=2)
        scheduler.plan(job)

        assert scheduler.next_start == 5.
        assert scheduler.has_due(5.)
        due = scheduler.take_due(5.)
        assert due == [(id(job), (0, 1))]
        assert scheduler.next_start is None

    def test_plan_delays_start_until_nodes_release(self):
        scheduler = Scheduler(num_nodes=2)
        scheduler.rebase({0: 20., 1: 30.}, 0.)

        wide = make_job(walltime=5., span=2)
        scheduler.plan(wide)

        assert scheduler.next_start == 30.

    def test_backfill_small_job_starts_first(self):
        scheduler = Scheduler(num_nodes=4)
        scheduler.rebase({0: 20., 1: 20., 2: 20.}, 0.)

        blocked = make_job(walltime=10., span=4)  # starts at 20
        small = make_job(walltime=10., span=1)    # fits node 3 now
        scheduler.plan(blocked)
        scheduler.plan(small)

        assert scheduler.next_start == 0.
        assert scheduler.is_fast_tracked(id(small))
        assert not scheduler.is_fast_tracked(id(blocked))

        assert scheduler.take_due(0.) == [(id(small), (3,))]
        assert scheduler.next_start == 20.

    def test_backfill_does_not_delay_planned_head(self):
        scheduler = Scheduler(num_nodes=2)
        scheduler.rebase({0: 10., 1: 10.}, 0.)

        head = make_job(walltime=10., span=2)   # planned at 10
        filler = make_job(walltime=20., span=1)  # would push head back
        scheduler.plan(head)
        scheduler.plan(filler)

        # the filler cannot start now: it must wait for the head
        assert scheduler.take_due(0.) == []
        assert scheduler.next_start == 10.

    def test_zero_walltime_job_is_ignored(self):
        scheduler = Scheduler(num_nodes=2)
        scheduler.plan(Job(runtime=0., span=1, walltime=0.))
        assert scheduler.next_start is None

    def test_span_validation(self):
        scheduler = Scheduler(num_nodes=2)
        with pytest.raises(ValueError):
            scheduler.plan(make_job(span=3))

    def test_plan_backlog_keeps_order(self):
        scheduler = Scheduler(num_nodes=1)
        first = make_job(walltime=5.)
        second = make_job(walltime=5.)
        scheduler.rebase({}, 0.)
        scheduler.plan_backlog([first, second])

        assert scheduler.take_due(0.) == [(id(first), (0,))]
        assert scheduler.next_start == 5.
