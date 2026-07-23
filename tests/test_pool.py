import pytest

from flodeck import NodePool, Job


def make_job(runtime=5., span=2, walltime=None, flow='x'):
    return Job(runtime=runtime, span=span, walltime=walltime,
               flow=flow, arrived_at=0.)


def test_launch_and_counts():
    pool = NodePool(size=10)
    job = make_job(span=4)
    pool.launch(job, 1.)

    assert job.started_at == 1.
    assert pool.busy_count == 4
    assert pool.idle_count == 6
    assert pool.running_count == 1
    assert pool.counts_by_flow() == [('x', 1)]
    assert pool.next_finished_at == 6.


def test_fits_and_launch_overflow():
    pool = NodePool(size=3)
    assert pool.fits(make_job(span=3))
    assert not pool.fits(make_job(span=4))

    with pytest.raises(RuntimeError):
        pool.launch(make_job(span=4), 0.)


def test_harvest_releases_nodes():
    pool = NodePool(size=10)
    fast = make_job(runtime=2., span=3)
    slow = make_job(runtime=5., span=3)
    pool.launch(slow, 0.)
    pool.launch(fast, 0.)

    assert pool.next_finished_at == 2.
    assert pool.harvest(1.) == []
    assert pool.harvest(2.) == [fast]
    assert pool.busy_count == 3
    assert pool.harvest(5.) == [slow]
    assert pool.busy_count == 0
    assert pool.next_finished_at is None


def test_launch_on_specific_nodes():
    pool = NodePool(size=5)
    job = make_job(span=2)
    pool.launch_on(job, [1, 3], 0.)

    assert pool.busy_count == 2
    other = make_job(span=2)
    with pytest.raises(RuntimeError):
        pool.launch_on(other, [3, 4], 0.)
    assert pool.busy_count == 2  # failed launch is rolled back

    with pytest.raises(ValueError):
        pool.launch_on(make_job(span=2), [0], 0.)


def test_busy_until_uses_walltime():
    pool = NodePool(size=4)
    job = make_job(runtime=2., walltime=6., span=2)
    pool.launch(job, 1.)

    assert pool.busy_until() == {0: 7., 1: 7.}


def test_clear():
    pool = NodePool(size=4)
    pool.launch(make_job(span=4), 0.)
    pool.clear()

    assert pool.idle_count == 4
    assert pool.running_count == 0
    assert pool.counts_by_flow() == []
