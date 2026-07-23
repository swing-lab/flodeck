from flodeck import Job


def test_walltime_defaults_to_runtime():
    job = Job(runtime=5., span=2)
    assert job.walltime == 5.

    job = Job(runtime=5., span=2, walltime=8.)
    assert job.walltime == 8.


def test_timestamps_before_start_are_none():
    job = Job(runtime=5., span=1, arrived_at=1.)
    assert job.finished_at is None
    assert job.planned_finished_at is None
    assert job.waited is None
    assert job.flow_time is None


def test_timestamps_after_start():
    job = Job(runtime=5., span=1, walltime=7., arrived_at=1.)
    job.started_at = 3.

    assert job.finished_at == 8.
    assert job.planned_finished_at == 10.
    assert job.waited == 2.
    assert job.flow_time == 7.


def test_boost_increases_priority():
    job = Job(runtime=1., span=1, priority=10.)
    job.boost(2.5)
    assert job.priority == 12.5


def test_tag_combines_flow_and_label():
    assert Job(runtime=1., span=1, flow='main').tag == 'main'
    assert Job(runtime=1., span=1, flow='main',
               label='proj').tag == 'main:proj'
