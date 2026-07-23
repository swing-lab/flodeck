import random

import pytest

from flodeck import FRONTIER, file_flow, poisson_flow, tiered_flow


@pytest.fixture(autouse=True)
def seeded():
    random.seed(13)


def test_poisson_flow_respects_num_jobs():
    jobs = list(poisson_flow(arrival_rate=1., execution_rate=1.,
                             num_jobs=5))
    assert len(jobs) == 5
    arrivals = [job.arrived_at for job in jobs]
    assert arrivals == sorted(arrivals)


def test_poisson_flow_respects_time_limit():
    jobs = list(poisson_flow(arrival_rate=2., execution_rate=1.,
                             time_limit=50.))
    assert jobs
    assert all(job.arrived_at < 50. for job in jobs)


def test_poisson_flow_defaults_and_overrides():
    job = next(poisson_flow(arrival_rate=1., execution_rate=1.,
                            num_jobs=1))
    assert job.span == 1 and job.flow == 'default'

    job = next(poisson_flow(arrival_rate=1., execution_rate=1.,
                            span=7, flow='main', label='proj',
                            first_arrival_at=3., num_jobs=1))
    assert (job.span, job.flow, job.label) == (7, 'main', 'proj')
    assert job.arrived_at == 3.


def test_flow_requires_a_limit():
    with pytest.raises(ValueError):
        next(poisson_flow(arrival_rate=1., execution_rate=1.))
    with pytest.raises(ValueError):
        next(tiered_flow(arrival_rate=1.,
                         tiers=FRONTIER.tiers.values()))


def test_tiered_flow_draws_from_tiers():
    jobs = list(tiered_flow(arrival_rate=1.,
                            tiers=FRONTIER.tiers.values(),
                            time_limit=200.))
    assert jobs
    for job in jobs:
        tier_id = FRONTIER.tier_of(job.span)
        assert tier_id is not None
        assert job.walltime % 60. == 0.
        assert job.walltime <= FRONTIER.tiers[tier_id].max_walltime


def test_file_flow_layouts(tmp_path):
    data = tmp_path / 'input.txt'
    data.write_text('3.0,2.5,10\n'
                    'not-a-number,skip,me\n'
                    '7.5,900.0,4.0,25\n'
                    '9.0,900.0,0.0,25\n'  # zero runtime is skipped
                    '12.0,900.0,3.0,50,external,demo\n')

    jobs = list(file_flow(str(data)))
    assert len(jobs) == 3

    short, medium, full = jobs
    assert (short.arrived_at, short.runtime, short.span) == (3., 2.5, 10)
    assert short.flow == 'default'

    assert medium.walltime == 900. and medium.runtime == 4.

    assert full.flow == 'external' and full.label == 'demo'
    assert full.span == 50


def test_file_flow_replays_until_time_limit(tmp_path):
    data = tmp_path / 'input.txt'
    data.write_text('1.0,2.0,1\n2.0,2.0,1\n')

    jobs = list(file_flow(str(data), time_limit=5.))
    assert [job.arrived_at for job in jobs] == [1., 2., 3., 4., 5.]


def test_file_flow_flow_override(tmp_path):
    data = tmp_path / 'input.txt'
    data.write_text('1.0,900.0,2.0,1,external,demo\n')

    job = next(file_flow(str(data), flow='forced'))
    assert job.flow == 'forced'


def test_file_flow_missing_file():
    with pytest.raises(ValueError):
        next(file_flow('no-such-file.txt'))


def test_poisson_flow_respects_both_limits():
    # num_jobs is large, but time_limit is small: time_limit should stop it
    jobs = list(poisson_flow(arrival_rate=2., execution_rate=1.,
                             num_jobs=1000, time_limit=5.))
    assert all(job.arrived_at < 5. for job in jobs)

    # num_jobs is small, but time_limit is large: num_jobs should stop it
    jobs = list(poisson_flow(arrival_rate=0.001, execution_rate=1.,
                             num_jobs=3, time_limit=100000.))
    assert len(jobs) == 3


def test_tiered_flow_respects_both_limits():
    jobs = list(tiered_flow(arrival_rate=2.,
                            tiers=FRONTIER.tiers.values(),
                            num_jobs=1000, time_limit=5.))
    assert all(job.arrived_at < 5. for job in jobs)

