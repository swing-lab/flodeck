import pytest

from flodeck import FRONTIER, Job, OrderingRule


def make_job(span):
    return Job(runtime=60., span=span)


def test_frontier_tiers_cover_full_machine():
    covered = set()
    for tier in FRONTIER.tiers.values():
        covered.update(range(tier.span_range[0],
                             tier.span_range[1] + 1))
    assert covered == set(range(1, FRONTIER.node_count + 1))


def test_tier_of():
    assert FRONTIER.tier_of(1) == 5
    assert FRONTIER.tier_of(92) == 4
    assert FRONTIER.tier_of(184) == 3
    assert FRONTIER.tier_of(1882) == 2
    assert FRONTIER.tier_of(9472) == 1
    assert FRONTIER.tier_of(10000) is None


def test_seed_priority_assigns_tier_and_boost():
    large = make_job(span=6000)
    FRONTIER.seed_priority(large)
    assert large.tier == 1
    assert large.priority == 8. * 86400.

    small = make_job(span=10)
    FRONTIER.seed_priority(small)
    assert small.tier == 5
    assert small.priority == 0.


def test_seed_priority_rejects_oversized_job():
    with pytest.raises(ValueError):
        FRONTIER.seed_priority(make_job(span=FRONTIER.node_count + 1))


def test_backlog_rules_reflect_policy():
    rules = FRONTIER.backlog_rules()
    assert rules.ordering == OrderingRule.PRIORITY
    assert rules.per_flow_limit == 4
    assert rules.on_admit == FRONTIER.seed_priority
