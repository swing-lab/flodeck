"""Generic site scheduling policies and the Frontier reference policy.

The Frontier numbers follow the OLCF Frontier User Guide, section
"Job Priority by Node Count" and the ``batch`` partition policy
(https://docs.olcf.ornl.gov/systems/frontier_user_guide.html).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .backlog import BacklogRules
from .enums import OrderingRule

if TYPE_CHECKING:
    from .job import Job

_DAY = 86400.
_HOUR = 3600.


@dataclass(frozen=True)
class PriorityTier:
    """Priority bin defined by the requested node count.

    Attributes:
        span_range: Inclusive range of node counts for the tier.
        max_walltime: Maximum requested walltime for the tier.
        aging_boost: Initial priority boost given to the tier.
    """

    span_range: tuple[int, int]
    max_walltime: float
    aging_boost: float


@dataclass(frozen=True)
class SitePolicy:
    """Scheduling policy of a specific (modeled) computing site.

    Attributes:
        name: Site or machine name.
        node_count: Total number of schedulable nodes.
        tiers: Priority tiers keyed by bin id.
        eligible_limit: Maximum eligible-to-run jobs per flow (user).
    """

    name: str
    node_count: int
    tiers: dict[int, PriorityTier] = field(default_factory=dict)
    eligible_limit: int | None = None

    def tier_of(self, span: int) -> int | None:
        """Find the tier (bin) id matching a node count.

        Args:
            span: Number of requested nodes.

        Returns:
            Tier id, or None if no tier matches.
        """

        for tier_id, tier in self.tiers.items():
            if tier.span_range[0] <= span <= tier.span_range[1]:
                return tier_id
        return None

    def seed_priority(self, job: Job):
        """Assign the tier and initial priority to an admitted job.

        Args:
            job: Job object.

        Raises:
            ValueError: If the job span exceeds the machine size.
        """

        if job.span > self.node_count:
            raise ValueError(
                'Job span exceeds the total number of nodes.')

        tier_id = self.tier_of(job.span)
        if tier_id is not None:
            job.tier = tier_id
            job.priority = self.tiers[tier_id].aging_boost

    def backlog_rules(self) -> BacklogRules:
        """Build backlog rules implementing this policy.

        Returns:
            Priority-ordered rules with the per-flow eligible limit
            and priority seeding hook.
        """

        return BacklogRules(ordering=OrderingRule.PRIORITY,
                            per_flow_limit=self.eligible_limit,
                            on_admit=self.seed_priority)


FRONTIER = SitePolicy(
    name='frontier',
    node_count=9472,
    tiers={
        1: PriorityTier(span_range=(5645, 9472),
                        max_walltime=12. * _HOUR,
                        aging_boost=8. * _DAY),
        2: PriorityTier(span_range=(1882, 5644),
                        max_walltime=12. * _HOUR,
                        aging_boost=4. * _DAY),
        3: PriorityTier(span_range=(184, 1881),
                        max_walltime=12. * _HOUR,
                        aging_boost=0.),
        4: PriorityTier(span_range=(92, 183),
                        max_walltime=6. * _HOUR,
                        aging_boost=0.),
        5: PriorityTier(span_range=(1, 91),
                        max_walltime=2. * _HOUR,
                        aging_boost=0.),
    },
    eligible_limit=4,
)


POLICIES: dict[str, SitePolicy] = {
    FRONTIER.name: FRONTIER,
}

