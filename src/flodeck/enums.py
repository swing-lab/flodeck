"""Enumerations shared across the FLoDeck simulator."""

from enum import Enum


class EventType(str, Enum):
    """Type of a discrete event recorded in the system trace."""

    ARRIVAL = "a"
    LAUNCH = "s"
    FINISH = "c"
    HALT = "x"


class FlowTag(str, Enum):
    """Well-known workload flow (stream) names."""

    MAIN = "main"
    EXTERNAL = "external"
    DEFAULT = "default"


class OrderingRule(str, Enum):
    """Ordering rule applied to the waiting backlog."""

    FIFO = "fifo"
    PRIORITY = "priority"


class BacklogScope(str, Enum):
    """Which part of the backlog to query.

    Selects the waiting line, the holding buffer, or both.
    """

    LINE = "line"
    HELD = "held"
    ALL = "all"
