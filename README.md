# FLoDeck

FLoDeck is a discrete-event queueing simulation framework for modeling
the flow of computational load through constrained computing systems —
it models the flow of work through queues and compute resources. In the
context of HPC, it simulates job arrivals, queueing, scheduling and
execution to generate realistic job traces and analyze resource
utilization under configurable workload and scheduling policies.

> The name is inspired by _Flow_ and _Load_.

## Features

* **Queueing-theory core** — arrival processes are workload flows
  (Poisson, tiered-random or replayed from trace files), the service
  process is a pool of identical nodes, and the queueing discipline is
  FIFO or Priority with aging.
* **Admission control** — backlog limits (total and/or per workload
  flow); over-limit jobs are either dropped and counted, or kept in a
  holding buffer and promoted back when a slot frees up.
* **Backfill scheduling** — an optional scheduler plans job starts
  over per-node timelines (walltime based), letting small jobs start
  in gaps ahead of wide blocked ones.
* **Pluggable site policies** — a `SitePolicy` maps node counts to
  priority tiers (aging boosts, walltime caps); the bundled `FRONTIER`
  policy follows the [OLCF Frontier user guide](https://docs.olcf.ornl.gov/systems/frontier_user_guide.html#job-priority-by-node-count)
  ("job priority by node count" bins and the batch-partition limit of
  four eligible jobs per user).
* **Traces and statistics** — per-job records and system-state traces
  written to files, plus time-averaged job count, mean flow time
  (delay) and node utilization.

## Package layout

| Module | Purpose |
|--------|---------|
| `flodeck.engine` | `FloDeck` — the discrete-event loop and statistics |
| `flodeck.job` | `Job` — one unit of work and its timestamps |
| `flodeck.backlog` | `Backlog`/`BacklogRules` — waiting line, limits, buffer |
| `flodeck.pool` | `NodePool` — the pool of service nodes |
| `flodeck.scheduler` | `Scheduler` — backfill planning over node timelines |
| `flodeck.workload` | `poisson_flow`, `tiered_flow`, `file_flow` generators |
| `flodeck.policy` | `SitePolicy`, `PriorityTier` and the `FRONTIER` policy |
| `flodeck.enums` | Shared enumerations (events, ordering, flow tags, backlog scope) |
| `flodeck.cli` | The `flodeck` command-line entry point |

## Installation

Install `flodeck` from [PyPI](https://pypi.org/project/flodeck/):

```bash
pip install flodeck
```

### Development Install

To install in editable development mode (for contributing or local
development):

```bash
# Clone the repository
git clone https://github.com/swing-lab/flodeck.git
cd flodeck

# Install in development mode with test and formatting extras
pip install -e ".[dev]"
```

> **Note:** flodeck uses setuptools-scm to derive its version from git
> tags; on a clone without a release tag, the fallback version from
> `pyproject.toml` is used.

With the `dev` extras installed, run `pytest` for the test suite
(`tests/`) and `ruff check .` for linting.

## Quick start

```python
from flodeck import FloDeck, poisson_flow

simulator = FloDeck(num_nodes=100)
simulator.run(flows=[poisson_flow(arrival_rate=22. / 72,
                                  execution_rate=1. / 3,
                                  time_limit=1000.)])
simulator.report()
```

or from the command line:

```shell
flodeck --nodes 100 --arrival-rate 0.3 --execution-rate 0.33 \
        --time-limit 1000
flodeck --policy frontier --backfill --hold-overflow \
        --arrival-rate 0.0017 --time-limit 86400
```

### Simulator options

`FloDeck(num_nodes, ...)` accepts:

* `backlog_limit` — total limit of the waiting backlog
* `backlog_rules` — `BacklogRules` (ordering, per-flow limits,
  admission hook)
* `hold_overflow` — buffer rejected jobs instead of dropping them
* `backfill` — enable the backfill scheduler
* `time_limit` — timestamp when the processing must stop (otherwise
  the run lasts while the flows produce jobs)
* `output_path` — per-job records: `arrived_at`, `started_at`,
  `finished_at`, `span`, `flow`, `label`
* `trace_path` — system-state trace: `at`, `event`, `running`,
  `queued`, `held` (also printed with `run(..., verbose=True)`)

### Workload flows

A workload flow is any generator that yields `Job` objects in arrival
order; predefined ones cover the common cases.

Rates are parameters of exponential distributions, expressed in
events per unit of simulated time: on average, jobs arrive every
`1 / arrival_rate` and run for `1 / execution_rate` time units. Time
itself is unit-free — the unit chosen for the rates is also the unit
of `time_limit`, walltimes and the reported statistics (the bundled
`FRONTIER` policy uses seconds).

```python
from flodeck import file_flow, poisson_flow

flows = [poisson_flow(arrival_rate=11. / 36, execution_rate=1. / 3,
                      span=100, flow='main', time_limit=1000.),
         file_flow(path='flodeck_input.txt', flow='external',
                   time_limit=1000.)]
```

Either `num_jobs` or `time_limit` **must** be set for generated flows.

### Modeling a specific machine

```python
from flodeck import FRONTIER, FloDeck, tiered_flow

simulator = FloDeck(num_nodes=FRONTIER.node_count,
                    backlog_rules=FRONTIER.backlog_rules(),
                    hold_overflow=True,
                    backfill=True,
                    time_limit=86400.)
simulator.run(flows=[tiered_flow(arrival_rate=1. / 600,
                                 tiers=FRONTIER.tiers.values(),
                                 time_limit=86400.)])
simulator.report()
```

After a run: `simulator.completed` holds the finished jobs,
`simulator.trace` the state snapshots, and `mean_job_count()`,
`mean_flow_time()`, `utilization()` and `dropped_count` provide the
summary statistics.

## Examples

See `examples/`:

* `general.py` — two merged flows on an M/M/c-like system
* `theory.py` — analytic Erlang-C reference values for validation
* `streamed.py` — a custom job generator with a holding buffer
* `frontier.py` — Frontier policy with priority aging and backfill

## Acknowledgments

FLoDeck is an independent implementation and design effort inspired
by the [ATLAS-Titan/allocation-modeling](https://github.com/ATLAS-Titan/allocation-modeling)
project. The original software was developed by M. Titov, with the
conceptualization and methodological framework designed in
collaboration with A. Poyda and S. Jha.

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
