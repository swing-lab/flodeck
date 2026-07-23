"""Command-line interface for running FLoDeck simulations."""

from __future__ import annotations

import argparse

from .engine import FloDeck
from .policy import POLICIES
from .workload import file_flow, poisson_flow, tiered_flow


def _build_parser() -> argparse.ArgumentParser:
    """Create the argument parser for the ``flodeck`` command."""

    parser = argparse.ArgumentParser(
        prog='flodeck',
        description='Discrete-event queueing simulator for modeling '
                    'the flow of computational load through '
                    'constrained computing systems.')

    system = parser.add_argument_group('system')
    system.add_argument(
        '--nodes', type=int,
        help='number of service nodes (defaults to the machine size '
             'of the selected --policy)')
    system.add_argument(
        '--policy', choices=sorted(POLICIES),
        help='site scheduling policy to apply (priority tiers with '
             'aging and the per-flow eligible-jobs limit)')
    system.add_argument(
        '--backfill', action='store_true',
        help='use the backfill scheduler for job placement')
    system.add_argument(
        '--backlog-limit', type=int,
        help='total limit of the waiting backlog')
    system.add_argument(
        '--hold-overflow', action='store_true',
        help='hold rejected jobs in a buffer instead of dropping them')

    workload = parser.add_argument_group('workload')
    workload.add_argument(
        '--arrival-rate', type=float,
        help='arrival rate of the generated flow, in jobs per unit '
             'of simulated time (mean inter-arrival time is 1/rate)')
    workload.add_argument(
        '--execution-rate', type=float,
        help='execution rate of the generated flow (mean runtime is '
             '1/rate); ignored with --policy, which draws jobs from '
             'the policy tiers')
    workload.add_argument(
        '--span', type=int,
        help='number of nodes per generated job (default: 1)')
    workload.add_argument(
        '--num-jobs', type=int,
        help='number of jobs to generate')
    workload.add_argument(
        '--input', metavar='FILE',
        help='file with job records to replay as a workload flow')

    run = parser.add_argument_group('run')
    run.add_argument(
        '--time-limit', type=float,
        help='timestamp when the processing must stop')
    run.add_argument(
        '--output', metavar='FILE',
        help='file to store per-job records')
    run.add_argument(
        '--trace', metavar='FILE',
        help='file to store the system-state trace')
    run.add_argument(
        '--verbose', action='store_true',
        help='print trace entries while processing')

    return parser


def _make_flows(args: argparse.Namespace,
                parser: argparse.ArgumentParser) -> list:
    """Build the workload flows requested on the command line."""

    flows = []

    if args.input:
        flows.append(file_flow(path=args.input,
                               time_limit=args.time_limit))

    if args.arrival_rate:
        if not args.num_jobs and not args.time_limit:
            parser.error('a generated flow needs --num-jobs '
                         'or --time-limit')
        if args.policy:
            policy = POLICIES[args.policy]
            flows.append(tiered_flow(arrival_rate=args.arrival_rate,
                                     tiers=policy.tiers.values(),
                                     num_jobs=args.num_jobs,
                                     time_limit=args.time_limit))
        elif args.execution_rate:
            flows.append(poisson_flow(arrival_rate=args.arrival_rate,
                                      execution_rate=args.execution_rate,
                                      span=args.span,
                                      num_jobs=args.num_jobs,
                                      time_limit=args.time_limit))
        else:
            parser.error('--arrival-rate needs --execution-rate '
                         '(or --policy)')

    if not flows:
        parser.error('no workload is defined: use --input and/or '
                     '--arrival-rate')

    return flows


def main(argv: list[str] | None = None) -> int:
    """Run one simulation described by command-line arguments.

    Args:
        argv: Argument list (defaults to ``sys.argv[1:]``).

    Returns:
        Process exit code.
    """

    parser = _build_parser()
    args = parser.parse_args(argv)

    policy = POLICIES[args.policy] if args.policy else None

    num_nodes = args.nodes
    if num_nodes is None:
        if policy is None:
            parser.error('--nodes is required (unless --policy '
                         'provides the machine size)')
        num_nodes = policy.node_count

    rules = policy.backlog_rules() if policy else None

    simulator = FloDeck(num_nodes=num_nodes,
                        backlog_limit=args.backlog_limit,
                        backlog_rules=rules,
                        hold_overflow=args.hold_overflow,
                        backfill=args.backfill,
                        time_limit=args.time_limit,
                        output_path=args.output,
                        trace_path=args.trace)

    simulator.run(flows=_make_flows(args, parser),
                  verbose=args.verbose)
    simulator.report()

    return 0
