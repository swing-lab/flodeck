"""Frontier example: priority backlog, backfill and tiered workload."""

from flodeck import FRONTIER, FloDeck, FlowTag, tiered_flow

ARRIVAL_RATE = 1. / 600  # one job every ~10 simulated minutes
TIME_LIMIT = 86400.      # one simulated day


if __name__ == '__main__':

    simulator = FloDeck(num_nodes=FRONTIER.node_count,
                        backlog_rules=FRONTIER.backlog_rules(),
                        hold_overflow=True,
                        backfill=True,
                        time_limit=TIME_LIMIT,
                        output_path='flodeck_frontier_output.txt',
                        trace_path='flodeck_frontier_trace.txt')

    simulator.run(flows=[tiered_flow(arrival_rate=ARRIVAL_RATE,
                                     tiers=FRONTIER.tiers.values(),
                                     flow=FlowTag.MAIN,
                                     time_limit=TIME_LIMIT)])

    simulator.report()
