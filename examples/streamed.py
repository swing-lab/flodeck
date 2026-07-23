"""Streamed example: a custom job generator with a holding buffer."""

import random

from flodeck import FloDeck, Job

TOTAL_NUM_NODES = 9472

ARRIVAL_RATE = 100.       # Poisson: 100 jobs per time unit on average
EXECUTION_TIME_MEAN = 4.  # Normal: mean runtime in time units
SPAN_RATE = 1 / 7.        # Poisson: ~7 nodes per job on average

TIME_LIMIT = 1000.


def custom_flow(arrival_rate, execution_time, span_rate, time_limit):
    """Yield jobs with normal runtimes and Poisson-distributed spans."""

    arrives_at = random.expovariate(arrival_rate)
    while time_limit and arrives_at < time_limit:

        span = int(round(random.expovariate(span_rate), 0)) + 1
        yield Job(arrived_at=arrives_at,
                  runtime=random.normalvariate(execution_time,
                                               execution_time / 2),
                  span=span,
                  flow='custom')

        arrives_at += random.expovariate(arrival_rate)


if __name__ == '__main__':

    simulator = FloDeck(num_nodes=TOTAL_NUM_NODES,
                        hold_overflow=True,
                        time_limit=TIME_LIMIT,
                        output_path='flodeck_streamed_output.txt',
                        trace_path='flodeck_streamed_trace.txt')

    simulator.run(flows=[custom_flow(arrival_rate=ARRIVAL_RATE,
                                     execution_time=EXECUTION_TIME_MEAN,
                                     span_rate=SPAN_RATE,
                                     time_limit=TIME_LIMIT)],
                  verbose=True)

    print(f'Utilization: {simulator.utilization()}')
