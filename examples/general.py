"""General example: two workload flows feeding an M/M/c-like system."""

from pathlib import Path

from flodeck import FloDeck, FlowTag, file_flow, poisson_flow

ARRIVAL_RATE = 22. / 72  # jobs arrive every 72/22 time units on avg
SERVICE_RATE = 1. / 3    # jobs run for 3 time units on average
NUM_NODES = 1000         # M/M/NUM_NODES

TIME_LIMIT = 1000.
NUM_ATTEMPTS = 2
VERBOSE = True


if __name__ == '__main__':

    simulator = FloDeck(num_nodes=NUM_NODES,
                        output_path='flodeck_general_output.txt')

    max_job_count, mean_job_count, mean_flow_time = 0, 0., 0.
    input_path = Path(__file__).parent / 'flodeck_input.txt'

    for _ in range(NUM_ATTEMPTS):

        simulator.run(
            flows=[file_flow(path=str(input_path),
                             flow=FlowTag.EXTERNAL,
                             time_limit=TIME_LIMIT),
                   poisson_flow(arrival_rate=ARRIVAL_RATE,
                                execution_rate=SERVICE_RATE,
                                span=100,
                                flow=FlowTag.MAIN,
                                time_limit=TIME_LIMIT)],
            verbose=VERBOSE)

        mean_job_count += simulator.mean_job_count()
        mean_flow_time += simulator.mean_flow_time()

        if simulator.trace:
            run_max = max(entry.held + entry.queued
                          for entry in simulator.trace)
            max_job_count = max(max_job_count, run_max)

        if VERBOSE:
            print('Output:',
                  [job.tag for job in simulator.completed])

    print(f'AVG number of jobs: {mean_job_count / NUM_ATTEMPTS} '
          f'(max waiting: {max_job_count}); '
          f'AVG delay: {mean_flow_time / NUM_ATTEMPTS}')

