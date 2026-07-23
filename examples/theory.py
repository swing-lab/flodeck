"""Analytic M/M/c reference values (Erlang-C) for validating runs."""

import math

from decimal import Decimal

ARRIVAL_RATE = 22. / 72  # jobs arrive every 72/22 time units on avg
SERVICE_RATE = 1. / 3    # jobs run for 3 time units on average
NUM_NODES = 1000         # M/M/NUM_NODES


def p_zero(n, a_rate, s_rate):
    """Probability that the system is empty."""

    rho = a_rate / (n * s_rate)
    return (
        Decimal(1) / (
            (((a_rate / s_rate) ** n)
             / (math.factorial(n) * (Decimal(1) - rho)))
            + sum((((a_rate / s_rate) ** i) / math.factorial(i))
                  for i in range(n))
        )
    )


def p_queued(n, a_rate, s_rate):
    """Probability that an arriving job has to wait (Erlang C)."""

    rho = a_rate / (n * s_rate)
    return (
        p_zero(n, a_rate, s_rate)
        * (((a_rate / s_rate) ** n)
           / (math.factorial(n) * (Decimal(1) - rho)))
    )


def mean_job_count(n, a_rate, s_rate):
    """Mean number of jobs in the system."""

    rho = a_rate / (n * s_rate)
    return (
        ((rho * p_queued(n, a_rate, s_rate)) / (Decimal(1) - rho))
        + (a_rate / s_rate)
    )


def mean_delay(n, a_rate, s_rate):
    """Mean time a job spends in the system."""

    return (
        (p_queued(n, a_rate, s_rate) / ((n * s_rate) - a_rate))
        + (Decimal(1) / s_rate)
    )


if __name__ == '__main__':

    arrival = Decimal(f'{ARRIVAL_RATE}')
    service = Decimal(f'{SERVICE_RATE}')

    print(f'AVG number of jobs: '
          f'{mean_job_count(NUM_NODES, arrival, service)}; '
          f'AVG delay: {mean_delay(NUM_NODES, arrival, service)}')
