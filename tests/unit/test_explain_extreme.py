"""Explain returns for a large number of servers and a far truncation."""

import math

import pytest

from desflow.explain.formulas import erlang_c, erlang_wait
from desflow.models.distributions import get_distribution
from desflow.models.params import DistributionConfig


def _erlang_c_by_sums(servers, load):
    last = load**servers / math.factorial(servers) / (1 - load / servers)
    return last / (sum(load**k / math.factorial(k) for k in range(servers)) + last)


@pytest.mark.parametrize("servers, load", [(1, 0.5), (5, 3.0), (50, 40.0), (100, 95.0)])
def test_the_recurrence_gives_what_the_sums_gave(servers, load):
    assert erlang_c(servers, load) == pytest.approx(_erlang_c_by_sums(servers, load))


def test_erlang_wait_for_150_servers_is_given():
    wait = erlang_wait(150, 120, 1.0)
    assert 0 < wait < 1


@pytest.mark.parametrize("low", [0.5, 1.0, 5.0, 50.0])
def test_a_truncated_normal_far_above_its_mean_has_a_mean_near_low(low):
    family = get_distribution("truncated_normal")
    config = DistributionConfig(dist="truncated_normal", mean=0, std=0.1, low=low)
    mean, variance = family.mean(config), family.variance(config)
    assert low <= mean < low + 0.1
    assert variance >= 0
