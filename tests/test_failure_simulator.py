import random

import pytest

from src.consumer.failure_simulator import FailureSimulator, Outcome


def test_fail_times_header_is_deterministic():
    sim = FailureSimulator(transient_rate=0.0)
    headers = {"x-fail-times": "2"}
    assert sim.classify(headers, retry_count=0) == Outcome.TRANSIENT
    assert sim.classify(headers, retry_count=1) == Outcome.TRANSIENT
    assert sim.classify(headers, retry_count=2) == Outcome.SUCCESS
    assert sim.classify(headers, retry_count=3) == Outcome.SUCCESS


def test_no_header_and_zero_rate_always_succeeds():
    sim = FailureSimulator(transient_rate=0.0)
    for _ in range(50):
        assert sim.classify({}, retry_count=0) == Outcome.SUCCESS


def test_rate_one_always_transient():
    sim = FailureSimulator(transient_rate=1.0, rng=random.Random(0))
    for _ in range(50):
        assert sim.classify({}, retry_count=0) == Outcome.TRANSIENT


def test_rate_is_seeded_and_reproducible():
    a = FailureSimulator(transient_rate=0.5, rng=random.Random(42))
    b = FailureSimulator(transient_rate=0.5, rng=random.Random(42))
    seq_a = [a.classify({}, 0) for _ in range(20)]
    seq_b = [b.classify({}, 0) for _ in range(20)]
    assert seq_a == seq_b
    assert Outcome.TRANSIENT in seq_a and Outcome.SUCCESS in seq_a


def test_invalid_rate_rejected():
    with pytest.raises(ValueError):
        FailureSimulator(transient_rate=1.5)
