"""Simulated processing outcomes for the consumer.

Real systems fail because a downstream service times out, a row lock is held, a
payment gateway 503s, and so on. This project has no such downstream, so failures
are simulated here in a controlled, demonstrable way:

* If a message carries an ``x-fail-times: N`` header (set by the producer's
  ``--flaky-rate``), it fails **transiently** on its first ``N`` attempts and
  succeeds afterwards -- a deterministic way to show the retry topic working.
* Otherwise, each message fails transiently with probability ``transient_rate``.

Permanent failures are *not* simulated here -- they arise naturally from Avro
decode errors and ``Order`` validation, and the consumer handles those directly.
"""

import random
from enum import Enum
from typing import Dict


class Outcome(str, Enum):
    SUCCESS = "SUCCESS"
    TRANSIENT = "TRANSIENT"
    PERMANENT = "PERMANENT"


class TransientError(Exception):
    """A failure that is expected to succeed on retry."""


class PermanentError(Exception):
    """A failure that will never succeed; route straight to the DLQ."""


class FailureSimulator:
    def __init__(self, transient_rate: float = 0.0, rng: random.Random = None):
        if not 0.0 <= transient_rate <= 1.0:
            raise ValueError("transient_rate must be between 0 and 1")
        self.transient_rate = transient_rate
        self._rng = rng or random

    def classify(self, headers: Dict[str, str], retry_count: int) -> Outcome:
        """Decide the outcome for a *valid* order. Returns SUCCESS or TRANSIENT."""
        fail_times_raw = headers.get("x-fail-times")
        if fail_times_raw is not None:
            try:
                fail_times = int(fail_times_raw)
            except (TypeError, ValueError):
                fail_times = 0
            return Outcome.TRANSIENT if retry_count < fail_times else Outcome.SUCCESS

        if self.transient_rate and self._rng.random() < self.transient_rate:
            return Outcome.TRANSIENT
        return Outcome.SUCCESS
