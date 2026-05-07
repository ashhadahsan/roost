# Retries

Every job has a `max_attempts` (default `20`). When a handler raises, Roost:

1. Records the exception (type, message, traceback) into the `errors` JSON column.
2. If `attempt < max_attempts`, marks the job `retryable` and schedules it for a future `scheduled_at` based on the configured backoff strategy.
3. Otherwise marks the job `discarded`.

## Backoff strategies

```python
from roost import exponential, linear, fixed

worker = roost.worker(retry_strategy=exponential(base=2, jitter=True))
worker = roost.worker(retry_strategy=linear(step=30.0))
worker = roost.worker(retry_strategy=fixed(60.0))
```

The default is `exponential(base=2, jitter=True)` — `~2, 4, 8, 16, … seconds`, capped at one day.

## Snoozing

Inside a handler you can raise `roost.SnoozeJob(seconds=…)` to push the job back into `available` after `seconds`, *without* counting the run as a failure or incrementing the attempt counter. Useful when an external dependency tells you to retry later.
