# Operating Roost

This page covers the day-2 operational surface — pausing queues, cancelling jobs, recovering from worker crashes, and bulk-reviving the dead-letter pile.

## Queue pause / resume

Halt a queue without killing workers — useful when a downstream service is down:

```bash
roost queue pause emails
roost queue resume emails
roost queue list
```

Workers will not fetch jobs from a paused queue. Jobs already in flight finish normally.

## Cancellation

```bash
roost cancel 42
```

If the job is `available` or `retryable`, it's moved straight to `cancelled`. If it's currently `executing`, Roost flips `cancel_requested = true` and fires `NOTIFY roost_cancel_requested`. Every running worker listens on that channel and cancels the matching `asyncio.Task`. The handler sees an `asyncio.CancelledError` and the row finalizes to `cancelled`.

Handlers that need to clean up gracefully should catch `CancelledError`, do their cleanup, and re-raise.

## Per-job timeouts

```python
await roost.enqueue(send_invoice, args={"id": 7}, timeout_seconds=30)
```

A timeout uses `asyncio.wait_for` around the handler. The job goes to `retryable` (or `discarded` if attempts exhausted) with the `TimeoutError` recorded in the `errors` array.

## Worker heartbeats and orphan recovery

Every worker writes its identity (host, pid, queues, in-flight count) into `roost.workers` on the heartbeat interval (default 15s). Stale rows are GC'd by peer workers.

If a worker is SIGKILL'd while a job is `executing`, the row stays `executing` until the orphan reaper running in another worker picks it up. Default staleness window is 5 minutes (`--orphan-stale-after`). On reap:

- `attempt < max_attempts` → back to `retryable`, scheduled to run immediately.
- `attempt = max_attempts` → `discarded`, with `WorkerCrash` recorded in errors.

## Mass requeue

Resurrect everything in the dead-letter pile:

```bash
roost requeue --discarded
```

This zeroes the attempt counter and clears `cancel_requested`. Use sparingly.

## Workers list

```bash
roost workers
```

Shows current heartbeats — host, pid, queues, last seen.
