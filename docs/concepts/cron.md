# Cron

```python
from roost import cron

@cron("*/5 * * * *", queue="emails")
async def send_digests() -> None:
    ...
```

The first worker process to acquire the cluster-wide advisory lock runs the cron scheduler. Every 60 seconds it checks the registry and enqueues any due entries; uniqueness on `cron:<name>:<unix-timestamp>` ensures the same slot is never enqueued twice even if scheduler instances race during a restart.

To run a worker without the cron loop:

```bash
roost run --no-cron
```
