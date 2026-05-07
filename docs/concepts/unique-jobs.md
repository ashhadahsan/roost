# Unique jobs

Pass `unique_key=…` to deduplicate against any active job (`available`, `executing`, or `retryable`) with the same key:

```python
await roost.enqueue(
    sync_user_to_billing,
    args={"user_id": 42},
    unique_key=f"sync_user:{user_id}",
)
```

If a matching active row exists, `enqueue` returns its id and inserts nothing new. Once the original job reaches a terminal state (`completed`, `discarded`, `cancelled`), the key becomes available again.

The check is enforced by a partial unique index — there is no application-level race window.
