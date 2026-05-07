# Transactional enqueue

The single most important property of Roost: **a job is inserted using your connection, in your transaction**.

If your business write rolls back, the job rolls back with it. If your business write commits, the job is queued. There is no window in which one happens without the other.

```python
async with pool.acquire() as conn:
    async with conn.transaction():
        user_id = await create_user(conn, email=email)
        await roost.enqueue(send_welcome_email, args={"user_id": user_id}, conn=conn)
        # both rows commit together — or roll back together
```

## Why it matters

In Celery / RQ / dramatiq, jobs go to Redis (or RabbitMQ). The user's transaction commits to Postgres; the enqueue goes to a separate broker. Two writes, no shared transaction.

This produces classic bugs:
- The DB write commits but the broker is down → job lost.
- The broker accepts the job but the DB rolls back → orphaned job that fires against nothing.

Roost can't have either bug. There is exactly **one** durable store, and it commits atomically.

## How it works

Roost's [`enqueue`](../reference/api.md) accepts a `conn=` argument. We use that connection — and only that connection — for the `INSERT INTO roost.jobs`. Whatever transaction you've started on it is the transaction we participate in.

Don't pass `conn=`, and we acquire one of our own and run a tiny single-statement transaction. Useful for fire-and-forget cases, but you give up the killer feature.
