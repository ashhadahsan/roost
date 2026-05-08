# Quickstart

## Install

```bash
pip install pgroost
```

> The PyPI distribution is `pgroost` (the bare `roost` name on PyPI is reserved). The import path stays `import roost` and the CLI command stays `roost`.

## Apply the schema

```bash
roost init --apply --dsn postgresql://user:pass@localhost/app
```

## Define a task

```python
# tasks.py
from roost import job

@job("send_welcome_email")
async def send_welcome_email(user_id: int) -> None:
    print(f"sending welcome email to user {user_id}")
```

## Enqueue from your app

### FastAPI / async

```python
from roost import AsyncRoost
from tasks import send_welcome_email  # noqa — registers the task

roost = AsyncRoost("postgresql://user:pass@localhost/app")

# inside a request handler — pass `conn=` to enqueue inside the caller's txn
await roost.enqueue(send_welcome_email, args={"user_id": 42}, conn=tx_conn)
```

### Django / sync

```python
from roost import Roost, job

@job("resize_image")
def resize_image(image_id: int) -> None:
    ...

roost = Roost("postgresql://user:pass@localhost/app")
roost.enqueue(resize_image, args={"image_id": 7})
```

## Run a worker

```bash
roost run --module tasks --queues default --concurrency 4
```

## Inspect

```bash
roost status
```

That's it. From here, read [`concepts/transactional-enqueue`](concepts/transactional-enqueue.md) — the load-bearing primitive that makes Roost different.
