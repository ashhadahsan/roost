# Flask

```python
import psycopg
from flask import Flask, request
from roost import Roost, job

app = Flask(__name__)
roost = Roost("postgresql://...")

@job("send_invoice")
def send_invoice(order_id: int) -> None:
    ...

@app.post("/orders")
def create_order():
    with psycopg.connect("postgresql://...") as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO orders (...) VALUES (...) RETURNING id")
                row = cur.fetchone()
                assert row is not None
                order_id = row[0]
            roost.enqueue(send_invoice, args={"order_id": order_id}, conn=conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    return {"id": order_id}, 201
```
