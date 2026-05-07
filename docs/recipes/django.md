# Django

```python
# myapp/tasks.py
from roost import Roost, job

roost = Roost("postgresql://...")

@job("resize_image")
def resize_image(image_id: int) -> None:
    ...
```

Inside a Django view, use the active connection so the job is committed inside Django's atomic block:

```python
from django.db import transaction, connection
from myapp.tasks import roost, resize_image

def upload(request):
    with transaction.atomic():
        image = Image.objects.create(...)
        # Django's `connection.connection` is the underlying psycopg connection.
        roost.enqueue(resize_image, args={"image_id": image.id}, conn=connection.connection)
    return HttpResponse("ok")
```

Run a worker:

```bash
roost run --module myapp.tasks
```
