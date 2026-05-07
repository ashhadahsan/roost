"""Web framework integrations.

Each submodule is optional and lazy-imports its host framework, so
``import roost.contrib`` is safe even when none of the frameworks are
installed. Import the specific module you need::

    from roost.contrib.fastapi import RoostDep
    from roost.contrib.django import enqueue_in_atomic
    from roost.contrib.flask import RoostExtension
"""

from __future__ import annotations
