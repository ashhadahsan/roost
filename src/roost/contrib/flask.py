"""Flask integration helpers.

The :class:`RoostExtension` exposes a Flask-style ``current_app.roost``::

    from flask import Flask, current_app
    from roost.contrib.flask import RoostExtension

    app = Flask(__name__)
    RoostExtension(app, dsn="postgresql://...")

    @app.post("/users")
    def create_user():
        current_app.roost.enqueue("send_welcome", args={"user_id": 42})
        return ""
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from roost.decorators import HandlerRegistry
from roost.sync_api import Roost

if TYPE_CHECKING:  # pragma: no cover
    from flask import Flask


class RoostExtension:
    """Flask extension that attaches a ``Roost`` instance to the app.

    Available afterwards as ``current_app.roost`` and ``app.roost``.
    """

    def __init__(
        self,
        app: Flask | None = None,
        *,
        dsn: str | None = None,
        registry: HandlerRegistry | None = None,
    ) -> None:
        self._dsn = dsn
        self._registry = registry
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Flask) -> None:
        dsn = self._dsn or app.config.get("ROOST_DSN")
        if not dsn:
            raise RuntimeError("ROOST_DSN not set — pass dsn= to RoostExtension or set it in app.config")
        roost = Roost(dsn, registry=self._registry)
        # Attach two ways: app.roost (canonical) and app.extensions["roost"] (Flask convention).
        app.roost = roost
        app.extensions = getattr(app, "extensions", {})
        app.extensions["roost"] = roost


__all__ = ["RoostExtension"]
