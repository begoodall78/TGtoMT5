from __future__ import annotations
from typing import Protocol
from app.models import Action, RouterResult

class IRouter(Protocol):
    """Routing contract: execute an Action on a venue (e.g., MT5) and return a RouterResult."""
    def execute(self, action: Action) -> RouterResult: ...
