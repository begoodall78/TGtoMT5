from __future__ import annotations
from typing import Literal, Optional, List
from pydantic import BaseModel, Field
import time

ActionType = Literal["OPEN", "MODIFY", "CLOSE", "CANCEL"]
Venue = Literal["MT5"]
Side = Literal["BUY", "SELL"]

class Leg(BaseModel):
    leg_id: str                  # e.g. "KAM_PRE#1"
    symbol: str                  # "XAUUSD"
    side: Side
    volume: float
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    tag: Optional[str] = None    # free tag, passed through to MT5 (e.g., comment)
    # NEW (optional)
    position_ticket: Optional[int] = None
    order_ticket: Optional[int] = None

class Action(BaseModel):
    action_id: str               # idempotency key (stable hash)
    type: ActionType
    venue: Venue = "MT5"
    legs: List[Leg]
    source_msg_id: Optional[str] = None
    created_ts: float = Field(default_factory=lambda: time.time())

class RouterResult(BaseModel):
    action_id: str
    status: Literal["OK", "ERROR", "DUPLICATE", "SKIPPED"]
    error_code: Optional[int] = None
    error_text: Optional[str] = None
    details: Optional[dict] = None
