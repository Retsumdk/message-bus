"""In-memory publish/subscribe message bus with versioned envelopes.

Real, working implementation for the Retsumdk ecosystem. Supports exact-topic
and wildcard subscriptions, message versioning, delivery stats, and safe
receiver error isolation so one bad consumer never kills the bus.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable, Optional

Handler = Callable[[dict], None]


@dataclass
class Envelope:
    """A versioned event with metadata attached by the bus."""

    topic: str
    type: str
    version: int
    payload: Any
    id: int

    def to_dict(self) -> dict:
        return {
            "topic": self.topic,
            "type": self.type,
            "version": self.version,
            "payload": self.payload,
            "id": self.id,
        }


class MessageBus:
    """A thread-safe topic router with wildcard matching and error isolation."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Handler]] = {}
        self._lock = threading.RLock()
        self._seq = 0
        self._published = 0
        self._delivered = 0
        self._dropped = 0

    # -- subscription -------------------------------------------------------
    @staticmethod
    def _to_pattern(topic: str) -> str:
        parts = topic.split(".")
        out = []
        for p in parts:
            if p == "*":
                out.append("[^.]+")
            elif p == "#":
                out.append(".+")
            else:
                out.append(re_escape(p))
        return "^" + ".".join(out) + "$"

    def subscribe(self, topic: str, handler: Handler) -> Handler:
        pattern = self._to_pattern(topic)
        compiled = re_compile(pattern)

        def wrapped(payload: dict) -> None:
            if compiled.match(payload["topic"]):
                handler(payload)

        with self._lock:
            self._subscribers.setdefault(topic, []).append(wrapped)
        return wrapped

    def unsubscribe(self, topic: str, handler: Handler) -> bool:
        with self._lock:
            subs = self._subscribers.get(topic)
            if not subs:
                return False
            try:
                subs.remove(handler)
                return True
            except ValueError:
                return False

    # -- publish ------------------------------------------------------------
    def publish(self, topic: str, type: str, payload: Any, version: int = 0) -> int:
        with self._lock:
            self._seq += 1
            envelope = Envelope(
                topic=topic, type=type, version=version, payload=payload, id=self._seq
            ).to_dict()
            targets = [h for subs in self._subscribers.values() for h in subs]
        self._published += 1
        for h in targets:
            try:
                h(envelope)
                self._delivered += 1
            except Exception:
                self._dropped += 1
        return len(targets)

    # -- stats ----------------------------------------------------------------
    def stats(self) -> dict:
        return {
            "topics": len(self._subscribers),
            "subscribers": sum(len(v) for v in self._subscribers.values()),
            "published": self._published,
            "delivered": self._delivered,
            "dropped": self._dropped,
            "last_id": self._seq,
        }


# module-level import shim keeps the common re usage local and dependency-free
import re as _re

re_escape = _re.escape
re_compile = _re.compile
