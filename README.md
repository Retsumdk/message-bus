# message-bus

> In-memory publish/subscribe message bus with exact- and wildcard-topic subscriptions and a versioned event envelope.

### What it is

In-memory publish/subscribe message bus with versioned envelopes.

Real, working Python for the Retsumdk ecosystem with an executable test suite.

## Getting started

```bash
pip install -r requirements.txt
pytest -q
```

## Features

- **Exact, wildcard and subtree routing** — subscribe to `orders.created`, `orders.*` (one level) or `orders.#` (any depth).
- **Versioned envelopes** — every message carries `topic`, `type`, `version`, `payload` and a monotonic `id`, so consumers can detect ordering and schema generation.
- **Failure isolation** — a handler that raises is counted as *dropped*; every other subscriber still receives the message.
- **Thread-safe** — subscription and publish are guarded by a re-entrant lock, and handlers run *outside* the lock, so a handler may publish a follow-up event without deadlocking.
- **Zero runtime dependencies** — standard library only (`threading`, `dataclasses`, `re`).

## Architecture

```
publisher
   │  publish(topic, type, payload, version)
   ▼
MessageBus
   ├── _to_pattern()   "orders.created" → ^orders\.created$
   │                   "orders.*"       → ^orders\.[^.]+$
   │                   "orders.#"       → ^orders\..+$
   ├── Envelope        {topic, type, version, payload, id}    id = monotonic sequence
   ├── dispatch        every registered wrapper is offered the envelope
   │                   ├── handler raises  → dropped += 1
   │                   └── otherwise       → delivered += 1
   └── stats()         topics, subscribers, published, delivered, dropped, last_id
   ▼
subscriber handlers (each wrapper self-filters with its compiled pattern)
```

Subscriptions are keyed by the *pattern string* the caller passed in, so `unsubscribe()`
needs the wrapper returned by `subscribe()` — not the original handler function.

## Usage

```python
from message_bus import MessageBus

bus = MessageBus()
received = []

def on_created(event: dict) -> None:
    received.append(event["payload"])

def audit(event: dict) -> None:
    print(f"{event['id']:>3}  {event['type']:<14} {event['topic']} v{event['version']}")

bus.subscribe("orders.created", on_created)   # exact topic
bus.subscribe("orders.#", audit)              # any depth under orders.

bus.publish("orders.created", "OrderCreated", {"order_id": "A-1001"}, version=2)
bus.publish("orders.line.item.added", "LineItemAdded", {"sku": "SKU-9"}, version=2)

print(received)
print(bus.stats())
```

Output:

```
  1  OrderCreated   orders.created v2
  2  LineItemAdded  orders.line.item.added v2
[{'order_id': 'A-1001'}]
{'topics': 2, 'subscribers': 2, 'published': 2, 'delivered': 4, 'dropped': 0, 'last_id': 2}
```

### Unsubscribing

```python
wrapper = bus.subscribe("billing.paid", lambda e: print(e["payload"]))
bus.unsubscribe("billing.paid", wrapper)   # returns True
bus.unsubscribe("billing.paid", wrapper)   # returns False — already removed
```

## API reference

| Member | Signature | Notes |
| --- | --- | --- |
| `subscribe` | `subscribe(topic, handler) -> Handler` | Returns the pattern-checking wrapper. Keep it if you intend to unsubscribe. |
| `unsubscribe` | `unsubscribe(topic, handler) -> bool` | `True` if the wrapper was registered under that topic and is now removed, `False` otherwise. |
| `publish` | `publish(topic, type, payload, version=0) -> int` | Returns the number of registered wrappers the envelope was **offered to**. |
| `stats` | `stats() -> dict` | Counters described below. |
| `Envelope` | dataclass | Fields: `topic`, `type`, `version`, `payload`, `id`; `to_dict()` for JSON-ready output. |

`version` is caller-supplied and travels untouched on the envelope — the bus never infers or
rewrites it, which keeps producers free to use semantic or schema versions.

## Delivery semantics and failure modes

These three counters are exact and worth reading carefully before you build on them:

- `published` — number of `publish()` calls. Use this for volume.
- `delivered` — offers that returned without raising. A wrapper whose pattern did **not** match
  also returns without raising, so `delivered` includes non-matching offers.
- `dropped` — offers where a handler raised an exception. The exception is swallowed so the
  remaining subscribers still run.

Consequently `publish()` returns an upper bound on matching subscribers (all wrappers are
offered every envelope and self-filter), not a precise match count. Track matches inside your
own handler if you need an exact figure.

Other behaviours to plan around:

- **In-memory only** — nothing is persisted; undelivered work is not replayed after a restart.
- **Synchronous dispatch** — `publish()` returns only after every matching handler has run.
- **Best-effort counters under concurrency** — `published` / `delivered` / `dropped` are
  incremented outside the lock, so they are accurate single-threaded and approximate under
  heavy parallel publishing.
- **Wildcards match topics, not payloads** — there is no content-based filtering.

## Real-world use case

A pipeline of agents emitting lifecycle events (`agent.started`, `agent.tool.called`,
`agent.finished`) can register one subtree subscription (`agent.#`) for a central audit
logger while individual components subscribe only to the exact topics they care about. The
audit logger keeps running even when a consumer throws, and the monotonic `id` gives the log
a total order without a database.

## Testing

```bash
pip install -r requirements.txt
pytest -q
```

Five tests cover exact delivery, single-level wildcard matching, subtree matching,
failure isolation across subscribers, and per-topic subscriber counting.


## License

[MIT](LICENSE) © Retsumdk
