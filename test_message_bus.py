from message_bus import MessageBus


def test_exact_topic_delivery():
    bus = MessageBus()
    got = []

    def handler(payload):
        got.append(payload)

    bus.subscribe("orders.created", handler)
    n = bus.publish("orders.created", "OrderCreated", {"id": 1})
    assert n == 1
    assert got[0]["payload"] == {"id": 1}
    assert got[0]["type"] == "OrderCreated"


def test_wildcard_matches_many_subtopics():
    bus = MessageBus()
    got = []
    bus.subscribe("orders.*", lambda p: got.append(p["topic"]))
    bus.publish("orders.created", "A", {})
    bus.publish("orders.shipped", "B", {})
    bus.publish("billing.paid", "C", {})
    assert got == ["orders.created", "orders.shipped"]


def test_hash_matches_whole_subtree():
    bus = MessageBus()
    got = []
    bus.subscribe("orders.#", lambda p: got.append(p["topic"]))
    bus.publish("orders.created", "A", {})
    bus.publish("orders.line.item.added", "B", {})
    assert sorted(got) == ["orders.created", "orders.line.item.added"]


def test_failing_consumer_does_not_break_others():
    bus = MessageBus()
    ok = []

    def good(p):
        ok.append(p["type"])

    def bad(p):
        raise RuntimeError("boom")

    bus.subscribe("t", good)
    bus.subscribe("t", bad)
    bus.publish("t", "event", {})
    assert ok == ["event"]
    assert bus.stats()["dropped"] == 1


def test_subscribers_counted_per_topic():
    bus = MessageBus()
    bus.subscribe("a", lambda p: None)
    bus.subscribe("a", lambda p: None)
    bus.subscribe("b", lambda p: None)
    assert bus.stats()["subscribers"] == 3
