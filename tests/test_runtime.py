import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from uuid import uuid4

from fastapi.testclient import TestClient
import pytest

from src.api import create_app
from src.broker import Publisher, topology
from src.store import KeyConflict, Store
from src.worker import handle


@pytest.fixture
def store(tmp_path):
    s = Store(tmp_path / "jobs.db")
    s.init()
    return s


class PublisherDouble:
    def __init__(self, store, broken=False):
        self.store, self.broken, self.ids = store, broken, []

    def dispatch(self, id_):
        if self.broken:
            raise ConnectionError()
        self.ids.append(id_)
        self.store.sent(id_)


class ChannelDouble:
    def __init__(self, fail_ack=False):
        self.events = []
        self.fail_ack = fail_ack

    def basic_ack(self, **kw):
        if self.fail_ack:
            raise ConnectionError("Connection lost after commit")
        self.events.append(("ack", kw))

    def basic_nack(self, **kw):
        self.events.append(("nack", kw))


def payload(mode="normal"):
    return {"title": "Test report", "duration": 4, "mode": mode}


def deliver(store, id_, channel=None):
    ch = channel or ChannelDouble()
    handle(ch, SimpleNamespace(delivery_tag=1, redelivered=False), None,
           json.dumps({"job_id": id_}).encode(), store, sleep=lambda _: None)
    return ch


def test_202_then_poll_and_validation(store):
    pub = PublisherDouble(store)
    with TestClient(create_app(store, pub, False)) as client:
        r = client.post("/api/jobs", json=payload())
        assert r.status_code == 202
        id_ = r.json()["id"]
        assert id_ in pub.ids
        assert client.get(r.headers["location"]).json()["status"] == "queued"
        assert float(r.headers["x-response-time-ms"]) >= 0
        deliver(store, id_)
        assert client.get(r.headers["location"]).json()["status"] == "completed"
        assert client.get("/api/jobs/not-here").status_code == 404
        assert client.post("/api/jobs", json={"title": ""}).status_code == 422
        assert client.post("/api/jobs", json={**payload(), "duration": True}).status_code == 422


def test_request_key_prevents_duplicate_and_conflicting_reuse(store):
    with TestClient(create_app(store, PublisherDouble(store), False)) as c:
        headers = {"Idempotency-Key": "same"}
        a = c.post("/api/jobs", json=payload(), headers=headers)
        b = c.post("/api/jobs", json=payload(), headers=headers)
        assert a.json()["id"] == b.json()["id"]
        assert c.post("/api/jobs", json={**payload(), "title": "Other"}, headers=headers).status_code == 409
        assert len(store.recent()) == 1


def test_outbox_survives_publish_failure_and_recovery(store):
    pub = PublisherDouble(store, broken=True)
    with TestClient(create_app(store, pub, False)) as c:
        r = c.post("/api/jobs", json=payload(), headers={"Idempotency-Key": "recover"})
        assert r.status_code == 503
        id_ = r.json()["id"]
        assert store.pending() == [id_]
        pub.broken = False
        pub.dispatch(id_)
        assert store.pending() == []
        r2 = c.post("/api/jobs", json=payload(), headers={"Idempotency-Key": "recover"})
        assert r2.json()["id"] == id_


def test_commit_before_ack_crash_does_not_repeat_effect(store):
    id_ = store.create(payload(), "commit-gap")
    with pytest.raises(ConnectionError):
        deliver(store, id_, ChannelDouble(fail_ack=True))
    assert store.get(id_)["status"] == "completed"
    ch = deliver(store, id_)
    assert ch.events[0][0] == "ack"
    assert store.effect_count(id_) == 1
    assert store.get(id_)["attempts"] == 1


def test_retry_once_requeues_then_acknowledges(store):
    id_ = store.create(payload("retry_once"), "retry")
    first = deliver(store, id_)
    assert first.events == [("nack", {"delivery_tag": 1, "requeue": True})]
    assert store.get(id_)["status"] == "queued"
    second = deliver(store, id_)
    assert second.events[0][0] == "ack"
    assert store.get(id_)["attempts"] == 2


def test_poison_is_terminal_and_nacked_to_dlx(store):
    id_ = store.create(payload("poison"), "poison")
    ch = deliver(store, id_)
    assert ch.events == [("nack", {"delivery_tag": 1, "requeue": False})]
    assert store.get(id_)["status"] == "failed"
    assert store.effect_count(id_) == 0


@pytest.mark.parametrize("body", [b"not json", b"[]", b"{}", b'{"job_id":"no"}',
                                  json.dumps({"job_id": str(uuid4())}).encode()])
def test_malformed_or_unknown_envelope_not_retried(store, body):
    ch = ChannelDouble()
    handle(ch, SimpleNamespace(delivery_tag=1), None, body, store)
    assert ch.events == [("nack", {"delivery_tag": 1, "requeue": False})]


def test_repeated_interruptions_have_bound(store):
    id_ = store.create(payload(), "interrupt")
    for _ in range(3):
        store.start(id_)
    ch = deliver(store, id_)
    assert store.get(id_)["status"] == "failed"
    assert ch.events[0][1]["requeue"] is False


def test_parallel_duplicate_completions_create_one_report(store):
    id_ = store.create(payload(), "duplicate")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: store.complete(id_), range(8)))
    assert store.effect_count(id_) == 1


def test_creation_rolls_back_when_outbox_insert_fails(store):
    with store.db() as db:
        db.execute("""CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox
                      BEGIN SELECT RAISE(ABORT,'forced'); END""")
    with pytest.raises(Exception):
        store.create(payload(), "rollback")
    assert store.recent() == []


def test_publisher_confirms_before_marking_sent(store, monkeypatch):
    import src.broker as broker
    id_ = store.create(payload(), "confirm")
    events = []

    class Channel:
        def exchange_declare(self, **kw): pass
        def queue_declare(self, **kw): pass
        def queue_bind(self, **kw): pass
        def confirm_delivery(self): events.append("confirm")
        def basic_publish(self, **kw):
            assert store.unsent(id_)
            assert kw["mandatory"]
            assert kw["properties"].delivery_mode == 2
            assert json.loads(kw["body"])["job_id"] == id_
            events.append("publish")

    class Connection:
        is_open = True
        def channel(self): return Channel()
        def close(self): events.append("close")
    monkeypatch.setattr(broker, "connect", Connection)
    Publisher(store).dispatch(id_)
    assert events == ["confirm", "publish", "close"]
    assert not store.unsent(id_)


def test_broker_topology_retains_rejected_messages():
    calls = []

    class Channel:
        def exchange_declare(self, **kw): calls.append(("exchange", kw))
        def queue_declare(self, **kw): calls.append(("queue", kw))
        def queue_bind(self, **kw): calls.append(("bind", kw))
    topology(Channel())
    args = calls[-1][1]["arguments"]
    assert args["x-dead-letter-strategy"] == "at-least-once"
    assert args["x-overflow"] == "reject-publish"
    assert args["x-delivery-limit"] == 20
    assert all(kw["durable"] for kind, kw in calls if kind == "queue")


def test_failed_broker_confirmation_retains_outbox(store, monkeypatch):
    import src.broker as broker
    id_ = store.create(payload(), "unconfirmed")

    class Channel:
        def exchange_declare(self, **kw): pass
        def queue_declare(self, **kw): pass
        def queue_bind(self, **kw): pass
        def confirm_delivery(self): pass
        def basic_publish(self, **kw): raise ConnectionError("lost confirmation")

    class Connection:
        is_open = True
        def channel(self): return Channel()
        def close(self): pass
    monkeypatch.setattr(broker, "connect", Connection)
    with pytest.raises(ConnectionError):
        Publisher(store).dispatch(id_)
    assert store.unsent(id_)


def test_main_registers_manual_ack_and_prefetch_one(store, monkeypatch):
    import src.worker as worker
    events = []

    class Channel:
        def exchange_declare(self, **kw): pass
        def queue_declare(self, **kw): pass
        def queue_bind(self, **kw): pass
        def basic_qos(self, **kw): events.append(("qos", kw))
        def basic_consume(self, **kw): events.append(("consume", kw))
        def start_consuming(self): raise KeyboardInterrupt()

    class Connection:
        is_open = True
        def channel(self): return Channel()
        def close(self): pass
    monkeypatch.setenv("JOB_DB", store.path)
    monkeypatch.setattr(worker, "connect", Connection)
    worker.main()
    assert events[0] == ("qos", {"prefetch_count": 1})
    assert events[1][1]["auto_ack"] is False
