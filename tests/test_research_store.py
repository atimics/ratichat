"""Saved request order, claims, source scope, and confirmed conversation history."""

from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3

import pytest

from chatbot.core.node_system.research_store import ResearchStore


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def snapshot(event="one", content="What changed?"):
    source = {"id": event, "sender_id": "human", "content": content, "timestamp": 1000}
    return {"channel": {"id": "general", "type": "discord", "recent_messages": [source]}, "source": source}


@pytest.fixture
def saved(tmp_path):
    clock = Clock()
    store = ResearchStore(str(tmp_path / "research.db"), clock=clock)
    yield store, clock
    store.close()


def finish(store, turn, text="Answer", receipt=None):
    claimed = store.claim(turn["platform"], turn["channel_id"])
    assert claimed["id"] == turn["id"]
    assert store.ready(turn["id"], claimed["lease_token"], text, sources=[{"tool": "read_feed", "url": "https://example.com/feed"}])
    delivery = store.claim_delivery(turn["platform"], turn["channel_id"])
    assert store.sent(turn["id"], delivery["delivery_token"], receipt or {"message_id": "reply-" + turn["event_id"]})


def test_quick_requests_keep_their_intake_order_and_snapshot(saved):
    store, _ = saved
    first_input = snapshot("one")
    first = store.enqueue("discord", "general", "one", first_input)
    second = store.enqueue("discord", "general", "two", snapshot("two", "Second request"))
    first_input["source"]["content"] = "A later edit"
    claimed = store.claim("discord", "general")
    assert claimed["id"] == first["id"]
    assert claimed["snapshot"]["source"]["content"] == "What changed?"
    assert store.claim("discord", "general") is None
    assert store.ready(first["id"], claimed["lease_token"], "First answer")
    assert store.claim("discord", "general") is None
    delivery = store.claim_delivery("discord", "general")
    assert store.claim_delivery("discord", "general") is None
    assert store.sent(first["id"], delivery["delivery_token"], {"message_id": "reply-one"})
    assert store.claim("discord", "general")["id"] == second["id"]


def test_commands_have_their_own_order_and_delivery_claim(saved):
    store, _ = saved
    request = store.enqueue("discord", "general", "ask", snapshot("ask"))
    first = store.enqueue("discord", "general", "command1", snapshot("command1"), kind="command")
    second = store.enqueue("discord", "general", "command2", snapshot("command2"), kind="command")
    assert store.pending_channels(include_processing=False) == [{"platform": "discord", "channel_id": "general"}]
    claim = store.claim("discord", "general", kind="command", include_processing=False)
    assert claim["id"] == first["id"]
    assert store.claim("discord", "general", kind="command") is None
    assert store.ready(first["id"], claim["lease_token"], "Watch removed")
    delivery = store.claim_delivery("discord", "general", kind="command")
    assert store.sent(first["id"], delivery["delivery_token"], {"message_id": "receipt1"})
    assert store.claim("discord", "general", kind="command")["id"] == second["id"]
    assert store.get(request["id"])["attempts"] == 0


def test_duplicate_intake_preserves_original_and_platform_scope(saved):
    store, _ = saved
    original = store.enqueue("discord", "general", "one", snapshot())
    duplicate = store.enqueue("discord", "general", "one", snapshot(content="Changed"))
    assert duplicate == original
    assert store.enqueue("matrix", "general", "one", snapshot())["id"] != original["id"]
    assert store.enqueue("discord", "private", "one", snapshot())["id"] != original["id"]


def test_processing_lease_survives_restart_and_fences_stale_worker(tmp_path):
    clock = Clock()
    path = str(tmp_path / "research.db")
    first_store = ResearchStore(path, clock=clock)
    turn = first_store.enqueue("discord", "general", "one", snapshot())
    old = first_store.claim("discord", "general", lease_seconds=10)
    first_store.close()
    restored = ResearchStore(path, clock=clock)
    assert restored.claim("discord", "general") is None
    clock.advance(10)
    new = restored.claim("discord", "general")
    assert new["id"] == turn["id"] and new["attempts"] == 2
    assert new["lease_token"] != old["lease_token"]
    assert not restored.ready(turn["id"], old["lease_token"], "Stale answer")
    assert not restored.fail(turn["id"], old["lease_token"], "Stale failure")
    assert not restored.heartbeat(turn["id"], old["lease_token"])
    assert restored.ready(turn["id"], new["lease_token"], "Current answer")
    restored.close()


def test_two_store_handles_share_one_atomic_claim(tmp_path):
    path = str(tmp_path / "research.db")
    stores = [ResearchStore(path), ResearchStore(path)]
    stores[0].enqueue("discord", "general", "one", snapshot())
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda store: store.claim("discord", "general"), stores))
    assert sum(result is not None for result in results) == 1
    for store in stores:
        store.close()


def test_known_failures_retry_with_backoff_and_a_fixed_attempt_limit(saved):
    store, clock = saved
    turn = store.enqueue("discord", "general", "one", snapshot())
    for attempt in range(1, 4):
        claimed = store.claim("discord", "general")
        assert claimed["attempts"] == attempt
        assert store.fail(turn["id"], claimed["lease_token"], "Temporary model failure", retry_after=5)
        assert store.claim("discord", "general") is None
        if attempt < 3:
            assert store.pending_channels() == []
            clock.advance(5)
    assert store.get(turn["id"])["status"] == "failed"
    assert store.pending_channels() == []


def test_prepared_reply_survives_restart_and_known_delivery_retry(tmp_path):
    path = str(tmp_path / "research.db")
    clock = Clock()
    store = ResearchStore(path, clock=clock)
    turn = store.enqueue("discord", "general", "one", snapshot())
    claimed = store.claim("discord", "general")
    assert store.ready(turn["id"], claimed["lease_token"], "Saved answer")
    store.close()
    store = ResearchStore(path, clock=clock)
    assert store.pending_channels() == [{"platform": "discord", "channel_id": "general"}]
    for attempt in range(1, 4):
        delivery = store.claim_delivery("discord", "general")
        assert delivery["reply"] == "Saved answer"
        assert delivery["delivery_attempts"] == attempt
        assert store.fail(turn["id"], delivery["delivery_token"], "Known HTTP rejection", phase="delivery")
    assert store.get(turn["id"])["status"] == "failed"
    assert store.memory_node("discord", "general")["turns"] == []
    store.close()


def test_lost_delivery_receipt_is_held_for_reconciliation(saved):
    store, clock = saved
    turn = store.enqueue("discord", "general", "one", snapshot())
    claimed = store.claim("discord", "general")
    assert store.ready(turn["id"], claimed["lease_token"], "Saved answer")
    delivery = store.claim_delivery("discord", "general", lease_seconds=10)
    clock.advance(10)
    assert store.claim_delivery("discord", "general") is None
    assert store.uncertain_deliveries()[0]["snapshot"] == snapshot()
    assert not store.sent(turn["id"], delivery["delivery_token"], {"message_id": "late"})
    assert store.resolve_delivery(turn["id"], {"message_id": "found"})
    assert store.memory_node("discord", "general")["turns"][0]["receipt"] == {"message_id": "found"}
    assert store.uncertain_deliveries() == []


def test_explicit_uncertain_send_can_retry_after_a_remote_check(saved):
    store, _ = saved
    turn = store.enqueue("discord", "general", "one", snapshot())
    claimed = store.claim("discord", "general")
    store.ready(turn["id"], claimed["lease_token"], "Saved answer")
    delivery = store.claim_delivery("discord", "general")
    assert store.uncertain(turn["id"], delivery["delivery_token"], "Connection ended during send")
    assert store.claim_delivery("discord", "general") is None
    assert store.reconcile(turn["id"], retry=True)
    retried = store.claim_delivery("discord", "general")
    assert retried["delivery_token"] != delivery["delivery_token"]
    assert not store.sent(turn["id"], delivery["delivery_token"], {"message_id": "stale"})
    assert store.sent(turn["id"], retried["delivery_token"], {"message_id": "current"})


def test_management_action_boundary_keeps_unknown_effect_from_repeating(saved):
    store, clock = saved
    turn = store.enqueue("matrix", "control", "one", snapshot())
    claimed = store.claim("matrix", "control", lease_seconds=10)
    assert store.mark_effect(turn["id"], claimed["lease_token"])
    clock.advance(10)
    assert store.claim("matrix", "control") is None
    assert store.get(turn["id"])["status"] == "uncertain"
    assert not store.ready(turn["id"], claimed["lease_token"], "Late result")
    assert store.uncertain_deliveries() == []


def test_management_receipt_can_prepare_the_factual_reply(saved):
    store, _ = saved
    turn = store.enqueue("matrix", "control", "one", snapshot())
    claimed = store.claim("matrix", "control")
    assert store.mark_effect(turn["id"], claimed["lease_token"])
    assert store.heartbeat(turn["id"], claimed["lease_token"])
    assert store.ready(turn["id"], claimed["lease_token"], "Room created")
    assert store.claim_delivery("matrix", "control")["reply"] == "Room created"


def test_action_failure_remains_uncertain_and_a_wait_can_complete(saved):
    store, _ = saved
    effect = store.enqueue("matrix", "control", "one", snapshot())
    claimed = store.claim("matrix", "control")
    store.mark_effect(effect["id"], claimed["lease_token"])
    assert store.fail(effect["id"], claimed["lease_token"], "Action connection ended")
    assert store.get(effect["id"])["status"] == "uncertain"
    wait = store.enqueue("matrix", "control", "two", snapshot())
    claimed = store.claim("matrix", "control")
    assert store.complete(wait["id"], claimed["lease_token"])
    assert store.get(wait["id"])["status"] == "complete"
    assert store.memory_node("matrix", "control")["turns"] == []


def test_memory_contains_only_actual_sent_pairs_in_the_current_scope(saved):
    store, _ = saved
    one = store.enqueue("discord", "general", "one", snapshot("one", "First question"))
    two = store.enqueue("discord", "general", "two", snapshot("two", "Second question"))
    private = store.enqueue("discord", "private", "secret", snapshot("secret", "PRIVATE SECRET"))
    finish(store, one, "First answer")
    finish(store, two, "Second answer")
    finish(store, private, "PRIVATE ANSWER")
    unsent = store.enqueue("discord", "general", "three", snapshot("three", "Unsent question"))
    claimed = store.claim("discord", "general")
    store.ready(unsent["id"], claimed["lease_token"], "Unsent answer")
    node = store.memory_node("discord", "general")
    assert [turn["event_id"] for turn in node["turns"]] == ["one", "two"]
    assert node["turns"][0]["request"]["content"] == "First question"
    assert node["turns"][1]["reply"] == "Second answer"
    assert "PRIVATE" not in json.dumps(node)
    assert "Unsent" not in json.dumps(node)
    assert store.memory_node("matrix", "general")["turns"] == []


def test_source_cache_has_expiry_hash_and_stable_scoped_keys(saved):
    store, clock = saved
    result = {"url": "https://example.com/feed", "result": "Public evidence"}
    cached = store.save_source("discord", "general", "read_feed", {"url": result["url"], "limit": 3}, result, ttl_seconds=10)
    found = store.cached_source("discord", "general", "read_feed", {"limit": 3, "url": result["url"]})
    assert found == cached
    assert found["trust_label"] == "untrusted"
    assert len(found["content_hash"]) == 64
    assert store.cached_source("matrix", "general", "read_feed", found["params"]) is None
    assert store.cached_source("discord", "private", "read_feed", found["params"]) is None
    assert store.cached_source("discord", "general", "read_webpage", found["params"]) is None
    assert store.cached_source("discord", "general", "read_feed", {"url": result["url"], "limit": 4}) is None
    clock.advance(10)
    assert store.cached_source("discord", "general", "read_feed", found["params"]) is None
    changed = store.save_source("discord", "general", "read_feed", found["params"], {"result": "New evidence"})
    assert changed["content_hash"] != found["content_hash"]


def test_memory_node_stays_bounded_with_long_turns_and_sources(saved):
    store, _ = saved
    for index in range(6):
        turn = store.enqueue("discord", "general", str(index), snapshot(str(index), "q" * 10000))
        finish(store, turn, "a" * 10000)
    store.save_source("discord", "general", "web_search", {"query": "topic"}, {"result": "s" * 50000})
    node = store.memory_node("discord", "general", max_chars=3000)
    assert len(json.dumps(node, ensure_ascii=False, separators=(",", ":"))) <= 3000
    assert node["turns"]


def test_cleanup_removes_old_content_and_keeps_event_dedup(saved):
    store, clock = saved
    turn = store.enqueue("discord", "general", "one", snapshot())
    finish(store, turn)
    pending = store.enqueue("discord", "general", "two", snapshot("two", "Pending"))
    store.save_source("discord", "general", "read_feed", {"url": "https://example.com/feed"}, {"result": "Old content"}, ttl_seconds=20 * 86400)
    clock.advance(7 * 86400 + 1)
    assert store.memory_node("discord", "general")["turns"] == []
    assert store.cleanup() == {"turns": 1, "sources": 1}
    assert store.get(turn["id"]) is None
    assert store.get(pending["id"])["status"] == "queued"
    assert store.enqueue("discord", "general", "one", snapshot(content="Old event again"))["status"] == "legacy"
    assert store.claim("discord", "general")["id"] == pending["id"]


def test_old_attempt_receipts_remain_terminal_across_platforms(tmp_path):
    path = str(tmp_path / "research.db")
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE node_request_receipts(channel_id TEXT,event_id TEXT,PRIMARY KEY(channel_id,event_id))")
        db.execute("INSERT INTO node_request_receipts VALUES('general','old')")
    store = ResearchStore(path)
    for platform in ["discord", "matrix"]:
        old = store.enqueue(platform, "general", "old", snapshot())
        assert old["status"] == "legacy"
        assert store.claim(platform, "general") is None
    fresh = store.enqueue("discord", "general", "new", snapshot())
    assert store.claim("discord", "general")["id"] == fresh["id"]
    store.close()


def test_memory_database_uses_one_connection_and_validates_receipts():
    store = ResearchStore(":memory:")
    turn = store.enqueue("discord", "general", "one", snapshot())
    claimed = store.claim("discord", "general")
    store.ready(turn["id"], claimed["lease_token"], "Answer")
    delivery = store.claim_delivery("discord", "general")
    with pytest.raises(ValueError, match="receipt"):
        store.sent(turn["id"], delivery["delivery_token"], {})
    assert store.sent(turn["id"], delivery["delivery_token"], {"message_id": "one"})
    assert store.state_counts() == {"sent": 1}
    store.close()
