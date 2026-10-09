"""Feed watch authority, quiet baselines, saved updates, and recovery."""

import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from chatbot.core.node_system.source_watches import SourceWatchService, WatchStore, _digest, _items


FEED = "https://example.com/releases.atom"


class Clock:
    def __init__(self):
        self.value = 1_700_000_000

    def __call__(self):
        return self.value

    def advance(self, seconds=3600):
        self.value += seconds


def scope(channel="20", sender="owner", event="event1"):
    return {"channel_type": "discord", "channel_id": channel, "sender_id": sender, "event_id": event}


def feed(*versions):
    return {"status": "success", "url": FEED, "items": [
        {"title": f"Release {version}", "url": f"https://example.com/releases/{version}", "summary": f"Changes in {version}"}
        for version in versions
    ]}


def make_store(tmp_path, clock):
    return WatchStore(tmp_path / "watches.db", owner_ids={"discord": {"owner"}},
                      allowed_channels={"discord": {"20", "21"}}, now=clock)


def make_service(store, clock, results, send=None, budget=24, reconcile=None):
    fetch = AsyncMock(side_effect=results)
    sender = send or AsyncMock(return_value={"status": "success", "message_id": "sent1"})
    service = SourceWatchService(store, fetch=fetch, send=sender, daily_lookup_budget=budget, now=clock, reconcile=reconcile)
    return service, fetch, sender


@pytest.mark.parametrize("sender,flag", [("owner", False), ("stranger", True), ("stranger", False)])
def test_store_requires_configured_owner_and_trusted_owner_flag(tmp_path, sender, flag):
    store = make_store(tmp_path, Clock())
    with pytest.raises(ValueError, match="owner"):
        store.create(scope(sender=sender), FEED, is_owner=flag)
    assert store.list(scope()) == []


def test_store_defaults_require_owner_and_channel_configuration(tmp_path):
    store = WatchStore(tmp_path / "closed.db")
    with pytest.raises(ValueError, match="configured channel"):
        store.create(scope(), FEED, is_owner=True)


@pytest.mark.parametrize("url", ["http://127.0.0.1/feed", "http://10.0.0.1/feed", "http://169.254.169.254/latest", "http://[::1]/feed", "http://localhost/feed", "https://host.local/feed", "file:///etc/passwd", "https://user:pass@example.com/feed", "https://example.com:8443/feed"])
def test_watch_uses_public_url_guard(tmp_path, url):
    store = make_store(tmp_path, Clock())
    with pytest.raises(ValueError):
        store.create(scope(), url, is_owner=True)


def test_channel_scope_applies_to_reads_and_removal(tmp_path):
    store = make_store(tmp_path, Clock())
    watch = store.create(scope(), FEED, is_owner=True)
    assert store.list(scope("21")) == []
    assert store.remove(scope("21"), watch["id"], is_owner=True) is False
    with pytest.raises(ValueError, match="configured channel"):
        store.list(scope("private"))
    with pytest.raises(ValueError, match="owner"):
        store.remove(scope(sender="stranger"), watch["id"], is_owner=True)
    assert store.remove(scope(), watch["id"], is_owner=True)


def test_same_event_supports_two_feeds_and_url_duplicates_stay_one_watch(tmp_path):
    store = make_store(tmp_path, Clock())
    first = store.create(scope(), FEED, is_owner=True)
    replay = store.create(scope(), "https://example.com/another.atom", is_owner=True)
    same_url = store.create(scope(event="event2"), FEED, is_owner=True)
    assert first["id"] != replay["id"]
    assert first["id"] == same_url["id"]
    assert len(store.list(scope())) == 2


@pytest.mark.parametrize("interval", [0, 899, 86401])
def test_interval_limits(tmp_path, interval):
    with pytest.raises(ValueError, match="15 minutes"):
        make_store(tmp_path, Clock()).create(scope(), FEED, interval, is_owner=True)


def test_five_watch_limit_is_per_channel(tmp_path):
    store = make_store(tmp_path, Clock())
    for number in range(5):
        store.create(scope(event=str(number)), f"https://example.com/{number}.atom", is_owner=True)
    with pytest.raises(ValueError, match="five watches"):
        store.create(scope(event="six"), "https://example.com/six.atom", is_owner=True)
    assert store.create(scope("21"), FEED, is_owner=True)["channel_id"] == "21"


@pytest.mark.asyncio
async def test_tools_create_list_digest_and_remove(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    service, fetch, send = make_service(store, clock, [feed("v1")])
    denied = await service.execute_tool("create_source_watch", {"url": FEED}, scope(sender="stranger"), True)
    assert denied["status"] == "failure" and "owner" in denied["message"]
    reply = await service.execute_tool("create_source_watch", {"url": FEED, "interval_minutes": 60}, scope(), True)
    watch = store.list(scope())[0]
    assert watch["interval_seconds"] == 3600 and reply["watch"]["watch_id"] == watch["id"]
    assert reply["status"] == "success" and reply["created"] is True
    assert await service.execute_tool("create_source_watch", {"url": FEED}, scope(), True) == reply
    listed = await service.execute_tool("list_source_watches", {}, scope(event="list"))
    assert listed["watches"][0]["watch_id"] == watch["id"]
    waiting = await service.execute_tool("get_source_digest", {}, scope(event="digest0"))
    assert "first check" in waiting["message"] and waiting["entries"] == []
    await service.tick()
    digest = await service.execute_tool("get_source_digest", {"watch_id": watch["id"]}, scope(event="digest1"))
    assert "Release v1" in digest["message"] and "https://example.com/releases/v1" in digest["message"]
    assert digest["entries"][0]["url"] == "https://example.com/releases/v1"
    assert digest["trust"] == "untrusted_source"
    removed = await service.execute_tool("remove_source_watch", {"watch_id": watch["id"]}, scope(event="remove"), True)
    assert removed["status"] == "success" and removed["removed"] is True
    assert store.list(scope()) == []
    assert fetch.await_count == 1 and send.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("minutes,expected", [(15, 900), (1440, 86400), (None, 3600)])
async def test_tool_intervals(tmp_path, minutes, expected):
    clock = Clock()
    store = make_store(tmp_path, clock)
    service, _, _ = make_service(store, clock, [])
    params = {"url": FEED}
    if minutes is not None:
        params["interval_minutes"] = minutes
    result = await service.execute_tool("create_source_watch", params, SimpleNamespace(**scope()), True)
    assert result["status"] == "success"
    assert store.list(scope())[0]["interval_seconds"] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("name,params", [
    ("create_source_watch", {}),
    ("create_source_watch", {"url": FEED, "interval_minutes": True}),
    ("create_source_watch", {"url": FEED, "interval_minutes": 60.0}),
    ("create_source_watch", {"url": FEED, "interval_minutes": "60"}),
    ("create_source_watch", {"url": FEED, "interval_minutes": 14}),
    ("create_source_watch", {"url": FEED, "interval_minutes": 1441}),
    ("create_source_watch", {"url": FEED, "channel_id": "21"}),
    ("create_source_watch", {"url": FEED, "sender_id": "owner"}),
    ("create_source_watch", {"url": FEED, "event_id": "other"}),
    ("list_source_watches", {"channel_id": "21"}),
    ("remove_source_watch", {}),
    ("remove_source_watch", {"watch_id": "abcdef123456", "channel_type": "matrix"}),
    ("get_source_digest", {"watch_id": None}),
    ("get_source_digest", {"watch_id": "abcdef123456", "url": FEED}),
    ("watch", {"url": FEED}),
    ("create_source_watch", None),
])
async def test_tool_parameters_are_strict_and_scope_stays_trusted(tmp_path, name, params):
    store = make_store(tmp_path, Clock())
    result = await SourceWatchService(store).execute_tool(name, params, scope(), True)
    assert result["status"] == "failure" and result["message"]
    assert store.list(scope()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("sender,flag", [("owner", False), ("stranger", True), ("stranger", False)])
async def test_tool_writes_check_owner_before_saved_receipt(tmp_path, sender, flag):
    store = make_store(tmp_path, Clock())
    service = SourceWatchService(store)
    created = await service.execute_tool("create_source_watch", {"url": FEED}, scope(), True)
    replay = await service.execute_tool("create_source_watch", {"url": FEED}, scope(sender=sender), flag)
    removed = await service.execute_tool("remove_source_watch", {"watch_id": created["watch"]["watch_id"]}, scope(sender=sender), flag)
    assert replay["status"] == removed["status"] == "failure"
    assert len(store.list(scope())) == 1


@pytest.mark.asyncio
async def test_tool_reads_and_removal_stay_in_current_channel(tmp_path):
    store = make_store(tmp_path, Clock())
    service = SourceWatchService(store)
    created = await service.execute_tool("create_source_watch", {"url": FEED}, scope(), True)
    watch_id = created["watch"]["watch_id"]
    other = await service.execute_tool("list_source_watches", {}, scope("21", sender="reader"))
    digest = await service.execute_tool("get_source_digest", {"watch_id": watch_id}, scope("21"))
    removed = await service.execute_tool("remove_source_watch", {"watch_id": watch_id}, scope("21"), True)
    assert other["watches"] == []
    assert digest["status"] == removed["status"] == "failure"
    assert "this channel" in removed["message"] and removed["removed"] is False
    for name, params in [("create_source_watch", {"url": FEED}), ("list_source_watches", {}),
                         ("remove_source_watch", {"watch_id": watch_id}), ("get_source_digest", {})]:
        result = await service.execute_tool(name, params, scope("private"), True)
        assert result["status"] == "failure" and "configured channel" in result["message"]
    assert len(store.list(scope())) == 1


@pytest.mark.asyncio
async def test_tools_use_platform_as_part_of_the_channel_scope(tmp_path):
    store = WatchStore(tmp_path / "watches.db", owner_ids={"discord": {"owner"}, "matrix": {"owner"}},
                       allowed_channels={"discord": {"20"}, "matrix": {"20"}})
    service = SourceWatchService(store)
    discord = await service.execute_tool("create_source_watch", {"url": FEED}, scope(), True)
    matrix_scope = {**scope(), "channel_type": "matrix"}
    matrix = await service.execute_tool("create_source_watch", {"url": FEED}, matrix_scope, True)
    assert discord["watch"]["watch_id"] != matrix["watch"]["watch_id"]
    removed = await service.execute_tool("remove_source_watch", {"watch_id": discord["watch"]["watch_id"]}, matrix_scope, True)
    assert removed["status"] == "failure"
    assert len(store.list(scope())) == len(store.list(matrix_scope)) == 1


@pytest.mark.asyncio
async def test_write_tools_require_a_human_event_and_reads_use_saved_state(tmp_path):
    store = make_store(tmp_path, Clock())
    watch = store.create(scope(), FEED, is_owner=True)
    service = SourceWatchService(store)
    for name, params in [("create_source_watch", {"url": FEED}), ("remove_source_watch", {"watch_id": watch["id"]})]:
        result = await service.execute_tool(name, params, scope(event=""), True)
        assert result["status"] == "failure" and "human request" in result["message"]
    listed = await service.execute_tool("list_source_watches", {}, scope(event="", sender="reader"))
    assert len(listed["watches"]) == 1


@pytest.mark.asyncio
async def test_tool_receipts_support_two_urls_in_one_event_and_canonical_replays(tmp_path):
    store = make_store(tmp_path, Clock())
    service = SourceWatchService(store)
    first = await service.execute_tool("create_source_watch", {"url": FEED}, scope(), True)
    replay = await service.execute_tool("create_source_watch", {"interval_minutes": 60, "url": FEED + "#part"}, scope(), True)
    second = await service.execute_tool("create_source_watch", {"url": "https://example.com/another.atom"}, scope(), True)
    duplicate = await service.execute_tool("create_source_watch", {"url": FEED}, scope(event="event2"), True)
    assert replay == first and first["watch"]["watch_id"] != second["watch"]["watch_id"]
    assert duplicate["watch"]["watch_id"] == first["watch"]["watch_id"] and duplicate["created"] is False
    assert len(store.list(scope())) == 2
    with store._db() as db:
        assert db.execute("SELECT COUNT(*) FROM source_watch_tool_receipts").fetchone()[0] == 3


@pytest.mark.asyncio
async def test_create_and_remove_replays_keep_original_results_after_restart(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    service = SourceWatchService(store)
    created = await service.execute_tool("create_source_watch", {"url": FEED}, scope(), True)
    watch_id = created["watch"]["watch_id"]
    removed = await service.execute_tool("remove_source_watch", {"watch_id": watch_id}, scope(event="remove"), True)
    clock.advance()
    restored = make_store(tmp_path, clock)
    restarted = SourceWatchService(restored)
    assert await restarted.execute_tool("create_source_watch", {"url": FEED}, scope(), True) == created
    assert await restarted.execute_tool("remove_source_watch", {"watch_id": watch_id.upper()}, scope(event="remove"), True) == removed
    assert restored.list(scope()) == []
    fresh = await restarted.execute_tool("create_source_watch", {"url": FEED}, scope(event="new"), True)
    assert fresh["created"] is True and fresh["watch"]["watch_id"] != watch_id


@pytest.mark.asyncio
async def test_missing_remove_result_is_saved_and_successful_remove_replays(tmp_path):
    store = make_store(tmp_path, Clock())
    service = SourceWatchService(store)
    missing = await service.execute_tool("remove_source_watch", {"watch_id": "abcdef123456"}, scope(), True)
    assert missing == {"status": "failure", "message": "Choose a source watch ID from this channel.",
                       "watch_id": "abcdef123456", "removed": False}
    assert await service.execute_tool("remove_source_watch", {"watch_id": "ABCDEF123456"}, scope(), True) == missing


@pytest.mark.asyncio
async def test_watch_limit_failure_replays_after_a_slot_becomes_free(tmp_path):
    store = make_store(tmp_path, Clock())
    for number in range(5):
        store.create(scope(event=str(number)), f"https://example.com/{number}.atom", is_owner=True)
    service = SourceWatchService(store)
    failed = await service.execute_tool("create_source_watch", {"url": FEED}, scope(event="full"), True)
    assert failed["status"] == "failure"
    store.remove(scope(), store.list(scope())[0]["id"], is_owner=True)
    assert await service.execute_tool("create_source_watch", {"url": FEED}, scope(event="full"), True) == failed
    fresh = await service.execute_tool("create_source_watch", {"url": FEED}, scope(event="fresh"), True)
    assert fresh["status"] == "success"


@pytest.mark.asyncio
async def test_list_and_digest_reads_follow_saved_changes_in_the_same_event(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    service, fetch, _ = make_service(store, clock, [feed("v1"), feed("v2", "v1")])
    initial = await service.execute_tool("list_source_watches", {}, scope())
    assert initial["watches"] == []
    created = await service.execute_tool("create_source_watch", {"url": FEED}, scope(), True)
    assert len((await service.execute_tool("list_source_watches", {}, scope()))["watches"]) == 1
    before = await service.execute_tool("get_source_digest", {}, scope())
    assert before["entries"] == []
    await service.tick()
    first = await service.execute_tool("get_source_digest", {}, scope())
    clock.advance()
    await service.tick()
    changed = await service.execute_tool("get_source_digest", {}, scope())
    assert first["entries"][0]["title"] == "Release v1"
    assert changed["entries"][0]["title"] == "Release v2" and changed["entry_count"] == 2
    await service.execute_tool("remove_source_watch", {"watch_id": created["watch"]["watch_id"]}, scope(), True)
    assert (await service.execute_tool("list_source_watches", {}, scope()))["watches"] == []
    with store._db() as db:
        assert db.execute("SELECT COUNT(*) FROM source_watch_tool_receipts").fetchone()[0] == 2
    assert fetch.await_count == 2


@pytest.mark.asyncio
async def test_digest_entry_data_is_bounded_and_keeps_full_public_links(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    entries = {"status": "success", "url": FEED, "items": [
        {"title": "t" * 300, "summary": "s" * 600, "published": "p" * 200,
         "url": f"https://example.com/{number}/" + "x" * 900} for number in range(10)]}
    service, _, _ = make_service(store, clock, [entries])
    await service.tick()
    result = await service.execute_tool("get_source_digest", {}, scope())
    assert result["entry_count"] == 10 and result["truncated"] is True and len(result["entries"]) == 5
    assert len(result["message"]) < 1900
    assert all(len(entry["title"]) <= 140 and len(entry["summary"]) <= 160 and len(entry["published"]) <= 100
               for entry in result["entries"])
    assert result["entries"][0]["url"] == entries["items"][0]["url"]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["create", "remove"])
async def test_receipt_storage_failure_rolls_back_the_watch_mutation(tmp_path, operation):
    store = make_store(tmp_path, Clock())
    service = SourceWatchService(store)
    if operation == "remove":
        saved = store.create(scope(), FEED, is_owner=True)
        params = {"watch_id": saved["id"]}
    else:
        params = {"url": FEED}
    with patch.object(store, "_save_tool_receipt", side_effect=sqlite3.OperationalError("disk full")):
        result = await service.execute_tool(f"{operation}_source_watch", params, scope(), True)
    assert result["status"] == "failure"
    assert len(store.list(scope())) == (1 if operation == "remove" else 0)
    with store._db() as db:
        assert db.execute("SELECT COUNT(*) FROM source_watch_tool_receipts").fetchone()[0] == 0
    retried = await service.execute_tool(f"{operation}_source_watch", params, scope(), True)
    assert retried["status"] == "success"


def test_concurrent_tool_calls_share_one_atomic_receipt(tmp_path):
    clock = Clock()
    stores = [make_store(tmp_path, clock), make_store(tmp_path, clock)]
    def create(store):
        return store.execute_mutation("create_source_watch", {"url": FEED}, scope(), True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, stores))
    assert results[0] == results[1] and results[0]["created"] is True
    assert len(stores[0].list(scope())) == 1
    with stores[0]._db() as db:
        assert db.execute("SELECT COUNT(*) FROM source_watch_tool_receipts").fetchone()[0] == 1


def test_legacy_receipts_stay_saved_and_event_only_index_is_removed(tmp_path):
    clock = Clock()
    old = make_store(tmp_path, clock)
    first = old.create(scope(), FEED, is_owner=True)
    with old._db() as db:
        db.execute("CREATE UNIQUE INDEX source_watch_request_event ON source_watches(channel_type,channel_id,request_event_id) WHERE request_event_id<>''")
        db.execute("CREATE TABLE source_watch_commands (response TEXT)")
        db.execute("INSERT INTO source_watch_commands VALUES ('saved legacy result')")
    restored = make_store(tmp_path, clock)
    second = restored.execute_mutation("create_source_watch", {"url": "https://example.com/another.atom"}, scope(), True)
    assert second["status"] == "success" and len(restored.list(scope())) == 2
    assert first["id"] in {watch["id"] for watch in restored.list(scope())}
    with restored._db() as db:
        assert db.execute("SELECT response FROM source_watch_commands").fetchone()[0] == "saved legacy result"
        assert db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='source_watch_request_event'").fetchone() is None


@pytest.mark.asyncio
async def test_baseline_and_unchanged_feed_are_quiet_then_change_delivers_once(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    watch = store.create(scope(), FEED, is_owner=True)
    service, fetch, send = make_service(store, clock, [feed("v1"), feed("v1"), feed("v2", "v1"), feed("v1", "v2")])
    await service.tick()
    assert store.list(scope())[0]["baseline"] == 1
    send.assert_not_awaited()
    clock.advance()
    await service.tick()
    send.assert_not_awaited()
    clock.advance()
    await service.tick()
    assert send.await_count == 1
    args = send.await_args.args
    assert args[:3] == (watch["id"], "discord", "20")
    assert "Release v2" in args[3] and "https://example.com/releases/v2" in args[3]
    assert "Release v1" not in args[3]
    assert len(args[3]) < 1900
    clock.advance()
    await service.tick()
    assert send.await_count == 1 and fetch.await_count == 4


@pytest.mark.asyncio
async def test_edited_reordered_or_returning_entries_keep_the_same_identity(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    edited = feed("v1", "v2")
    edited["items"][0].update(title="Edited release title", summary="Updated description", published="today")
    service, _, send = make_service(store, clock, [feed("v2", "v1"), edited, feed("v2"), feed("v1", "v2")])
    for _ in range(4):
        await service.tick()
        clock.advance()
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_send_failure_retries_saved_digest_after_restart(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    send = AsyncMock(side_effect=[{"status": "failure"}, {"status": "success", "message_id": "sent2"}])
    service, fetch, _ = make_service(store, clock, [feed("v1"), feed("v2", "v1")], send)
    await service.tick()
    clock.advance()
    await service.tick()
    saved = store.list(scope())[0]
    assert saved["pending_status"] == "pending"
    first_args = send.await_args.args
    clock.advance(61)
    restored = make_store(tmp_path, clock)
    restart, new_fetch, _ = make_service(restored, clock, [], send)
    await restart.tick()
    assert send.await_count == 2 and send.await_args.args == first_args
    assert restored.list(scope())[0]["delivery_message_id"] == "sent2"
    new_fetch.assert_not_awaited()
    assert fetch.await_count == 2


@pytest.mark.asyncio
async def test_unknown_send_holds_until_sender_reconciles(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    send = AsyncMock(return_value={"status": "unknown"})
    service, _, _ = make_service(store, clock, [feed("v1"), feed("v2", "v1")], send)
    await service.tick()
    clock.advance()
    await service.tick()
    assert store.list(scope())[0]["pending_status"] == "unknown"
    clock.advance(61)
    reconcile = AsyncMock(return_value={"status": "success", "message_id": "confirmed"})
    restart, new_fetch, _ = make_service(make_store(tmp_path, clock), clock, [], send, reconcile=reconcile)
    await restart.tick()
    reconcile.assert_awaited_once()
    assert send.await_count == 1
    assert restart.store.list(scope())[0]["delivery_message_id"] == "confirmed"
    new_fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_crash_after_begin_delivery_reconciles_after_lease_expires(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    service, _, send = make_service(store, clock, [feed("v1")])
    await service.tick()
    clock.advance()
    claimed = store.claim()
    prepared = store.save_feed(claimed, _items(feed("v2", "v1"), FEED))
    assert store.begin_delivery(prepared)
    clock.advance(181)
    reconcile = AsyncMock(return_value={"status": "success", "message_id": "recovered"})
    restarted, fetch, _ = make_service(make_store(tmp_path, clock), clock, [], send, reconcile=reconcile)
    await restarted.tick()
    reconcile.assert_awaited_once()
    send.assert_not_awaited()
    fetch.assert_not_awaited()
    assert restarted.store.list(scope())[0]["delivery_message_id"] == "recovered"


@pytest.mark.asyncio
async def test_crash_after_preparing_update_resumes_after_lease(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    service, _, _ = make_service(store, clock, [feed("v1")])
    await service.tick()
    clock.advance()
    claimed = store.claim()
    prepared = store.save_feed(claimed, _items(feed("v2", "v1"), FEED))
    clock.advance(181)
    assert store.begin_delivery(prepared) is False
    restarted, fetch, send = make_service(make_store(tmp_path, clock), clock, [])
    await restarted.tick()
    send.assert_awaited_once()
    fetch.assert_not_awaited()
    assert send.await_args.args[4] == prepared["delivery_key"]


@pytest.mark.asyncio
async def test_interrupted_send_saves_unknown_delivery(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    send = AsyncMock(side_effect=asyncio.CancelledError)
    service, _, _ = make_service(store, clock, [feed("v1"), feed("v2", "v1")], send)
    await service.tick()
    clock.advance()
    with pytest.raises(asyncio.CancelledError):
        await service.tick()
    assert store.list(scope())[0]["pending_status"] == "unknown"


@pytest.mark.asyncio
async def test_success_needs_delivery_receipt(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    service, _, _ = make_service(store, clock, [feed("v1"), feed("v2", "v1")], AsyncMock(return_value={"status": "success"}))
    await service.tick()
    clock.advance()
    await service.tick()
    assert store.list(scope())[0]["pending_status"] == "unknown"


@pytest.mark.asyncio
async def test_daily_fetch_budget_is_atomic_per_channel_and_resets(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope("20"), FEED, is_owner=True)
    store.create(scope("21"), FEED, is_owner=True)
    service, fetch, _ = make_service(store, clock, [feed("v1")] * 4, budget=1)
    await service.tick()
    assert fetch.await_count == 2
    clock.advance()
    await service.tick()
    assert fetch.await_count == 2
    clock.value = (clock.value // 86400 + 1) * 86400 + 1
    await service.tick()
    assert fetch.await_count == 4


@pytest.mark.asyncio
async def test_failed_fetches_use_daily_budget(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, 900, is_owner=True)
    service, fetch, send = make_service(store, clock, [{"status": "failure"}], budget=1)
    await service.tick()
    clock.advance(901)
    await service.tick()
    assert fetch.await_count == 1
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_removed_channel_or_owner_stops_watch_work(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    store.owner_ids = {"discord": {"new-owner"}}
    service, fetch, send = make_service(store, clock, [])
    await service.tick()
    fetch.assert_not_awaited()
    send.assert_not_awaited()


@pytest.mark.asyncio
async def test_default_fetch_uses_existing_feed_reader_and_action_context(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    context = object()
    service = SourceWatchService(store, action_context=context, now=clock)
    with patch("chatbot.core.node_system.source_watches.ReadFeedTool.execute", AsyncMock(return_value=feed("v1"))) as execute:
        await service.tick()
    execute.assert_awaited_once_with({"url": FEED}, context)


@pytest.mark.asyncio
async def test_digest_bounds_and_escapes_public_feed_text(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    store.create(scope(), FEED, is_owner=True)
    update = feed(*range(10))
    for item in update["items"]:
        item["title"] = "@everyone [change](http://127.0.0.1) " + "x" * 200
        item["summary"] = "<script>hidden</script>" + "y" * 500
    service, _, send = make_service(store, clock, [feed("old"), update])
    await service.tick()
    clock.advance()
    await service.tick()
    text = send.await_args.args[3]
    assert len(text) < 1900
    assert "@everyone" not in text and "hidden" not in text
    assert "https://example.com/releases/0" in text


def test_two_services_share_job_lease_and_lookup_budget(tmp_path):
    clock = Clock()
    first = make_store(tmp_path, clock)
    second = make_store(tmp_path, clock)
    first.create(scope(), FEED, is_owner=True)
    assert first.claim() is not None
    assert second.claim() is None
    assert first.reserve_lookup(scope(), 1)
    assert second.reserve_lookup(scope(), 1) is False


def prepare_uncertain(store, clock):
    claimed = store.claim()
    store.save_feed(claimed, _items(feed("v1"), FEED))
    clock.advance()
    claimed = store.claim()
    prepared = store.save_feed(claimed, _items(feed("v2", "v1"), FEED))
    store.begin_delivery(prepared)
    store.reconcile_delivery(prepared["delivery_key"], "unknown")


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, True])
async def test_reconcile_failure_or_timeout_keeps_other_channel_work_running(tmp_path, timeout):
    clock = Clock()
    store = make_store(tmp_path, clock)
    first = store.create(scope(), FEED, is_owner=True)
    prepare_uncertain(store, clock)
    store.create(scope("21"), FEED, is_owner=True)
    async def blocked(*args):
        await asyncio.Event().wait()
    reconcile = AsyncMock(side_effect=blocked if timeout else RuntimeError("History lookup failed"))
    fetch = AsyncMock(return_value=feed("v1"))
    service = SourceWatchService(store, fetch=fetch, reconcile=reconcile, now=clock, callback_timeout=0.001)
    await service.tick()
    assert store.list(scope("21"))[0]["baseline"] == 1
    assert store.list(scope())[0]["pending_status"] == "unknown"
    assert store.list(scope())[0]["id"] == first["id"]
    await service.tick()
    reconcile.assert_awaited_once()
    fetch.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, True])
async def test_send_failure_or_timeout_holds_receipt_and_runs_other_feeds(tmp_path, timeout):
    clock = Clock()
    store = make_store(tmp_path, clock)
    first = store.create(scope(), FEED, is_owner=True)
    claimed = store.claim()
    store.save_feed(claimed, _items(feed("v1"), FEED))
    clock.advance(3601)
    store.create(scope("21"), FEED, is_owner=True)
    async def blocked(*args):
        await asyncio.Event().wait()
    send = AsyncMock(side_effect=blocked if timeout else RuntimeError("Send outcome needs checking"))
    fetch = AsyncMock(side_effect=[feed("v2", "v1"), feed("v1")])
    service = SourceWatchService(store, fetch=fetch, send=send, now=clock, callback_timeout=0.001)
    await service.tick()
    assert store.list(scope("21"))[0]["baseline"] == 1
    assert store.list(scope())[0]["pending_status"] == "unknown"
    assert store.list(scope())[0]["id"] == first["id"]
    send.assert_awaited_once()
    assert fetch.await_count == 2


@pytest.mark.asyncio
async def test_uncertain_queue_reconciles_all_channels_fairly_after_restart(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    watches = [store.create(scope("20" if number < 5 else "21", event=str(number)),
                            f"https://example.com/{number}.atom", is_owner=True) for number in range(6)]
    service, _, _ = make_service(store, clock, [feed("v1")] * 6 + [feed("v2", "v1")] * 6,
                                  AsyncMock(return_value={"status": "unknown"}))
    await service.tick()
    await service.tick()
    clock.advance()
    await service.tick()
    await service.tick()
    assert len(store.uncertain_deliveries()) == 6
    reconcile = AsyncMock(return_value={"status": "unknown"})
    restart, fetch, _ = make_service(make_store(tmp_path, clock), clock, [], reconcile=reconcile)
    await restart.tick()
    await restart.tick()
    assert {call.args[0] for call in reconcile.await_args_list} == {watch["id"] for watch in watches}
    assert reconcile.await_count == 6
    await restart.tick()
    assert reconcile.await_count == 6
    fetch.assert_not_awaited()


def test_uncertain_queue_claims_are_shared_between_services(tmp_path):
    clock = Clock()
    first = make_store(tmp_path, clock)
    first.create(scope(), FEED, is_owner=True)
    prepare_uncertain(first, clock)
    second = make_store(tmp_path, clock)
    assert len(first.claim_uncertain()) == 1
    assert second.claim_uncertain() == []
    clock.advance(61)
    assert len(second.claim_uncertain()) == 1


def test_saved_watches_gain_reconciliation_fields_without_losing_state(tmp_path):
    clock = Clock()
    old = make_store(tmp_path, clock)
    watch = old.create(scope(), FEED, is_owner=True)
    with old._db() as db:
        db.execute("ALTER TABLE source_watches DROP COLUMN reconcile_after")
        db.execute("ALTER TABLE source_watches DROP COLUMN reconcile_attempts")
    restored = make_store(tmp_path, clock)
    row = restored.list(scope())[0]
    assert row["id"] == watch["id"]
    assert row["reconcile_after"] == 0 and row["reconcile_attempts"] == 0


def test_link_escaping_is_counted_in_the_public_url_limit(tmp_path):
    store = make_store(tmp_path, Clock())
    expanding_url = "https://example.com/" + "(" * 970
    with pytest.raises(ValueError, match="1000 characters"):
        store.create(scope(), expanding_url, is_owner=True)
    with pytest.raises(ValueError, match="1000 characters"):
        _digest({"id": "example", "url": expanding_url}, [{"title": "Entry", "url": expanding_url}])
    accepted_url = "https://example.com/" + "(" * 320
    watch = store.create(scope(), accepted_url, is_owner=True)
    digest = _digest(watch, [{"title": "Entry", "url": accepted_url}])
    assert len(digest) <= 1900 and "%28" in digest


@pytest.mark.asyncio
async def test_watch_list_preserves_every_id_with_long_public_urls(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    watches = [store.create(scope(event=str(number)), f"https://example.com/{number}/" + "x" * 940,
                            is_owner=True) for number in range(5)]
    result = await SourceWatchService(store).execute_tool("list_source_watches", {}, scope(event="list"))
    assert {watch["watch_id"] for watch in result["watches"]} == {watch["id"] for watch in watches}
    assert {watch["url"] for watch in result["watches"]} == {watch["url"] for watch in watches}
    assert len(result["watches"]) == 5 and len(result["message"]) < 1900
    assert all(set(watch) == {"watch_id", "url", "interval_minutes", "baseline", "fetched_at", "next_due", "delivery_status"}
               for watch in result["watches"])


@pytest.mark.asyncio
async def test_multi_watch_digest_keeps_complete_links_and_every_id(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    watches = [store.create(scope(event=str(number)), f"https://example.com/{number}.atom", is_owner=True) for number in range(5)]
    long_entry = {"status": "success", "url": FEED, "items": [{"title": "Release", "url": "https://example.com/" + "x" * 900}]}
    service, _, _ = make_service(store, clock, [long_entry] * 5)
    await service.tick()
    result = await service.execute_tool("get_source_digest", {}, scope(event="digest"))
    assert len(result["message"]) < 1900 and all(watch["id"] in result["message"] for watch in watches)
    assert len(result["entries"]) == 5
    assert all(entry["url"] == long_entry["items"][0]["url"] for entry in result["entries"])
    assert result["truncated"] is False and result["entry_count"] == 5


def test_redirected_feed_entries_with_feed_links_have_separate_keys():
    canonical = "https://example.com/final.atom"
    result = {"url": canonical, "items": [
        {"title": "One", "published": "2026-10-01", "url": canonical},
        {"title": "Two", "published": "2026-10-02", "url": canonical},
        {"title": "Three", "published": "2026-10-02", "url": canonical},
    ]}
    assert len(_items(result, FEED)) == 3


def test_feed_guids_keep_edited_entries_stable_after_a_redirect():
    canonical = "https://example.com/final.atom"
    first = {"url": canonical, "items": [{"title": "Original", "url": canonical, "id": "urn:entry:one"}]}
    edit = {"url": canonical, "items": [{"title": "Edited", "url": canonical, "id": "urn:entry:one"}]}
    assert _items(first, FEED)[0]["key"] == _items(edit, FEED)[0]["key"]
