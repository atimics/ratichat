"""Feed watch authority, quiet baselines, saved updates, and recovery."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from chatbot.core.node_system.source_watches import HELP, SourceWatchService, WatchStore, _digest, _items


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


def test_create_receipt_and_url_are_idempotent(tmp_path):
    store = make_store(tmp_path, Clock())
    first = store.create(scope(), FEED, is_owner=True)
    replay = store.create(scope(), "https://example.com/another.atom", is_owner=True)
    same_url = store.create(scope(event="event2"), FEED, is_owner=True)
    assert first["id"] == replay["id"] == same_url["id"]
    assert len(store.list(scope())) == 1


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
async def test_commands_create_list_digest_remove_and_help(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    service, fetch, send = make_service(store, clock, [feed("v1")])
    assert await service.manage("hello", scope(), True) is None
    assert await service.manage("watch", scope(), True) == HELP
    assert "owner" in await service.manage(f"watch {FEED}", scope(sender="stranger"), True)
    reply = await service.manage(f"watch {FEED} every 60m", scope(), True)
    watch = store.list(scope())[0]
    assert watch["interval_seconds"] == 3600 and watch["id"] in reply
    assert await service.manage(f"watch {FEED} every 60m", scope(), True) == reply
    assert watch["id"] in await service.manage("watches", scope(event="list"), False)
    assert "first check" in await service.manage("digest", scope(event="digest0"), False)
    await service.tick()
    digest = await service.manage(f"digest {watch['id']}", scope(event="digest1"), False)
    assert "Release v1" in digest and "https://example.com/releases/v1" in digest
    assert "Removed" in await service.manage(f"unwatch {watch['id']}", scope(event="remove"), True)
    assert store.list(scope()) == []
    assert fetch.await_count == 1 and send.await_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix,expected", [("every 15m", 900), ("every 24h", 86400), ("", 3600)])
async def test_command_intervals(tmp_path, suffix, expected):
    clock = Clock()
    store = make_store(tmp_path, clock)
    service, _, _ = make_service(store, clock, [])
    await service.manage(f"watch {FEED} {suffix}", SimpleNamespace(**scope()), True)
    assert store.list(scope())[0]["interval_seconds"] == expected


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
    text = await SourceWatchService(store).manage("watches", scope(event="list"), False)
    assert all(watch["id"] in text for watch in watches)
    assert len(text) < 1900 and "…" in text


@pytest.mark.asyncio
async def test_multi_watch_digest_keeps_complete_links_and_every_id(tmp_path):
    clock = Clock()
    store = make_store(tmp_path, clock)
    watches = [store.create(scope(event=str(number)), f"https://example.com/{number}.atom", is_owner=True) for number in range(5)]
    long_entry = {"status": "success", "url": FEED, "items": [{"title": "Release", "url": "https://example.com/" + "x" * 900}]}
    service, _, _ = make_service(store, clock, [long_entry] * 5)
    await service.tick()
    text = await service.manage("digest", scope(event="digest"))
    assert len(text) < 1900 and all(watch["id"] in text for watch in watches)
    assert text.count("source links") == 5


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
