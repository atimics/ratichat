"""Observations require a saved AI choice before a public watch update."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from chatbot.core.ai_engine import AIDecisionEngine
from chatbot.core.node_system.live_monitors import MonitorStore, LiveMonitorService
from chatbot.core.node_system.source_watches import WatchStore, SourceWatchService, _items
from chatbot.core.node_system.watch_review import WatchReview


SCOPE = {"channel_type": "discord", "channel_id": "20", "sender_id": "owner", "event_id": "request"}
URL = "https://example.com/feed"


def pending(tmp_path, kind="feed"):
    clock = [1700000000]
    store_type = MonitorStore if kind == "chain" else WatchStore
    store = store_type(tmp_path / "watches.db", owner_ids={"discord": {"owner"}},
                       allowed_channels={"discord": {"20", "21"}}, now=lambda: clock[0])
    if kind == "chain":
        store.execute_mutation("create_live_monitor", {"targets": [{"address": "0x" + "a" * 40, "network": "ethereum"}]}, SCOPE, True)
        event = {"key": "tx1", "network": "ethereum", "transaction_hash": "0xabc", "url": "https://example.com/tx1"}
        snapshot = {"events": [event], "streams": [{"id": "eth", "network": "ethereum", "stream": "transactions", "status": "ok", "keys": ["tx1"]}]}
        store.save_check(store.claim(), {"events": [], "streams": [{**snapshot["streams"][0], "keys": []}]})
        clock[0] += 3601
        watch = store.save_check(store.claim(), snapshot)
        fetch = SimpleNamespace(check=AsyncMock(return_value=snapshot))
    else:
        store.create(SCOPE, URL, is_owner=True)
        store.save_feed(store.claim(), [])
        clock[0] += 3601
        snapshot = {"status": "success", "items": [{"title": "New release", "url": "https://example.com/release", "summary": "Changes"}]}
        watch = store.save_feed(store.claim(), _items(snapshot, URL))
        fetch = AsyncMock(return_value=snapshot)
    return store, clock, watch, fetch


def service(store, fetch, send, review):
    if store.kind == "chain":
        return LiveMonitorService(store, reader=fetch, send=send, review=review, now=store.now)
    return SourceWatchService(store, fetch=fetch, send=send, review=review, now=store.now)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["feed", "chain"])
async def test_ai_quiet_choice_consumes_observation_across_restart(tmp_path, kind):
    store, clock, watch, fetch = pending(tmp_path, kind)
    send = AsyncMock()
    review = AsyncMock(return_value={"publish": False, "reason": "Routine activity"})
    await service(store, fetch, send, review)._deliver(watch)
    send.assert_not_awaited()
    with store._db() as db:
        receipt = dict(db.execute("SELECT * FROM source_watch_deliveries").fetchone())
        assert receipt["status"] == "quiet" and receipt["message_id"] is None
        assert json.loads(receipt["review_json"])["reason"] == "Routine activity"
    restarted = type(store)(store.db_path, owner_ids=store.owner_ids, allowed_channels=store.allowed_channels, now=store.now)
    clock[0] += 3601
    await service(restarted, fetch, send, review).tick()
    send.assert_not_awaited()
    review.assert_awaited_once()
    assert restarted.list(SCOPE)[0]["pending_status"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", [None, {}, {"publish": "true"}, {"publish": True}, {"publish": True, "content": ""}])
async def test_missing_or_invalid_ai_choice_keeps_update_private(tmp_path, choice):
    store, _, watch, fetch = pending(tmp_path)
    send = AsyncMock()
    await service(store, fetch, send, AsyncMock(return_value=choice))._deliver(watch)
    send.assert_not_awaited()
    assert store.list(SCOPE)[0]["pending_status"] == "pending"


@pytest.mark.asyncio
async def test_unconfigured_review_and_model_error_keep_update_private(tmp_path):
    store, clock, watch, fetch = pending(tmp_path)
    send = AsyncMock()
    await service(store, fetch, send, None)._deliver(watch)
    clock[0] += 61
    await service(store, fetch, send, AsyncMock(side_effect=RuntimeError("model unavailable")))._deliver(store.claim(pending=True))
    send.assert_not_awaited()
    assert store.list(SCOPE)[0]["pending_status"] == "pending"


@pytest.mark.asyncio
async def test_approved_ai_text_and_choice_survive_failed_send_and_restart(tmp_path):
    store, clock, watch, fetch = pending(tmp_path)
    review = AsyncMock(return_value={"publish": True, "content": "This release fixes the issue we discussed. @everyone", "reason": "Relevant fix"})
    send = AsyncMock(side_effect=[{"status": "failure"}, {"status": "success", "message_id": "posted"}])
    await service(store, fetch, send, review)._deliver(watch)
    clock[0] += 61
    restarted = WatchStore(store.db_path, owner_ids=store.owner_ids, allowed_channels=store.allowed_channels, now=store.now)
    retry_review = AsyncMock(side_effect=AssertionError("Reuse saved AI choice"))
    await service(restarted, fetch, send, retry_review).tick()
    assert send.await_count == 2 and send.await_args_list[0] == send.await_args_list[1]
    text = send.await_args.args[3]
    assert text.startswith("This release fixes the issue we discussed. ＠everyone")
    assert "https://example.com/release" in text and "Receipt:" in text
    retry_review.assert_not_awaited()
    assert restarted.list(SCOPE)[0]["delivery_message_id"] == "posted"


@pytest.mark.asyncio
async def test_expired_observation_lease_cannot_publish_ai_result(tmp_path):
    store, clock, watch, fetch = pending(tmp_path)
    async def review(_):
        clock[0] += 181
        return {"publish": True, "content": "A relevant update"}
    send = AsyncMock()
    await service(store, fetch, send, review)._deliver(watch)
    send.assert_not_awaited()
    assert store.saved_review(watch) is None


@pytest.mark.asyncio
async def test_review_uses_scoped_conversation_goal_history_and_saved_budget(tmp_path):
    store, _, watch, _ = pending(tmp_path)
    world = SimpleNamespace(get_channel=Mock(return_value=SimpleNamespace(recent_messages=[SimpleNamespace(sender="owner", content="Only major developments please", timestamp=1)])))
    awareness = SimpleNamespace(get_task_for_event=Mock(return_value={"goal": "Track recovery of the lost funds"}))
    engine = SimpleNamespace(compose_watch_update=AsyncMock(return_value={"publish": False}))
    reviewer = WatchReview(store, engine, SimpleNamespace(world_state_manager=world, awareness_store=awareness), daily_budget=1)
    assert await reviewer(watch) == {"publish": False}
    payload = engine.compose_watch_update.await_args.args[0]
    assert payload["watch"]["original_goal"] == "Track recovery of the lost funds"
    assert payload["conversation"][0]["content"] == "Only major developments please"
    assert payload["observation"]["items"][0]["title"] == "New release"
    assert payload["observation"]["trust"] == "untrusted_source"
    world.get_channel.assert_called_once_with("20")
    assert await WatchReview(store, engine, SimpleNamespace(), daily_budget=1)(watch) is None
    engine.compose_watch_update.assert_awaited_once()


@pytest.mark.asyncio
async def test_model_receives_review_policy_and_can_choose_quiet():
    requests = []
    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": '{"publish":false,"reason":"Routine transfer"}'}}]})
    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    engine = AIDecisionEngine(api_key="test")
    with patch("chatbot.core.ai_engine.httpx.AsyncClient", return_value=client):
        assert (await engine.compose_watch_update({"conversation": ["Only major changes"]}))["publish"] is False
    prompt = requests[0]["messages"][0]["content"]
    assert "routine transfers" in prompt and "untrusted evidence" in prompt
    assert "Only major changes" in requests[0]["messages"][1]["content"]


def test_upgrade_keeps_existing_pending_receipt_for_ai_review(tmp_path):
    store, _, watch, _ = pending(tmp_path)
    with store._db() as db:
        db.execute("ALTER TABLE source_watch_deliveries DROP COLUMN review_json")
    upgraded = WatchStore(store.db_path, owner_ids=store.owner_ids, allowed_channels=store.allowed_channels, now=store.now)
    assert upgraded.saved_review(watch) is None
    assert upgraded.list(SCOPE)[0]["delivery_key"] == watch["delivery_key"]
    with upgraded._db() as db:
        assert db.execute("SELECT text FROM source_watch_deliveries").fetchone()[0] == watch["pending_text"]


@pytest.mark.asyncio
async def test_legacy_uncertain_matrix_update_keeps_reconciliation_quiet(tmp_path):
    store, _, watch, _ = pending(tmp_path)
    store.allowed_channels = {"matrix": {"!room:test"}}
    store.owner_ids = {"matrix": {"owner"}}
    with store._db() as db:
        db.execute("UPDATE source_watches SET channel_type='matrix',channel_id='!room:test',pending_status='unknown',lease_token=NULL,lease_until=0")
    reconcile = AsyncMock(return_value={"status": "success", "message_id": "$sent"})
    await SourceWatchService(store, reconcile=reconcile, now=store.now).deliver_pending()
    reconcile.assert_not_awaited()
