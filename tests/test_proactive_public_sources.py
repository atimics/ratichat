"""Saved proactive settings, source limits, quiet hours, and delivery recovery."""

import asyncio
from datetime import datetime
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from zoneinfo import ZoneInfo

import pytest

from chatbot.core.ai_engine import ActionPlan, DecisionResult
from chatbot.core.node_system.proactive_sources import ProactiveStore, ProactiveSourceService, TOPICS, POST_GAP
from chatbot.core.node_system.processor import NodeProcessor
from chatbot.core.orchestration.capability_policy import CapabilityPolicy, ExecutionScope, PROACTIVE_SOURCE_TOOLS
from chatbot.core.orchestration.main_orchestrator import MainOrchestrator, OrchestratorConfig, TraditionalProcessor
from chatbot.core.world_state import WorldStateManager
from chatbot.core.world_state.payload_builder import PayloadBuilder
from chatbot.core.world_state.structures import Message
from chatbot.tools.proactive_source_tools import ConfigureProactiveTool, GetProactiveStatusTool
from chatbot.tools.registry import ToolRegistry


class Clock:
    def __init__(self, hour=10):
        self.value = datetime(2026, 10, 8, hour, tzinfo=ZoneInfo("America/Vancouver")).timestamp()

    def __call__(self):
        return self.value

    def advance(self, seconds=3600):
        self.value += seconds


def scope(sender="owner", channel="20", event="40"):
    return {"channel_type": "discord", "channel_id": channel, "sender_id": sender, "event_id": event}


def store(tmp_path, clock):
    value = ProactiveStore(tmp_path / "proactive.db", {"owner"}, {"20", "21"}, now=clock)
    value.seed("20", enabled=True)
    return value


def item(clock, suffix="one"):
    return {"url": "https://example.com/" + suffix, "title": "A useful story", "summary": "The source explains a new tool.",
            "published": datetime.fromtimestamp(clock(), ZoneInfo("UTC")).isoformat(), "publisher": "Publisher"}


def draft(items):
    return {"publish": True, "item_id": items[0]["id"], "content": "This tool may help with Python projects. What would you use it for?"}


def prepare(value, clock, suffix="one"):
    claim = value.claim()
    assert claim
    items = value.fresh_items("20", [item(clock, suffix)])
    value.finish(claim, items, draft(items))
    return value.deliveries()[0]


def test_owner_settings_and_receipts_keep_the_original_request(tmp_path):
    clock = Clock()
    value = store(tmp_path, clock)
    with pytest.raises(ValueError, match="owner"):
        value.configure({"enabled": False}, scope(sender="guest"))
    with pytest.raises(ValueError, match="configured"):
        value.configure({"enabled": True}, scope(channel="private"))
    params = {"enabled": False, "topics": ["world", "tech"], "max_daily_posts": 2}
    result = value.configure(params, scope())
    assert result["settings"]["enabled"] is False
    restored = store(tmp_path, clock)
    assert restored.status(scope())["enabled"] is False
    assert restored.configure(params, scope()) == result
    assert restored.tool_results(scope())[0]["settings"] == result["settings"]
    for other in (scope(sender="guest"), scope(event="41"), scope(channel="21")):
        assert restored.tool_results(other) == []
    assert restored.status(scope(sender="guest"))["max_daily_posts"] == 2


@pytest.mark.parametrize("params", [{"enabled": "yes"}, {"enabled": True, "max_daily_posts": 5},
    {"enabled": True, "interval_minutes": 1}, {"enabled": True, "topics": []},
    {"enabled": True, "topics": ["private"]}, {"enabled": True, "channel_id": "21"}])
def test_settings_validate_limits_and_destinations(tmp_path, params):
    value = store(tmp_path, Clock())
    with pytest.raises(ValueError):
        value.configure(params, scope())


@pytest.mark.parametrize("hour", [0, 7, 22, 23])
def test_quiet_hours_hold_source_checks(tmp_path, hour):
    assert store(tmp_path, Clock(hour)).claim() is None


def test_two_workers_share_one_source_claim_and_recover_expired_lease(tmp_path):
    clock = Clock()
    first = store(tmp_path, clock)
    second = store(tmp_path, clock)
    original = first.claim()
    assert original and second.claim() is None
    clock.advance()
    current = second.claim()
    assert current and current["lease_token"] != original["lease_token"]
    items = first.fresh_items("20", [item(clock)])
    first.finish(original, items, draft(items))
    assert first.deliveries() == []


def test_daily_source_decisions_and_paid_searches_are_bounded(tmp_path):
    clock = Clock(8)
    value = store(tmp_path, clock)
    for _ in range(8):
        claim = value.claim()
        assert claim
        value.finish(claim, [], None)
        clock.advance()
    assert value.claim() is None
    with sqlite3.connect(value.db_path) as db:
        row = db.execute("SELECT lookups,decisions,web_searches FROM proactive_usage").fetchone()
    assert row[0] == 8 and row[1] == 8 and row[2] <= 3
    clock.advance(16 * 3600)
    assert value.claim()


def test_posts_keep_source_links_receipts_spacing_and_daily_limit(tmp_path):
    clock = Clock(8)
    value = store(tmp_path, clock)
    for number in range(4):
        post = prepare(value, clock, str(number))
        assert "Publisher:" in post["text"] and "https://example.com/" in post["text"]
        assert "Receipt: proactive:" in post["text"] and len(post["text"]) < 1900
        claimed = value.claim_delivery(post)
        assert claimed
        value.record_delivery(claimed, {"status": "success", "message_id": str(number)})
        clock.advance()
        assert value.claim() is None
        clock.advance(POST_GAP - 3600)
    assert value.status(scope())["posts_today"] == 4
    assert value.claim() is None


def test_pause_during_source_work_cancels_the_old_generation(tmp_path):
    clock = Clock()
    value = store(tmp_path, clock)
    claim = value.claim()
    value.configure({"enabled": False}, scope())
    items = value.fresh_items("20", [item(clock)])
    value.finish(claim, items, draft(items))
    assert value.deliveries() == [] and value.claim() is None


def test_pause_cancels_a_saved_post_before_delivery(tmp_path):
    clock = Clock()
    value = store(tmp_path, clock)
    post = prepare(value, clock)
    value.configure({"enabled": False}, scope())
    assert value.claim_delivery(post) is None and value.deliveries() == []


def test_fresh_sources_keep_public_urls_dates_and_repeat_history(tmp_path):
    clock = Clock()
    value = store(tmp_path, clock)
    old = item(clock, "old")
    old["published"] = "2020-01-01T00:00:00Z"
    sources = [item(clock), item(clock), old, {**item(clock), "url": "http://localhost/secret"}]
    items = value.fresh_items("20", sources)
    assert len(items) == 1
    value.finish(value.claim(), items, {"publish": False})
    assert value.fresh_items("20", sources) == []


@pytest.mark.parametrize("bad", [{"publish": True, "item_id": "made-up", "content": "Hello"},
    {"publish": True, "content": "Hello https://attacker.example"},
    {"publish": True, "content": "x" * 851}, {"publish": "true", "content": "Hello"}])
def test_drafts_require_an_actual_source_and_bounded_text(tmp_path, bad):
    clock = Clock()
    value = store(tmp_path, clock)
    claim = value.claim()
    items = value.fresh_items("20", [item(clock)])
    if "item_id" not in bad:
        bad = {**bad, "item_id": items[0]["id"]}
    value.finish(claim, items, bad)
    assert value.deliveries() == []


def test_delivery_claim_and_uncertain_receipt_survive_restart(tmp_path):
    clock = Clock()
    value = store(tmp_path, clock)
    post = prepare(value, clock)
    claimed = value.claim_delivery(post)
    assert value.claim_delivery(post) is None
    restored = store(tmp_path, clock)
    clock.advance(120)
    pending = restored.deliveries()[0]
    assert pending["status"] == "unknown"
    assert restored.claim() is None and restored.claim_delivery(pending) is None
    restored.reconcile(pending, {"status": "success", "message_id": "sent"})
    assert restored.deliveries() == [] and restored.status(scope())["posts_today"] == 1


@pytest.mark.asyncio
async def test_service_saves_a_draft_then_sends_it_once_to_its_channel(tmp_path):
    clock = Clock()
    value = store(tmp_path, clock)
    async def compose(payload):
        assert set(payload["expanded_nodes"]) == {"sources.public"}
        assert "channels" not in payload
        return draft(payload["expanded_nodes"]["sources.public"]["items"])
    ai = SimpleNamespace(api_key="linked", compose_proactive=AsyncMock(side_effect=compose))
    delivery = SimpleNamespace(send=AsyncMock(return_value={"status": "success", "message_id": "sent"}), reconcile=AsyncMock())
    service = ProactiveSourceService(value, ai, delivery, fetch=AsyncMock(return_value=[item(clock)]))
    await service.tick()
    delivery.send.assert_not_awaited()
    await service.tick()
    await service.tick()
    delivery.send.assert_awaited_once()
    assert delivery.send.await_args.args[1:3] == ("discord", "20")
    ai.compose_proactive.assert_awaited_once()


@pytest.mark.asyncio
async def test_unknown_send_uses_receipt_check_before_more_posts(tmp_path):
    clock = Clock()
    value = store(tmp_path, clock)
    prepare(value, clock)
    delivery = SimpleNamespace(send=AsyncMock(return_value={"status": "unknown"}), reconcile=AsyncMock(return_value={"status": "success", "message_id": "sent"}))
    ai = SimpleNamespace(api_key="linked", compose_proactive=AsyncMock())
    service = ProactiveSourceService(value, ai, delivery, fetch=AsyncMock())
    await service.tick()
    clock.advance(60)
    await service.tick()
    delivery.send.assert_awaited_once()
    delivery.reconcile.assert_awaited_once()
    ai.compose_proactive.assert_not_awaited()


@pytest.mark.asyncio
async def test_owner_tool_uses_saved_scope_and_recovers_receipt_after_a_failed_checkpoint(tmp_path):
    clock = Clock()
    value = store(tmp_path, clock)
    service = ProactiveSourceService(value, Mock(), Mock())
    world = WorldStateManager()
    world.add_channel("20", "discord", "general")
    world.add_message("20", Message("40", "discord", "owner", "Pause proactive speaking.", clock(), metadata={"bot_mentioned": True}))
    ai = SimpleNamespace(api_key="linked", make_decision=AsyncMock(side_effect=[
        DecisionResult([ActionPlan("configure_proactive", {"enabled": False}, "Pause", 1)], "", "", "first"),
        DecisionResult([ActionPlan("wait", {}, "Wait", 1)], "", "", "retry"),
    ]), compose_reply=AsyncMock(return_value="Proactive speaking is paused."))
    registry = ToolRegistry()
    registry.register_tool(ConfigureProactiveTool())
    registry.register_tool(GetProactiveStatusTool())
    send = AsyncMock(return_value={"status": "success", "message_id": "reply"})
    registry.register_tool(SimpleNamespace(name="send_discord_reply", description="Reply", parameters_schema={}, execute=send))
    registry.register_tool(SimpleNamespace(name="wait", description="Wait", parameters_schema={}, execute=AsyncMock()))
    rules = CapabilityPolicy(approved_discord_channel_ids=["20"], discord_owner_user_ids=["owner"])
    context = SimpleNamespace(proactive_source_service=service, discord_observer=SimpleNamespace(can_reply=lambda c, e: True))
    executor = TraditionalProcessor(ai, registry, Mock(), SimpleNamespace(add_tool_result=AsyncMock()), context, rules)
    processor = NodeProcessor(world, PayloadBuilder(), executor, str(tmp_path / "nodes.db"))
    processor.research_store.clock = lambda: 1000
    processor.research_store.save_sources = Mock(side_effect=RuntimeError("Checkpoint needs another attempt"))
    assert (await processor.process_cycle("first", "20"))["failed"]
    assert value.status(scope())["enabled"] is False
    processor.research_store.close()
    restarted = NodeProcessor(world, PayloadBuilder(), executor, processor.db_path)
    restarted.research_store.clock = lambda: 1011
    assert not (await restarted.process_cycle("retry", "20"))["failed"]
    receipt = ai.compose_reply.await_args.args[0]["answer_nodes"]["sources.proactive_result_1"]
    assert receipt["settings"]["enabled"] is False
    send.assert_awaited_once()
    assert send.await_args.args[0]["reply_to_id"] == "40"


@pytest.mark.asyncio
async def test_tool_and_policy_keep_owner_and_current_channel_checks(tmp_path):
    rules = CapabilityPolicy(approved_discord_channel_ids=["20"], discord_owner_user_ids=["owner"])
    owner = ExecutionScope("20", "discord", frozenset({"40"}), "40", "owner")
    guest = ExecutionScope("20", "discord", frozenset({"40"}), "40", "guest")
    assert rules.filter_tool_names(PROACTIVE_SOURCE_TOOLS, owner) == PROACTIVE_SOURCE_TOOLS
    assert rules.filter_tool_names(PROACTIVE_SOURCE_TOOLS, guest) == {"get_proactive_status"}
    assert rules.filter_tool_names(PROACTIVE_SOURCE_TOOLS, None) == set()
    value = store(tmp_path, Clock())
    context = SimpleNamespace(proactive_source_service=ProactiveSourceService(value, Mock(), Mock()), execution_scope=guest)
    assert (await ConfigureProactiveTool().execute({"enabled": False}, context))["status"] == "failure"
    context.execution_scope = owner
    assert (await ConfigureProactiveTool().execute({"enabled": False, "channel_id": "21"}, context))["status"] == "blocked"
    assert value.status(scope())["enabled"] is True
