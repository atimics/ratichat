"""Autonomous participation, durable choices, and bounded peer exchanges."""

import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from chatbot.core.model_router import DECISIONS_URL, JevDecisionClient
from chatbot.core.node_system.participation import ParticipationService
from chatbot.core.world_state.structures import Channel, Message
from tests.test_shared_task_runtime import message, setup


def receipt(choice="join", topic="developer"):
    return {"kind": "jev", "usage": {"cost": 0.00005}, "answers": {
        "participation": {"choice": choice}, "topic": {"choice": topic}}}


def event(identity="one", *, bot=True, parent=None, timestamp=None):
    source = Message(identity, "discord", "peer" if bot else "person", "Which Python version did we agree on?",
        time.time() if timestamp is None else timestamp, channel_id="general", reply_to=parent,
        metadata={"is_bot": bot, "conversation_candidate": True})
    return Channel(id="general", type="discord", name="general", recent_messages=[source]), source


@pytest.mark.asyncio
@pytest.mark.parametrize("bot", [True, False])
async def test_jev_can_join_bot_or_human_conversation_without_a_mention(tmp_path, bot):
    seen = []
    async def provider(request):
        assert str(request.url) == DECISIONS_URL
        body = json.loads(request.content)
        seen.append(body)
        answers = {}
        for name, question in body["questions"].items():
            choice = "join" if name == "participation" else "developer"
            answers[name] = {"type": "choice", "choice": choice, "confidence": 1,
                "probabilities": {key: float(key == choice) for key in question["criteria"]}}
        return httpx.Response(200, json={"model": "typesafe/jev-1.13-20260917", "answers": answers,
                                        "usage": {"cost": 0.00005}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        service = ParticipationService(str(tmp_path / "participation.db"), JevDecisionClient("key", client=client))
        channel, source = event(bot=bot)
        assert (await service.evaluate(channel, source))["join"]
    assert seen[0]["state"]["event"]["author_is_bot"] is bot
    assert seen[0]["state"]["event"]["mentioned"] is False
    service.close()


@pytest.mark.asyncio
async def test_wait_choice_survives_restart_and_skips_full_task(tmp_path):
    processor, task_service, store, ai, sends = setup(tmp_path)
    source = message(processor, store, "discord", "general", "peer", "d1", "Routine build status")
    source.metadata.update(is_bot=True, conversation_candidate=True, bot_mentioned=False)
    decisions = SimpleNamespace(decide=AsyncMock(return_value=receipt("wait")))
    db = str(tmp_path / "participation.db")
    processor.participation_service = ParticipationService(db, decisions, store)
    result = await processor.process_cycle("one", "general")
    assert result["participation"]["join"] is False
    assert store.get_task_for_event("discord", "general", "d1") is None
    ai.make_decision.assert_not_awaited()
    ai.compose_reply.assert_not_awaited()
    sends["send_discord_reply"].assert_not_awaited()
    processor.participation_service.close()
    restored = ParticipationService(db, decisions, store)
    assert not (await restored.evaluate(processor.world_state.get_channel("general"), source))["join"]
    decisions.decide.assert_awaited_once()
    restored.close()


@pytest.mark.asyncio
async def test_peer_bot_gets_scoped_saved_reply(tmp_path):
    processor, task_service, store, ai, sends = setup(tmp_path)
    source = message(processor, store, "discord", "general", "peer", "d1", "Compare the shared Python project")
    source.metadata.update(is_bot=True, conversation_candidate=True, bot_mentioned=False)
    processor.participation_service = ParticipationService(str(tmp_path / "participation.db"),
        SimpleNamespace(decide=AsyncMock(return_value=receipt())), store)
    result = await processor.process_cycle("one", "general")
    assert not result["failed"]
    sends["send_discord_reply"].assert_awaited_once()
    params = sends["send_discord_reply"].call_args.args[0]
    assert params["channel_id"] == "general" and params["reply_to_id"] == "d1"
    sends["send_matrix_reply"].assert_not_awaited()
    assert store.get_task_for_event("discord", "general", "d1")["status"] == "complete"
    assert (await processor.process_cycle("duplicate", "general"))["duplicate"]
    assert sends["send_discord_reply"].await_count == 1


@pytest.mark.asyncio
async def test_thread_and_topic_limits_survive_restart_and_reply_chains(tmp_path):
    now = [time.time()]
    db = str(tmp_path / "participation.db")
    decisions = SimpleNamespace(decide=AsyncMock(return_value=receipt()))
    service = ParticipationService(db, decisions, clock=lambda: now[0], min_gap_seconds=0)
    for index in range(3):
        channel, source = event(str(index), parent="reply-" + str(index - 1) if index else None, timestamp=now[0])
        assert (await service.evaluate(channel, source))["join"]
        service.sent("discord", "general", source.id, "reply-" + str(index))
    service.close()
    service = ParticipationService(db, decisions, clock=lambda: now[0], min_gap_seconds=0)
    channel, source = event("four", parent="reply-2", timestamp=now[0])
    assert not (await service.evaluate(channel, source))["join"]
    assert decisions.decide.await_count == 3
    channel, source = event("new-thread", timestamp=now[0])
    assert (await service.evaluate(channel, source))["reason"] == "topic_or_thread_limit"
    service.close()


@pytest.mark.asyncio
async def test_spacing_and_decision_budget_bound_paid_calls(tmp_path):
    now = [time.time()]
    decisions = SimpleNamespace(decide=AsyncMock(return_value=receipt()))
    service = ParticipationService(str(tmp_path / "spacing.db"), decisions, clock=lambda: now[0])
    channel, source = event("one", timestamp=now[0])
    assert (await service.evaluate(channel, source))["join"]
    channel, source = event("two", timestamp=now[0])
    assert (await service.evaluate(channel, source))["reason"] == "conversation_spacing"
    decisions.decide.assert_awaited_once()
    service.close()
    decisions = SimpleNamespace(decide=AsyncMock(return_value=receipt("wait")))
    service = ParticipationService(str(tmp_path / "budget.db"), decisions, max_decisions_per_hour=2)
    for identity in ("one", "two"):
        channel, source = event(identity)
        await service.evaluate(channel, source)
    channel, source = event("three")
    assert (await service.evaluate(channel, source))["reason"] == "decision_budget"
    assert decisions.decide.await_count == 2
    service.close()


@pytest.mark.asyncio
async def test_jev_failure_keeps_observing_and_records_reserved_cost(tmp_path):
    decisions = SimpleNamespace(decide=AsyncMock(side_effect=httpx.ConnectError("offline")))
    service = ParticipationService(str(tmp_path / "participation.db"), decisions, max_decisions_per_hour=1)
    channel, source = event("one")
    assert not (await service.evaluate(channel, source))["join"]
    channel, source = event("two")
    assert (await service.evaluate(channel, source))["reason"] == "decision_budget"
    service.close()
