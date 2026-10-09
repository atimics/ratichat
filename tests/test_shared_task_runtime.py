"""Cross-platform task recovery, fixed destinations, workers, and local routes."""

import asyncio
import copy
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from chatbot.core.ai_engine import AIDecisionEngine, ActionPlan, DecisionResult
from chatbot.core.model_router import CHAT_URL, DECISIONS_URL, ModelRouter
from chatbot.core.node_system.awareness_store import AwarenessStore
from chatbot.core.node_system.processor import NodeProcessor
from chatbot.core.node_system.task_service import TaskService
from chatbot.core.orchestration.capability_policy import CapabilityPolicy
from chatbot.core.orchestration.main_orchestrator import TraditionalProcessor
from chatbot.core.world_state import WorldStateManager
from chatbot.core.world_state.payload_builder import PayloadBuilder
from chatbot.core.world_state.structures import Channel, Message
from chatbot.tools.registry import ToolRegistry
from chatbot.tools.task_tools import GetTaskStatusTool, RunTaskWorkersTool


ALLOWED = {"discord": ["general"], "matrix": ["!public:test", "!local:test"]}
COMMUNITY = {"discord": ["general"], "matrix": ["!public:test"]}


def route(model="anthropic/claude-haiku-5.5", expanded=(), resume=None):
    return {"model": model, "endpoint": CHAT_URL, "mode": "json_action_plan",
        "supported_parameters": ["response_format", "max_tokens", "reasoning"],
        "reasoning": None, "persona": "researcher", "topic": "developer", "max_tokens": 900,
        "catalog_version": "test", "decision_receipt": {"usage": {"cost": 0.00001}},
        "expanded_nodes": list(expanded), "resume_task_id": resume,
        "pricing": {"prompt": 0.0000001, "completion": 0.0000005},
        "limits": {"context_tokens": 200000, "budget_usd": 0.1}}


class Router:
    def __init__(self):
        self.calls = []

    async def select_route(self, state, **kwargs):
        self.calls.append(copy.deepcopy((state, kwargs)))
        resume = next((t["id"] for t in kwargs.get("tasks", []) if t["id"] != kwargs.get("current_task_id")), None) if "continue" in state["request"].lower() else None
        return route(expanded=[n["id"] for n in kwargs["nodes"]], resume=resume)


def setup(tmp_path, *, store=None, ai=None):
    db = str(tmp_path / "shared.db")
    store = store or AwarenessStore(db, allowed_channels=ALLOWED, community_channels=COMMUNITY)
    world = WorldStateManager()
    ai = ai or SimpleNamespace(api_key="key", make_decision=AsyncMock(return_value=DecisionResult([], "", "", "test")),
        compose_reply=AsyncMock(return_value="Saved evidence reply"), compose_task_worker=AsyncMock(return_value="Worker evidence"))
    router = Router()
    service = TaskService(store, router, ai)
    registry = ToolRegistry()
    sends = {}
    for name in ["send_discord_reply", "send_matrix_reply"]:
        sends[name] = AsyncMock(return_value={"status": "success", "message_id": "bot-" + name})
        registry.register_tool(SimpleNamespace(name=name, description=name, parameters_schema={}, execute=sends[name]))
    for tool in [GetTaskStatusTool(), RunTaskWorkersTool()]:
        registry.register_tool(tool)
    policy = CapabilityPolicy(approved_discord_channel_ids=ALLOWED["discord"], approved_matrix_room_ids=ALLOWED["matrix"])
    context = SimpleNamespace(discord_observer=SimpleNamespace(can_reply=Mock(return_value=True)),
        matrix_observer=SimpleNamespace(user_id="@ratichat:test"), task_service=service)
    executor = TraditionalProcessor(ai, registry, Mock(), AsyncMock(), context, policy)
    processor = NodeProcessor(world, PayloadBuilder(), executor, db, awareness_store=store, task_service=service)
    return processor, service, store, ai, sends


def message(processor, store, platform, channel, sender, event, content):
    processor.world_state.add_channel(channel, platform, channel)
    value = Message(event, platform, sender, content, time.time(), channel_id=channel, metadata={"bot_mentioned": True})
    processor.world_state.add_message(channel, value)
    from dataclasses import asdict
    store.ingest_message(platform, channel, asdict(value))
    return value


@pytest.mark.asyncio
async def test_discord_task_survives_restart_and_continues_in_matrix(tmp_path):
    processor, service, store, ai, sends = setup(tmp_path)
    message(processor, store, "matrix", "!local:test", "@local:test", "$private", "LOCAL SECRET")
    message(processor, store, "discord", "general", "discord-owner", "d1", "Research the shared Python project")
    assert not (await processor.process_cycle("d", "general"))["failed"]
    task = store.get_task_for_event("discord", "general", "d1")
    saved_route = task["route"]
    assert task["status"] == "complete"
    assert task["result"]["content"] == "Saved evidence reply"
    store.close()
    second, second_service, second_store, second_ai, second_sends = setup(tmp_path)
    message(second, second_store, "matrix", "!public:test", "@matrix-owner:test", "$m1", "Continue the shared Python project")
    assert not (await second.process_cycle("m", "!public:test"))["failed"]
    resumed = second_store.get_task_for_event("matrix", "!public:test", "$m1")
    assert resumed["id"] == task["id"]
    assert resumed["route"] == saved_route
    payload = second_ai.compose_reply.call_args.args[0]
    assert "Python project" in json.dumps(payload)
    assert "LOCAL SECRET" not in json.dumps(payload)
    assert set(payload["channels"]) == {"!public:test"}
    assert second_sends["send_matrix_reply"].call_args.args[0]["reply_to_id"] == "$m1"
    second_sends["send_discord_reply"].assert_not_awaited()
    assert second_store.load_view("discord:general:discord-owner", "discord", "general", "discord-owner")["revision"] == 1
    assert second_store.load_view("matrix:!public:test:@matrix-owner:test", "matrix", "!public:test", "@matrix-owner:test")["revision"] == 1


@pytest.mark.asyncio
async def test_deleted_saved_request_stops_delivery(tmp_path):
    processor, service, store, ai, sends = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "Research Python")
    async def delete_before_reply(payload):
        store.delete_message("discord", "general", "d1")
        return "This reply uses an old request"
    ai.compose_reply.side_effect = delete_before_reply
    result = await processor.process_cycle("d", "general")
    assert result["failed"]
    sends["send_discord_reply"].assert_not_awaited()


@pytest.mark.asyncio
async def test_worker_results_survive_restart_and_share_root_budget(tmp_path):
    processor, service, store, ai, sends = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "Compare the Python project")
    binding = await service.prepare(processor.world_state.get_channel("general"), source)
    scope = processor.policy.scope_from_payload(processor.payload_builder.build_request_node_payload(
        processor.world_state.get_channel("general"), processor.node_manager))
    jobs = [{"goal": "Review evidence", "persona": "researcher"}, {"goal": "Check gaps", "persona": "critic"}]
    with service.activate(binding):
        result = await service.execute_tool("run_task_workers", {"jobs": jobs}, scope)
    assert result["status"] == "success"
    assert ai.compose_task_worker.await_count == 2
    assert all(item["content"] == "Worker evidence" for item in result["workers"])
    root = store.get_task(binding.task["id"])
    assert root["spent_usd"] >= 0.00003
    store.close()
    second, next_service, next_store, next_ai, next_sends = setup(tmp_path)
    restored = await next_service.prepare(processor.world_state.get_channel("general"), source)
    with next_service.activate(restored):
        retry = await next_service.execute_tool("run_task_workers", {"jobs": jobs}, scope)
        blocked = await next_service.execute_tool("run_task_workers", {"jobs": [{"goal": "nested", "model": "expensive"}]}, scope)
    assert all(item["saved"] for item in retry["workers"])
    next_ai.compose_task_worker.assert_not_awaited()
    assert blocked["status"] == "blocked"


@pytest.mark.asyncio
async def test_model_choices_are_local_during_parallel_calls(tmp_path):
    processor, service, store, ai, sends = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "research")
    first = await service.prepare(processor.world_state.get_channel("general"), source)
    other = Message("d2", "discord", "owner", "code", time.time(), channel_id="general")
    second = await service.prepare(processor.world_state.get_channel("general"), other)
    second.route = route("openai/gpt-6-luna")
    engine = AIDecisionEngine("key", model="default/model")
    requests = []
    async def respond(request):
        body = json.loads(request.content)
        requests.append(body)
        await asyncio.sleep(0)
        return httpx.Response(200, json={"choices": [{"message": {"content": "{}"}}], "model": body["model"],
            "provider": "test", "usage": {"prompt_tokens": 10, "completion_tokens": 2, "cost": 0.00001}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        async def call(binding):
            with service.activate(binding):
                return await engine._post(client, state={}, json={"messages": [{"role": "user", "content": "hello"}],
                    "model": "wrong", "temperature": 0.2}, headers={"X-Title": "test"})
        await asyncio.gather(call(first), call(second))
    assert {request["model"] for request in requests} == {"anthropic/claude-haiku-5.5", "openai/gpt-6-luna"}
    assert all("temperature" not in request for request in requests)
    assert engine.model == "default/model"
    assert store.get_task(first.task["id"])["spent_usd"] >= 0.00002


@pytest.mark.asyncio
async def test_real_router_contract_includes_current_task_and_fits_large_catalog(tmp_path):
    from tests.test_model_router import catalog, answer_body
    processor, service, store, ai, sends = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "Research Python " + "details " * 500)
    for index in range(16):
        store.put_node(f"knowledge.{index}", "knowledge", {"text": "details " * 80}, "discord", "general", "owner")
    for index in range(12):
        store.task_for_request("discord", "general", "owner", f"old{index}", goal="details " * 200, budget_usd=0.1)
    requests = []
    def respond(request):
        assert str(request.url) == DECISIONS_URL
        requests.append(request)
        assert len(request.content) <= 24 * 1024
        return httpx.Response(200, json=answer_body(request))
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        service.router = ModelRouter("key", client=client, catalog=catalog())
        binding = await service.prepare(processor.world_state.get_channel("general"), source)
    assert len(requests) == 1
    assert binding.route["decision_receipt"]["kind"] == "jev"
    assert store.get_task(binding.task["id"])["spent_usd"] > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 500])
async def test_failed_inference_settles_budget_for_restart(tmp_path, status):
    processor, service, store, ai, sends = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "research")
    binding = await service.prepare(processor.world_state.get_channel("general"), source)
    engine = AIDecisionEngine("key")
    before = store.get_task(binding.task["id"])["spent_usd"]
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(status))) as client:
        with service.activate(binding):
            await engine._post(client, state={}, json={"messages": [{"role": "user", "content": "hello"}]}, headers={})
    task = store.get_task(binding.task["id"])
    assert task["reserved_usd"] == 0
    assert task["spent_usd"] == before if status == 400 else task["spent_usd"] > before
    assert task["attempts"][-1]["status"] == "failed"


@pytest.mark.asyncio
async def test_workers_read_fresh_sources_and_only_expanded_details(tmp_path):
    processor, service, store, ai, sends = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "research")
    hidden = store.put_node("knowledge.collapsed", "knowledge", {"text": "COLLAPSED DETAILS"}, "discord", "general", "owner")
    binding = await service.prepare(processor.world_state.get_channel("general"), source)
    fresh = store.source_result("discord", "general", "owner", "read_webpage", {"url": "https://example.com"},
        {"status": "success", "content": "FRESH SOURCE EVIDENCE"})
    binding.nodes[fresh["node_id"]] = fresh
    binding.fresh_nodes.add(fresh["node_id"])
    binding.input_versions = store.snapshot_versions("discord", "general", "owner", binding.nodes)
    async def select(state, **kwargs):
        return route(expanded=[])
    service.router.select_route = select
    await service._worker(binding, {"goal": "Review facts"}, 0)
    payload = ai.compose_task_worker.call_args.args[0]
    assert "FRESH SOURCE EVIDENCE" in json.dumps(payload)
    assert "data" not in payload["nodes"]["knowledge.collapsed"]


@pytest.mark.asyncio
async def test_proactive_composition_uses_shared_context_and_task_route(tmp_path):
    from chatbot.core.node_system.proactive_sources import ProactiveSourceService
    processor, service, store, ai, sends = setup(tmp_path)
    message(processor, store, "matrix", "!public:test", "@owner:test", "$m1", "Our shared project is Python")
    ai.compose_proactive = AsyncMock(return_value={"publish": False})
    channel = Channel(id="general", type="discord", name="general")
    source = Message("proactive:test", "discord", "ratichat", "Choose a Python story", time.time())
    binding = await service.prepare(channel, source, proactive=True)
    proactive = ProactiveSourceService(SimpleNamespace(fresh_items=lambda channel, items: items, now=time.time), ai, None,
        SimpleNamespace(task_service=service, awareness_store=store), fetch=AsyncMock(return_value=[{"url": "https://example.com", "title": "Python news"}]))
    items, draft = await proactive._compose({"channel_id": "general", "topic": "developer", "topics_json": '["developer"]'}, binding, service)
    payload = ai.compose_proactive.call_args.args[0]
    assert "Python" in json.dumps(payload["community_nodes"])
    assert payload["task_route"]["model"] == binding.route["model"]
    assert payload["community"]["channel_id"] == "general"


@pytest.mark.asyncio
async def test_budget_limit_returns_status_to_original_channel(tmp_path):
    processor, service, store, ai, sends = setup(tmp_path)
    service.budget_usd = 0.0005
    message(processor, store, "discord", "general", "owner", "d1", "Research Python")
    result = await processor.process_cycle("limited", "general")
    assert result["failed"]
    reply = sends["send_discord_reply"].call_args.args[0]
    assert reply["reply_to_id"] == "d1"
    assert "fresh start" in reply["content"]
    ai.make_decision.assert_not_awaited()


@pytest.mark.asyncio
async def test_passive_current_channel_context_keeps_request_authority(tmp_path):
    processor, service, store, ai, sends = setup(tmp_path)
    store.ingest_message("discord", "general", {"id": "passive", "sender": "guest", "content": "Our project uses Python 3.14", "timestamp": time.time() - 1})
    message(processor, store, "discord", "general", "owner", "d1", "What version does our project use?")
    await processor.process_cycle("passive", "general")
    payload = ai.compose_reply.call_args.args[0]
    assert "Python 3.14" in json.dumps(payload["answer_nodes"]["channels.discord.general"])
    assert payload["current_request"]["id"] == "d1"
    assert payload["channels"]["general"]["recent_messages"][-1]["sender_id"] == "owner"
    assert sends["send_discord_reply"].call_args.args[0]["reply_to_id"] == "d1"


@pytest.mark.asyncio
async def test_account_confirmation_requires_the_human_message(tmp_path):
    processor, service, store, ai, sends = setup(tmp_path)
    link = store.begin_link("discord", "general", "owner", "matrix", "@owner:test")
    store.prove_link(link["link_id"], link["code"], "matrix", "!public:test", "@owner:test")
    source = message(processor, store, "discord", "general", "owner", "d1", "What is the weather?")
    binding = await service.prepare(processor.world_state.get_channel("general"), source)
    scope = ExecutionScope("general", "discord", frozenset({"d1"}), "d1", "owner")
    with service.activate(binding):
        blocked = await service.execute_tool("link_chat_account", {"stage": "confirm", "link_id": link["link_id"]}, scope)
    assert blocked["status"] == "blocked"
    source = message(processor, store, "discord", "general", "owner", "d2", "Confirm link " + link["link_id"])
    binding = await service.prepare(processor.world_state.get_channel("general"), source)
    scope = ExecutionScope("general", "discord", frozenset({"d2"}), "d2", "owner")
    with service.activate(binding):
        result = await service.execute_tool("link_chat_account", {"stage": "confirm", "link_id": link["link_id"]}, scope)
    assert result["stage"] == "complete"
