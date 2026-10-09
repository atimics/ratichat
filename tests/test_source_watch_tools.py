"""The agent gets watch tools with a fixed sender and channel."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from chatbot.core.orchestration.capability_policy import CapabilityPolicy, ExecutionScope, SOURCE_WATCH_TOOLS, WATCH_READ_TOOLS
from chatbot.core.orchestration.main_orchestrator import MainOrchestrator, OrchestratorConfig, TraditionalProcessor
from chatbot.core.ai_engine import ActionPlan
from chatbot.core.node_system.source_watches import WatchStore, SourceWatchService
from chatbot.tools.source_watch_tools import CreateSourceWatchTool, ListSourceWatchesTool, RemoveSourceWatchTool, GetSourceDigestTool
from chatbot.tools.registry import ToolRegistry


def policy(profile="public"):
    return CapabilityPolicy(profile, approved_discord_channel_ids=["20", "21"], discord_owner_user_ids=["owner"],
        approved_matrix_room_ids=["!room:server"], operator_user_ids=["@owner:server"])


def scope(channel="20", sender="owner", platform="discord", event="40"):
    return ExecutionScope(channel, platform, frozenset({event}), event, sender)


def service(tmp_path):
    store = WatchStore(tmp_path / "tools.db", owner_ids={"discord": {"owner"}, "matrix": {"@owner:server"}},
        allowed_channels={"discord": {"20", "21"}, "matrix": {"!room:server"}})
    return SourceWatchService(store)


@pytest.mark.parametrize("profile", ["public", "matrix_steward", "operator"])
def test_owner_tools_and_guest_reads_follow_current_scope(profile):
    rules = policy(profile)
    assert rules.filter_tool_names(SOURCE_WATCH_TOOLS, scope()) == SOURCE_WATCH_TOOLS
    assert rules.filter_tool_names(SOURCE_WATCH_TOOLS, scope(sender="guest")) == WATCH_READ_TOOLS
    assert rules.filter_tool_names(SOURCE_WATCH_TOOLS, scope(channel="private")) == set()
    assert rules.filter_tool_names(SOURCE_WATCH_TOOLS, scope(platform="farcaster")) == set()
    for name in SOURCE_WATCH_TOOLS:
        assert rules.denial_reason(name, {}, scope()) is None
        assert rules.denial_reason(name, {}, None)
        assert rules.denial_reason(name, {"channel_id": "21"}, scope())
    assert rules.denial_reason("create_source_watch", {}, scope(sender="guest"))
    assert rules.denial_reason("remove_source_watch", {}, scope(sender="guest"))
    assert rules.filter_tool_names(SOURCE_WATCH_TOOLS, scope("!room:server", "@owner:server", "matrix")) == SOURCE_WATCH_TOOLS


def test_main_orchestrator_registers_the_agent_watch_tools(tmp_path):
    orchestrator = MainOrchestrator(OrchestratorConfig(db_path=str(tmp_path / "main.db")))
    assert SOURCE_WATCH_TOOLS <= set(orchestrator.tool_registry.get_tool_names())
    orchestrator._setup_processing_components()
    assert orchestrator.action_context.source_watch_service is orchestrator.source_watch_service


@pytest.mark.asyncio
async def test_direct_tool_uses_trusted_sender_and_rejects_override_params(tmp_path):
    backend = service(tmp_path)
    context = SimpleNamespace(source_watch_service=backend, execution_scope=scope(sender="guest"))
    result = await CreateSourceWatchTool().execute({"url": "https://example.com/feed"}, context)
    assert result["status"] in {"blocked", "failure"}
    context.execution_scope = scope()
    for override in [{"is_owner": True}, {"sender_id": "owner"}, {"channel_id": "21"}, {"source_event_id": "99"}]:
        result = await CreateSourceWatchTool().execute({"url": "https://example.com/feed", **override}, context)
        assert result["status"] == "blocked"
    assert backend.store.list({"channel_type": "discord", "channel_id": "20"}) == []


@pytest.mark.asyncio
async def test_tools_require_current_request_context(tmp_path):
    result = await ListSourceWatchesTool().execute({}, SimpleNamespace(source_watch_service=service(tmp_path)))
    assert result["status"] == "blocked"


def executor(backend):
    registry = ToolRegistry()
    for tool in (CreateSourceWatchTool(), ListSourceWatchesTool(), RemoveSourceWatchTool(), GetSourceDigestTool()):
        registry.register_tool(tool)
    return TraditionalProcessor(Mock(), registry, Mock(), SimpleNamespace(add_tool_result=AsyncMock()),
        SimpleNamespace(source_watch_service=backend), policy())


@pytest.mark.asyncio
async def test_concurrent_tool_contexts_keep_their_own_channel(tmp_path):
    processor = executor(service(tmp_path))
    tool = processor.tool_registry.get_tool("list_source_watches")
    original = tool.execute
    seen = []
    async def capture(params, context):
        await asyncio.sleep(0)
        seen.append(context.execution_scope.channel_id)
        return await original(params, context)
    tool.execute = capture
    action = ActionPlan("list_source_watches", {}, "Read watches", 1)
    results = await asyncio.gather(processor._execute_action_and_return_result(action, scope("20")),
                                   processor._execute_action_and_return_result(action, scope("21")))
    assert seen == ["20", "21"]
    assert all(result["status"] == "success" for result in results)
    assert not hasattr(processor.action_context, "execution_scope")


@pytest.mark.asyncio
async def test_saved_watch_receipt_survives_tool_log_failure(tmp_path):
    processor = executor(service(tmp_path))
    processor.context_manager.add_tool_result.side_effect = RuntimeError("log error")
    result = await processor._execute_action_and_return_result(
        ActionPlan("create_source_watch", {"url": "https://example.com/feed"}, "Track feed", 1), scope())
    assert result["status"] == "success"
    assert len(processor.action_context.source_watch_service.store.list({"channel_type": "discord", "channel_id": "20"})) == 1
