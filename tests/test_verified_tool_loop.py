"""Source reviews, summary freshness, and decisions after each result."""

import copy
import json
from unittest.mock import AsyncMock
from types import SimpleNamespace

import pytest
import httpx

from chatbot.core.node_system.evidence import check_worker_review, worker_sources
from chatbot.core.node_system.node_manager import NodeManager
from chatbot.core.node_system.awareness_store import AwarenessStore
from chatbot.core.ai_engine import AIDecisionEngine
from tests.test_node_processor import make_processor, decision, plan
from tests.test_shared_task_runtime import setup, message


@pytest.mark.asyncio
async def test_new_result_replaces_the_rest_of_a_lookup_plan(tmp_path):
    processor, ai, tools = make_processor(tmp_path, [
        decision(plan("web_search", query="alert"), plan("read_webpage", url="https://old.example.com")),
        decision(plan("read_webpage", url="https://new.example.com")),
        decision(plan("send_discord_reply", channel_id="20", reply_to_id="40", content="Checked reply")),
    ])
    tools["web_search"].execute.return_value = {"status": "success", "url": "https://new.example.com"}
    payloads = []
    answers = ai.make_decision.side_effect

    async def choose(payload, cycle):
        payloads.append(copy.deepcopy(payload))
        return next(answers)

    ai.make_decision.side_effect = choose
    result = await processor.process_cycle("test", "20")
    assert not result["failed"]
    assert tools["read_webpage"].execute.await_count == 1
    assert tools["read_webpage"].execute.call_args.args[0]["url"] == "https://new.example.com"
    assert "https://new.example.com" in json.dumps(payloads[1])


@pytest.mark.asyncio
async def test_worker_review_corrects_missing_address_claim_from_full_request(tmp_path):
    processor, service, store, ai, _ = setup(tmp_path)
    address = "0x" + "a" * 40
    source = message(processor, store, "discord", "general", "owner", "d1",
                     "Review this alert. " + "context " * 700 + address)
    binding = await service.prepare(processor.world_state.get_channel("general"), source)
    ai.compose_task_worker.return_value = "The full address is missing."

    async def review(payload):
        assert payload["draft"]["trust"] == "unreviewed_worker"
        assert payload["sources"]["request:d1"]["text"].endswith(address)
        return {"claims": [{"text": "The alert includes the full address " + address,
                            "evidence": [{"source_id": "request:d1", "quote": address}]}]}

    ai.review_task_worker.side_effect = review
    result = await service._worker(binding, {"goal": "Find the alert address"}, 0)
    assert result["review_status"] == "source_checked"
    assert address in result["content"]
    assert "missing" not in result["content"]
    saved = store.get_task(result["task_id"])["result"]
    assert saved["claims"][0]["evidence"][0]["source_hash"]
    assert saved["checked_at"] > 0
    assert "missing" not in json.dumps(saved)


@pytest.mark.asyncio
@pytest.mark.parametrize("review", [None, {}, {"claims": []}, {"claims": [{"text": "Made up fact", "evidence": [
    {"source_id": "request:d1", "quote": "A fabricated source quote"}]}]}])
async def test_failed_review_keeps_worker_claim_out_of_saved_result(tmp_path, review):
    processor, service, store, ai, _ = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "Review this original alert")
    binding = await service.prepare(processor.world_state.get_channel("general"), source)
    ai.review_task_worker = AsyncMock(return_value=review)
    result = await service._worker(binding, {"goal": "Review facts"}, 0)
    assert result["status"] == "retry"
    assert "content" not in result
    assert "Worker evidence" not in json.dumps(store.get_task(result["task_id"])["result"])


@pytest.mark.asyncio
async def test_source_edit_during_review_rejects_worker_result(tmp_path):
    processor, service, store, ai, _ = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "Review the original alert")
    binding = await service.prepare(processor.world_state.get_channel("general"), source)

    async def review(payload):
        store.edit_message("discord", "general", "d1", {"sender": "owner", "content": "Review the corrected alert", "source_revision": 2})
        return {"claims": [{"text": "Original alert", "evidence": [
            {"source_id": "request:d1", "quote": "original alert"}]}]}

    ai.review_task_worker.side_effect = review
    result = await service._worker(binding, {"goal": "Check alert"}, 0)
    assert result["status"] == "retry"
    assert "content" not in result
    assert store.get_task(result["task_id"])["result"] is None


@pytest.mark.asyncio
async def test_worker_refresh_updates_parent_and_invalidates_earlier_claims(tmp_path):
    processor, service, store, ai, _ = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "Review the original alert")
    params = {"url": "https://example.com"}
    old = store.source_result("discord", "general", "owner", "read_webpage", params,
                              {"status": "success", "content": "Old page evidence"})
    binding = await service.prepare(processor.world_state.get_channel("general"), source)
    live = AsyncMock(return_value={"status": "success", "content": "New page evidence"})
    registry = processor.executor.tool_registry
    registry.register_tool(SimpleNamespace(name="read_webpage", description="Read", parameters_schema={"url": "string"}, execute=live))
    service.configure_read_tools(registry, processor.executor.action_context, processor.policy)
    ai.plan_task_worker = AsyncMock(side_effect=[{"tool": "read_webpage", "parameters": params}, {"tool": None}])
    async def review(payload):
        assert "Old page evidence" not in json.dumps(payload["sources"])
        return {"claims": [{"text": "New page evidence", "evidence": [
            {"source_id": "lookup:0", "quote": "New page evidence"}]}]}
    ai.review_task_worker = AsyncMock(side_effect=review)
    result = await service._worker(binding, {"goal": "Read current page"}, 0)
    assert result["review_status"] == "source_checked"
    assert binding.input_versions[old["node_id"]] > old["version"]
    assert "New page evidence" in json.dumps(binding.nodes[old["node_id"]])
    assert store.inputs_current(binding.task["id"])
    assert service.current_worker_results({"workers": [result]})["workers"][0]["content"] == "New page evidence"
    store.source_result("discord", "general", "owner", "read_webpage", params,
                        {"status": "success", "content": "Later correction"})
    stale = service.current_worker_results({"workers": [result]})["workers"][0]
    assert stale["status"] == "retry" and "content" not in stale
    assert not store.inputs_current(binding.task["id"])


@pytest.mark.asyncio
async def test_source_edit_after_planning_stops_the_action(tmp_path):
    from chatbot.core.ai_engine import ActionPlan, DecisionResult
    processor, service, store, ai, sends = setup(tmp_path)
    message(processor, store, "discord", "general", "owner", "d1", "Review the original alert")

    async def choose(payload, cycle):
        store.edit_message("discord", "general", "d1", {"sender": "owner", "content": "Review the corrected alert", "source_revision": 2})
        return DecisionResult([ActionPlan("send_discord_reply", {"channel_id": "general", "reply_to_id": "d1", "content": "Old answer"}, "test", 1)], "", "", "test")

    ai.make_decision.side_effect = choose
    assert (await processor.process_cycle("edit", "general"))["failed"]
    sends["send_discord_reply"].assert_not_awaited()


@pytest.mark.asyncio
async def test_source_edit_after_composition_stops_delivery(tmp_path):
    processor, service, store, ai, sends = setup(tmp_path)
    message(processor, store, "discord", "general", "owner", "d1", "Review the original alert")
    store.put_node("source", "source", {"content": "Original page"}, "discord", "general", "owner")

    async def compose(payload):
        store.put_node("source", "source", {"content": "Corrected page"}, "discord", "general", "owner")
        return "Answer based on original page"

    ai.compose_reply.side_effect = compose
    assert (await processor.process_cycle("edit", "general"))["failed"]
    sends["send_discord_reply"].assert_not_awaited()


def test_worker_sources_use_original_data_and_keep_source_metadata():
    sources = worker_sources({"source_event_id": "one", "original_request": "Read the original alert",
        "nodes": {"collapsed": {"summary": "DRAFT FACT", "kind": "source"},
                  "worker": {"data": {"content": "DRAFT FACT"}, "kind": "task"}},
        "tool_results": [{"tool": "read_webpage", "fetched_at": 100,
            "result": {"status": "success", "content": "The original page text", "url": "https://example.com"}},
            {"tool": "read_webpage", "result": {"status": "error", "content": "DRAFT FACT"}}]}, 200)
    assert "DRAFT FACT" not in json.dumps(sources)
    review = {"claims": [{"text": "The page has text", "evidence": [
        {"source_id": "lookup:0", "quote": "The original page text"}]}]}
    result = check_worker_review(review, sources, 200)
    evidence = result["claims"][0]["evidence"][0]
    assert evidence["links"] == ["https://example.com"]
    assert evidence["fetched_at"] == 100 and evidence["checked_at"] == 200
    review["claims"][0]["evidence"][0]["source_id"] = "invented"
    assert check_worker_review(review, sources, 200) is None


def test_summary_refreshes_after_source_changes_while_expanded(tmp_path):
    processor, _, _ = make_processor(tmp_path, [])
    channel = processor.world_state.get_channel("20")
    manager = NodeManager()
    node = {"kind": "source", "version": 1, "summary_version": 1, "summary": "Old summary",
            "summary_checked_at": 100, "data": {"content": "Old source", "url": "https://example.com/old"}}
    first = processor.payload_builder.build_request_node_payload(channel, manager, {"source": node})
    old = first["collapsed_node_summaries"]["source"]
    manager.expand_node("source")
    node.update({"version": 2, "data": {"content": "Corrected source", "url": "https://example.com/new"}})
    processor.payload_builder.build_request_node_payload(channel, manager, {"source": node})
    manager.collapse_node("source")
    current = processor.payload_builder.build_request_node_payload(channel, manager, {"source": node})["collapsed_node_summaries"]["source"]
    assert "Corrected source" in current["summary"]
    assert "Old summary" not in current["summary"]
    assert current["source_version"] == 2
    assert current["source_hash"] != old["source_hash"]
    assert current["checked_at"] >= old["checked_at"]
    assert current["source_links"] == ["https://example.com/new"]


def test_saved_summary_keeps_its_check_time_across_restart(tmp_path):
    path = tmp_path / "summaries.db"
    config = {"allowed_channels": {"discord": ["general"]}}
    store = AwarenessStore(path, clock=lambda: 100, **config)
    node = store.put_node("source", "source", {"content": "Original page"}, "discord", "general", "owner")
    store.clock = lambda: 200
    assert store.publish_summary("source", node["version"], "Original summary")["updated"]
    store.close()
    store = AwarenessStore(path, clock=lambda: 300, **config)
    saved = store.catalog("discord", "general", "owner")["source"]
    assert saved["summary_checked_at"] == 200
    assert saved["summary_version"] == saved["version"]
    store.put_node("source", "source", {"content": "Edited page"}, "discord", "general", "owner")
    refreshed = store.catalog("discord", "general", "owner")["source"]
    assert refreshed["summary_checked_at"] == 300
    assert "Edited page" in refreshed["summary"]
    assert store.publish_summary("source", node["version"], "Old summary")["conflict"]
    store.close()


@pytest.mark.asyncio
async def test_legacy_worker_requires_review_before_reuse(tmp_path):
    processor, service, store, ai, _ = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "Review the original alert")
    binding = await service.prepare(processor.world_state.get_channel("general"), source)
    result = await service._worker(binding, {"goal": "Review facts"}, 0)
    child_id = result["task_id"]
    store.record_result(child_id, {"kind": "worker", "content": "LEGACY DRAFT CLAIM"}, input_versions=binding.input_versions)
    assert "LEGACY DRAFT CLAIM" not in json.dumps(store.get_node("tasks." + child_id, "discord", "general", "owner"))
    assert "LEGACY DRAFT CLAIM" not in json.dumps(service.task_context(store.get_task(child_id)))
    ai.review_task_worker.reset_mock()
    result = await service._worker(binding, {"goal": "Review facts"}, 0)
    ai.review_task_worker.assert_awaited_once()
    assert result["review_status"] == "source_checked"


@pytest.mark.asyncio
async def test_review_call_uses_shared_budget_and_saves_usage(tmp_path, monkeypatch):
    processor, service, store, ai, _ = setup(tmp_path)
    source = message(processor, store, "discord", "general", "owner", "d1", "Review the original alert")
    binding = await service.prepare(processor.world_state.get_channel("general"), source)
    review = {"claims": [{"text": "The request contains an alert", "evidence": [
        {"source_id": "request:d1", "quote": "original alert"}]}]}
    client_type = httpx.AsyncClient
    requests = []

    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"model": requests[-1]["model"], "choices": [{"message": {"content": json.dumps(review)}}],
            "usage": {"cost": 0.004}})

    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: client_type(transport=httpx.MockTransport(respond), **kwargs))
    ai.review_task_worker = AIDecisionEngine("test").review_task_worker
    result = await service._worker(binding, {"goal": "Check alert"}, 0)
    assert result["review_status"] == "source_checked"
    assert len(requests) == 1
    assert store.get_task(binding.task["id"])["spent_usd"] >= 0.004
    assert store.get_task(binding.task["id"])["reserved_usd"] == 0
    root = store.get_task(binding.task["id"])
    hold = store.reserve_attempt(root["id"], root["remaining_usd"] - 0.00001, "other-work")
    with service.activate(binding):
        assert await ai.review_task_worker({"sources": {}}) is None
    assert len(requests) == 1
    store.record_result(root["id"], {}, attempt_id=hold["id"], cost_usd=0, success=False, status="active")
