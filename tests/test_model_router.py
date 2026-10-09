"""Exact task routing, typed Jev answers, and model-aware request settings."""

import asyncio
import json

import httpx
import pytest

from chatbot.core.model_router import (
    CATALOG_URL, CHAT_URL, DECISIONS_URL, DEFAULT_MODELS, FAST_MODELS, JEV_MODEL,
    JevDecisionClient, ModelRouter, RoutingError, build_chat_payload,
    record_inference, validate_answers,
)


def model(model_id=FAST_MODELS[0], *, parameters=None, prompt="0.0000001", completion="0.0000005"):
    return {"id": model_id, "name": model_id, "context_length": 1000000,
            "architecture": {"input_modalities": ["text", "image"], "output_modalities": ["text"]},
            "top_provider": {"max_completion_tokens": 128000},
            "supported_parameters": parameters if parameters is not None else ["max_tokens", "reasoning", "response_format", "structured_outputs", "tools", "tool_choice"],
            "pricing": {"prompt": prompt, "completion": completion}}


def catalog():
    return [model(), model(FAST_MODELS[1]), model(DEFAULT_MODELS[2], prompt="0.000002", completion="0.00001")]


def answer_body(request, *, route=None, task="new"):
    body = json.loads(request.content)
    answers = {}
    for name, question in body["questions"].items():
        if question["type"] == "noul":
            answers[name] = {"type": "noul", "noul": 0.9 if name == "node0" else 0.1}
        else:
            options = list(question["criteria"])
            selected = route if name == "route" and route else task if name == "task" else options[0]
            answers[name] = {"type": "choice", "choice": selected, "confidence": 1.0,
                             "probabilities": {option: float(option == selected) for option in options}}
    return {"id": "safe-decision-id", "model": JEV_MODEL + "-20260917", "provider": "TypeSafe",
            "answers": answers, "usage": {"input_tokens": 600, "output_tokens": 50, "cost": 0.0000252}}


@pytest.mark.asyncio
async def test_catalog_cache_is_versioned_saved_and_restored(tmp_path):
    requests, clock = [], [1000]
    def handler(request):
        requests.append(request)
        assert str(request.url) == CATALOG_URL
        assert "authorization" not in request.headers
        return httpx.Response(200, json={"data": catalog()})
    path = tmp_path / "models.json"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        router = ModelRouter("private-test-key", client=client, catalog_path=path, now=lambda: clock[0])
        first = await router.refresh()
        version = router.catalog_version
        await router.refresh()
        assert len(requests) == 1
        restored = ModelRouter("key", client=client, catalog_path=path, now=lambda: clock[0])
        assert await restored.refresh() == first
        assert restored.catalog_version == version
        assert len(requests) == 1
        assert restored.catalog_nodes()[0]["id"].startswith("models.")
        assert "private-test-key" not in path.read_text()


@pytest.mark.asyncio
async def test_catalog_refresh_failure_uses_saved_snapshot_and_backs_off(tmp_path):
    clock, calls = [1000], []
    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"data": catalog()}) if len(calls) == 1 else httpx.Response(503)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        router = ModelRouter("key", client=client, catalog_path=tmp_path / "catalog", now=lambda: clock[0], catalog_ttl=10)
        first = await router.refresh()
        clock[0] += 11
        assert await router.refresh() == first
        assert await router.refresh() == first
        assert len(calls) == 2


@pytest.mark.asyncio
async def test_corrupt_cache_requires_valid_public_catalog(tmp_path):
    path = tmp_path / "catalog"
    path.write_text('{"schema_version": 1, "models": [], "raw_models": [], "catalog_version":"fake"}')
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(503))) as client:
        router = ModelRouter("key", client=client, catalog_path=path)
        with pytest.raises(RoutingError, match="valid snapshot"):
            await router.refresh()


@pytest.mark.asyncio
async def test_catalog_rejects_unpriced_and_alias_entries():
    broken = model("~openai/gpt-luna-latest")
    free_unknown = model("example/unknown")
    free_unknown.pop("pricing")
    router = ModelRouter("key", catalog=[model(), broken, free_unknown])
    assert [entry["id"] for entry in router.models] == [FAST_MODELS[0]]


@pytest.mark.asyncio
async def test_selection_saves_exact_route_attention_and_allowed_task_continuity():
    requests = []
    def handler(request):
        requests.append(request)
        assert str(request.url) == DECISIONS_URL
        assert request.headers["authorization"] == "Bearer linked-key"
        return httpx.Response(200, json=answer_body(request, route="route1", task="task0"))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        router = ModelRouter(lambda: "linked-key", client=client, catalog=catalog())
        route = await router.select_route({"request": "Continue the shared Python research"},
                    nodes=[{"id": "topics.python", "summary": "Our Python research"}, {"id": "topics.crypto", "summary": "Token news"}],
                    tasks=[{"id": "task.saved", "summary": "Python research"}], current_task_id="task.saved")
    assert route["model"] == FAST_MODELS[1]
    assert route["expanded_nodes"] == ["topics.python"]
    assert route["resume_task_id"] == "task.saved"
    assert route["persona"] == "rati"
    assert route["endpoint"] == CHAT_URL
    assert route["decision_receipt"]["usage"]["cost"] == 0.0000252
    assert route["catalog_version"].startswith("openrouter-")
    assert json.loads(requests[0].content)["session_id"] == "task.saved"
    assert "linked-key" not in json.dumps(route)


@pytest.mark.asyncio
async def test_persona_and_model_are_one_valid_route_choice():
    def handler(request):
        body = json.loads(request.content)
        choices = body["questions"]["route"]["criteria"]
        selected = next(key for key, value in choices.items() if value["persona"] == "reviewer")
        return httpx.Response(200, json=answer_body(request, route=selected))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        router = ModelRouter("key", client=client, catalog=catalog())
        route = await router.select_route("Review conflicting research", personas=[
            {"id": "reporter", "model": FAST_MODELS[0]}, {"id": "reviewer", "model": DEFAULT_MODELS[2]}])
    assert route["persona"] == "reviewer"
    assert route["model"] == DEFAULT_MODELS[2]
    assert route["reasoning"] == {"effort": "low"}


@pytest.mark.asyncio
async def test_dynamic_explicit_model_choices_are_filtered_by_capability_and_budget():
    custom = model("example/fast-worker")
    text_only = model("example/plain", parameters=["max_tokens"])
    expensive = model("example/expensive", prompt="1", completion="1")
    def handler(request):
        choices = json.loads(request.content)["questions"]["route"]["criteria"]
        assert [choice["model"] for choice in choices.values()] == ["example/fast-worker"]
        return httpx.Response(200, json=answer_body(request))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        router = ModelRouter("key", client=client, catalog=[custom, text_only, expensive])
        route = await router.select_route("Extract a summary", allowed_models=[custom["id"], text_only["id"], expensive["id"]])
    assert route["model"] == "example/fast-worker"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["timeout", "invalid_choice", "provider_error", "oversize"])
async def test_failed_decision_uses_eligible_fast_fallback(failure):
    def handler(request):
        if failure == "timeout":
            raise httpx.ReadTimeout("private provider text", request=request)
        if failure == "provider_error":
            return httpx.Response(503, text="private provider text")
        body = answer_body(request)
        if failure == "invalid_choice":
            body["answers"]["route"]["choice"] = "unlisted-expensive-model"
        if failure == "oversize":
            body["extra"] = "x" * 140000
        return httpx.Response(200, json=body)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        router = ModelRouter("key", client=client, catalog=catalog())
        route = await router.select_route("Quick reply", tasks=[{"id": "same-task"}], current_task_id="same-task")
    assert route["model"] in FAST_MODELS
    assert route["resume_task_id"] == "same-task"
    assert route["decision_receipt"]["kind"] == "fallback"
    assert "private provider text" not in json.dumps(route)


@pytest.mark.asyncio
async def test_decision_deadline_bounds_slow_client():
    async def handler(request):
        await asyncio.sleep(1)
        return httpx.Response(200, json=answer_body(request))
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        router = ModelRouter("key", client=client, catalog=catalog(), decision_timeout=0.01)
        route = await asyncio.wait_for(router.select_route("Quick reply"), 0.2)
    assert route["decision_receipt"]["kind"] == "fallback"


@pytest.mark.asyncio
async def test_decision_request_bound_is_checked_before_network():
    calls = []
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: calls.append(request))) as client:
        decisions = JevDecisionClient("key", client=client)
        with pytest.raises(RoutingError, match="24 KiB"):
            await decisions.decide("x" * 25000, {"route": {"type": "choice", "instructions": "Choose", "criteria": {"fast": "Fast"}}})
    assert calls == []


@pytest.mark.parametrize("change", ["choice", "sum", "nan", "bool", "missing", "type", "confidence", "extra", "winner"])
def test_typed_answer_validation_rejects_invalid_provider_shapes(change):
    questions = {"route": {"type": "choice", "criteria": {"a": "A", "b": "B"}, "instructions": "Choose"}}
    answer = {"type": "choice", "choice": "a", "probabilities": {"a": 0.7, "b": 0.3}, "confidence": 0.4}
    response = {"answers": {"route": answer}}
    if change == "choice": answer["choice"] = "elsewhere"
    if change == "sum": answer["probabilities"]["b"] = 0.5
    if change == "nan": answer["probabilities"]["b"] = float("nan")
    if change == "bool": answer["probabilities"]["b"] = True
    if change == "missing": answer["probabilities"].pop("b")
    if change == "type": answer["type"] = "noul"
    if change == "confidence": answer["confidence"] = 2
    if change == "extra": response["answers"]["unexpected"] = answer
    if change == "winner": answer["choice"] = "b"
    with pytest.raises(RoutingError):
        validate_answers(questions, response)


def test_noul_and_score_answers_are_checked_as_different_types():
    questions = {"relevant": {"type": "noul"}, "difficulty": {"type": "score", "criteria": ["easy", "hard"]}}
    response = {"answers": {"relevant": {"type": "noul", "noul": 0.1},
        "difficulty": {"type": "score", "score": 0.7, "probabilities": {"0": 0.3, "1": 0.7}, "confidence": 0.4}}}
    assert validate_answers(questions, response)["relevant"]["noul"] == 0.1
    response["answers"]["difficulty"]["score"] = 1
    with pytest.raises(RoutingError):
        validate_answers(questions, response)


@pytest.mark.asyncio
async def test_saved_model_builder_removes_unsupported_settings_and_pins_route():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(503))) as client:
        router = ModelRouter("key", client=client, catalog=[model(FAST_MODELS[1])])
        route = await router.select_route("Summarise a source")
    payload = {"model": "unlisted/model", "messages": [{"role": "user", "content": "Summarise"}],
               "temperature": 0.2, "top_p": 0.9, "max_tokens": 999999,
               "tools": [{"type": "function"}], "plugins": [{"id": "jev-router"}],
               "response_format": {"type": "json_object"}, "reasoning": {"effort": "max"}}
    result = build_chat_payload(route, payload)
    assert result["model"] == FAST_MODELS[1]
    assert result["reasoning"] == {"effort": "none"}
    assert result["max_tokens"] == 1400
    assert "temperature" not in result and "top_p" not in result
    assert "tools" not in result and "plugins" not in result
    assert payload["model"] == "unlisted/model"
    assert result["provider"]["require_parameters"] is True


@pytest.mark.asyncio
async def test_actual_worker_payload_rechecks_context_and_price_overrides():
    small = model()
    small["pricing"]["overrides"] = [{"min_prompt_tokens": 1000, "prompt": "0.1", "completion": "0.1"}]
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(503))) as client:
        route = await ModelRouter("key", client=client, catalog=[small]).select_route("Short input")
    with pytest.raises(RoutingError, match="cost budget"):
        build_chat_payload(route, {"messages": [{"role": "user", "content": "x" * 2000}]})
    with pytest.raises(RoutingError, match="context budget"):
        build_chat_payload(route, {"messages": [{"role": "user", "content": "x" * 1000000}]})


@pytest.mark.asyncio
async def test_unknown_current_task_and_native_tool_transport_require_valid_contracts():
    router = ModelRouter("key", catalog=catalog())
    with pytest.raises(RoutingError, match="allowed task"):
        await router.select_route("Continue", current_task_id="unlisted", tasks=[{"id": "listed"}])
    with pytest.raises(RoutingError, match="dedicated"):
        await router.select_route("Use native tools", required_capabilities=("native_tools",))


def test_inference_receipt_accepts_pinned_revision_and_keeps_safe_usage():
    route = {"model": FAST_MODELS[0], "catalog_version": "v1"}
    response = {"model": FAST_MODELS[0] + "-20261007", "provider": "Anthropic", "id": "generation1",
                "usage": {"cost": 0.0001, "prompt_tokens": 10, "completion_tokens": 5, "secret": "hidden"},
                "choices": [{"message": {"content": "private conversation"}}]}
    receipt = record_inference(route, response, latency_ms=100)
    assert receipt["actual_model"].endswith("20261007")
    assert receipt["usage"] == {"cost": 0.0001, "prompt_tokens": 10, "completion_tokens": 5}
    assert "private conversation" not in json.dumps(receipt)
    assert "hidden" not in json.dumps(receipt)
    with pytest.raises(RoutingError, match="saved route"):
        record_inference(route, {"model": DEFAULT_MODELS[2]})
