"""Regressions from real Luna replies containing several nested JSON objects."""

import json
from unittest.mock import AsyncMock

import httpx
import pytest

from chatbot.core.ai_engine import AIDecisionEngine


def response(text):
    return httpx.Response(200, request=httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions"),
        json={"choices": [{"message": {"content": text}}]})


@pytest.mark.asyncio
async def test_worker_reads_full_nested_plan_when_model_repeats_json_with_extra_text():
    engine = AIDecisionEngine("test")
    plan = {"tool": "check_onchain_activity", "parameters": {"targets": [
        {"address": "0x" + "a" * 40, "network": "ethereum"}], "fresh": True}}
    text = json.dumps(plan) + " - JSON only.\n\n" + json.dumps(plan)
    engine._post = AsyncMock(return_value=response(text))
    assert await engine.plan_task_worker({"goal": "Read live activity"}) == plan


@pytest.mark.asyncio
async def test_worker_uses_answer_object_after_a_large_parameter_echo():
    engine = AIDecisionEngine("test")
    parameters = {"targets": [{"address": "0x" + str(i) * 40, "network": "ethereum"} for i in range(8)]}
    text = json.dumps(parameters) + '\n\n{"content":"Live evidence with a source link."}'
    engine._post = AsyncMock(return_value=response(text))
    assert await engine.compose_task_worker({"goal": "Report the evidence"}) == "Live evidence with a source link."


@pytest.mark.asyncio
async def test_worker_accepts_complete_json_in_fences_and_handles_unfinished_json():
    engine = AIDecisionEngine("test")
    engine._post = AsyncMock(side_effect=[response('```json\n{"tool":null}\n```'), response('{"tool":')])
    assert await engine.plan_task_worker({}) == {"tool": None}
    assert await engine.plan_task_worker({}) is None
