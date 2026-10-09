"""Saved model choices for shared tasks, using OpenRouter and Jev."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Callable

import httpx


CATALOG_URL = "https://openrouter.ai/api/v1/models"
DECISIONS_URL = "https://openrouter.ai/api/alpha/decisions"
CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"
JEV_MODEL = "typesafe/jev-1.13"
FAST_MODELS = ("anthropic/claude-haiku-5.5", "openai/gpt-6-luna")
DEFAULT_MODELS = (*FAST_MODELS, "openai/gpt-6.1-sol")
DEFAULT_TOPICS = ("general", "tech", "ai", "developer", "crypto", "social", "world")
MAX_DECISION_BYTES = 24 * 1024
MAX_RESPONSE_BYTES = 128 * 1024
MAX_CATALOG_BYTES = 8 * 1024 * 1024
MODEL_ID = re.compile(r"^[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+$")


class RoutingError(ValueError):
    """A request or provider answer could not produce a valid saved route."""


def _number(value: Any, *, maximum: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RoutingError("Expected a finite number")
    value = float(value)
    if not math.isfinite(value) or value < 0 or (maximum is not None and value > maximum):
        raise RoutingError("Number is outside the allowed range")
    return value


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                          separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as error:
        raise RoutingError("Expected JSON data") from error


def _safe_usage(value: Any) -> dict:
    if not isinstance(value, dict):
        return {}
    result = {}
    for name in ("input_tokens", "output_tokens", "prompt_tokens", "completion_tokens", "total_tokens", "cost"):
        if name in value:
            try:
                amount = _number(value[name])
                if name == "cost" or amount.is_integer():
                    result[name] = amount if name == "cost" else int(amount)
            except RoutingError:
                pass
    return result


async def _key(source: str | Callable) -> str:
    value = source() if callable(source) else source
    if inspect.isawaitable(value):
        value = await value
    if not isinstance(value, str) or not value.strip():
        raise RoutingError("OpenRouter account is ready when its key is available")
    return value.strip()


async def _request(client: httpx.AsyncClient | None, method: str, url: str, *, timeout: float, **kwargs):
    async def run(current):
        response = await current.request(method, url, timeout=timeout, **kwargs)
        response.raise_for_status()
        bound = MAX_CATALOG_BYTES if url == CATALOG_URL else MAX_RESPONSE_BYTES
        if len(response.content) > bound:
            raise RoutingError("Provider reply exceeds the size limit")
        return response.json()
    if client is not None:
        return await asyncio.wait_for(run(client), timeout)
    async with httpx.AsyncClient(follow_redirects=False) as current:
        return await asyncio.wait_for(run(current), timeout)


def validate_answers(questions: dict, response: dict) -> dict:
    """Check every typed answer against the exact choices in this request."""
    if not isinstance(response, dict) or not isinstance(response.get("answers"), dict):
        raise RoutingError("Expected typed decision answers")
    answers = response["answers"]
    if set(answers) != set(questions):
        raise RoutingError("Decision answer IDs must match the request")
    for name, question in questions.items():
        answer = answers[name]
        kind = question["type"]
        if not isinstance(answer, dict) or answer.get("type") != kind:
            raise RoutingError("Decision answer type must match the request")
        if kind == "noul":
            _number(answer.get("noul"), maximum=1)
            continue
        criteria = question["criteria"]
        expected = set(criteria) if kind == "choice" else {str(i) for i in range(len(criteria))}
        probabilities = answer.get("probabilities")
        if not isinstance(probabilities, dict) or set(probabilities) != expected:
            raise RoutingError("Decision probabilities must cover the allowed choices")
        values = {option: _number(probability, maximum=1) for option, probability in probabilities.items()}
        if abs(sum(values.values()) - 1) > 0.0001:
            raise RoutingError("Decision probabilities must sum to one")
        _number(answer.get("confidence"), maximum=1)
        if kind == "choice":
            selected = answer.get("choice")
            if selected not in expected or values[selected] + 0.0001 < max(values.values()):
                raise RoutingError("Decision choice must be a most likely allowed option")
        else:
            score = _number(answer.get("score"), maximum=len(criteria) - 1)
            expected_score = sum(int(option) * probability for option, probability in values.items())
            if abs(score - expected_score) > 0.0001:
                raise RoutingError("Decision score must match its probabilities")
    return answers


class JevDecisionClient:
    def __init__(self, api_key: str | Callable, *, client: httpx.AsyncClient | None = None, timeout: float = 4.0):
        self.api_key, self.client, self.timeout = api_key, client, timeout

    async def decide(self, state: Any, questions: dict, *, session_id: str | None = None) -> dict:
        if not isinstance(questions, dict) or not 1 <= len(questions) <= 20:
            raise RoutingError("Ask between one and twenty typed questions")
        for name, question in questions.items():
            if not isinstance(name, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", name):
                raise RoutingError("Use a short question ID")
            if not isinstance(question, dict) or question.get("type") not in ("noul", "choice", "score"):
                raise RoutingError("Use a supported question type")
            if not isinstance(question.get("instructions"), str) or not question["instructions"].strip():
                raise RoutingError("Give each question clear instructions")
            criteria = question.get("criteria")
            if question["type"] == "choice" and (
                not isinstance(criteria, dict) or not 1 <= len(criteria) <= 255
                or not all(isinstance(option, str) and option for option in criteria)
            ):
                raise RoutingError("Give a choice question its allowed options")
            if question["type"] == "score" and (not isinstance(criteria, list) or not 2 <= len(criteria) <= 255):
                raise RoutingError("Give a score question ordered levels")
        body = {"model": JEV_MODEL, "state": state, "questions": questions}
        if session_id is not None:
            if not isinstance(session_id, str) or len(session_id) > 256:
                raise RoutingError("Use a short session ID")
            body["session_id"] = session_id
        if len(_json_bytes(body)) > MAX_DECISION_BYTES:
            raise RoutingError("Decision request exceeds 24 KiB")
        started = time.monotonic()
        response = await _request(self.client, "POST", DECISIONS_URL, timeout=self.timeout,
                                  headers={"Authorization": "Bearer " + await _key(self.api_key)}, json=body)
        answers = validate_answers(questions, response)
        model = response.get("model")
        if not isinstance(model, str) or not re.fullmatch(r"typesafe/jev-1\.13(?:-[0-9]{8})?", model):
            raise RoutingError("Decision serving model must match the pinned Jev version")
        return {"kind": "jev", "model": model, "provider": str(response.get("provider", ""))[:100],
                "request_id": str(response.get("id", ""))[:200], "answers": answers,
                "usage": _safe_usage(response.get("usage")),
                "latency_ms": round((time.monotonic() - started) * 1000), "schema_version": 1}


def _normalise_catalog(raw: Any) -> list[dict]:
    if not isinstance(raw, list):
        raise RoutingError("Expected a model catalog")
    models = {}
    for item in raw:
        if not isinstance(item, dict) or not MODEL_ID.fullmatch(str(item.get("id", ""))):
            continue
        architecture, prices = item.get("architecture"), item.get("pricing")
        parameters = item.get("supported_parameters")
        if not isinstance(architecture, dict) or not isinstance(prices, dict) or not isinstance(parameters, list):
            continue
        if not all(isinstance(value, str) for value in parameters):
            continue
        try:
            prompt, completion = _number(float(prices["prompt"])), _number(float(prices["completion"]))
            context = int(item["context_length"])
            if context <= 0 or isinstance(item["context_length"], bool):
                continue
            output = (item.get("top_provider") or {}).get("max_completion_tokens")
            output = int(output) if output is not None else context
            if output <= 0:
                continue
            overrides = []
            for override in prices.get("overrides", []):
                if isinstance(override, dict):
                    overrides.append({"min_prompt_tokens": int(override["min_prompt_tokens"]),
                                      "prompt": _number(float(override["prompt"])),
                                      "completion": _number(float(override["completion"]))})
        except (KeyError, ValueError, TypeError, OverflowError, RoutingError):
            continue
        inputs, outputs = architecture.get("input_modalities"), architecture.get("output_modalities")
        if not isinstance(inputs, list) or not isinstance(outputs, list) or not all(
            isinstance(value, str) for value in [*inputs, *outputs]
        ):
            continue
        models[item["id"]] = {"id": item["id"], "name": str(item.get("name", item["id"]))[:200],
                              "input_modalities": inputs,
                              "output_modalities": outputs,
                              "supported_parameters": sorted(set(parameters)), "context_length": context,
                              "max_completion_tokens": output,
                              "pricing": {"prompt": prompt, "completion": completion, "overrides": overrides}}
    if not models:
        raise RoutingError("The catalog needs valid model entries")
    return [models[key] for key in sorted(models)]


def _catalog_version(models: list[dict]) -> str:
    return "openrouter-" + hashlib.sha256(_json_bytes(models)).hexdigest()[:20]


def _compact(value: Any, depth: int = 0) -> Any:
    """Keep decision input small while the full source stays in the awareness store."""
    if depth >= 5:
        return "[expanded in the awareness store]"
    if isinstance(value, str):
        return value[:2000]
    if isinstance(value, dict):
        return {str(key)[:100]: _compact(item, depth + 1) for key, item in list(value.items())[:30]}
    if isinstance(value, (list, tuple)):
        return [_compact(item, depth + 1) for item in value[:20]]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)[:300]


def _records(value: Any, maximum: int, default: dict | None = None) -> list[dict]:
    result, seen = [], set()
    for item in value or []:
        if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not 1 <= len(item["id"]) <= 200:
            raise RoutingError("Use records with short stable IDs")
        if item["id"] in seen:
            raise RoutingError("Candidate IDs must be unique")
        seen.add(item["id"])
        result.append(item)
        if len(result) >= maximum:
            break
    return result or ([default] if default is not None else [])


class ModelRouter:
    """Catalog discovery, typed attention decisions, and exact worker profiles."""

    def __init__(self, api_key: str | Callable, *, catalog_path: str | Path | None = None,
                 client: httpx.AsyncClient | None = None, catalog: list[dict] | None = None,
                 now: Callable[[], float] = time.time, catalog_ttl: float = 86400,
                 decision_timeout: float = 4.0):
        self.api_key, self.client, self.now = api_key, client, now
        self.catalog_path = Path(catalog_path) if catalog_path else None
        self.catalog_ttl, self.fetched_at, self.retry_at = catalog_ttl, 0.0, 0.0
        self.models: list[dict] = []
        self.catalog_version = ""
        self._lock = asyncio.Lock()
        self.decisions = JevDecisionClient(api_key, client=client, timeout=decision_timeout)
        if catalog is not None:
            self.models = _normalise_catalog(catalog)
            self.fetched_at = self.now()
            self.catalog_version = _catalog_version(self.models)
        elif self.catalog_path:
            self._load()

    def _load(self):
        try:
            if self.catalog_path.stat().st_size > MAX_CATALOG_BYTES:
                return
            snapshot = json.loads(self.catalog_path.read_text())
            models = snapshot["models"]
            if not isinstance(models, list) or snapshot.get("schema_version") != 1:
                return
            # Saved records use the same public catalog structure as fresh records.
            restored = _normalise_catalog(snapshot["raw_models"])
            if _catalog_version(restored) != snapshot["catalog_version"]:
                return
            self.models = restored
            self.catalog_version = snapshot["catalog_version"]
            self.fetched_at = _number(snapshot["fetched_at"])
        except (OSError, KeyError, TypeError, ValueError, RoutingError):
            return

    def _save(self, raw: list[dict]):
        if self.catalog_path is None:
            return
        temporary = None
        try:
            self.catalog_path.parent.mkdir(parents=True, exist_ok=True)
            import tempfile
            fd, temporary = tempfile.mkstemp(prefix=self.catalog_path.name + ".", dir=self.catalog_path.parent)
            with os.fdopen(fd, "wb") as output:
                output.write(_json_bytes({"schema_version": 1, "fetched_at": self.fetched_at,
                                          "catalog_version": self.catalog_version,
                                          "models": self.models, "raw_models": raw}))
            os.replace(temporary, self.catalog_path)
        except OSError:
            pass
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)

    async def refresh(self, *, force: bool = False) -> list[dict]:
        async with self._lock:
            if not force and (self.now() < self.retry_at or (self.models and self.now() - self.fetched_at < self.catalog_ttl)):
                return self.models
            try:
                response = await _request(self.client, "GET", CATALOG_URL, timeout=6.0)
                raw = response.get("data") if isinstance(response, dict) else None
                models = _normalise_catalog(raw)
            except (httpx.HTTPError, asyncio.TimeoutError, RoutingError, ValueError):
                self.retry_at = self.now() + 300
                if self.models:
                    return self.models
                raise RoutingError("The model catalog is ready when a valid snapshot is available") from None
            self.models, self.fetched_at = models, self.now()
            self.catalog_version, self.retry_at = _catalog_version(models), 0.0
            self._save(raw)
            return self.models

    def catalog_nodes(self) -> list[dict]:
        return [{**model, "id": "models." + model["id"], "model_id": model["id"], "catalog_version": self.catalog_version}
                for model in self.models]

    def _profiles(self, state: Any, personas: list[dict], required: tuple, preferred: list | None,
                  allowed: list | None, max_tokens: int, budget: float) -> list[dict]:
        if any(capability not in ("json_action_plan", "text", "vision", "native_tools") for capability in required):
            raise RoutingError("Use supported worker capabilities")
        if "native_tools" in required:
            raise RoutingError("Native tool routes use a dedicated Responses or Messages adapter")
        candidates = []
        input_bound = len(_json_bytes(state))
        for persona in personas:
            preferred_ids = persona.get("preferred_models") or preferred or list(DEFAULT_MODELS)
            pinned = persona.get("model")
            wanted = [pinned] if pinned else (allowed if allowed is not None else preferred_ids)
            if not isinstance(wanted, (list, tuple)) or not all(isinstance(value, str) for value in wanted):
                raise RoutingError("Use exact model IDs for worker preferences")
            for model in self.models:
                model_id, parameters = model["id"], model["supported_parameters"]
                if model_id not in wanted or (allowed is not None and model_id not in allowed):
                    continue
                if "text" not in model["input_modalities"] or "text" not in model["output_modalities"]:
                    continue
                if "vision" in required and "image" not in model["input_modalities"]:
                    continue
                if "json_action_plan" in required and "response_format" not in parameters:
                    continue
                if not ("max_tokens" in parameters or "max_completion_tokens" in parameters):
                    continue
                if max_tokens > model["max_completion_tokens"] or input_bound + max_tokens > model["context_length"]:
                    continue
                prices = dict(model["pricing"])
                for override in prices["overrides"]:
                    if input_bound >= override["min_prompt_tokens"]:
                        prices.update(prompt=override["prompt"], completion=override["completion"])
                estimate = input_bound * prices["prompt"] + max_tokens * prices["completion"]
                if estimate > budget:
                    continue
                reasoning = None
                if "reasoning" in parameters:
                    reasoning = {"effort": "none" if model_id.startswith("openai/gpt-6-luna") else "low"}
                candidates.append({"model": model_id, "endpoint": CHAT_URL,
                                   "mode": "json_action_plan" if "json_action_plan" in required else "text",
                                   "supported_parameters": parameters, "reasoning": reasoning,
                                   "persona": persona["id"], "max_tokens": max_tokens,
                                   "pricing": prices, "limits": {"context_tokens": model["context_length"],
                                   "max_output_tokens": model["max_completion_tokens"], "budget_usd": budget},
                                   "estimated_max_cost": estimate})
        order = {model: index for index, model in enumerate(preferred or DEFAULT_MODELS)}
        candidates.sort(key=lambda item: (order.get(item["model"], 100), item["estimated_max_cost"], item["persona"], item["model"]))
        if not candidates:
            raise RoutingError("The task needs an eligible model within its budget")
        return candidates[:12]

    async def select_route(self, state: Any, *, nodes: list[dict] | None = None, tasks: list[dict] | None = None,
                           personas: list[dict] | None = None, topics: list[str] | None = None,
                           current_task_id: str | None = None, required_capabilities=("json_action_plan",),
                           preferred_models: list[str] | None = None, allowed_models: list[str] | None = None,
                           max_tokens: int = 1400, budget_usd: float = 0.03) -> dict:
        """Choose only supplied node, task, persona and catalog IDs; return a saved-route record."""
        if isinstance(max_tokens, bool) or not isinstance(max_tokens, int) or not 1 <= max_tokens <= 16000:
            raise RoutingError("Use a worker output limit from one to 16000 tokens")
        budget = _number(budget_usd)
        await self.refresh()
        node_records = _records(nodes, 16)
        task_records = _records(tasks, 12)
        persona_records = _records(personas, 4, {"id": "rati", "summary": "RatiChat's usual voice"})
        topic_ids = topics or list(DEFAULT_TOPICS)
        if not isinstance(topic_ids, (list, tuple)) or not 1 <= len(topic_ids) <= 20 or not all(
            isinstance(topic, str) and 1 <= len(topic) <= 100 for topic in topic_ids
        ) or len(set(topic_ids)) != len(topic_ids):
            raise RoutingError("Use a short set of topic IDs")
        if current_task_id is not None and current_task_id not in {item["id"] for item in task_records}:
            raise RoutingError("The current task must be in the allowed task catalog")
        profiles = self._profiles(state, persona_records, tuple(required_capabilities), preferred_models,
                                  allowed_models, max_tokens, budget)
        route_choices = {"route" + str(index): profile for index, profile in enumerate(profiles)}
        topic_choices = {"topic" + str(index): topic for index, topic in enumerate(topic_ids)}
        questions = {
            "route": {"type": "choice", "instructions": "Choose a worker route and persona for the current task. Prefer fast routes for routine work. Choose Sol when complex reasoning would help. Use the task's topic, persona and model preferences.",
                      "criteria": {key: _compact(profile) for key, profile in route_choices.items()}},
            "topic": {"type": "choice", "instructions": "Choose the topic of the current task.",
                      "criteria": topic_choices},
        }
        task_choices = {"new": None}
        if task_records:
            task_choices.update({"task" + str(index): item["id"] for index, item in enumerate(task_records)})
            questions["task"] = {"type": "choice", "instructions": "Does the current request continue an allowed saved task? Choose new for a separate goal. Account links and audiences were checked by the caller.",
                                 "criteria": {"new": "Start a new task", **{"task" + str(index): _compact(item) for index, item in enumerate(task_records)}}}
        for index, node in enumerate(node_records):
            questions["node" + str(index)] = {"type": "noul", "instructions": "Is this node useful to the current request and should its full content be expanded? Node " + node["id"] + ": " + str(node.get("summary", ""))[:500]}
        decision_state = {"request": _compact(state), "current_task_id": current_task_id,
                          "personas": [_compact(item) for item in persona_records],
                          "node_summaries": [{"id": item["id"], "summary": str(item.get("summary", ""))[:500]} for item in node_records]}
        try:
            receipt = await self.decisions.decide(decision_state, questions, session_id=current_task_id)
            answers = receipt["answers"]
            route = dict(route_choices[answers["route"]["choice"]])
            topic = topic_choices[answers["topic"]["choice"]]
            resume = task_choices[answers["task"]["choice"]] if task_records else None
            expanded = [node["id"] for index, node in enumerate(node_records) if answers["node" + str(index)]["noul"] >= 0.6]
        except (httpx.HTTPError, asyncio.TimeoutError, RoutingError, ValueError):
            # An unavailable decision service still has a known exact worker profile.
            fallback = next((item for item in profiles if item["model"] in FAST_MODELS), profiles[0])
            route, topic, resume = dict(fallback), topic_ids[0], current_task_id
            expanded = [node["id"] for node in node_records[:3]]
            receipt = {"kind": "fallback", "schema_version": 1,
                       "reason": "decision_service_unavailable_or_invalid", "usage": {}}
        route.update(topic=topic, resume_task_id=resume, expanded_nodes=expanded,
                     catalog_version=self.catalog_version, decision_receipt=receipt)
        return route


def build_chat_payload(route: dict, payload: dict) -> dict:
    """Pin a server-chosen worker profile and shape its JSON action-plan request."""
    if not isinstance(route, dict) or not MODEL_ID.fullmatch(str(route.get("model", ""))):
        raise RoutingError("Use an exact saved model route")
    if route.get("endpoint") != CHAT_URL or route.get("mode") not in ("json_action_plan", "text"):
        raise RoutingError("Use a supported chat worker route")
    if not isinstance(payload, dict) or not isinstance(payload.get("messages"), list):
        raise RoutingError("Give the worker its message list")
    supported = set(route.get("supported_parameters", []))
    result = {key: value for key, value in payload.items() if key in supported or key in ("messages", "stream")}
    for key in ("reasoning", "reasoning_effort", "max_tokens", "max_completion_tokens", "tools", "tool_choice"):
        result.pop(key, None)
    result["model"] = route["model"]
    result["max_tokens" if "max_tokens" in supported else "max_completion_tokens"] = route["max_tokens"]
    if route.get("reasoning") and "reasoning" in supported:
        result["reasoning"] = route["reasoning"]
    if route["mode"] == "json_action_plan":
        if "response_format" not in supported:
            raise RoutingError("The worker route needs JSON output support")
        result["response_format"] = payload.get("response_format", {"type": "json_object"})
    token_bound = len(_json_bytes(result["messages"]))
    limits, pricing = route["limits"], dict(route["pricing"])
    if token_bound + route["max_tokens"] > limits["context_tokens"]:
        raise RoutingError("The worker input exceeds its context budget")
    for override in pricing.get("overrides", []):
        if token_bound >= override["min_prompt_tokens"]:
            pricing.update(prompt=override["prompt"], completion=override["completion"])
    if token_bound * pricing["prompt"] + route["max_tokens"] * pricing["completion"] > limits["budget_usd"]:
        raise RoutingError("The worker input exceeds its saved cost budget")
    result["provider"] = {"require_parameters": True, "max_price": {
        "prompt": pricing["prompt"] * 1_000_000, "completion": pricing["completion"] * 1_000_000}}
    return result


def record_inference(route: dict, response: dict, *, latency_ms: float | None = None) -> dict:
    """Return safe serving-model and usage facts for the task's attempt record."""
    actual = response.get("model") if isinstance(response, dict) else None
    selected = route.get("model")
    if not isinstance(actual, str) or not isinstance(selected, str) or not (
        actual == selected or re.fullmatch(re.escape(selected) + r"-[0-9]{8}", actual)
    ):
        raise RoutingError("The worker serving model must match its exact saved route")
    result = {"model": selected, "actual_model": actual,
              "provider": str(response.get("provider", ""))[:100],
              "request_id": str(response.get("id", ""))[:200],
              "usage": _safe_usage(response.get("usage")), "catalog_version": route.get("catalog_version", "")}
    if latency_ms is not None:
        result["latency_ms"] = _number(latency_ms)
    return result
