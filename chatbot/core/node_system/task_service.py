"""Saved tasks, local model routes, and a shared budget for worker calls."""

import asyncio
import copy
import hashlib
import json
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field

from ...config import settings
from ..model_router import record_inference


PERSONAS = [
    {"id": "ratichat", "summary": "Helpful community host. Answer clearly in simple English."},
    {"id": "researcher", "summary": "Find evidence, compare sources, and cite facts."},
    {"id": "developer", "summary": "Explain code, systems, and technical tradeoffs."},
    {"id": "critic", "summary": "Check claims, evidence, and missing details."},
]
TOPICS = ["general", "tech", "ai", "developer", "crypto", "reddit", "social", "world"]
ACTIVE_TASK = ContextVar("ratichat_active_task", default=None)


@dataclass
class TaskBinding:
    service: object
    task: dict
    platform: str
    channel_id: str
    sender_id: str
    event_id: str
    nodes: dict
    route: dict
    sequence: int = 0
    input_versions: dict = field(default_factory=dict)
    fresh_nodes: set = field(default_factory=set)
    issue: str = ""
    request_text: str = ""


class TaskService:
    def __init__(self, store, router, ai_engine, budget_usd=None):
        self.store = store
        self.router = router
        self.ai_engine = ai_engine
        self.budget_usd = budget_usd or settings.TASK_BUDGET_USD

    async def prepare(self, channel, source, *, proactive=False):
        nodes = self.store.catalog(channel.type, channel.id, source.sender, query=source.content)
        inputs = self.store.snapshot_versions(channel.type, channel.id, source.sender,
            {k: v for k, v in nodes.items() if v["kind"] != "task"}, event_id=None if proactive else source.id)
        task = self.store.task_for_request(channel.type, channel.id, source.sender, source.id,
            goal=source.content[:2000], budget_usd=self.budget_usd)
        route = task.get("route")
        if not route:
            tasks = [] if proactive else self.store.list_tasks(channel.type, channel.id, source.sender)
            route = await self._route(task, {"request": source.content[:1500], "platform": channel.type,
                "channel_id": channel.id, "proactive": proactive}, nodes, tasks)
            resume = route.get("resume_task_id")
            if resume and resume != task["id"]:
                task = self.store.continue_task(resume, channel.type, channel.id, source.sender, source.id, replace_task_id=task["id"])
                route = task.get("route") or route
        saved = self.store.save_route(task["id"], route, input_versions=inputs)
        if not saved.get("saved"):
            raise ValueError("Use a fresh task source snapshot.")
        return TaskBinding(self, task, channel.type, channel.id, source.sender, source.id,
            copy.deepcopy(nodes), route, input_versions=inputs, request_text=source.content)

    async def _route(self, task, state, nodes, tasks=(), preferred_models=None):
        attempt = self.store.reserve_attempt(task["id"], 0.001,
            request_key="route:" + hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest() + f":{time.time_ns()}")
        route = await self.router.select_route(state,
            nodes=[{"id": k, "summary": v["summary"][:180]} for k, v in list(nodes.items())[:12]],
            tasks=[{"id": task["id"], "summary": task.get("goal", "")[:240]},
                   *[{"id": t["id"], "summary": t.get("goal", "")[:240]} for t in tasks if t["id"] != task["id"]][:7]],
            personas=[p for p in PERSONAS if p["id"] == state.get("persona")] or PERSONAS,
            topics=TOPICS, current_task_id=task["id"],
            preferred_models=preferred_models, allowed_models=preferred_models,
            max_tokens=1400, budget_usd=max(0, task["budget_usd"] - task.get("spent_usd", 0) - task.get("reserved_usd", 0)))
        receipt = route.get("decision_receipt", {})
        cost = receipt.get("usage", {}).get("cost") if isinstance(receipt, dict) else None
        self.store.record_result(task["id"], {"kind": "route", "receipt": receipt},
            attempt_id=attempt["id"], cost_usd=cost if cost is not None else attempt["reserved_usd"], status="active")
        return route

    @contextmanager
    def activate(self, binding):
        token = ACTIVE_TASK.set(binding)
        try:
            yield binding
        finally:
            ACTIVE_TASK.reset(token)

    def reserve_call(self, binding, request, label):
        binding.sequence += 1
        pricing = binding.route.get("pricing", {})
        max_price = request.get("provider", {}).get("max_price", {})
        if max_price:
            pricing = {k: float(max_price[k]) / 1_000_000 for k in ("prompt", "completion")}
        # Use a conservative byte bound for input and the full output allowance.
        size = len(json.dumps(request).encode())
        estimate = size * float(pricing.get("prompt", 0)) + int(request.get("max_tokens", request.get("max_completion_tokens", 1400))) * float(pricing.get("completion", 0))
        estimate = max(0.0001, estimate)
        try:
            attempt = self.store.reserve_attempt(binding.task["id"], estimate,
                request_key=f"{binding.event_id}:{label}:{time.time_ns()}:{binding.sequence}",
                input_versions=binding.input_versions)
        except ValueError:
            binding.issue = "budget"
            raise
        if not attempt.get("reserved"):
            binding.issue = "source"
            raise ValueError("Use a fresh source snapshot for this task.")
        return attempt

    def record_call(self, binding, attempt, response, latency_ms):
        receipt = record_inference(binding.route, response, latency_ms=latency_ms)
        cost = receipt.get("usage", {}).get("cost")
        saved = self.store.record_result(binding.task["id"], {"kind": "inference", **receipt},
            attempt_id=attempt["id"], cost_usd=cost if cost is not None else attempt["reserved_usd"], input_versions=binding.input_versions, status="active")
        if not saved.get("accepted"):
            raise ValueError("Use a fresh source snapshot and the remaining task budget.")

    def record_failure(self, binding, attempt, *, status_code=None, uncertain=False):
        self.store.record_result(binding.task["id"], {"kind": "inference_failure", "status_code": status_code,
            "outcome": "unknown" if uncertain else "rejected", "cost_source": "reserved_bound" if uncertain else "pre_inference_rejection"},
            attempt_id=attempt["id"], cost_usd=attempt["reserved_usd"] if uncertain else 0,
            input_versions=attempt["input_versions"], success=False, status="active")

    @staticmethod
    def task_context(task, *, history=True):
        fields = ("id", "goal", "topic", "persona_id", "status", "root_task_id", "budget_usd", "spent_usd", "reserved_usd", "remaining_usd")
        result = {k: task[k] for k in fields if k in task}
        result["goal"] = str(result.get("goal", ""))[:1500 if history else 300]
        route = task.get("route") or {}
        result["route"] = {k: route[k] for k in ("model", "persona", "topic", "catalog_version") if k in route}
        if history:
            result["results"] = [{"status": item["status"], "result": json.dumps(item["result"])[:1800]}
                                 for item in task.get("results", [])[-3:]]
        return result

    async def execute_tool(self, name, params, scope):
        binding = ACTIVE_TASK.get()
        if not binding or (binding.platform, binding.channel_id, binding.sender_id, binding.event_id) != (
                scope.channel_type, scope.channel_id, scope.latest_sender_id, scope.latest_event_id):
            return {"status": "blocked", "message": "Use an active saved task for this request."}
        try:
            if name == "get_model_catalog":
                await self.router.refresh()
                query = str(params.get("query", "")).lower()[:100]
                limit = params.get("limit", 20)
                if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 30:
                    raise ValueError("Choose a catalog limit from one to thirty.")
                models = [m for m in self.router.catalog_nodes() if query in m["model_id"].lower()]
                return {"status": "success", "catalog_version": self.router.catalog_version,
                        "matching_models": len(models), "models": models[:limit]}
            if name == "get_task_status":
                return {"status": "success", "task": self.task_context(self.store.get_task(binding.task["id"])),
                        "tasks": [self.task_context(t, history=False) for t in
                            self.store.list_tasks(binding.platform, binding.channel_id, binding.sender_id)[:10]]}
            if name == "link_chat_account":
                stage = params.get("stage")
                text = binding.request_text
                link_id, code = str(params.get("link_id", "")), str(params.get("code", ""))
                if stage == "start" and ("link" not in text.lower() or not params.get("target_account_id") or params["target_account_id"] not in text):
                    raise ValueError("Ask to link the exact target account ID in your message.")
                if stage == "prove" and (not link_id or not code or link_id not in text or code not in text):
                    raise ValueError("Include the link ID and proof code in your message from the target account.")
                if stage == "confirm" and ("confirm" not in text.lower() or not link_id or link_id not in text):
                    raise ValueError("Confirm the exact link ID in a message from the original account.")
                identity = (binding.platform, binding.channel_id, binding.sender_id)
                if stage == "start":
                    return {"status": "success", **self.store.begin_link(*identity,
                        params.get("target_platform", ""), params.get("target_account_id", ""))}
                if stage == "prove":
                    return {"status": "success", **self.store.prove_link(params.get("link_id", ""), params.get("code", ""), *identity)}
                if stage == "confirm":
                    return {"status": "success", **self.store.confirm_link(params.get("link_id", ""), *identity)}
                raise ValueError("Choose start, prove, or confirm for account linking.")
            if name == "run_task_workers":
                jobs = params.get("jobs")
                if not isinstance(jobs, list) or not 1 <= len(jobs) <= 3:
                    raise ValueError("Choose one to three worker jobs.")
                for job in jobs:
                    if not isinstance(job, dict) or set(job) - {"goal", "persona", "preferred_model"}:
                        raise ValueError("Use a goal and a listed persona for each worker.")
                    if job.get("preferred_model"):
                        await self.router.refresh()
                        if job["preferred_model"] not in {m["model_id"] for m in self.router.catalog_nodes()}:
                            raise ValueError("Choose an exact model from the current catalog.")
                    if job.get("persona", "researcher") not in {p["id"] for p in PERSONAS} or not isinstance(job.get("goal"), str) or not job["goal"].strip():
                        raise ValueError("Give each worker a goal and a listed persona.")
                results = await asyncio.gather(*(self._worker(binding, job, index) for index, job in enumerate(jobs)))
                return {"status": "success", "workers": results}
        except (ValueError, PermissionError) as error:
            return {"status": "blocked", "message": str(error)}
        return {"status": "blocked", "message": "Choose a listed task tool."}

    async def _worker(self, parent, job, index):
        child = self.store.create_child(parent.task["id"], request_key=f"{parent.event_id}:{index}:{hashlib.sha256(json.dumps(job, sort_keys=True).encode()).hexdigest()}",
            goal=job["goal"][:2000], persona_id=job.get("persona", "researcher"))
        if child.get("status") == "complete" and child.get("result"):
            return {"task_id": child["id"], "result": child["result"], "saved": True}
        try:
            route = child.get("route") or await self._route(child,
                {"request": job["goal"][:1500], "persona": job.get("persona", "researcher")}, parent.nodes,
                preferred_models=[job["preferred_model"]] if job.get("preferred_model") else None)
            route["persona"] = job.get("persona", "researcher")
            self.store.save_route(child["id"], route, input_versions=parent.input_versions)
            binding = TaskBinding(self, child, parent.platform, parent.channel_id, parent.sender_id,
                parent.event_id, parent.nodes, route, input_versions=parent.input_versions)
            with self.activate(binding):
                result = await asyncio.wait_for(self.ai_engine.compose_task_worker({
                    "goal": job["goal"][:2000], "persona": route["persona"],
                    "nodes": {k: v if k in route.get("expanded_nodes", []) or k in parent.fresh_nodes
                        else {"summary": v["summary"], "version": v["version"], "kind": v["kind"]}
                        for k, v in parent.nodes.items()}, "task_route": route}), 60)
            if result:
                saved = self.store.record_result(child["id"], {"kind": "worker", "content": result},
                    input_versions=parent.input_versions)
                return {"task_id": child["id"], "content": result if saved.get("accepted") else "", "saved": saved}
            return {"task_id": child["id"], "status": "retry"}
        except (ValueError, PermissionError, asyncio.TimeoutError):
            return {"task_id": child["id"], "status": "retry", "message": "This worker needs a fresh budget or source snapshot."}
