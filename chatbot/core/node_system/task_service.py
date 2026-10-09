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


class TaskService:
    def __init__(self, store, router, ai_engine, budget_usd=None):
        self.store = store
        self.router = router
        self.ai_engine = ai_engine
        self.budget_usd = budget_usd or settings.TASK_BUDGET_USD

    async def prepare(self, channel, source, *, proactive=False):
        nodes = self.store.catalog(channel.type, channel.id, source.sender, query=source.content)
        task = self.store.task_for_request(channel.type, channel.id, source.sender, source.id,
            goal=source.content[:2000], budget_usd=self.budget_usd)
        route = task.get("route")
        if not route:
            tasks = [] if proactive else self.store.list_tasks(channel.type, channel.id, source.sender)
            route = await self._route(task, {"request": source.content[:3000], "platform": channel.type,
                "channel_id": channel.id, "proactive": proactive}, nodes, tasks)
            resume = route.get("resume_task_id")
            if resume and resume != task["id"]:
                task = self.store.continue_task(resume, channel.type, channel.id, source.sender, source.id)
                route = task.get("route") or route
            self.store.save_route(task["id"], route, input_versions={k: v["version"] for k, v in nodes.items() if v.get("kind") != "task"})
        return TaskBinding(self, task, channel.type, channel.id, source.sender, source.id,
            copy.deepcopy(nodes), route, input_versions={k: v["version"] for k, v in nodes.items() if v.get("kind") != "task"})

    async def _route(self, task, state, nodes, tasks=()):
        attempt = self.store.reserve_attempt(task["id"], 0.001,
            request_key="route:" + hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest() + f":{time.time_ns()}")
        route = await self.router.select_route(state,
            nodes=[{"id": k, "summary": v["summary"]} for k, v in nodes.items()],
            tasks=[{"id": t["id"], "summary": t.get("goal", "")[:1000]} for t in tasks if t["id"] != task["id"]],
            personas=PERSONAS, topics=TOPICS, current_task_id=task["id"],
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
        attempt = self.store.reserve_attempt(binding.task["id"], estimate,
            request_key=f"{binding.event_id}:{label}:{time.time_ns()}:{binding.sequence}",
            input_versions=binding.input_versions)
        if not attempt.get("reserved"):
            raise ValueError("Use a fresh source snapshot for this task.")
        return attempt

    def record_call(self, binding, attempt, response, latency_ms):
        receipt = record_inference(binding.route, response, latency_ms=latency_ms)
        cost = receipt.get("usage", {}).get("cost")
        saved = self.store.record_result(binding.task["id"], {"kind": "inference", **receipt},
            attempt_id=attempt["id"], cost_usd=cost if cost is not None else attempt["reserved_usd"], input_versions=binding.input_versions, status="active")
        if not saved.get("accepted"):
            raise ValueError("Use a fresh source snapshot and the remaining task budget.")

    async def execute_tool(self, name, params, scope):
        binding = ACTIVE_TASK.get()
        if not binding or (binding.platform, binding.channel_id, binding.sender_id, binding.event_id) != (
                scope.channel_type, scope.channel_id, scope.latest_sender_id, scope.latest_event_id):
            return {"status": "blocked", "message": "Use an active saved task for this request."}
        try:
            if name == "get_task_status":
                return {"status": "success", "task": self.store.get_task(binding.task["id"]),
                        "tasks": self.store.list_tasks(binding.platform, binding.channel_id, binding.sender_id)}
            if name == "link_chat_account":
                stage = params.get("stage")
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
                    if not isinstance(job, dict) or set(job) - {"goal", "persona"}:
                        raise ValueError("Use a goal and a listed persona for each worker.")
                    if job.get("persona", "researcher") not in {p["id"] for p in PERSONAS} or not isinstance(job.get("goal"), str) or not job["goal"].strip():
                        raise ValueError("Give each worker a goal and a listed persona.")
                results = await asyncio.gather(*(self._worker(binding, job, index) for index, job in enumerate(jobs)))
                return {"status": "success", "workers": results}
        except (ValueError, PermissionError) as error:
            return {"status": "blocked", "message": str(error)}
        return {"status": "blocked", "message": "Choose a listed task tool."}

    async def _worker(self, parent, job, index):
        child = self.store.create_child(parent.task["id"], request_key=f"{parent.event_id}:{index}:{job['goal'][:2000]}",
            goal=job["goal"][:2000], persona_id=job.get("persona", "researcher"))
        if child.get("status") == "complete" and child.get("result"):
            return {"task_id": child["id"], "result": child["result"], "saved": True}
        try:
            route = child.get("route") or await self._route(child,
                {"request": job["goal"][:2000], "persona": job.get("persona", "researcher")}, parent.nodes)
            route["persona"] = job.get("persona", "researcher")
            self.store.save_route(child["id"], route, input_versions=parent.input_versions)
            binding = TaskBinding(self, child, parent.platform, parent.channel_id, parent.sender_id,
                parent.event_id, parent.nodes, route, input_versions=parent.input_versions)
            with self.activate(binding):
                result = await asyncio.wait_for(self.ai_engine.compose_task_worker({
                    "goal": job["goal"][:2000], "persona": route["persona"],
                    "nodes": parent.nodes, "task_route": route}), 60)
            if result:
                attempt = self.store.reserve_attempt(child["id"], 0.000001,
                    request_key="worker-result:" + child["id"], input_versions=parent.input_versions)
                if not attempt.get("reserved"):
                    raise ValueError("Use a fresh worker source snapshot.")
                saved = self.store.record_result(child["id"], {"kind": "worker", "content": result},
                    attempt_id=attempt["id"], input_versions=parent.input_versions)
                return {"task_id": child["id"], "content": result if saved.get("accepted") else "", "saved": saved}
            return {"task_id": child["id"], "status": "retry"}
        except (ValueError, PermissionError, asyncio.TimeoutError):
            return {"task_id": child["id"], "status": "retry", "message": "This worker needs a fresh budget or source snapshot."}
