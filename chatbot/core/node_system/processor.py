"""Process one chat request through scoped nodes and bounded source lookups."""

import asyncio
import copy
import json
import logging
import sqlite3
from collections import OrderedDict
from dataclasses import asdict, replace
import time

from ...config import settings
from ..ai_engine import ActionPlan
from ..orchestration.capability_policy import READ_ONLY_SOURCE_TOOLS, SOURCE_WATCH_TOOLS, PROACTIVE_SOURCE_TOOLS, STATE_TOOLS, TASK_TOOLS
from .interaction_tools import NodeInteractionTools
from .node_manager import NodeManager
from .research_store import ResearchStore
from ..world_state.structures import Channel, Message

logger = logging.getLogger(__name__)
NODE_TOOLS = frozenset({"expand_node", "collapse_node", "pin_node", "unpin_node", "get_expansion_status"})
MANAGEMENT_TOOLS = frozenset({"manage_matrix_room", "manage_matrix_server", "matrix_server_status"})
REPLY_TOOLS = frozenset({"send_discord_reply", "send_matrix_reply", "send_farcaster_reply"})
STATE_RESULT_PREFIXES = ("sources.watch_result_", "sources.proactive_result_", "sources.task_result_")


def capture_request(world_state, policy, store, channel_id, message, *, accepted=False):
    """Save one accepted human request with its channel context."""
    channel = world_state.get_channel(channel_id)
    if not channel or message.metadata.get("is_bot") or message.metadata.get("historical"):
        return
    if channel.type == "discord":
        if channel_id not in policy.approved_discord_channel_ids:
            return
        if not accepted and not message.metadata.get("bot_mentioned"):
            return
    elif channel.type == "matrix":
        if channel_id not in policy.approved_matrix_room_ids or message.sender == settings.MATRIX_USER_ID:
            return
    else:
        return
    messages = list(channel.recent_messages)
    index = next((i for i, item in enumerate(messages) if item.id == message.id), len(messages) - 1)
    snapshot = {"channel": {"id": channel.id, "type": channel.type, "name": channel.name,
                           "recent_messages": [asdict(item) for item in messages[max(0, index - 6):index + 1]]},
                "source": asdict(message)}
    return store.enqueue(channel.type, channel.id, message.id, snapshot)


class NodeProcessor:
    MAX_STEPS = 5
    MAX_LOOKUPS = 3

    def __init__(self, world_state, payload_builder, executor, db_path, research_store=None, awareness_store=None, task_service=None):
        self.world_state = world_state
        self.payload_builder = payload_builder
        self.executor = executor
        self.ai_engine = executor.ai_engine
        self.policy = executor.capability_policy
        self.node_manager = NodeManager(max_expanded_nodes=4)
        self.last_payload = None
        self.last_result = {}
        self.catalog = {}
        self._claimed = OrderedDict()
        self.db_path = db_path
        self.research_store = research_store or ResearchStore(db_path, retention_days=settings.RESEARCH_RETENTION_DAYS)
        self._last_receipt_check = {}
        self._last_cleanup = 0
        self.watch_service = None
        self.awareness_store = awareness_store
        self.task_service = task_service
        self._cycle_lock = asyncio.Lock()
        self._binding = None
        self._view = None
        if db_path != ":memory:":
            with sqlite3.connect(db_path) as db:
                db.execute("CREATE TABLE IF NOT EXISTS node_request_receipts (channel_id TEXT, event_id TEXT, PRIMARY KEY (channel_id, event_id))")

    def _claim(self, channel_id, event_id):
        key = (channel_id, event_id)
        if self.db_path == ":memory:":
            if key in self._claimed:
                return False
            self._claimed[key] = True
            while len(self._claimed) > 2000:
                self._claimed.popitem(last=False)
            return True
        with sqlite3.connect(self.db_path) as db:
            result = db.execute("INSERT OR IGNORE INTO node_request_receipts VALUES (?, ?)", key)
            return result.rowcount == 1

    def execute_node_action(self, name, params):
        if name not in NODE_TOOLS:
            return {"success": False, "message": "Choose a listed node tool"}
        if name != "get_expansion_status" and params.get("node_path") not in self.catalog:
            return {"success": False, "message": "Choose a node from the current request"}
        return NodeInteractionTools(self.node_manager).execute_tool(name, params)

    def pending_channels(self):
        if time.time() - self._last_cleanup >= 3600:
            self.research_store.cleanup()
            self._last_cleanup = time.time()
        channels = {item["channel_id"] for item in self.research_store.pending_channels(include_processing=bool(self.ai_engine.api_key))}
        channels.update(item["channel_id"] for item in self.research_store.uncertain_deliveries()
                        if time.time() - self._last_receipt_check.get(item["channel_id"], 0) >= 60)
        return sorted(channels)

    @staticmethod
    def _saved_channel(turn):
        saved = turn["snapshot"]["channel"]
        return Channel(id=saved["id"], type=saved["type"], name=saved["name"],
                       recent_messages=[Message(**item) for item in saved["recent_messages"]])

    def _restore_discord(self, turn):
        observer = getattr(self.executor.action_context, "discord_observer", None)
        if observer and hasattr(observer, "restore_request"):
            observer.restore_request(turn["channel_id"], turn["snapshot"]["source"])

    def _watch_node(self, channel):
        node = {"trust_label": "untrusted", "watches": []}
        for watch in self.watch_service.store.list({"channel_type": channel.type, "channel_id": channel.id}):
            node["watches"].append({"id": watch["id"], "url": watch["url"], "fetched_at": watch["fetched_at"],
                "interval_seconds": watch["interval_seconds"], "latest_entries": json.loads(watch["last_items"])[:3]})
            if len(json.dumps(node)) > 6000:
                node["watches"].pop()
                break
        return node

    async def process_cycle(self, cycle_id, primary_channel_id, context=None):
        async with self._cycle_lock:
            return await self._process_cycle(cycle_id, primary_channel_id, context)

    async def _process_cycle(self, cycle_id, primary_channel_id, context=None):
        channel = self.world_state.get_channel(primary_channel_id)
        if channel and channel.type == "farcaster":
            return await self._run_cycle(cycle_id, primary_channel_id, context)
        if channel and channel.recent_messages:
            observer = getattr(self.executor.action_context, "discord_observer", None)
            for message in list(channel.recent_messages):
                accepted = bool(channel.type == "discord" and observer and observer.can_reply(channel.id, message.id))
                capture_request(self.world_state, self.policy, self.research_store, channel.id, message, accepted=accepted)
        pending = self.research_store.pending_channels()
        records = pending + self.research_store.uncertain_deliveries()
        platform = channel.type if channel else next((row["platform"] for row in records if row["channel_id"] == primary_channel_id), None)
        if platform not in {"discord", "matrix"}:
            return {"actions_executed": 0}
        if time.time() - self._last_receipt_check.get(primary_channel_id, 0) >= 60:
            await self._check_receipts(primary_channel_id)
        delivery = self.research_store.claim_delivery(platform, primary_channel_id, lease_seconds=900)
        if delivery:
            result = await self._deliver(delivery)
            return self._result(1, 0, 0, failed=result.get("status") != "success")
        turn = self.research_store.claim(platform, primary_channel_id, lease_seconds=900) if self.ai_engine.api_key else None
        if not turn:
            return {"actions_executed": 0, "duplicate": True}
        saved = self._saved_channel(turn)
        if (platform == "discord" and saved.id not in self.policy.approved_discord_channel_ids
                or platform == "matrix" and saved.id not in self.policy.approved_matrix_room_ids):
            self.research_store.fail(turn["id"], turn["lease_token"], "Use a configured channel", retryable=False)
            return self._result(0, 0, 0, failed=True)
        self._restore_discord(turn)
        return await self._run_cycle(cycle_id, primary_channel_id, context, saved, turn)

    async def _save_and_send(self, action, scope, turn, sources):
        if turn is None:
            return await self.executor._execute_action_and_return_result(action, scope)
        denial = self.policy.denial_reason(action.action_type, action.parameters, scope)
        if denial:
            return await self.executor._record_blocked_action(action, denial)
        text = str(action.parameters.get("content", "")).strip()[:1900]
        if not text:
            return {"status": "blocked", "error": "Provide reply text"}
        records = [data for path, data in sources.items() if path.startswith(("sources.result_", *STATE_RESULT_PREFIXES))]
        if not self.research_store.ready(turn["id"], turn["lease_token"], text, sources=records):
            return {"status": "blocked", "error": "The saved turn needs a current claim"}
        delivery = self.research_store.claim_delivery(turn["platform"], turn["channel_id"], lease_seconds=900)
        return await self._deliver(delivery) if delivery else {"status": "failure", "error": "Reply is saved for delivery"}

    async def _deliver(self, turn):
        channel = self._saved_channel(turn)
        self._restore_discord(turn)
        if self.awareness_store and not self.awareness_store.request_current(channel.type, channel.id,
                turn["event_id"], turn["snapshot"]["source"]["content"]):
            self.research_store.fail(turn["id"], turn["delivery_token"], "Use the current message version",
                                     phase="delivery", retryable=False)
            return {"status": "blocked", "message": "Use the current message version."}
        payload = self.payload_builder.build_request_node_payload(channel, self.node_manager)
        scope = self.policy.scope_from_payload(payload)
        name = {"discord": "send_discord_reply", "matrix": "send_matrix_reply"}[channel.type]
        reply = ActionPlan(name, {"channel_id": channel.id, "reply_to_id": turn["event_id"],
                                "content": turn["reply"], "delivery_id": "research:" + str(turn["id"]),
                                "format_as_markdown": False}, "Deliver the saved answer", 1)
        result = await self.executor._execute_action_and_return_result(reply, scope)
        if channel.type == "matrix" and result.get("status") != "success":
            observer = getattr(self.executor.action_context, "matrix_observer", None)
            receipts = getattr(observer, "reply_receipts", {})
            receipt = receipts.get(reply.parameters["delivery_id"]) if isinstance(receipts, dict) else None
            if receipt:
                result = {"status": "success", **receipt}
        token = turn["delivery_token"]
        if result.get("status") == "success":
            self.research_store.sent(turn["id"], token, result)
            self._confirmed_reply(turn, result)
            if channel.type == "discord" and result.get("message_id"):
                self.world_state.add_message(channel.id, Message(
                    id=result["message_id"], channel_type="discord", channel_id=channel.id,
                    sender="ratichat", content=turn["reply"], timestamp=time.time(),
                    reply_to=turn["event_id"], metadata={"is_bot": True},
                ))
        elif result.get("status") in {"unknown", "error"}:
            self.research_store.uncertain(turn["id"], token)
        else:
            self.research_store.fail(turn["id"], token, result.get("error", "Reply needs another attempt"),
                                     phase="delivery", retry_after=10 * turn["delivery_attempts"],
                                     retryable=result.get("status") != "blocked" and result.get("retryable", True))
        return result

    def _confirmed_reply(self, turn, receipt):
        if not self.awareness_store:
            return
        task = self.awareness_store.get_task_for_event(turn["platform"], turn["channel_id"], turn["event_id"])
        if task:
            self.awareness_store.record_result(task["id"], {"kind": "reply", "content": turn["reply"], "receipt": receipt},
                input_versions=task["input_versions"])
        message_id = receipt.get("message_id") or receipt.get("event_id")
        if message_id:
            self.awareness_store.ingest_message(turn["platform"], turn["channel_id"], {
                "id": message_id, "sender": "ratichat", "content": turn["reply"], "timestamp": time.time(),
                "reply_to": turn["event_id"], "metadata": {"is_bot": True, "confirmed": True}})

    async def _check_receipts(self, channel_id):
        self._last_receipt_check[channel_id] = time.time()
        for turn in self.research_store.uncertain_deliveries():
            if turn["channel_id"] != channel_id:
                continue
            channel = self._saved_channel(turn)
            observer = getattr(self.executor.action_context, f"{channel.type}_observer", None)
            result = None
            if channel.type == "discord" and channel.id in self.policy.approved_discord_channel_ids and observer and hasattr(observer, "reconcile_reply"):
                result = await observer.reconcile_reply(channel.id, turn["event_id"], turn["reply"])
            elif channel.type == "matrix" and observer and channel.id in self.policy.approved_matrix_room_ids:
                result = await observer.send_reply(channel.id, turn["reply"], turn["event_id"], tx_id="research:" + str(turn["id"]))
                if result.get("success"):
                    result["status"] = "success"
            if result and result.get("status") == "success":
                self.research_store.resolve_delivery(turn["id"], result)
                self._confirmed_reply(turn, result)

    async def _run_cycle(self, cycle_id, primary_channel_id, context=None, channel=None, turn=None):
        channel = channel or self.world_state.get_channel(primary_channel_id)
        self._binding, self._view = None, None
        approved = channel and (channel.type == "discord" and channel.id in self.policy.approved_discord_channel_ids
            or channel.type == "matrix" and channel.id in self.policy.approved_matrix_room_ids)
        if not self.task_service or not approved or not channel.recent_messages:
            return await self._run_cycle_inner(cycle_id, primary_channel_id, context, channel, turn)
        source = channel.recent_messages[-1]
        if source.metadata.get("is_bot") or not self.ai_engine.api_key:
            return self._result(0, 0, 0)
        for message in channel.recent_messages:
            # Intake already holds the latest version. Replaying a saved request
            # must preserve edits and tombstones from newer platform events.
            if not turn:
                self.awareness_store.ingest_message(channel.type, channel.id, asdict(message))
        if not self.awareness_store.request_current(channel.type, channel.id, source.id, source.content):
            if turn:
                self.research_store.fail(turn["id"], turn["lease_token"], "Use the current message version", retryable=False)
            return self._result(0, 0, 0, failed=True)
        try:
            self._binding = await self.task_service.prepare(channel, source)
            self._view = self.awareness_store.load_view(self._view_id(channel, source), channel.type, channel.id, source.sender)
            with self.task_service.activate(self._binding):
                return await self._run_cycle_inner(cycle_id, primary_channel_id, context, channel, turn)
        except (ValueError, PermissionError) as error:
            logger.warning("Saved task needs attention: %s", error)
            if turn and self.awareness_store.request_current(channel.type, channel.id, source.id, source.content):
                scope = self.policy.scope_from_payload(self.payload_builder.build_request_node_payload(channel, self.node_manager))
                action = ActionPlan("send_discord_reply" if channel.type == "discord" else "send_matrix_reply",
                    {"channel_id": channel.id, "reply_to_id": source.id,
                     "content": "This saved task needs a fresh start. Please ask me to start a new task for this topic."}, "Report saved task status", 1)
                await self._save_and_send(action, scope, turn, {})
            elif turn:
                self.research_store.fail(turn["id"], turn["lease_token"], "Use a current source version", retryable=False)
            return self._result(0, 0, 0, failed=True)
        finally:
            if self._binding and self._view is not None:
                paths = set(self._binding.nodes) & set(self.awareness_store.catalog(channel.type, channel.id, source.sender))
                self.awareness_store.save_view(self._view_id(channel, source), channel.type, channel.id, source.sender,
                    expanded=[k for k in self.node_manager.get_expanded_nodes() if k in paths],
                    collapsed=[k for k in paths if not self.node_manager.get_node_metadata(k).is_expanded],
                    pins=[k for k in paths if self.node_manager.get_node_metadata(k).is_pinned],
                    expected_revision=self._view["revision"])

    @staticmethod
    def _view_id(channel, source):
        return f"{channel.type}:{channel.id}:{source.sender}"

    async def _run_cycle_inner(self, cycle_id, primary_channel_id, context=None, channel=None, turn=None):
        channel = channel or self.world_state.get_channel(primary_channel_id)
        if not channel or not channel.recent_messages or not self.ai_engine.api_key:
            return {"actions_executed": 0}
        # Snapshot the source before any asynchronous lookup. Later chat messages
        # and web text cannot change the authority of this request.
        channel = replace(channel, recent_messages=list(channel.recent_messages))
        source = channel.recent_messages[-1]
        observer = getattr(self.executor.action_context, f"{channel.type}_observer", None)
        if source.metadata.get("is_bot"):
            return {"actions_executed": 0}
        if channel.type == "discord":
            if channel.id not in self.policy.approved_discord_channel_ids or not observer or not observer.can_reply(channel.id, source.id):
                return {"actions_executed": 0}
        elif channel.type == "matrix":
            if channel.id not in self.policy.approved_matrix_room_ids:
                return {"actions_executed": 0}
            if observer and source.sender == observer.user_id:
                return {"actions_executed": 0}
            if self.world_state.has_bot_replied_to_matrix_event(source.id):
                return {"actions_executed": 0}
        elif channel.type != "farcaster":
            return {"actions_executed": 0}
        if turn is None and not self._claim(channel.id, source.id):
            return {"actions_executed": 0, "duplicate": True}

        self.node_manager.node_metadata.clear()
        self.node_manager.system_events.clear()
        self.node_manager.max_expanded_nodes = settings.MAX_EXPANDED_NODES if self._binding else 4
        if self._binding:
            for path, data in self._binding.nodes.items():
                self.node_manager.update_node_summary(path, data["summary"])
            for path in (self._view or {}).get("expanded", []):
                if path in self._binding.nodes:
                    self.node_manager.expand_node(path)
            for path in (self._view or {}).get("pins", []):
                if path in self._binding.nodes:
                    self.node_manager.pin_node(path)
            for path in self._binding.route.get("expanded_nodes", []):
                if path in self._binding.nodes and path not in (self._view or {}).get("collapsed", []):
                    self.node_manager.expand_node(path)
        channel_path = f"channels.{channel.type}.{channel.id}"
        self.node_manager.expand_node(channel_path)
        self.node_manager.pin_node(channel_path)
        sources = {
            **({k: v for k, v in self._binding.nodes.items() if k != channel_path} if self._binding else {}),
            "sources.web": {"description": "Search current public web information with web_search."},
            "sources.pages": {"description": "Read public pages, project docs, or raw GitHub files with read_webpage."},
            "sources.feeds": {"description": "Read news, blogs, or GitHub release RSS/Atom feeds with read_feed."},
            "sources.news": {"description": "Use read_news for BBC News, BBC Technology, Hacker News, and CoinDesk headlines."},
            "sources.social": {"description": "Use search_social for indexed public Reddit, Farcaster, X, or Bluesky pages and posts. Use read_bluesky_feed for a public author feed."},
        }
        if turn:
            sources["channel.memory"] = self.research_store.memory_node(channel.type, channel.id)
            self.node_manager.expand_node("channel.memory")
            self.node_manager.pin_node("channel.memory")
        if self.watch_service and channel.type in {"discord", "matrix"}:
            sources["sources.watches"] = self._watch_node(channel)
        scope_payload = self.payload_builder.build_request_node_payload(channel, self.node_manager)
        channel_node = scope_payload["expanded_nodes"][channel_path]["data"]
        scope = self.policy.scope_from_payload(scope_payload)
        proactive = getattr(self.executor.action_context, "proactive_source_service", None)
        proactive_scope = {"channel_type": scope.channel_type, "channel_id": scope.channel_id,
                           "sender_id": scope.latest_sender_id, "event_id": scope.latest_event_id}
        proactive_available = proactive and scope.channel_type == "discord" and scope.channel_id in proactive.store.allowed_channels
        if proactive_available:
            sources["sources.proactive"] = proactive.store.status(proactive_scope)
        if turn:
            saved_watch_results = [data for data in turn["sources"] if data.get("tool") in STATE_TOOLS]
            if self.watch_service:
                trusted_scope = {"channel_type": scope.channel_type, "channel_id": scope.channel_id,
                                 "sender_id": scope.latest_sender_id, "event_id": scope.latest_event_id}
                for data in self.watch_service.store.tool_results(trusted_scope):
                    if data not in saved_watch_results:
                        saved_watch_results.append(data)
            if proactive_available:
                for data in proactive.store.tool_results(proactive_scope):
                    if data not in saved_watch_results:
                        saved_watch_results.append(data)
            for index, data in enumerate(saved_watch_results, 1):
                prefix = "sources.task_result_" if data["tool"] in TASK_TOOLS else "sources.proactive_result_" if data["tool"] in PROACTIVE_SOURCE_TOOLS else "sources.watch_result_"
                path = prefix + str(index)
                sources[path] = data
                self.node_manager.expand_node(path)
        allowed = self.policy.filter_tool_names(self.executor.tool_registry.get_tool_names(), scope)
        if turn:
            allowed &= READ_ONLY_SOURCE_TOOLS | STATE_TOOLS | REPLY_TOOLS | MANAGEMENT_TOOLS | {"wait"}
        lookups, executed = 0, 0
        results = [{"tool": data["tool"], "node_path": path, "result": data}
                   for path, data in sources.items() if path.startswith(STATE_RESULT_PREFIXES)]
        seen_lookups = set()
        force_answer = False
        try:
            for step in range(self.MAX_STEPS):
                if turn and not self.research_store.heartbeat(turn["id"], turn["lease_token"], lease_seconds=900):
                    return self._result(executed, lookups, step, failed=True)
                final_step = force_answer or step == self.MAX_STEPS - 1 or lookups >= self.MAX_LOOKUPS
                names = allowed - READ_ONLY_SOURCE_TOOLS - TASK_TOOLS if final_step else allowed
                payload = self.payload_builder.build_request_node_payload(channel, self.node_manager, sources)
                self.catalog = {channel_path: {}, **sources}
                if self._binding:
                    payload["task_route"] = self._binding.route
                    payload["task"] = self.awareness_store.get_task(self._binding.task["id"])
                payload.update({
                    "cycle_id": cycle_id, "final_step": final_step,
                    "lookup_budget_remaining": self.MAX_LOOKUPS - lookups,
                    "available_tools": self.executor.tool_registry.get_tool_descriptions_for_ai(names),
                    "node_tools": {} if final_step else {
                        name: definition for name, definition in NodeInteractionTools(self.node_manager).get_tool_definitions().items()
                        if name in NODE_TOOLS
                    },
                    "tool_results": results[-8:],
                })
                if context and context.get("processing_mode") == "traditional":
                    payload["processing_mode"] = "traditional"
                    payload["channels"][channel.id]["recent_messages"] = channel_node["recent_messages"]
                    payload["node_tools"] = {}
                if scope.channel_type == "matrix" and self.policy.profile == "matrix_steward":
                    payload["matrix_management"] = {
                        "request_event_id": scope.latest_event_id,
                        "managed_room_ids": sorted(self.policy.managed_room_ids),
                    }
                self.last_payload = copy.deepcopy(payload)
                if final_step:
                    # Include the bounded source data even when the planner has
                    # collapsed it. Answer composition has one fixed destination.
                    payload["answer_nodes"] = {
                        channel_path: channel_node,
                        **{path: data for path, data in sources.items()
                           if not self._binding or path not in self._binding.nodes
                           or self.node_manager.get_node_metadata(path).is_expanded},
                    }
                    self.last_payload = copy.deepcopy(payload)
                    content = await self.ai_engine.compose_reply(payload)
                    if not content and self._binding and self._binding.issue:
                        content = "This saved task needs a fresh start. Please ask me to start a new task for this topic."
                    failed = not content
                    if failed and turn and turn["attempts"] < self.research_store.max_attempts:
                        self.research_store.fail(turn["id"], turn["lease_token"], "AI reply needs another attempt", retry_after=10 * turn["attempts"])
                        return self._result(executed, lookups, step + 1, failed=True)
                    content = content or "Please try a fresh request. The AI reply service needs another attempt."
                    tool_name = {"discord": "send_discord_reply", "matrix": "send_matrix_reply", "farcaster": "send_farcaster_reply"}[scope.channel_type]
                    parameters = {"content": content}
                    if scope.channel_type == "farcaster":
                        parameters["reply_to_hash"] = scope.latest_event_id
                    else:
                        parameters.update({"channel_id": scope.channel_id, "reply_to_id": scope.latest_event_id})
                    reply = ActionPlan(tool_name, parameters, "Answer the current request from fetched sources", 1)
                    result = await self._save_and_send(reply, scope, turn, sources)
                    return self._result(executed + 1, lookups, step + 1, failed=failed or result.get("status") != "success")
                decision = await self.ai_engine.make_decision(payload, f"{cycle_id}_step_{step}")
                actions = decision.selected_actions[:3]
                if not actions:
                    force_answer = True
                    continue
                watch_actions = [action for action in actions if action.action_type in STATE_TOOLS]
                if watch_actions:
                    for action in watch_actions:
                        result = await self.executor._execute_action_and_return_result(action, scope)
                        prefix = "sources.task_result_" if action.action_type in TASK_TOOLS else "sources.proactive_result_" if action.action_type in PROACTIVE_SOURCE_TOOLS else "sources.watch_result_"
                        path = prefix + str(1 + sum(key.startswith(STATE_RESULT_PREFIXES) for key in sources))
                        sources[path] = {"tool": action.action_type, "trust": "untrusted_source", **result}
                        if turn:
                            records = [data for key, data in sources.items() if key.startswith(STATE_RESULT_PREFIXES)]
                            if not self.research_store.save_sources(turn["id"], turn["lease_token"], records):
                                return self._result(executed, lookups, step + 1, failed=True)
                        self.node_manager.expand_node(path)
                        results.append({"tool": action.action_type, "node_path": path, "result": result})
                        executed += 1
                    if self.watch_service and channel.type in {"discord", "matrix"}:
                        sources["sources.watches"] = self._watch_node(channel)
                    if proactive_available:
                        sources["sources.proactive"] = proactive.store.status(proactive_scope)
                    # The next AI step sees the actual result before writing its reply.
                    continue
                # A reply selected alongside a lookup is based on stale context.
                reads = [action for action in actions if action.action_type in READ_ONLY_SOURCE_TOOLS | NODE_TOOLS]
                if reads and not final_step:
                    for action in reads:
                        if action.action_type in NODE_TOOLS:
                            result = self.execute_node_action(action.action_type, action.parameters)
                            results.append({"tool": action.action_type, "result": result})
                            continue
                        signature = json.dumps([action.action_type, action.parameters], sort_keys=True)
                        if action.action_type not in names or signature in seen_lookups or lookups >= self.MAX_LOOKUPS:
                            results.append({"tool": action.action_type, "error": "Use the fetched results or choose another lookup within the budget"})
                            continue
                        seen_lookups.add(signature)
                        lookups += 1
                        parameters = dict(action.parameters)
                        fresh = parameters.pop("fresh", False)
                        cached = self.research_store.cached_source(channel.type, channel.id, action.action_type, parameters) if turn and not fresh else None
                        if cached:
                            result = {**cached["result"], "cached": True, "fetched_at": cached["fetched_at"], "expires_at": cached["expires_at"]}
                        else:
                            action.parameters = parameters
                            lookup_attempt = None
                            if self._binding and action.action_type in {"web_search", "search_social"}:
                                lookup_attempt = self.awareness_store.reserve_attempt(self._binding.task["id"], 0.02,
                                    request_key=f"lookup:{source.id}:{time.time_ns()}:{signature}")
                            result = await self.executor._execute_action_and_return_result(action, scope)
                            if lookup_attempt:
                                cost = result.get("usage", {}).get("cost")
                                if cost is None:
                                    cost = 0 if result.get("http_status") in {400, 401, 402, 403, 404, 413, 422, 429} else lookup_attempt["reserved_usd"]
                                self.awareness_store.record_result(self._binding.task["id"],
                                    {"kind": "source_read", "tool": action.action_type, "status": result.get("status"),
                                     "cost_source": "provider" if result.get("usage", {}).get("cost") is not None else "reserved_bound"},
                                    attempt_id=lookup_attempt["id"], cost_usd=cost, status="active")
                            if self._binding and result.get("status") == "success":
                                shared_source = self.awareness_store.source_result(channel.type, channel.id, source.sender,
                                    action.action_type, parameters, result)
                                node_id = shared_source["node_id"]
                                self._binding.nodes[node_id] = {k: v for k, v in shared_source.items() if k != "node_id"}
                                self._binding.fresh_nodes.add(node_id)
                                self._binding.input_versions = self.awareness_store.snapshot_versions(
                                    channel.type, channel.id, source.sender, {k: v for k, v in self._binding.nodes.items() if v["kind"] != "task"})
                                self.awareness_store.save_route(self._binding.task["id"], self._binding.route,
                                    input_versions=self._binding.input_versions)
                            if turn and result.get("status") == "success":
                                self.research_store.save_source(channel.type, channel.id, action.action_type, parameters, result, ttl_seconds=settings.RESEARCH_SOURCE_TTL_SECONDS)
                        path = f"sources.result_{lookups}"
                        sources[path] = {"tool": action.action_type, "trust": "untrusted_source", **result}
                        self.node_manager.update_node_summary(path, f"{action.action_type} result: {result.get('status', 'unknown')}")
                        self.node_manager.expand_node(path)
                        results.append({"tool": action.action_type, "node_path": path, "status": result.get("status")})
                        executed += 1
                    continue
                if self.policy.profile == "matrix_steward":
                    management = [action for action in actions if action.action_type in MANAGEMENT_TOOLS]
                    actions = management[:1] if management else actions
                for action in actions:
                    if action.action_type not in names:
                        continue
                    if action.action_type == "wait" and any(path.startswith(STATE_RESULT_PREFIXES) for path in sources):
                        force_answer = True
                        break
                    if turn and action.action_type in REPLY_TOOLS:
                        result = await self._save_and_send(action, scope, turn, sources)
                    else:
                        if turn and action.action_type in {"manage_matrix_room", "manage_matrix_server"}:
                            if self.policy.denial_reason(action.action_type, action.parameters, scope) is None:
                                if not self.research_store.mark_effect(turn["id"], turn["lease_token"]):
                                    return self._result(executed, lookups, step + 1, failed=True)
                        result = await self.executor._execute_action_and_return_result(action, scope)
                    executed += 1
                    if action.action_type in MANAGEMENT_TOOLS:
                        content = result.get("message", result.get("error", "Matrix action finished"))
                        if result.get("receipt_id"):
                            content += "\n\nReceipt: " + result["receipt_id"][:12]
                        reply = ActionPlan("send_matrix_reply", {
                            "channel_id": scope.channel_id, "reply_to_id": scope.latest_event_id,
                            "content": content, "format_as_markdown": False,
                        }, "Report the actual result", 1)
                        delivery = await self._save_and_send(reply, scope, turn, sources)
                        return self._result(executed + 1, lookups, step + 1, failed=result.get("status") != "success" or delivery.get("status") != "success")
                    if action.action_type in REPLY_TOOLS | {"wait"}:
                        if action.action_type in REPLY_TOOLS and result.get("status") in {"blocked", "failure", "error"}:
                            results.append({"tool": action.action_type, "result": result})
                            force_answer = True
                            break
                        if turn and action.action_type == "wait":
                            self.research_store.finish_without_reply(turn["id"], turn["lease_token"])
                        return self._result(executed, lookups, step + 1, failed=action.action_type in REPLY_TOOLS and result.get("status") != "success")
            return self._result(executed, lookups, step + 1)
        except Exception:
            # A claimed turn never falls through to a second processor after an
            # external action. This prevents duplicate sends after partial failure.
            logger.exception("Node request failed")
            if turn:
                self.research_store.fail(turn["id"], turn["lease_token"], "Research needs another attempt", retry_after=10 * turn["attempts"])
            return self._result(executed, lookups, 0, failed=True)

    def _result(self, executed, lookups, steps, failed=False):
        self.last_result = {"actions_executed": executed, "lookups": lookups, "steps": steps, "failed": failed}
        return self.last_result
