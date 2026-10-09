"""Process one chat request through scoped nodes and bounded source lookups."""

import copy
import json
import logging
import sqlite3
from collections import OrderedDict
from dataclasses import asdict, replace
import time

from ...config import settings
from ..ai_engine import ActionPlan
from ..orchestration.capability_policy import READ_ONLY_SOURCE_TOOLS
from .interaction_tools import NodeInteractionTools
from .node_manager import NodeManager
from .research_store import ResearchStore
from ..world_state.structures import Channel, Message

logger = logging.getLogger(__name__)
NODE_TOOLS = frozenset({"expand_node", "collapse_node", "pin_node", "unpin_node", "get_expansion_status"})
MANAGEMENT_TOOLS = frozenset({"manage_matrix_room", "manage_matrix_server", "matrix_server_status"})
REPLY_TOOLS = frozenset({"send_discord_reply", "send_matrix_reply", "send_farcaster_reply"})


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

    def __init__(self, world_state, payload_builder, executor, db_path, research_store=None):
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

    async def process_cycle(self, cycle_id, primary_channel_id, context=None):
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
        if not self.ai_engine.api_key:
            return {"actions_executed": 0}
        turn = self.research_store.claim(platform, primary_channel_id, lease_seconds=900)
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
        records = [data for path, data in sources.items() if path.startswith("sources.result_")]
        if not self.research_store.ready(turn["id"], turn["lease_token"], text, sources=records):
            return {"status": "blocked", "error": "The saved turn needs a current claim"}
        delivery = self.research_store.claim_delivery(turn["platform"], turn["channel_id"], lease_seconds=900)
        return await self._deliver(delivery) if delivery else {"status": "failure", "error": "Reply is saved for delivery"}

    async def _deliver(self, turn):
        channel = self._saved_channel(turn)
        self._restore_discord(turn)
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

    async def _run_cycle(self, cycle_id, primary_channel_id, context=None, channel=None, turn=None):
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
        channel_path = f"channels.{channel.type}.{channel.id}"
        self.node_manager.expand_node(channel_path)
        self.node_manager.pin_node(channel_path)
        sources = {
            "sources.web": {"description": "Search current public web information with web_search."},
            "sources.pages": {"description": "Read public pages, project docs, or raw GitHub files with read_webpage."},
            "sources.feeds": {"description": "Read news, blogs, or GitHub release RSS/Atom feeds with read_feed."},
        }
        if turn:
            sources["channel.memory"] = self.research_store.memory_node(channel.type, channel.id)
            self.node_manager.expand_node("channel.memory")
            self.node_manager.pin_node("channel.memory")
        scope_payload = self.payload_builder.build_request_node_payload(channel, self.node_manager)
        channel_node = scope_payload["expanded_nodes"][channel_path]["data"]
        scope = self.policy.scope_from_payload(scope_payload)
        allowed = self.policy.filter_tool_names(self.executor.tool_registry.get_tool_names(), scope)
        if turn:
            allowed &= READ_ONLY_SOURCE_TOOLS | REPLY_TOOLS | MANAGEMENT_TOOLS | {"wait"}
        lookups, executed = 0, 0
        results = []
        seen_lookups = set()
        force_answer = False
        try:
            for step in range(self.MAX_STEPS):
                if turn and not self.research_store.heartbeat(turn["id"], turn["lease_token"], lease_seconds=900):
                    return self._result(executed, lookups, step, failed=True)
                final_step = force_answer or step == self.MAX_STEPS - 1 or lookups >= self.MAX_LOOKUPS
                names = allowed - READ_ONLY_SOURCE_TOOLS if final_step else allowed
                payload = self.payload_builder.build_request_node_payload(channel, self.node_manager, sources)
                self.catalog = {channel_path: {}, **sources}
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
                        **sources,
                    }
                    self.last_payload = copy.deepcopy(payload)
                    content = await self.ai_engine.compose_reply(payload)
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
                            result = await self.executor._execute_action_and_return_result(action, scope)
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
