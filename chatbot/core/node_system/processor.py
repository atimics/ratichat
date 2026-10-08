"""Process one chat request through scoped nodes and bounded source lookups."""

import copy
import json
import logging
import sqlite3
from collections import OrderedDict
from dataclasses import replace

from ...config import settings
from ..ai_engine import ActionPlan
from ..orchestration.capability_policy import READ_ONLY_SOURCE_TOOLS
from .interaction_tools import NodeInteractionTools
from .node_manager import NodeManager

logger = logging.getLogger(__name__)
NODE_TOOLS = frozenset({"expand_node", "collapse_node", "pin_node", "unpin_node", "get_expansion_status"})
MANAGEMENT_TOOLS = frozenset({"manage_matrix_room", "manage_matrix_server", "matrix_server_status"})
REPLY_TOOLS = frozenset({"send_discord_reply", "send_matrix_reply", "send_farcaster_reply"})


class NodeProcessor:
    MAX_STEPS = 5
    MAX_LOOKUPS = 3

    def __init__(self, world_state, payload_builder, executor, db_path):
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

    async def process_cycle(self, cycle_id, primary_channel_id, context=None):
        channel = self.world_state.get_channel(primary_channel_id)
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
        if not self._claim(channel.id, source.id):
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
        scope_payload = self.payload_builder.build_request_node_payload(channel, self.node_manager)
        scope = self.policy.scope_from_payload(scope_payload)
        allowed = self.policy.filter_tool_names(self.executor.tool_registry.get_tool_names(), scope)
        lookups, executed = 0, 0
        results = []
        seen_lookups = set()
        try:
            for step in range(self.MAX_STEPS):
                final_step = step == self.MAX_STEPS - 1 or lookups >= self.MAX_LOOKUPS
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
                if scope.channel_type == "matrix" and self.policy.profile == "matrix_steward":
                    payload["matrix_management"] = {
                        "request_event_id": scope.latest_event_id,
                        "managed_room_ids": sorted(self.policy.managed_room_ids),
                    }
                self.last_payload = copy.deepcopy(payload)
                decision = await self.ai_engine.make_decision(payload, f"{cycle_id}_step_{step}")
                actions = decision.selected_actions[:3]
                if not actions:
                    break
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
                        result = await self.executor._execute_action_and_return_result(action, scope)
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
                    result = await self.executor._execute_action_and_return_result(action, scope)
                    executed += 1
                    if action.action_type in MANAGEMENT_TOOLS:
                        if result.get("status") != "blocked":
                            content = result.get("message", result.get("error", "Matrix action finished"))
                            if result.get("receipt_id"):
                                content += "\n\nReceipt: " + result["receipt_id"][:12]
                            reply = ActionPlan("send_matrix_reply", {
                                "channel_id": scope.channel_id, "reply_to_id": scope.latest_event_id,
                                "content": content, "format_as_markdown": False,
                            }, "Report the actual result", 1)
                            await self.executor._execute_action_and_return_result(reply, scope)
                        return self._result(executed, lookups, step + 1)
                    if action.action_type in REPLY_TOOLS | {"wait"}:
                        return self._result(executed, lookups, step + 1)
            return self._result(executed, lookups, step + 1)
        except Exception:
            # A claimed turn never falls through to a second processor after an
            # external action. This prevents duplicate sends after partial failure.
            logger.exception("Node request failed")
            return self._result(executed, lookups, 0, failed=True)

    def _result(self, executed, lookups, steps, failed=False):
        self.last_result = {"actions_executed": executed, "lookups": lookups, "steps": steps, "failed": failed}
        return self.last_result
