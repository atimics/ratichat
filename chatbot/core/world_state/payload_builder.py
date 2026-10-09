#!/usr/bin/env python3
"""
Payload Builder

This module is responsible for constructing different types of AI payloads from 
WorldStateData. It supports both traditional full payloads and node-based payloads
for handling large datasets efficiently.

The PayloadBuilder implements the separation of concerns principle by moving payload
construction logic out of the WorldStateManager, making the system more modular
and testable.
"""

import json
import logging
import time
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING

from .structures import WorldStateData, Channel

if TYPE_CHECKING:
    from ..node_system.node_manager import NodeManager

logger = logging.getLogger(__name__)


class PayloadBuilder:
    """
    Constructs different types of AI payloads from WorldStateData.
    
    This class provides methods to build:
    1. Full payloads - Complete world state data with intelligent filtering
    2. Node-based payloads - Using expanded/collapsed node system for large datasets
    3. Payload size estimation for strategy selection
    """

    def __init__(self, world_state_manager=None, node_manager=None):
        """
        Initialize PayloadBuilder.
        
        Args:
            world_state_manager: Optional WorldStateManager instance
            node_manager: Optional NodeManager for node-based payloads
        """
        self.world_state_manager = world_state_manager
        self.node_manager = node_manager

    def _build_action_history_payload(self, world_state_data: WorldStateData, max_history: int, optimize: bool) -> List[Dict[str, Any]]:
        """Builds a consistent action history payload."""
        history = world_state_data.action_history[-max_history:]
        if optimize:
            return [
                {
                    "action_type": action.action_type,
                    "result": (action.result[:100] + "..." if action.result and len(action.result) > 100 else action.result),
                    "timestamp": action.timestamp,
                }
                for action in history
            ]
        else:
            return [asdict(action) for action in history]

    def _build_thread_context(
        self,
        world_state_data: WorldStateData,
        primary_channel_id: Optional[str],
        max_messages: int,
        optimize: bool,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Build a focused thread context for the primary channel."""
        if not primary_channel_id:
            return {}

        thread_context: Dict[str, List[Dict[str, Any]]] = {}
        primary_channel = world_state_data.channels.get(primary_channel_id)

        if not primary_channel or not primary_channel.recent_messages:
            return {}

        # Gather all unique thread IDs from the primary channel's recent messages
        thread_ids_in_channel: Set[str] = set()
        for msg in primary_channel.recent_messages:
            thread_id = msg.reply_to or msg.id
            if thread_id:
                thread_ids_in_channel.add(thread_id)

        # Build the context for each relevant thread
        for thread_id in thread_ids_in_channel:
            if thread_id in world_state_data.threads:
                # Sort messages chronologically and take the last N
                thread_messages = sorted(
                    world_state_data.threads[thread_id], key=lambda m: m.timestamp
                )[-max_messages:]

                if optimize:
                    thread_context[thread_id] = [m.to_ai_summary_dict() for m in thread_messages]
                else:
                    thread_context[thread_id] = [asdict(m) for m in thread_messages]
        
        return thread_context

    def build_full_payload(
        self,
        world_state_data: WorldStateData,
        primary_channel_id: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build a traditional full payload from world state data with size optimizations.
        
        This is the original approach that includes complete world state information
        with intelligent filtering and prioritization based on channel activity.
        
        Args:
            world_state_data: The world state data to convert
            primary_channel_id: Channel to prioritize in the payload
            config: Configuration dict with options like:
                - max_messages_per_channel: Max messages to include per channel
                - max_action_history: Max action history items
                - max_thread_messages: Max thread messages
                - max_other_channels: Max non-primary channels with full detail
                - message_snippet_length: Length for message truncation
                - include_detailed_user_info: Whether to include full user data
                - bot_fid: Bot's Farcaster ID for message filtering
                - bot_username: Bot's username for message filtering
        
        Returns:
            Dictionary optimized for AI consumption
        """
        # Set default config values, falling back to global settings
        from chatbot.config import settings
        if config is None: config = {}
        
        max_messages_per_channel = config.get("max_messages_per_channel", settings.AI_CONVERSATION_HISTORY_LENGTH)
        max_action_history = config.get("max_action_history", settings.AI_ACTION_HISTORY_LENGTH)
        max_thread_messages = config.get("max_thread_messages", settings.AI_THREAD_HISTORY_LENGTH)
        max_other_channels = config.get("max_other_channels", settings.AI_OTHER_CHANNELS_SUMMARY_COUNT)
        message_snippet_length = config.get("message_snippet_length", settings.AI_OTHER_CHANNELS_MESSAGE_SNIPPET_LENGTH)
        include_detailed_user_info = config.get("include_detailed_user_info", settings.AI_INCLUDE_DETAILED_USER_INFO)
        optimize_for_size = config.get("optimize_for_size", True)
        bot_fid = settings.FARCASTER_BOT_FID
        bot_username = settings.FARCASTER_BOT_USERNAME

        # Sort channels with improved cross-platform balance
        active_integrations = set()
        current_time = time.time()
        recent_threshold = current_time - (30 * 60)  # 30 minutes
        for ch_id, ch_data in world_state_data.channels.items():
            if ch_data.recent_messages and ch_data.recent_messages[-1].timestamp > recent_threshold:
                active_integrations.add(ch_data.type)
        
        def sort_key(channel_item):
            ch_id, ch_data = channel_item
            last_activity = ch_data.recent_messages[-1].timestamp if ch_data.recent_messages else 0
            if ch_id == primary_channel_id: return (0, -last_activity)
            if ch_data.type == "farcaster" and ("home" in ch_id or "notification" in ch_id): return (1, -last_activity)
            platform_boost = 2 if ch_data.type == "farcaster" else 3
            return (platform_boost, -last_activity)
        
        sorted_channels = sorted(world_state_data.channels.items(), key=sort_key)

        channels_payload = {}
        detailed_count = 0
        for ch_id, ch_data in sorted_channels:
            is_primary = ch_id == primary_channel_id
            is_key_farcaster = ch_data.type == "farcaster" and ("home" in ch_id or "notification" in ch_id or "reply" in ch_id)
            include_detailed = is_primary or is_key_farcaster or (detailed_count < max_other_channels)

            if include_detailed and ch_data.recent_messages:
                truncated_messages = ch_data.recent_messages[-max_messages_per_channel:]
                messages_for_payload = []
                for msg in truncated_messages:
                    has_replied = world_state_data.has_replied_to_cast(msg.id)
                    msg_dict = msg.to_ai_summary_dict() if optimize_for_size else asdict(msg)
                    msg_dict['already_replied'] = has_replied
                    messages_for_payload.append(msg_dict)
                
                channels_payload[ch_id] = {
                    "id": ch_data.id,
                    "type": ch_data.type,
                    "name": ch_data.name,
                    "recent_messages": messages_for_payload
                }
                if not is_primary and not is_key_farcaster:
                    detailed_count += 1
            else:
                # Include a summary payload for channels without recent messages, retaining essential identifiers
                summary = ch_data.get_activity_summary()
                # Ensure type, id, and name are always present for cross-platform awareness
                summary["id"] = ch_data.id
                summary["type"] = ch_data.type
                summary["name"] = ch_data.name
                channels_payload[ch_id] = summary

        action_history_payload = self._build_action_history_payload(world_state_data, max_action_history, optimize_for_size)
        thread_context_payload = self._build_thread_context(world_state_data, primary_channel_id, max_thread_messages, optimize_for_size)

        payload = {
            "current_processing_channel_id": primary_channel_id,
            "channels": channels_payload,
            "action_history": action_history_payload,
            "thread_context": thread_context_payload,
            "system_status": {**world_state_data.system_status, "rate_limits": world_state_data.rate_limits},
            "pending_matrix_invites": world_state_data.pending_matrix_invites,
            "payload_stats": {
                "primary_channel": primary_channel_id,
                "detailed_channels": detailed_count,
                "summary_channels": len(sorted_channels) - detailed_count,
                "bot_identity": {"fid": bot_fid, "username": bot_username},
            }
        }

        if not optimize_for_size:
            payload.update({
                "generated_media_library": [asdict(m) for m in world_state_data.generated_media_library[-10:]],
                "ecosystem_token_info": {
                    "contract_address": world_state_data.ecosystem_token_contract,
                    "token_metadata": asdict(world_state_data.token_metadata) if world_state_data.token_metadata else None,
                    "monitored_holders_activity": [asdict(h) for h in world_state_data.monitored_token_holders.values()]
                },
                "research_knowledge": {
                    "available_topics": list(world_state_data.research_database.keys()),
                    "topic_count": len(world_state_data.research_database),
                    "note": "Use query_research tool to access detailed research information"
                },
            })

        # Add user profiling data (Initiative B: Enhanced User Profiling Implementation)
        user_profiling_data = self._build_user_profiling_payload(world_state_data, optimize_for_size)
        if user_profiling_data:
            payload["user_profiling"] = user_profiling_data

        return payload

    def build_node_based_payload(
        self,
        world_state_data: WorldStateData,
        node_manager: "NodeManager",
        primary_channel_id: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build a node-based payload using the expanded/collapsed node system.
        
        This approach shows expanded nodes in full detail and collapsed nodes as
        AI-generated summaries, allowing for interactive exploration of large datasets.
        
        Args:
            world_state_data: The world state data to convert
            node_manager: Node manager for expansion/collapse state
            primary_channel_id: Primary channel being processed
            config: Configuration dict with options like:
                - bot_fid: Bot's Farcaster ID
                - bot_username: Bot's username
        
        Returns:
            Dictionary with node-based structure for AI consumption
        """
        # Set default config values
        if config is None:
            config = {}
        
        bot_fid = config.get("bot_fid")
        bot_username = config.get("bot_username")

        # Start with system status and core info
        payload = {
            "current_processing_channel_id": primary_channel_id,
            "system_status": {
                "timestamp": world_state_data.last_update,
                "rate_limits": world_state_data.rate_limits
            }
        }
        
        # Get all available node paths from world state
        all_node_paths = self._get_node_paths_from_world_state(world_state_data)
        
        # Separate expanded and collapsed nodes
        expanded_nodes = {}
        collapsed_node_summaries = {}
        
        for node_path in all_node_paths:
            metadata = node_manager.get_node_metadata(node_path)
            node_data = self._get_node_data_by_path(world_state_data, node_path)
            
            if node_data is None:
                continue
            
            if metadata.is_expanded:
                # Include full data for expanded nodes
                expanded_nodes[node_path] = {
                    "data": node_data,
                    "is_pinned": metadata.is_pinned,
                    "last_expanded": metadata.last_expanded_ts
                }
            else:
                # Include summary for collapsed nodes
                summary = metadata.ai_summary or f"Node {node_path} (no summary available)"
                
                # Check if data has changed since last summary
                data_changed = node_manager.is_data_changed(node_path, node_data)
                
                collapsed_node_summaries[node_path] = {
                    "summary": summary,
                    "data_changed": data_changed,
                    "last_summary_update": metadata.last_summary_update_ts
                }
        
        payload["expanded_nodes"] = expanded_nodes
        payload["collapsed_node_summaries"] = collapsed_node_summaries
        
        # Add expansion status info
        payload["expansion_status"] = node_manager.get_expansion_status_summary()
        
        # Add system events (auto-collapses, etc.)
        payload["system_events"] = node_manager.get_system_events()
        
        # Calculate payload size
        payload_size = len(json.dumps(payload, default=str).encode('utf-8'))
        payload["payload_stats"] = {
            "size_bytes": payload_size,
            "size_kb": payload_size / 1024,
            "expanded_nodes_count": len(expanded_nodes),
            "collapsed_nodes_count": len(collapsed_node_summaries),
            "total_nodes": len(all_node_paths),
            "bot_identity": {"fid": bot_fid, "username": bot_username},
        }
        
        return payload

    def build_request_node_payload(self, channel, node_manager, source_nodes=None):
        """Build a node view from one trusted chat channel and its fetched sources."""
        from ...config import settings

        messages = [
            {"id": message.id, "sender_id": message.sender,
             "sender": message.sender_display_name or message.sender_username or message.sender,
             "content": message.content[:4000], "timestamp": message.timestamp}
            for message in channel.recent_messages[-settings.AI_CONVERSATION_HISTORY_LENGTH:]
        ]
        channel_path = f"channels.{channel.type}.{channel.id}"
        sources = dict(source_nodes or {})
        observed = sources.pop(channel_path, None)
        current = {"id": channel.id, "type": channel.type,
                   "name": channel.name, "recent_messages": messages}
        if observed and observed.get("kind") == "conversation":
            current["observed_messages"] = observed["data"].get("messages", [])
            current["awareness_version"] = observed["version"]
            current["audience"] = observed["audience"]
        catalog = {channel_path: current, **sources}
        expanded, collapsed = {}, {}
        for path, data in catalog.items():
            metadata = node_manager.get_node_metadata(path)
            if metadata.is_expanded:
                expanded[path] = {"data": data, "is_pinned": metadata.is_pinned}
            else:
                collapsed[path] = {"summary": metadata.ai_summary or data.get("description", f"Fetched source {path}")}
        return {
            "processing_mode": "node_based",
            "current_processing_channel_id": channel.id,
            "current_request": messages[-1] if messages else {},
            # Action authority comes from observer IDs, separate from source text.
            "channels": {channel.id: {"type": channel.type, "recent_messages": [
                {key: message[key] for key in ("id", "sender_id", "timestamp")}
                for message in messages
            ]}},
            "expanded_nodes": expanded,
            "collapsed_node_summaries": collapsed,
            "expansion_status": node_manager.get_expansion_status_summary(),
        }

    @staticmethod
    def estimate_payload_size(
        world_state_data: WorldStateData,
        config: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Estimate the size of a full payload in bytes.
        
        This provides a heuristic estimate without building the full payload,
        useful for deciding between full and node-based processing strategies.
        
        Args:
            world_state_data: The world state data to estimate
            config: Configuration options (currently unused but reserved)
        
        Returns:
            Estimated payload size in bytes
        """
        try:
            metrics = world_state_data.get_state_metrics()
            
            # Use heuristic calculations based on typical data sizes
            estimated_size = (
                metrics["channel_count"] * 1500 +       # ~1.5KB per channel
                metrics["total_messages"] * 800 +       # ~0.8KB per message  
                metrics["action_history_count"] * 500 + # ~0.5KB per action
                metrics["thread_count"] * 300 +         # ~0.3KB per thread
                len(world_state_data.generated_media_library) * 200 +  # ~0.2KB per media item
                5000  # 5KB base overhead
            )
            
            return int(estimated_size)
            
        except Exception as e:
            logger.error(f"Error estimating payload size: {e}")
            # Return conservative estimate if calculation fails
            return 50000  # 50KB fallback

    def _get_node_paths_from_world_state(self, world_state_data: WorldStateData) -> List[str]:
        """
        Extract all available node paths from the world state data.
        
        Args:
            world_state_data: The world state to extract paths from
            
        Returns:
            List of node paths that can be expanded/collapsed
        """
        paths = []
        
        # Channel nodes
        for channel_id, channel in world_state_data.channels.items():
            paths.append(f"channels.{channel.type}.{channel_id}")
        
        # Farcaster feed nodes (always available if Farcaster is active)
        has_farcaster = any(ch.type == "farcaster" for ch in world_state_data.channels.values())
        if has_farcaster:
            paths.extend([
                "farcaster.feeds.home",
                "farcaster.feeds.notifications",
                "farcaster.feeds.trending"
            ])
        
        # Enhanced user nodes with cached data
        user_fids = set()
        user_usernames = set()
        
        for channel in world_state_data.channels.values():
            for msg in channel.recent_messages[-10:]:  # Recent users
                if msg.sender_fid:
                    user_fids.add(msg.sender_fid)
                if msg.sender_username:
                    user_usernames.add(msg.sender_username)
        
        # Add users from enhanced user tracking
        for fid in world_state_data.farcaster_users.keys():
            user_fids.add(fid)
        
        for user_id in world_state_data.matrix_users.keys():
            user_usernames.add(user_id)
        
        # Create user node paths with enhanced data
        for fid in user_fids:
            base_path = f"users.farcaster.{fid}"
            paths.append(base_path)
            
            # Add sub-nodes for cached data
            if fid in world_state_data.farcaster_users:
                user = world_state_data.farcaster_users[fid]
                if user.timeline_cache:
                    paths.append(f"{base_path}.timeline_cache")
                if user.sentiment:
                    paths.append(f"{base_path}.sentiment")
                if user.memory_entries:
                    paths.append(f"{base_path}.memories")
        
        for username in user_usernames:
            base_path = f"users.matrix.{username}"
            paths.append(base_path)
            
            # Add sub-nodes for enhanced Matrix user data
            if username in world_state_data.matrix_users:
                user = world_state_data.matrix_users[username]
                if user.sentiment:
                    paths.append(f"{base_path}.sentiment")
                if user.memory_entries:
                    paths.append(f"{base_path}.memories")
        
        # Optimized tool cache nodes - only include if substantial data exists
        if world_state_data.tool_cache and len(world_state_data.tool_cache) > 1:
            paths.append("tools.cache")
            # Only add specific tool nodes for frequently used tools
            tool_counts = {}
            for cache_key in world_state_data.tool_cache.keys():
                tool_name = cache_key.split(":")[0] if ":" in cache_key else cache_key
                tool_counts[tool_name] = tool_counts.get(tool_name, 0) + 1
            
            # Only include tools with multiple cached results
            for tool_name, count in tool_counts.items():
                if count > 1:  # Only include if multiple cached results
                    paths.append(f"tools.cache.{tool_name}")
        
        # Optimized search cache nodes - limit to recent searches
        if world_state_data.search_cache and len(world_state_data.search_cache) > 0:
            paths.append("farcaster.search_cache")
            # Only include recent search hashes (limit to 3 most recent)
            recent_searches = sorted(
                world_state_data.search_cache.items(),
                key=lambda x: x[1].get("timestamp", 0),
                reverse=True
            )[:3]
            for query_hash, _ in recent_searches:
                paths.append(f"farcaster.search_cache.{query_hash}")
        
        # Optimized memory bank nodes - only include platforms with significant memories
        if world_state_data.user_memory_bank:
            platform_memory_counts = {}
            for user_platform_id, memories in world_state_data.user_memory_bank.items():
                platform = user_platform_id.split(":")[0] if ":" in user_platform_id else "unknown"
                platform_memory_counts[platform] = platform_memory_counts.get(platform, 0) + len(memories)
            
            # Only include memory bank if there are substantial memories
            if sum(platform_memory_counts.values()) > 5:
                paths.append("memory_bank")
                for platform, count in platform_memory_counts.items():
                    if count > 2:  # Only include platforms with multiple memories
                        paths.append(f"memory_bank.{platform}")
        
        # Optimized thread nodes - limit to active threads only
        if world_state_data.threads:
            active_threads = []
            current_time = time.time()
            for thread_id, msgs in world_state_data.threads.items():
                if msgs and msgs[-1].timestamp > (current_time - 7200):  # Active in last 2 hours
                    active_threads.append(thread_id)
            
            # Only include recent active threads (limit to 3)
            for thread_id in active_threads[:3]:
                paths.append(f"threads.farcaster.{thread_id}")
        
        # Essential system nodes only
        paths.extend([
            "system.rate_limits", 
            "system.status"
        ])
        
        # Only include notifications if there are pending invites
        if world_state_data.pending_matrix_invites:
            paths.append("system.notifications")
        
        # Only include action history node for node system
        paths.append("system.action_history")
        
        return paths

    def _get_node_data_by_path(self, world_state_data: WorldStateData, node_path: str) -> Any:
        """
        Get the actual data for a specific node path.
        
        Args:
            world_state_data: The world state to extract data from
            node_path: The path to the node (e.g., "channels.matrix.!room_id")
        
        Returns:
            The data for that node, or None if not found
        """
        try:
            path_parts = node_path.split(".")
            
            if len(path_parts) < 2:
                return None
            
            if path_parts[0] == "channels" and len(path_parts) >= 3:
                channel_type, channel_id = path_parts[1], path_parts[2]
                channel = world_state_data.channels.get(channel_id)
                if channel and channel.type == channel_type:
                    return {
                        "id": channel.id,
                        "name": channel.name[:30] + "..." if len(channel.name) > 30 else channel.name,
                        "type": channel.type,
                        "status": channel.status,
                        "recent_messages": [
                            {
                                "id": msg.id,
                                "content": msg.content[:100] + "..." if len(msg.content) > 100 else msg.content,
                                "sender": msg.sender_username or msg.sender,
                                "timestamp": msg.timestamp,
                                "has_images": bool(getattr(msg, 'image_urls', []))
                            }
                            for msg in channel.recent_messages[-5:]  # Reduced from 10 to 5
                        ],
                        "msg_count": len(channel.recent_messages),
                        "last_activity": channel.recent_messages[-1].timestamp if channel.recent_messages else channel.last_checked
                    }
            
            elif path_parts[0] == "users" and len(path_parts) >= 3:
                user_type, user_id = path_parts[1], path_parts[2]
                
                # Handle enhanced user data with sub-nodes
                if user_type == "farcaster":
                    farcaster_user = world_state_data.farcaster_users.get(user_id)
                    if farcaster_user:
                        # Check for sub-node requests
                        if len(path_parts) == 4:
                            sub_node = path_parts[3]
                            if sub_node == "timeline_cache" and farcaster_user.timeline_cache:
                                return farcaster_user.timeline_cache
                            elif sub_node == "sentiment" and farcaster_user.sentiment:
                                return asdict(farcaster_user.sentiment)
                            elif sub_node == "memories" and farcaster_user.memory_entries:
                                return [asdict(memory) for memory in farcaster_user.memory_entries[-5:]]
                        
                        # Return compact user data
                        user_data = {
                            "fid": farcaster_user.fid,
                            "username": farcaster_user.username,
                            "display_name": farcaster_user.display_name,
                            "follower_count": farcaster_user.follower_count,
                            "power_badge": farcaster_user.power_badge
                        }
                        # Truncate bio for display
                        if farcaster_user.bio and len(farcaster_user.bio) > 50:
                            user_data["bio"] = farcaster_user.bio[:50] + "..."
                        elif farcaster_user.bio:
                            user_data["bio"] = farcaster_user.bio
                        return user_data
                    else:
                        # Fallback - compact extraction from messages
                        user_info = {"type": user_type, "id": user_id}
                        for channel in world_state_data.channels.values():
                            for msg in channel.recent_messages[-3:]:  # Reduced from 5 to 3
                                if str(msg.sender_fid) == user_id:
                                    bio = msg.sender_bio
                                    if bio and len(bio) > 50:
                                        bio = bio[:50] + "..."
                                    user_info.update({
                                        "username": msg.sender_username,
                                        "display_name": msg.sender_display_name,
                                        "fid": msg.sender_fid,
                                        "follower_count": msg.sender_follower_count,
                                        "bio": bio if bio else None
                                    })
                                    break
                        return user_info
                
                elif user_type == "matrix":
                    matrix_user = world_state_data.matrix_users.get(user_id)
                    if matrix_user:
                        # Check for sub-node requests
                        if len(path_parts) == 4:
                            sub_node = path_parts[3]
                            if sub_node == "sentiment" and matrix_user.sentiment:
                                return asdict(matrix_user.sentiment)
                            elif sub_node == "memories" and matrix_user.memory_entries:
                                return [asdict(memory) for memory in matrix_user.memory_entries[-3:]]  # Reduced from 5 to 3
                        
                        # Return compact user info
                        return {
                            "user_id": matrix_user.user_id,
                            "display_name": matrix_user.display_name,
                            "avatar_url": matrix_user.avatar_url
                        }
                    else:
                        # Fallback - compact extraction from messages
                        user_info = {"type": user_type, "id": user_id}
                        for channel in world_state_data.channels.values():
                            for msg in channel.recent_messages[-3:]:  # Reduced from 5 to 3
                                if msg.sender_username == user_id:
                                    user_info.update({
                                        "username": msg.sender_username,
                                        "display_name": msg.sender_display_name
                                    })
                                    break
                        return user_info
            
            elif path_parts[0] == "tools" and len(path_parts) >= 2:
                if path_parts[1] == "cache":
                    if len(path_parts) == 2:
                        # Return compact overview of cached tools
                        tool_summary = {}
                        for cache_key, cache_data in list(world_state_data.tool_cache.items())[:10]:  # Limit to 10 entries
                            tool_name = cache_key.split(":")[0] if ":" in cache_key else cache_key
                            if tool_name not in tool_summary:
                                tool_summary[tool_name] = {
                                    "count": 0,
                                    "most_recent": 0
                                }
                            tool_summary[tool_name]["count"] += 1
                            tool_summary[tool_name]["most_recent"] = max(
                                tool_summary[tool_name]["most_recent"],
                                cache_data.get("timestamp", 0)
                            )
                        return {
                            "cached_tools": tool_summary,
                            "total_entries": len(world_state_data.tool_cache)
                        }
                    elif len(path_parts) == 3:
                        # Return compact cached results for specific tool
                        tool_name = path_parts[2]
                        tool_results = {}
                        count = 0
                        for cache_key, cache_data in world_state_data.tool_cache.items():
                            if cache_key.startswith(f"{tool_name}:") and count < 3:  # Limit to 3 results
                                # Return only essential cache data
                                tool_results[cache_key] = {
                                    "timestamp": cache_data.get("timestamp"),
                                    "result_type": cache_data.get("result_type"),
                                    "size": len(str(cache_data)) if cache_data else 0
                                }
                                count += 1
                        return {
                            "tool_name": tool_name,
                            "cached_results": tool_results,
                            "total_cached": count
                        }
            
            elif path_parts[0] == "memory_bank":
                if len(path_parts) == 1:
                    # Return overview of memory bank
                    memory_stats = {}
                    for user_platform_id, memories in world_state_data.user_memory_bank.items():
                        platform = user_platform_id.split(":")[0] if ":" in user_platform_id else "unknown"
                        if platform not in memory_stats:
                            memory_stats[platform] = {"users": 0, "total_memories": 0}
                        memory_stats[platform]["users"] += 1
                        memory_stats[platform]["total_memories"] += len(memories)
                    return {
                        "platform_breakdown": memory_stats,
                        "total_users_with_memories": len(world_state_data.user_memory_bank)
                    }
                elif len(path_parts) == 2:
                    # Return memories for specific platform
                    platform = path_parts[1]
                    platform_memories = {}
                    for user_platform_id, memories in world_state_data.user_memory_bank.items():
                        if user_platform_id.startswith(f"{platform}:"):
                            platform_memories[user_platform_id] = [
                                {
                                    "memory_id": mem.memory_id,
                                    "content": mem.content[:100] + "..." if len(mem.content) > 100 else mem.content,
                                    "memory_type": mem.memory_type,
                                    "importance": mem.importance,
                                    "timestamp": mem.timestamp
                                }
                                for mem in memories[-3:]  # Recent memories
                            ]
                    return {
                        "platform": platform,
                        "user_memories": platform_memories
                    }
            
            elif path_parts[0] == "farcaster" and len(path_parts) >= 2:
                if path_parts[1] == "search_cache":
                    if len(path_parts) == 2:
                        # Return overview of search cache
                        search_overview = {}
                        for query_hash, search_data in world_state_data.search_cache.items():
                            search_overview[query_hash] = {
                                "query": search_data.get("query", "Unknown"),
                                "channel_id": search_data.get("channel_id"),
                                "result_count": search_data.get("result_count", 0),
                                "timestamp": search_data.get("timestamp", 0)
                            }
                        return {
                            "cached_searches": search_overview,
                            "total_searches": len(world_state_data.search_cache)
                        }
                    elif len(path_parts) == 3:
                        # Return specific search results
                        query_hash = path_parts[2]
                        search_data = world_state_data.search_cache.get(query_hash)
                        if search_data:
                            return search_data
                        return {"error": f"Search cache not found for hash: {query_hash}"}
                
                # Handle farcaster.feeds.* nodes
                elif len(path_parts) >= 3 and path_parts[1] == "feeds":
                    feed_type = path_parts[2]
                    
                    if feed_type == "home":
                        # Aggregate recent activity from all Farcaster channels
                        home_messages = []
                        for channel in world_state_data.channels.values():
                            if channel.type == "farcaster" and "home" in channel.id:
                                home_messages.extend(channel.recent_messages[-5:])
                        
                        # Sort by timestamp and take most recent
                        home_messages.sort(key=lambda x: x.timestamp, reverse=True)
                        return {
                            "feed_type": "home",
                            "recent_activity": [msg.to_ai_summary_dict() for msg in home_messages[:10]],
                            "activity_summary": f"{len(home_messages)} recent home feed messages"
                        }
                    
                    elif feed_type == "notifications":
                        # Find notification/mention related messages
                        notification_messages = []
                        for channel in world_state_data.channels.values():
                            if channel.type == "farcaster" and ("notification" in channel.id or "mention" in channel.id):
                                notification_messages.extend(channel.recent_messages[-5:])
                        
                        notification_messages.sort(key=lambda x: x.timestamp, reverse=True)
                        return {
                            "feed_type": "notifications",
                            "recent_mentions": [msg.to_ai_summary_dict() for msg in notification_messages[:10]],
                            "notification_summary": f"{len(notification_messages)} recent notifications/mentions"
                        }
                    
                    elif feed_type == "trending":
                        # Placeholder for trending feed data
                        return {
                            "feed_type": "trending",
                            "status": "Available for expansion via get_trending_casts tool",
                            "note": "Use get_trending_casts to fetch current trending content"
                        }
            
            elif path_parts[0] == "threads" and len(path_parts) >= 3:
                thread_type, thread_id = path_parts[1], path_parts[2]
                thread_messages = world_state_data.threads.get(thread_id, [])
                return {
                    "thread_id": thread_id,
                    "type": thread_type,
                    "messages": [asdict(msg) for msg in thread_messages[-5:]]  # Recent thread messages
                }
            
            elif path_parts[0] == "system":
                if len(path_parts) >= 2:
                    system_component = path_parts[1]
                    
                    if system_component == "notifications":
                        return {"pending_matrix_invites": world_state_data.pending_matrix_invites}
                    elif system_component == "rate_limits":
                        return world_state_data.rate_limits
                    elif system_component == "status":
                        return world_state_data.system_status
                    elif system_component == "action_history":
                        return [asdict(action) for action in world_state_data.action_history[-10:]]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting data for node path {node_path}: {e}")
            return None

    def _build_user_profiling_payload(self, world_state_data: WorldStateData, optimize_for_size: bool) -> Dict[str, Any]:
        """
        Build user profiling data for inclusion in AI payloads.
        
        Args:
            world_state_data: The world state data to extract profiling info from
            optimize_for_size: Whether to optimize payload size
            
        Returns:
            Dictionary containing user profiling data or empty dict if no data
        """
        user_profiling = {}
        
        # Extract Farcaster user profiles with sentiment and memories
        farcaster_users = getattr(world_state_data, 'farcaster_users', {})
        if farcaster_users:
            farcaster_profiles = {}
            for fid, user in farcaster_users.items():
                profile = {
                    "username": getattr(user, 'username', None),
                    "display_name": getattr(user, 'display_name', None),
                    "follower_count": getattr(user, 'follower_count', 0),
                }
                
                # Add bio (truncated if optimizing)
                bio = getattr(user, 'bio', None)
                if bio:
                    if optimize_for_size and len(bio) > 50:
                        profile["bio"] = bio[:50] + "..."
                    else:
                        profile["bio"] = bio
                
                # Add sentiment data
                sentiment = getattr(user, 'sentiment', None)
                if sentiment:
                    profile["sentiment"] = {
                        "current_sentiment": getattr(sentiment, 'current_sentiment', None),
                        "sentiment_score": getattr(sentiment, 'sentiment_score', 0.0),
                        "message_count": getattr(sentiment, 'message_count', 0),
                        "last_interaction": getattr(sentiment, 'last_interaction_time', None)
                    }
                
                # Add recent memories (limited for optimization)
                memory_entries = getattr(user, 'memory_entries', [])
                if memory_entries:
                    memory_limit = 3 if optimize_for_size else 5
                    profile["recent_memories"] = [
                        {
                            "content": mem.content[:100] + "..." if optimize_for_size and len(mem.content) > 100 else mem.content,
                            "memory_type": mem.memory_type,
                            "importance": mem.importance,
                            "timestamp": mem.timestamp
                        }
                        for mem in memory_entries[-memory_limit:]
                    ]
                
                farcaster_profiles[fid] = profile
            
            if farcaster_profiles:
                user_profiling["farcaster_users"] = farcaster_profiles
        
        # Extract Matrix user profiles with sentiment and memories
        matrix_users = getattr(world_state_data, 'matrix_users', {})
        if matrix_users:
            matrix_profiles = {}
            for user_id, user in matrix_users.items():
                profile = {
                    "user_id": getattr(user, 'user_id', user_id),
                    "display_name": getattr(user, 'display_name', None),
                }
                
                # Add sentiment data
                sentiment = getattr(user, 'sentiment', None)
                if sentiment:
                    profile["sentiment"] = {
                        "current_sentiment": getattr(sentiment, 'current_sentiment', None),
                        "sentiment_score": getattr(sentiment, 'sentiment_score', 0.0),
                        "message_count": getattr(sentiment, 'message_count', 0),
                        "last_interaction": getattr(sentiment, 'last_interaction_time', None)
                    }
                
                # Add recent memories (limited for optimization)
                memory_entries = getattr(user, 'memory_entries', [])
                if memory_entries:
                    memory_limit = 3 if optimize_for_size else 5
                    profile["recent_memories"] = [
                        {
                            "content": mem.content[:100] + "..." if optimize_for_size and len(mem.content) > 100 else mem.content,
                            "memory_type": mem.memory_type,
                            "importance": mem.importance,
                            "timestamp": mem.timestamp
                        }
                        for mem in memory_entries[-memory_limit:]
                    ]
                
                matrix_profiles[user_id] = profile
            
            if matrix_profiles:
                user_profiling["matrix_users"] = matrix_profiles
        
        # Add memory bank overview
        memory_bank = getattr(world_state_data, 'user_memory_bank', {})
        if memory_bank:
            memory_stats = {}
            total_memories = 0
            for user_platform_id, memories in memory_bank.items():
                platform = user_platform_id.split(":")[0] if ":" in user_platform_id else "unknown"
                if platform not in memory_stats:
                    memory_stats[platform] = {"users": 0, "memories": 0}
                memory_stats[platform]["users"] += 1
                memory_stats[platform]["memories"] += len(memories)
                total_memories += len(memories)
            
            user_profiling["memory_bank_stats"] = {
                "total_memories": total_memories,
                "platforms": memory_stats,
                "note": "Use get_user_profile tool to access detailed user memories"
            }
        
        # Add profiling tools availability
        if user_profiling:
            user_profiling["available_tools"] = [
                "analyze_user_sentiment - Analyze sentiment from user messages",
                "store_user_memory - Store important user information",
                "get_user_profile - Retrieve comprehensive user profile data"
            ]
        
        return user_profiling
