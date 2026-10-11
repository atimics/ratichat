"""
Node Manager for JSON Observer and Interactive Executor Pattern

This module provides the core infrastructure for managing expandable/collapsible nodes
in the WorldState with LRU auto-collapse functionality and pinning support.
"""

import hashlib
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from datetime import datetime


@dataclass
class SystemEvent:
    """Represents a system event in node management."""
    timestamp: float
    event_type: str
    message: str
    affected_nodes: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "message": self.message,
            "affected_nodes": self.affected_nodes,
            "time_str": datetime.fromtimestamp(self.timestamp).strftime("%H:%M:%S")
        }


@dataclass
class NodeMetadata:
    """Metadata for a single node in the expandable/collapsible tree."""
    is_expanded: bool = False
    is_pinned: bool = False
    ai_summary: Optional[str] = None
    last_summary_update_ts: Optional[float] = None
    last_expanded_ts: Optional[float] = None
    data_hash: Optional[str] = None
    summary_source_hash: Optional[str] = None
    summary_checked_at: Optional[float] = None
    summary_links: List[str] = field(default_factory=list)
    
    def update_expanded_timestamp(self):
        """Update the last expanded timestamp to current time."""
        self.last_expanded_ts = time.time()
    
    def update_summary_timestamp(self):
        """Update the last summary update timestamp to current time."""
        self.last_summary_update_ts = time.time()


class NodeManager:
    """
    Manages the node metadata, expansion/collapse logic, and LRU auto-collapse functionality.
    """
    
    def __init__(self, max_expanded_nodes: int = 8, default_pinned_nodes: Optional[List[str]] = None):
        self.max_expanded_nodes = max_expanded_nodes
        self.default_pinned_nodes = default_pinned_nodes or []
        self.node_metadata: Dict[str, NodeMetadata] = {}
        self.system_events: deque = deque(maxlen=20)  # Keep last 20 events
        self._initialize_default_pins()
    
    def _log_system_event(self, event_type: str, message: str, affected_nodes: Optional[List[str]] = None):
        """Log a system event for later reporting to the AI."""
        event = SystemEvent(
            timestamp=time.time(),
            event_type=event_type,
            message=message,
            affected_nodes=affected_nodes or []
        )
        self.system_events.append(event)
    
    def _initialize_default_pins(self):
        """Initialize default pinned nodes."""
        for node_path in self.default_pinned_nodes:
            self.get_node_metadata(node_path).is_pinned = True
            self._log_system_event(
                "auto_pin",
                f"Node '{node_path}' auto-pinned as critical integration point.",
                [node_path]
            )
    
    def get_node_metadata(self, node_path: str) -> NodeMetadata:
        """Get or create node metadata for the given path."""
        if node_path not in self.node_metadata:
            self.node_metadata[node_path] = NodeMetadata()
        return self.node_metadata[node_path]
    
    def calculate_data_hash(self, data: Any) -> str:
        """Calculate a hash of the node's data for change detection."""
        try:
            json_str = json.dumps(data, sort_keys=True, default=str)
            return hashlib.md5(json_str.encode()).hexdigest()
        except Exception:
            # Fallback for non-serializable data
            return hashlib.md5(str(data).encode()).hexdigest()
    
    def is_data_changed(self, node_path: str, current_data: Any) -> bool:
        """Check if the node's data has changed since last hash calculation."""
        metadata = self.get_node_metadata(node_path)
        current_hash = self.calculate_data_hash(current_data)
        
        if metadata.data_hash is None:
            metadata.data_hash = current_hash
            return True  # First time seeing this data
        
        if metadata.data_hash != current_hash:
            metadata.data_hash = current_hash
            return True
        
        return False
    
    def get_expanded_nodes(self) -> List[str]:
        """Get list of all currently expanded node paths."""
        return [
            path for path, metadata in self.node_metadata.items()
            if metadata.is_expanded
        ]
    
    def get_unpinned_expanded_nodes(self) -> List[str]:
        """Get list of currently expanded but unpinned node paths, sorted by LRU."""
        unpinned_expanded = [
            (path, metadata) for path, metadata in self.node_metadata.items()
            if metadata.is_expanded and not metadata.is_pinned
        ]
        
        # Sort by last_expanded_ts (oldest first)
        unpinned_expanded.sort(key=lambda x: x[1].last_expanded_ts or 0)
        
        return [path for path, _ in unpinned_expanded]
    
    def find_lru_unpinned_node(self) -> Optional[str]:
        """Find the least recently used unpinned expanded node."""
        unpinned_nodes = self.get_unpinned_expanded_nodes()
        return unpinned_nodes[0] if unpinned_nodes else None
    
    def can_expand_node(self, node_path: str) -> tuple[bool, Optional[str]]:
        """
        Check if a node can be expanded, considering the max limit.
        
        Returns:
            (can_expand, auto_collapse_candidate): 
            - can_expand: Whether expansion is possible
            - auto_collapse_candidate: Node path to auto-collapse if needed, None otherwise
        """
        metadata = self.get_node_metadata(node_path)
        
        # If already expanded, no need to check limits
        if metadata.is_expanded:
            return True, None
        
        expanded_nodes = self.get_expanded_nodes()
        
        # If under limit, can expand freely
        if len(expanded_nodes) < self.max_expanded_nodes:
            return True, None
        
        # At or over limit - need to find a node to auto-collapse
        lru_candidate = self.find_lru_unpinned_node()
        
        if lru_candidate is None:
            # All expanded nodes are pinned - cannot expand
            return False, None
        
        return True, lru_candidate
    
    def expand_node(self, node_path: str) -> tuple[bool, Optional[str], Optional[str]]:
        """
        Expand a node, potentially auto-collapsing an LRU unpinned node.
        
        Returns:
            (success, auto_collapsed_node, message):
            - success: Whether the expansion succeeded
            - auto_collapsed_node: Path of node that was auto-collapsed, None if none
            - message: Status message for logging/AI feedback
        """
        can_expand, auto_collapse_candidate = self.can_expand_node(node_path)
        
        if not can_expand:
            return False, None, f"Cannot expand {node_path}: all {self.max_expanded_nodes} expanded nodes are pinned"
        
        metadata = self.get_node_metadata(node_path)
        
        # If already expanded, just update timestamp
        if metadata.is_expanded:
            metadata.update_expanded_timestamp()
            return True, None, f"Node {node_path} was already expanded, updated access time"
        
        auto_collapsed_node = None
        
        # Auto-collapse if needed
        if auto_collapse_candidate:
            self.collapse_node(auto_collapse_candidate, is_auto_collapse=True)
            auto_collapsed_node = auto_collapse_candidate
        
        # Expand the requested node
        metadata.is_expanded = True
        metadata.update_expanded_timestamp()
        
        message = f"Expanded {node_path}"
        if auto_collapsed_node:
            message += f", auto-collapsed {auto_collapsed_node} (LRU unpinned)"
        
        # Log system event
        self._log_system_event(
            "node_expanded",
            message,
            [node_path] + ([auto_collapsed_node] if auto_collapsed_node else [])
        )
        
        return True, auto_collapsed_node, message
    
    def collapse_node(self, node_path: str, is_auto_collapse: bool = False) -> tuple[bool, str]:
        """
        Collapse a node.
        
        Returns:
            (success, message): Whether collapse succeeded and status message
        """
        metadata = self.get_node_metadata(node_path)
        
        if not metadata.is_expanded:
            return False, f"Node {node_path} was already collapsed"
        
        metadata.is_expanded = False
        # Don't update expanded timestamp on collapse
        
        collapse_type = "auto-collapsed" if is_auto_collapse else "collapsed"
        
        # Log system event
        self._log_system_event(
            "node_collapsed",
            f"Node {node_path} was {collapse_type}",
            [node_path]
        )
        
        return True, f"Successfully {collapse_type} {node_path}"
    
    def pin_node(self, node_path: str) -> tuple[bool, str]:
        """
        Pin a node to prevent auto-collapse.
        
        Returns:
            (success, message): Whether pin succeeded and status message
        """
        metadata = self.get_node_metadata(node_path)
        
        if metadata.is_pinned:
            return False, f"Node {node_path} was already pinned"
        
        metadata.is_pinned = True
        
        # Log system event
        self._log_system_event(
            "node_pinned",
            f"Node {node_path} was pinned",
            [node_path]
        )
        
        return True, f"Successfully pinned {node_path}"
    
    def unpin_node(self, node_path: str) -> tuple[bool, str]:
        """
        Unpin a node to allow auto-collapse.
        
        Returns:
            (success, message): Whether unpin succeeded and status message
        """
        metadata = self.get_node_metadata(node_path)
        
        if not metadata.is_pinned:
            return False, f"Node {node_path} was already unpinned"
        
        metadata.is_pinned = False
        
        # Log system event
        self._log_system_event(
            "node_unpinned",
            f"Node {node_path} was unpinned",
            [node_path]
        )
        
        return True, f"Successfully unpinned {node_path}"
    
    def get_nodes_needing_summary(self, all_node_paths: List[str]) -> List[str]:
        """
        Get list of node paths that need AI summary generation.
        
        This includes:
        - Collapsed nodes without summaries
        - Collapsed nodes whose data has changed since last summary
        """
        needs_summary = []
        
        for node_path in all_node_paths:
            metadata = self.get_node_metadata(node_path)
            
            # Only collapsed nodes need summaries
            if metadata.is_expanded:
                continue
            
            # Needs summary if no summary exists or data hash doesn't match
            if (metadata.ai_summary is None or 
                metadata.last_summary_update_ts is None):
                needs_summary.append(node_path)
        
        return needs_summary
    
    def update_node_summary(self, node_path: str, summary: str):
        """Update the AI-generated summary for a node."""
        metadata = self.get_node_metadata(node_path)
        metadata.ai_summary = summary
        metadata.update_summary_timestamp()
    
    def get_expansion_status_summary(self) -> Dict[str, Any]:
        """Get a summary of current expansion status for logging/debugging."""
        expanded_nodes = self.get_expanded_nodes()
        pinned_expanded = [
            path for path in expanded_nodes
            if self.node_metadata[path].is_pinned
        ]
        unpinned_expanded = [
            path for path in expanded_nodes
            if not self.node_metadata[path].is_pinned
        ]
        
        return {
            "total_expanded": len(expanded_nodes),
            "max_allowed": self.max_expanded_nodes,
            "pinned_expanded": len(pinned_expanded),
            "unpinned_expanded": len(unpinned_expanded),
            "pinned_nodes": pinned_expanded,
            "unpinned_nodes": unpinned_expanded,
            "utilization": f"{len(expanded_nodes)}/{self.max_expanded_nodes}"
        }
    
    def get_system_events(self) -> List[Dict[str, Any]]:
        """
        Get recent system events for AI context and clear the event queue.
        This provides the AI with information about automatic node management actions.
        """
        events = [event.to_dict() for event in self.system_events]
        self.system_events.clear()  # Clear after reporting
        return events
    
    def auto_expand_active_channels(self, channel_activity_data: Dict[str, float]) -> List[str]:
        """
        Auto-expand recently active channels based on activity timestamps.
        
        Args:
            channel_activity_data: Dict mapping channel_id to last_activity_timestamp
            
        Returns:
            List of channels that were auto-expanded
        """
        current_time = time.time()
        auto_expanded = []
        
        # Sort channels by activity (most recent first)
        sorted_channels = sorted(
            channel_activity_data.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        for channel_id, last_activity in sorted_channels:
            # Only consider channels active in the last 10 minutes
            if current_time - last_activity > 600:  # 10 minutes
                continue
                
            node_path = f"channels.{channel_id}"
            metadata = self.get_node_metadata(node_path)
            
            # Skip if already expanded or manually pinned
            if metadata.is_expanded:
                continue
            
            # Try to auto-expand
            success, auto_collapsed, message = self.expand_node(node_path)
            if success:
                auto_expanded.append(channel_id)
                # Mark as auto-expanded (not manually pinned)
                metadata.is_pinned = False  # Auto-expanded nodes are not pinned
                
                # Stop if we've expanded enough active channels
                if len(auto_expanded) >= 3:  # Limit auto-expansion to 3 most active
                    break
        
        return auto_expanded
    
    def manual_expand_node(self, node_path: str) -> tuple[bool, Optional[str], str]:
        """
        Manually expand a node, which also PINS it to prevent auto-collapse.
        This is the main difference from regular expand_node - manual expansion = pinning.
        
        Returns:
            (success, auto_collapsed_node, message)
        """
        success, auto_collapsed_node, message = self.expand_node(node_path)
        
        if success:
            # Manual expansion automatically pins the node
            metadata = self.get_node_metadata(node_path)
            metadata.is_pinned = True
            message += " [PINNED for focused attention]"
        
        return success, auto_collapsed_node, message
    
    def manual_collapse_node(self, node_path: str) -> tuple[bool, str]:
        """
        Manually collapse a node, which also UNPINS it.
        This allows the node to be auto-collapsed in the future.
        
        Returns:
            (success, message)
        """
        success, message = self.collapse_node(node_path, is_auto_collapse=False)
        
        if success:
            # Manual collapse also unpins the node
            metadata = self.get_node_metadata(node_path)
            metadata.is_pinned = False
            message += " [UNPINNED, can be auto-collapsed]"
        
        return success, message
