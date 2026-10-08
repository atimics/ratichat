"""
World state management router for the chatbot API.

This module handles all world state-related endpoints including:
- Getting current world state information
- Managing channel information
- Accessing AI world state payloads
- Executing node-based actions
"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from datetime import datetime
import logging

from chatbot.core.orchestration import MainOrchestrator
from ..dependencies import get_orchestrator
from ..schemas import StatusResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/worldstate", tags=["worldstate"])


class NodeAction(BaseModel):
    """Model for node actions."""
    node_id: str
    action: str  # expand, collapse, pin, unpin, refresh_summary
    force: bool = False


@router.get("")
async def get_world_state(orchestrator: MainOrchestrator = Depends(get_orchestrator)):
    """Get current world state information."""
    try:
        # Get traditional world state
        state_dict = orchestrator.world_state.to_dict()
        
        # Add node-based information if available
        node_info = {}
        if hasattr(orchestrator.processing_hub, 'node_manager') and orchestrator.processing_hub.node_manager:
            node_manager = orchestrator.processing_hub.node_manager
            node_info = {
                "expanded_nodes": node_manager.get_expanded_nodes(),
                "collapsed_summaries": [path for path, meta in node_manager.node_metadata.items() if not meta.is_expanded],
                "pinned_nodes": [path for path, meta in node_manager.node_metadata.items() if meta.is_pinned],
                "system_events": [event.to_dict() for event in node_manager.system_events][-10:],
                "expansion_status": node_manager.get_expansion_status_summary()
            }
        
        return {
            "traditional_state": state_dict,
            "node_state": node_info,
            "processing_mode": orchestrator.processing_hub.get_processing_status()["current_mode"],
            "timestamp": datetime.now().isoformat()
        }
    except Exception:
        logger.exception("Error getting world state")
        raise HTTPException(status_code=500, detail="Could not get world state")


@router.get("/channels")
async def get_channels(orchestrator: MainOrchestrator = Depends(get_orchestrator)):
    """Get detailed channel information."""
    try:
        channels = {}
        for channel_id, channel in orchestrator.world_state.state.channels.items():
            channels[channel_id] = {
                "id": channel.id,
                "name": channel.name,
                "platform": channel.type,
                "message_count": len(channel.recent_messages),
                "recent_messages": [
                    {
                        "id": msg.id,
                        "content": msg.content[:100] + "..." if len(msg.content) > 100 else msg.content,
                        "author": msg.sender,
                        "timestamp": msg.timestamp,
                        "platform": msg.channel_type
                    }
                    for msg in channel.recent_messages[-5:]  # Last 5 messages
                ]
            }
        
        return {
            "channels": channels,
            "total_channels": len(channels),
            "timestamp": datetime.now().isoformat()
        }
    except Exception:
        logger.exception("Error getting channels")
        raise HTTPException(status_code=500, detail="Could not get channels")


@router.get("/ai-payload")
async def get_ai_world_state_payload(orchestrator: MainOrchestrator = Depends(get_orchestrator)):
    """Get the actual world state payload as used by the AI system."""
    try:
        processor = orchestrator.processing_hub.node_processor
        if processor and orchestrator.processing_hub.current_processing_mode == "node_based":
            return {"ai_world_state": processor.last_payload or {},
                    "metadata": {"payload_type": "node_based", "timestamp": datetime.now().isoformat()}}
        # Get the payload builder from the orchestrator
        payload_builder = orchestrator.payload_builder
        world_state_data = orchestrator.world_state.state
        
        # Get the current primary channel (if any)
        primary_channel_id = getattr(orchestrator, 'current_primary_channel_id', None)
        
        # Build the actual AI payload
        ai_payload = payload_builder.build_full_payload(
            world_state_data=world_state_data,
            primary_channel_id=primary_channel_id,
            config={
                "optimize_for_size": False,  # Get full detail for API
                "include_detailed_user_info": True,
                "max_messages_per_channel": 10,
                "max_action_history": 10,
                "max_thread_messages": 10,
                "max_other_channels": 10
            }
        )
        
        return {
            "ai_world_state": ai_payload,
            "metadata": {
                "primary_channel_id": primary_channel_id,
                "payload_type": "full",
                "optimization_enabled": False,
                "timestamp": datetime.now().isoformat()
            }
        }
    except Exception:
        logger.exception("Error getting AI world state payload")
        raise HTTPException(
            status_code=500, detail="Could not build AI world state payload"
        )


@router.post("/node/action")
async def execute_node_action(
    action: NodeAction,
    orchestrator: MainOrchestrator = Depends(get_orchestrator)
):
    """Execute an action on a node (expand, collapse, pin, unpin, refresh)."""
    try:
        if not hasattr(orchestrator.processing_hub, 'node_manager') or not orchestrator.processing_hub.node_manager:
            raise HTTPException(status_code=400, detail="Node-based processing not available")
        
        if action.action not in {"expand", "collapse", "pin", "unpin"}:
            raise HTTPException(status_code=400, detail=f"Unknown action: {action.action}")
        return orchestrator.processing_hub.node_processor.execute_node_action(
            action.action + "_node", {"node_path": action.node_id},
        )
    except HTTPException:
        raise
    except Exception:
        logger.exception("Error executing node action")
        raise HTTPException(status_code=500, detail="Could not execute node action")
