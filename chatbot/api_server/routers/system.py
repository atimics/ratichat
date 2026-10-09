"""
System management router - handles system status, control, and health checks.
"""

import logging
from datetime import datetime
from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse

from ..schemas import SystemCommand, StatusResponse
from chatbot.core.orchestration import MainOrchestrator
from ..dependencies import get_orchestrator
from ..readiness import bot_readiness

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/system", tags=["system"])


@router.get("/readiness")
async def get_readiness(orchestrator: MainOrchestrator = Depends(get_orchestrator)):
    readiness = await bot_readiness(orchestrator)
    return JSONResponse(
        readiness,
        status_code=200 if readiness["status"] == "ready" else 503,
        headers={"Cache-Control": "no-store"},
    )


@router.get("/health")
async def health_check():
    """Health check endpoint for container orchestration."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "chatbot_api"
    }


@router.get("/status")
async def get_system_status(orchestrator: MainOrchestrator = Depends(get_orchestrator)):
    """Get overall system status and metrics."""
    try:
        logger.info("Starting get_system_status")
        
        # Get processing hub status
        logger.info("Getting processing status")
        processing_status = orchestrator.processing_hub.get_processing_status()
        logger.info(f"Got processing status: {type(processing_status)}")
        
        # Get basic metrics
        logger.info("Getting world state metrics")
        world_state_metrics = {
            "channels_count": len(orchestrator.world_state.state.channels),
            "total_messages": sum(len(ch.recent_messages) for ch in orchestrator.world_state.state.channels.values()),
            "action_history_count": len(orchestrator.world_state.state.action_history),
            "pending_invites": len(orchestrator.world_state.get_pending_matrix_invites()),
            "generated_media_count": len(orchestrator.world_state.state.generated_media_library),
            "research_entries": len(orchestrator.world_state.state.research_database)
        }
        logger.info(f"Got world state metrics: {type(world_state_metrics)}")
        
        # Get tool stats
        logger.info("Getting tool stats")
        tool_stats = orchestrator.tool_registry.get_tool_stats()
        logger.info(f"Got tool stats: {type(tool_stats)}")
        
        # Rate limiter status
        logger.info("Getting rate limit status")
        rate_limit_status = orchestrator.rate_limiter.get_rate_limit_status(datetime.now().timestamp())
        logger.info(f"Got rate limit status: {type(rate_limit_status)}")
        
        # Integration status
        logger.info("Getting integration status")
        try:
            integration_status = await orchestrator.integration_manager.list_integrations()
            logger.info(f"Got integration status: {type(integration_status)}")
        except Exception:
            logger.warning("Failed to get integration status", exc_info=True)
            integration_status = {"error": "Integration status unavailable"}
        
        logger.info("Assembling final status response")
        status = {
            "system_running": orchestrator.running,
            "config": {
                "processing_mode": processing_status["current_mode"],
                "ai_model": orchestrator.ai_engine.model,
                "max_actions_per_cycle": 3
            },
            "world_state": world_state_metrics,
            "tools": tool_stats,
            "rate_limits": rate_limit_status,
            "integrations": integration_status,
            "processing": processing_status,
            "research": orchestrator.research_store.state_counts() if hasattr(orchestrator, "research_store") else {},
            "live_monitoring": {
                "configured": bool(getattr(orchestrator, "live_monitor_service", None)),
                "loop_running": bool(getattr(orchestrator, "live_monitor_task", None)
                    and not orchestrator.live_monitor_task.done()),
            },
            "uptime_seconds": (datetime.now() - orchestrator.start_time).total_seconds() if hasattr(orchestrator, 'start_time') else 0
        }
        
        logger.info("get_system_status completed successfully")
        return status
        
    except Exception:
        logger.exception("Error getting system status")
        raise HTTPException(status_code=500, detail="Failed to get system status")


@router.post("/command", response_model=StatusResponse)
async def execute_system_command(
    command: SystemCommand,
    orchestrator: MainOrchestrator = Depends(get_orchestrator)
):
    """Execute system commands like start, stop, restart."""
    try:
        logger.info(f"Executing system command: {command.command}")
        
        if command.command == "start":
            if orchestrator.running:
                return StatusResponse(
                    status="info",
                    message="System is already running",
                    timestamp=datetime.now().isoformat()
                )
            
            # Start the orchestrator
            await orchestrator.start()
            return StatusResponse(
                status="success",
                message="System started successfully",
                timestamp=datetime.now().isoformat()
            )
            
        elif command.command == "stop":
            if not orchestrator.running:
                return StatusResponse(
                    status="info",
                    message="System is already stopped",
                    timestamp=datetime.now().isoformat()
                )
            
            # Stop the orchestrator
            await orchestrator.stop()
            return StatusResponse(
                status="success",
                message="System stopped successfully",
                timestamp=datetime.now().isoformat()
            )
            
        elif command.command == "restart":
            # Stop if running
            if orchestrator.running:
                await orchestrator.stop()
            
            # Start again
            await orchestrator.start()
            return StatusResponse(
                status="success",
                message="System restarted successfully",
                timestamp=datetime.now().isoformat()
            )
            
        elif command.command == "reset_processing_mode":
            # Reset processing mode based on current configuration
            orchestrator.processing_hub.reset_processing_mode()
            return StatusResponse(
                status="success",
                message="Processing mode reset successfully",
                timestamp=datetime.now().isoformat()
            )
            
        elif command.command == "force_processing_mode":
            # Force specific processing mode
            mode = command.parameters.get("mode") if command.parameters else None
            if mode not in ["traditional", "node-based"]:
                raise HTTPException(status_code=400, detail="Mode must be 'traditional' or 'node-based'")
            
            changed = await orchestrator.processing_hub.force_processing_mode(mode.replace("-", "_"))
            if not changed:
                raise HTTPException(status_code=400, detail="Requested processor is unavailable")
            return StatusResponse(
                status="success",
                message=f"Processing mode forced to {mode}",
                timestamp=datetime.now().isoformat()
            )
            
        else:
            raise HTTPException(status_code=400, detail=f"Unknown command: {command.command}")
            
    except HTTPException:
        raise
    except Exception:
        logger.exception("Error executing system command %s", command.command)
        raise HTTPException(status_code=500, detail="Command execution failed")
