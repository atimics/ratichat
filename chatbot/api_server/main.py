"""
Main FastAPI server with modular router architecture.

This is the refactored version of the original api_server.py, broken down into
organized routers for better maintainability and separation of concerns.
"""

import logging
from datetime import datetime
from typing import Any, Dict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from chatbot.core.orchestration import MainOrchestrator
from .services import SetupManager, LogWebSocketManager
from .routers import system, tools, config, integrations, ai, worldstate, setup, logs, ui_frames
from .schemas import StatusResponse
from .auth import allowed_origins, configured_admin_token, is_admin_authorized
from .readiness import bot_readiness

logger = logging.getLogger(__name__)


class ChatbotAPIServer:
    """Main API server for chatbot management UI with modular router architecture."""
    
    def __init__(self, orchestrator: MainOrchestrator):
        self.orchestrator = orchestrator
        self._start_time = datetime.now()
        
        self.app = FastAPI(
            title="Chatbot Management API",
            description="REST API for monitoring and controlling the chatbot system",
            version="1.0.0"
        )
        
        self.log_manager = LogWebSocketManager()
        self.setup_manager = SetupManager()
        
        self._setup_middleware()
        self._setup_dependency_overrides()
        self._setup_routers()
        self._setup_websocket_routes()
        self._setup_static_files()
        self._setup_log_handler()
        
    def _setup_middleware(self):
        """Configure CORS and other middleware."""
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=allowed_origins(),
            allow_credentials=False,
            allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
            allow_headers=["Authorization", "Content-Type", "X-Admin-Token"],
        )

        @self.app.middleware("http")
        async def require_admin_authentication(request, call_next):
            public_paths = {"/health", "/api/system/health"}
            protected_documentation = {
                "/docs",
                "/docs/oauth2-redirect",
                "/openapi.json",
                "/redoc",
            }
            if (
                request.method != "OPTIONS"
                and (
                    request.url.path.startswith("/api/")
                    or request.url.path in protected_documentation
                )
                and request.url.path not in public_paths
            ):
                if configured_admin_token() is None:
                    return JSONResponse(
                        status_code=503,
                        content={
                            "detail": "Management API authentication is not configured"
                        },
                    )
                if not is_admin_authorized(request.headers):
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Invalid or missing management API token"},
                        headers={"WWW-Authenticate": "Bearer"},
                    )
            return await call_next(request)
    
    def _setup_dependency_overrides(self):
        """Set up dependency injection overrides for routers."""
        def get_orchestrator():
            return self.orchestrator
        
        def get_setup_manager():
            return self.setup_manager
        
        # Override the get_orchestrator dependency in all routers
        self.app.dependency_overrides[system.get_orchestrator] = get_orchestrator
        self.app.dependency_overrides[tools.get_orchestrator] = get_orchestrator
        self.app.dependency_overrides[config.get_orchestrator] = get_orchestrator
        self.app.dependency_overrides[integrations.get_orchestrator] = get_orchestrator
        self.app.dependency_overrides[ai.get_orchestrator] = get_orchestrator
        self.app.dependency_overrides[worldstate.get_orchestrator] = get_orchestrator
        self.app.dependency_overrides[setup.get_orchestrator] = get_orchestrator
        self.app.dependency_overrides[setup.get_setup_manager] = get_setup_manager
        self.app.dependency_overrides[logs.get_orchestrator] = get_orchestrator
        self.app.dependency_overrides[ui_frames.get_orchestrator] = get_orchestrator
        
    def _setup_routers(self):
        """Include all modular routers."""
        from .routers.openrouter import create_router
        self.app.include_router(create_router(self.orchestrator))
        self.app.include_router(system.router)
        self.app.include_router(tools.router)
        self.app.include_router(config.router)
        self.app.include_router(integrations.router)
        self.app.include_router(ai.router)
        self.app.include_router(worldstate.router)
        self.app.include_router(setup.router)
        self.app.include_router(logs.router)  # already has /api prefix
        
        # UI and frames routers (no prefix)
        self.app.include_router(ui_frames.ui_router)
        self.app.include_router(ui_frames.frames_router)
        
        # Add root-level health endpoint for Docker health checks
        @self.app.get("/health")
        async def root_health_check():
            """Root-level health check endpoint for Docker containers."""
            return {
                "status": "healthy",
                "timestamp": datetime.now().isoformat(),
                "service": "chatbot_api"
            }

        @self.app.get("/ready")
        async def root_readiness_check():
            readiness = await bot_readiness(self.orchestrator)
            return JSONResponse(
                {"status": readiness["status"]},
                status_code=200 if readiness["status"] == "ready" else 503,
                headers={"Cache-Control": "no-store"},
            )
        
        # Add compatibility route for legacy /api/status endpoint
        @self.app.get("/api/status")
        async def legacy_status_endpoint():
            """Legacy status endpoint for backwards compatibility - redirects to /api/system/status."""
            from .routers.system import get_system_status
            from .dependencies import get_orchestrator
            orchestrator = self.orchestrator
            return await get_system_status(orchestrator)
        
    def _setup_websocket_routes(self):
        """Set up WebSocket routes for real-time features."""
        @self.app.websocket("/ws/logs")
        async def websocket_logs(websocket: WebSocket):
            """WebSocket endpoint for real-time log streaming."""
            if configured_admin_token() is None:
                await websocket.close(code=1013, reason="Authentication not configured")
                return
            if not is_admin_authorized(websocket.headers):
                await websocket.close(code=4401, reason="Unauthorized")
                return
            requested_protocols = {
                protocol.strip()
                for protocol in websocket.headers.get(
                    "sec-websocket-protocol", ""
                ).split(",")
            }
            selected_protocol = (
                "ratichat-admin"
                if "ratichat-admin" in requested_protocols
                else None
            )
            await self.log_manager.connect(
                websocket, subprotocol=selected_protocol
            )
            try:
                while True:
                    # Keep connection alive and handle client messages if needed
                    await websocket.receive_text()
            except WebSocketDisconnect:
                self.log_manager.disconnect(websocket)
                
    def _setup_static_files(self):
        """Set up static file serving for the UI."""
        # UI serving is now handled by ui_frames router
        # This method is kept for backwards compatibility but does nothing
        pass
        
    def _setup_log_handler(self):
        """Set up log handler to stream logs to WebSocket clients."""
        websocket_handler = self.log_manager.create_log_handler()
        websocket_handler.setLevel(logging.INFO)
        logging.getLogger().addHandler(websocket_handler)


def create_api_server(orchestrator: MainOrchestrator) -> FastAPI:
    """Factory function to create the API server."""
    server = ChatbotAPIServer(orchestrator)
    return server.app
