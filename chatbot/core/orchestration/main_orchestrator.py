"""
Main Orchestrator

Lean coordinator that manages observers, components, and overall system lifecycle.
Acts as the primary entry point and coordinates between different subsystems.
"""

import asyncio
from copy import copy
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from ...config import matrix_auth_is_configured, settings
from ...core.ai_engine import AIDecisionEngine, ActionPlan
from ...core.context import ContextManager
from ...core.integration_manager import IntegrationManager
from ...integrations.arweave_uploader_client import ArweaveUploaderClient
from ...integrations.farcaster import FarcasterObserver
from ..node_system.node_manager import NodeManager
from ...integrations.matrix.observer import MatrixObserver
from ...integrations.telegram import TelegramObserver
from ...integrations.discord import DiscordObserver
from ...integrations.telegram.operator_queue import OperatorQueueBridge
from ...integrations.base_nft_service import BaseNFTService
from ...integrations.eligibility_service import UserEligibilityService
from ...tools.registry import ToolRegistry
from ..world_state.manager import WorldStateManager
from ..world_state.payload_builder import PayloadBuilder
from .processing_hub import ProcessingHub, ProcessingConfig
from .rate_limiter import RateLimiter, RateLimitConfig
from ..proactive import ProactiveConversationEngine
from .capability_policy import CapabilityPolicy, ExecutionScope, STATE_TOOLS

logger = logging.getLogger(__name__)


class TraditionalProcessor:
    """
    Traditional AI processing wrapper that processes full payloads.
    
    This class acts as a bridge between the processing hub and the AI engine,
    handling the traditional full-payload processing approach.
    """
    
    def __init__(
        self,
        ai_engine,
        tool_registry,
        rate_limiter,
        context_manager,
        action_context,
        capability_policy,
    ):
        self.ai_engine = ai_engine
        self.tool_registry = tool_registry
        self.rate_limiter = rate_limiter
        self.context_manager = context_manager
        self.action_context = action_context
        self.capability_policy = capability_policy
        
    async def process_payload(self, payload: Dict[str, Any], active_channels: list) -> None:
        """
        Process a payload using the traditional approach.
        
        Args:
            payload: The world state payload to process
            active_channels: List of active channel IDs
        """
        try:
            execution_scope = self.capability_policy.scope_from_payload(payload)
            if execution_scope.channel_type == "discord":
                observer = getattr(self.action_context, "discord_observer", None)
                if not self.ai_engine.api_key or not observer:
                    return
                if execution_scope.channel_id not in self.capability_policy.approved_discord_channel_ids:
                    return
                if not observer.can_reply(execution_scope.channel_id, execution_scope.latest_event_id):
                    return
                # Give each Discord request only its own channel's context.
                payload = {
                    "current_processing_channel_id": execution_scope.channel_id,
                    "channels": {execution_scope.channel_id: payload["channels"][execution_scope.channel_id]},
                    "cycle_id": payload.get("cycle_id"),
                }
            elif self.capability_policy.profile == "matrix_steward":
                if not self.ai_engine.api_key:
                    return
                if execution_scope.channel_id not in self.capability_policy.approved_matrix_room_ids:
                    return
                # Each request gets only its own room's messages. Global history,
                # profiles, and thread summaries may contain private room data.
                payload = {
                    "current_processing_channel_id": execution_scope.channel_id,
                    "channels": {execution_scope.channel_id: payload["channels"][execution_scope.channel_id]},
                    "cycle_id": payload.get("cycle_id"),
                }

            # Add available tools to the payload
            allowed_tool_names = self.capability_policy.filter_tool_names(
                self.tool_registry.get_tool_names(), execution_scope
            )
            payload["available_tools"] = self.tool_registry.get_tool_descriptions_for_ai(
                allowed_tool_names=allowed_tool_names
            )
            if self.capability_policy.profile == "matrix_steward" and execution_scope.channel_type == "matrix":
                payload["matrix_management"] = {
                    "request_event_id": execution_scope.latest_event_id,
                    "managed_room_ids": sorted(self.capability_policy.managed_room_ids),
                    "role": "RATi manages this Matrix community. Use the tools available for this request and report their results in plain English.",
                }
            
            # Generate a cycle ID for this decision
            cycle_id = payload.get("cycle_id", f"cycle_{int(time.time() * 1000)}")
            
            # Get AI decision
            decision_result = await self.ai_engine.make_decision(payload, cycle_id)
            
            if not decision_result.selected_actions:
                logger.debug("No actions selected by AI")
                return
                
            # Management actions produce their own factual reply after execution.
            actions = decision_result.selected_actions
            if self.capability_policy.profile == "matrix_steward":
                management = [action for action in actions if action.action_type in {
                    "manage_matrix_room", "manage_matrix_server", "matrix_server_status",
                }]
                actions = management[:1] if management else actions
            for action in actions:
                try:
                    if action.action_type in {"manage_matrix_room", "manage_matrix_server", "matrix_server_status"}:
                        result = await self._execute_action_and_return_result(action, execution_scope)
                        if result.get("status") != "blocked":
                            # Send the actual result as the reply, then finish this
                            # turn. Model text produced before execution is stale.
                            receipt = result.get("receipt_id")
                            message = result.get("message", str(result))
                            if receipt:
                                message += "\n\nReceipt: " + receipt[:12]
                            reply = ActionPlan("send_matrix_reply", {
                                "channel_id": execution_scope.channel_id,
                                "reply_to_id": execution_scope.latest_event_id,
                                "content": message,
                                "format_as_markdown": False,
                            }, "Report the Matrix action result", 1)
                            await self._execute_action(reply, execution_scope)
                        break
                    await self._execute_action(action, execution_scope)
                except Exception as e:
                    logger.error(f"Error executing action {action.action_type}: {e}")
                    
        except Exception as e:
            logger.error(f"Error in traditional processing: {e}")
            raise
            
    async def _execute_action(
        self,
        action: ActionPlan,
        execution_scope: ExecutionScope | None = None,
    ) -> None:
        """Execute a single action."""
        try:
            denial_reason = self.capability_policy.denial_reason(
                action.action_type, action.parameters, execution_scope
            )
            if denial_reason:
                await self._record_blocked_action(action, denial_reason)
                return

            # Get the tool from registry
            tool = self.tool_registry.get_enabled_tool(action.action_type)
            if not tool:
                logger.error(f"Tool unavailable: {action.action_type}")
                return
                
            # Execute the tool with parameters and context
            result = await tool.execute(action.parameters, self._tool_context(action.action_type, execution_scope))
            
            # Log the action result
            await self.context_manager.add_tool_result(
                channel_id="system",  # Use system channel for orchestrator actions
                tool_name=action.action_type,
                result={
                    "status": result.get("status", "unknown"),
                    "message": result.get("message", str(result)),
                    "reasoning": action.reasoning,
                    "parameters": action.parameters
                }
            )
        except Exception as e:
            logger.error(f"Error executing action {action.action_type}: {e}")
            # Log the failed action
            await self.context_manager.add_tool_result(
                channel_id="system",
                tool_name=action.action_type,
                result={
                    "status": "error",
                    "message": f"Error: {str(e)}",
                    "reasoning": action.reasoning,
                    "parameters": action.parameters
                }
            )

    def _tool_context(self, tool_name, execution_scope):
        if tool_name not in STATE_TOOLS:
            return self.action_context
        context = copy(self.action_context)
        context.execution_scope = execution_scope
        return context

    async def _record_blocked_action(
        self, action: ActionPlan, message: str
    ) -> Dict[str, str]:
        """Record a model action blocked by the active capability profile."""
        logger.warning(message)
        await self.context_manager.add_tool_result(
            channel_id="system",
            tool_name=action.action_type,
            result={
                "status": "blocked",
                "message": message,
                "reasoning": action.reasoning,
                "parameters": action.parameters,
            },
        )
        return {"status": "blocked", "error": message}

    async def _execute_actions(
        self,
        actions: list,
        execution_scope: ExecutionScope | None = None,
    ) -> None:
        """
        Execute a list of actions with coordination logic.
        
        This method handles special coordination cases like injecting generated
        image URLs into posting actions.
        """
        # Track results from executed actions for coordination
        execution_results = {}
        
        # Sort actions by priority (higher priority first)
        sorted_actions = sorted(actions, key=lambda a: getattr(a, 'priority', 0), reverse=True)
        
        for action in sorted_actions:
            try:
                # Check for coordination opportunities
                if action.action_type in ["send_farcaster_post", "send_matrix_message"]:
                    # Check if we have a generated image to coordinate with
                    if "generate_image" in execution_results:
                        image_result = execution_results["generate_image"]
                        if image_result.get("status") == "success" and "embed_page_url" in image_result:
                            # Inject the embed URL into the posting action
                            action.parameters["embed_url"] = image_result["embed_page_url"]
                
                # Execute the action
                result = await self._execute_action_and_return_result(
                    action, execution_scope
                )
                execution_results[action.action_type] = result
                
            except Exception as e:
                logger.error(f"Error executing action {action.action_type}: {e}")
                execution_results[action.action_type] = {"status": "error", "error": str(e)}
    
    async def _execute_action_and_return_result(
        self,
        action: ActionPlan,
        execution_scope: ExecutionScope | None = None,
    ) -> dict:
        """Execute a single action and return the result for coordination."""
        result = None
        try:
            denial_reason = self.capability_policy.denial_reason(
                action.action_type, action.parameters, execution_scope
            )
            if denial_reason:
                return await self._record_blocked_action(action, denial_reason)

            # Get the tool from registry
            tool = self.tool_registry.get_enabled_tool(action.action_type)
            if not tool:
                logger.error(f"Tool unavailable: {action.action_type}")
                return {"status": "error", "error": f"Tool unavailable: {action.action_type}"}
                
            # Execute the tool with parameters and context
            result = await tool.execute(action.parameters, self._tool_context(action.action_type, execution_scope))
            
            # Log the action result
            await self.context_manager.add_tool_result(
                channel_id="system",  # Use system channel for orchestrator actions
                tool_name=action.action_type,
                result={
                    "status": result.get("status", "unknown"),
                    "message": result.get("message", str(result)),
                    "reasoning": action.reasoning,
                    "parameters": action.parameters
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing action {action.action_type}: {e}")
            if action.action_type in STATE_TOOLS and isinstance(result, dict):
                # The store saved the change and receipt in one transaction.
                return result
            # Log the failed action
            await self.context_manager.add_tool_result(
                channel_id="system",
                tool_name=action.action_type,
                result={
                    "status": "error",
                    "message": f"Error: {str(e)}",
                    "reasoning": action.reasoning,
                    "parameters": action.parameters
                }
            )
            return {"status": "error", "error": str(e)}

@dataclass
class OrchestratorConfig:
    """Configuration for the main orchestrator."""

    # Database and storage
    db_path: str = "chatbot.db"
    
    # Processing configuration
    processing_config: ProcessingConfig = field(default_factory=ProcessingConfig)
    rate_limit_config: RateLimitConfig = field(default_factory=RateLimitConfig)
    
    # AI Model settings
    ai_model: str = "openai/gpt-4o-mini"
    capability_profile: str = field(
        default_factory=lambda: settings.BOT_CAPABILITY_PROFILE
    )
    public_matrix_room_ids: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            room_id.strip()
            for room_id in (
                settings.PUBLIC_MATRIX_ROOM_IDS or settings.MATRIX_ROOM_ID
            ).split(",")
            if room_id.strip()
        )
    )


class MainOrchestrator:
    """
    Main orchestrator that coordinates all chatbot components.
    
    This is the primary entry point that:
    1. Manages system lifecycle (start/stop)
    2. Initializes and coordinates all components
    3. Manages external observers (Matrix, Farcaster)
    4. Provides unified system status and control
    """
    
    def __init__(self, config: Optional[OrchestratorConfig] = None):
        self.config = config or OrchestratorConfig()
        self.capability_policy = CapabilityPolicy(
            self.config.capability_profile,
            approved_matrix_room_ids=self.config.public_matrix_room_ids,
            control_room_id=settings.MATRIX_CONTROL_ROOM_ID,
            operator_user_ids=settings.MATRIX_OPERATOR_USER_IDS.split(","),
            managed_room_ids=settings.MATRIX_MANAGED_ROOM_IDS.split(","),
            approved_discord_channel_ids=settings.DISCORD_ALLOWED_CHANNEL_IDS.split(","),
            discord_owner_user_ids=settings.DISCORD_OWNER_USER_IDS.split(","),
        )
        
        # Core components
        self.world_state = WorldStateManager()
        from ..node_system.research_store import ResearchStore
        from ..node_system.processor import capture_request
        self.research_store = ResearchStore(self.config.db_path, retention_days=settings.RESEARCH_RETENTION_DAYS)
        from ..node_system.awareness_store import AwarenessStore
        approved = {"discord": self.capability_policy.approved_discord_channel_ids,
                    "matrix": self.capability_policy.approved_matrix_room_ids}
        self.awareness_store = AwarenessStore(self.config.db_path, allowed_channels=approved,
            community_channels=approved) if settings.SHARED_AWARENESS_ENABLED else None
        def intake(channel_id, message):
            channel = self.world_state.get_channel(channel_id)
            if self.awareness_store and channel and channel_id in approved.get(channel.type, ()):
                self.awareness_store.ingest_message(channel.type, channel_id, asdict(message))
            capture_request(self.world_state, self.capability_policy, self.research_store, channel_id, message)
        self.world_state.on_message_added = intake
        self.payload_builder = PayloadBuilder()
        self.rate_limiter = RateLimiter(self.config.rate_limit_config)
        self.context_manager = ContextManager(self.world_state, self.config.db_path)
        
        # Integration management
        self.integration_manager = IntegrationManager(
            db_path=self.config.db_path,
            encryption_key=(
                settings.INTEGRATION_CREDENTIAL_KEY.encode("utf-8")
                if settings.INTEGRATION_CREDENTIAL_KEY
                else None
            ),
            world_state_manager=self.world_state
        )
        
        # Processing hub
        self.processing_hub = ProcessingHub(
            world_state_manager=self.world_state,
            payload_builder=self.payload_builder,
            rate_limiter=self.rate_limiter,
            config=self.config.processing_config
        )
        
        # Proactive conversation engine (Initiative C)
        self.proactive_engine = ProactiveConversationEngine(
            world_state_manager=self.world_state,
            context_manager=self.context_manager
        )
        
        # Connect proactive engine to world state manager for easy access
        self.world_state.proactive_engine = self.proactive_engine
        
        # Tool Registry and AI Engine
        self.tool_registry = ToolRegistry()
        self.ai_engine = AIDecisionEngine(
            api_key=settings.OPENROUTER_API_KEY,
            model=self.config.ai_model
        )
        from ..openrouter_link import OpenRouterLink
        self.openrouter_link = OpenRouterLink(
            self.config.db_path, settings.INTEGRATION_CREDENTIAL_KEY,
            settings.BOT_PUBLIC_URL,
        )
        self.ai_engine.api_key = self.openrouter_link.api_key() or settings.OPENROUTER_API_KEY
        
        # Initialize Arweave client for internal uploader service
        self.arweave_client = None
        if settings.ARWEAVE_INTERNAL_UPLOADER_SERVICE_URL:
            self.arweave_client = ArweaveUploaderClient(
                uploader_service_url=settings.ARWEAVE_INTERNAL_UPLOADER_SERVICE_URL,
                gateway_url=settings.ARWEAVE_GATEWAY_URL,
                api_key=settings.ARWEAVE_UPLOADER_API_KEY,
            )
            logger.info("Arweave client initialized for internal uploader service.")
        
        # Create action context for tool execution
        from ...tools.base import ActionContext
        from ...tools.arweave_service import ArweaveService
        
        # Initialize arweave service with our client
        arweave_service_instance = ArweaveService(arweave_client=self.arweave_client)
        
        self.action_context = ActionContext(
            world_state_manager=self.world_state,
            context_manager=self.context_manager,
            arweave_client=self.arweave_client,
            arweave_service=arweave_service_instance
        )
        self.action_context.ai_engine = self.ai_engine
        from ..model_router import ModelRouter
        from ..node_system.task_service import TaskService
        self.model_router = ModelRouter(lambda: self.ai_engine.api_key,
            catalog_path=str(Path(self.config.db_path).with_suffix(".models.json"))) if settings.TASK_MODEL_ROUTING_ENABLED else None
        self.task_service = TaskService(self.awareness_store, self.model_router, self.ai_engine) if self.awareness_store and self.model_router else None
        self.action_context.task_service = self.task_service
        self.action_context.awareness_store = self.awareness_store
        from ...integrations.matrix.steward import MatrixSteward
        self.matrix_steward = MatrixSteward(settings, self.config.db_path)
        self.action_context.matrix_steward = self.matrix_steward
        self.steward_task = None
        self.source_watch_service = None
        self.source_watch_task = None
        self.proactive_source_service = None
        self.proactive_source_task = None
        
        # External observers
        self.matrix_observer: Optional[MatrixObserver] = None
        self.farcaster_observer: Optional[FarcasterObserver] = None
        self.telegram_observer: Optional[TelegramObserver] = None
        self.discord_observer: Optional[DiscordObserver] = None
        
        # NFT and eligibility services
        self.base_nft_service: Optional[BaseNFTService] = None
        self.eligibility_service: Optional[UserEligibilityService] = None
        
        # System state
        self.running = False
        self.cycle_count = 0  # Track processing cycles
        
        # Initialize tool registry and register tools
        self._register_all_tools()

    def _register_all_tools(self):
        """Register all available tools with the tool registry."""
        from ...tools.core_tools import WaitTool
        from ...tools.describe_image_tool import DescribeImageTool
        from ...tools.farcaster_tools import (
            FollowFarcasterUserTool,
            GetUserTimelineTool,
            LikeFarcasterPostTool,
            QuoteFarcasterPostTool,
            SendFarcasterDMTool,
            SendFarcasterPostTool,
            SendFarcasterReplyTool,
            UnfollowFarcasterUserTool,
            DeleteFarcasterPostTool,
            DeleteFarcasterReactionTool,
            CollectWorldStateTool,
            GetTrendingCastsTool,
            SearchCastsTool,
            GetCastByUrlTool,
        )
        from ...tools.frame_tools import (
            CreateTransactionFrameTool,
            CreatePollFrameTool,
            CreateCustomFrameTool,
            SearchFramesTool,
            GetFrameCatalogTool,
            CreateMintFrameTool,
            CreateAirdropClaimFrameTool,
        )
        from ...tools.telegram_tools import SendTelegramMessageTool, SendTelegramReplyTool
        from ...tools.discord_tools import SendDiscordReplyTool
        from ...tools.matrix_tools import (
            AcceptMatrixInviteTool,
            IgnoreMatrixInviteTool,
            JoinMatrixRoomTool,
            LeaveMatrixRoomTool,
            ReactToMatrixMessageTool,
            SendMatrixImageTool,
            SendMatrixMessageTool,
            SendMatrixReplyTool,
            SendMatrixVideoTool,
        )
        try:
            from ...tools.media_generation_tools import GenerateImageTool, GenerateVideoTool
            _has_media_tools = True
        except ImportError:
            GenerateImageTool = GenerateVideoTool = None
            _has_media_tools = False
            logger.warning("Media generation tools unavailable — skipping")
        from ...tools.permaweb_tools import StorePermanentMemoryTool
        from ...tools.web_tools import WebSearchTool, ReadWebpageTool, ReadFeedTool
        from ...tools.research_tools import UpdateResearchTool, QueryResearchTool
        from ...tools.developer_tools import (
            GetGitHubIssuesTool, GetGitHubIssueDetailsTool, CommentOnGitHubIssueTool,
            CreateGitHubIssueTool, AnalyzeChannelForIssuesTool,
            GetCodebaseStructureTool, SetupDevelopmentWorkspaceTool, ExploreCodebaseTool,
            AnalyzeAndProposeChangeTool, ImplementCodeChangesTool,
            CreatePullRequestTool
        )
        from ...tools.user_profiling_tools import (
            SentimentAnalysisTool,
            StoreUserMemoryTool,
            GetUserProfileTool
        )
        from ...tools.proactive_conversation_tools import (
            InitiateProactiveConversationTool,
            DetectConversationOpportunitiesTool,
            ScheduleProactiveEngagementTool,
            GetProactiveEngagementStatusTool
        )
        
        # Core tools
        self.tool_registry.register_tool(WaitTool())
        from ...tools.matrix_management_tools import MatrixServerStatusTool, ManageMatrixRoomTool, ManageMatrixServerTool
        for tool in (MatrixServerStatusTool(), ManageMatrixRoomTool(), ManageMatrixServerTool()):
            self.tool_registry.register_tool(tool)
        self.tool_registry.register_tool(DescribeImageTool())
        
        # Web search and research tools
        self.tool_registry.register_tool(WebSearchTool())
        self.tool_registry.register_tool(ReadWebpageTool())
        self.tool_registry.register_tool(ReadFeedTool())
        from ...tools.task_tools import GetTaskStatusTool, RunTaskWorkersTool, LinkChatAccountTool, GetModelCatalogTool
        if self.task_service:
            self.tool_registry.register_tool(GetTaskStatusTool())
            self.tool_registry.register_tool(RunTaskWorkersTool())
            self.tool_registry.register_tool(LinkChatAccountTool())
            self.tool_registry.register_tool(GetModelCatalogTool())
        from ...tools.public_source_tools import ListPublicSourcesTool, ReadNewsTool, SearchSocialTool, ReadBlueskyFeedTool
        from ...tools.proactive_source_tools import ConfigureProactiveTool, GetProactiveStatusTool
        for tool in (ListPublicSourcesTool(), ReadNewsTool(), SearchSocialTool(), ReadBlueskyFeedTool(), ConfigureProactiveTool(), GetProactiveStatusTool()):
            self.tool_registry.register_tool(tool)
        from ...tools.source_watch_tools import CreateSourceWatchTool, ListSourceWatchesTool, RemoveSourceWatchTool, GetSourceDigestTool
        for tool in (CreateSourceWatchTool(), ListSourceWatchesTool(), RemoveSourceWatchTool(), GetSourceDigestTool()):
            self.tool_registry.register_tool(tool)
        self.tool_registry.register_tool(UpdateResearchTool())
        self.tool_registry.register_tool(QueryResearchTool())
        
        # Matrix tools
        self.tool_registry.register_tool(SendMatrixMessageTool())
        self.tool_registry.register_tool(SendMatrixReplyTool())
        self.tool_registry.register_tool(SendMatrixImageTool())
        self.tool_registry.register_tool(SendMatrixVideoTool())
        self.tool_registry.register_tool(ReactToMatrixMessageTool())
        self.tool_registry.register_tool(JoinMatrixRoomTool())
        self.tool_registry.register_tool(LeaveMatrixRoomTool())
        self.tool_registry.register_tool(AcceptMatrixInviteTool())
        self.tool_registry.register_tool(IgnoreMatrixInviteTool())
        
        # Telegram tools
        self.tool_registry.register_tool(SendTelegramMessageTool())
        self.tool_registry.register_tool(SendTelegramReplyTool())
        self.tool_registry.register_tool(SendDiscordReplyTool())

        # Farcaster tools
        self.tool_registry.register_tool(SendFarcasterPostTool())
        self.tool_registry.register_tool(SendFarcasterReplyTool())
        self.tool_registry.register_tool(SendFarcasterDMTool())
        self.tool_registry.register_tool(LikeFarcasterPostTool())
        self.tool_registry.register_tool(QuoteFarcasterPostTool())
        self.tool_registry.register_tool(FollowFarcasterUserTool())
        self.tool_registry.register_tool(UnfollowFarcasterUserTool())
        self.tool_registry.register_tool(DeleteFarcasterPostTool())
        self.tool_registry.register_tool(DeleteFarcasterReactionTool())
        self.tool_registry.register_tool(GetUserTimelineTool())
        self.tool_registry.register_tool(SearchCastsTool())
        self.tool_registry.register_tool(GetTrendingCastsTool())
        self.tool_registry.register_tool(GetCastByUrlTool())
        self.tool_registry.register_tool(CollectWorldStateTool())
        
        # Farcaster Frame tools
        self.tool_registry.register_tool(CreateTransactionFrameTool())
        self.tool_registry.register_tool(CreatePollFrameTool())
        self.tool_registry.register_tool(CreateCustomFrameTool())
        self.tool_registry.register_tool(SearchFramesTool())
        self.tool_registry.register_tool(GetFrameCatalogTool())
        
        # NFT Frame tools
        self.tool_registry.register_tool(CreateMintFrameTool())
        self.tool_registry.register_tool(CreateAirdropClaimFrameTool())
        
        # Media generation tools
        if _has_media_tools:
            self.tool_registry.register_tool(GenerateImageTool())
            self.tool_registry.register_tool(GenerateVideoTool())
        
        # Permaweb tools
        self.tool_registry.register_tool(StorePermanentMemoryTool())
        
        # GitHub-Centric Developer tools (ACE Phase 2 & 3)
        self.tool_registry.register_tool(GetGitHubIssuesTool())
        self.tool_registry.register_tool(GetGitHubIssueDetailsTool())
        self.tool_registry.register_tool(CommentOnGitHubIssueTool())
        self.tool_registry.register_tool(CreateGitHubIssueTool())
        self.tool_registry.register_tool(AnalyzeChannelForIssuesTool())
        
        # Core Developer tools (ACE Phase 1, 2 & 3)
        self.tool_registry.register_tool(GetCodebaseStructureTool())
        self.tool_registry.register_tool(SetupDevelopmentWorkspaceTool())
        self.tool_registry.register_tool(ExploreCodebaseTool())
        self.tool_registry.register_tool(AnalyzeAndProposeChangeTool())
        self.tool_registry.register_tool(ImplementCodeChangesTool())
        self.tool_registry.register_tool(CreatePullRequestTool())
        
        # User Profiling tools (Initiative B)
        self.tool_registry.register_tool(SentimentAnalysisTool())
        self.tool_registry.register_tool(StoreUserMemoryTool())
        self.tool_registry.register_tool(GetUserProfileTool())
        
        # Proactive Conversation tools (Initiative C)
        self.tool_registry.register_tool(InitiateProactiveConversationTool())
        self.tool_registry.register_tool(DetectConversationOpportunitiesTool())
        self.tool_registry.register_tool(ScheduleProactiveEngagementTool())
        self.tool_registry.register_tool(GetProactiveEngagementStatusTool())

    async def start(self) -> None:
        """Start the entire orchestrator system."""
        if self.running:
            logger.warning("Main orchestrator already running")
            return

        logger.info("Starting main orchestrator system...")
        self.running = True

        try:
            # Initialize integration manager
            await self.integration_manager.initialize()
            
            # Initialize external observers (legacy method for backward compatibility)
            await self._initialize_observers()
            
            # Register integrations from environment variables
            await self._register_integrations_from_env()
            
            # Connect all active integrations from database
            await self.integration_manager.connect_all_active()
            
            # Update action context with properly connected integrations
            await self._update_action_context_integrations()
            if self.config.capability_profile == "matrix_steward":
                self.steward_task = asyncio.create_task(self.matrix_steward.backup_loop())
            
            # Ensure the media gallery channel exists or create it
            await self._ensure_media_gallery_exists()
            
            # Initialize NFT and blockchain services
            await self._initialize_nft_services()
            
            # Set up processing hub with traditional processor
            self._setup_processing_components()
            self.source_watch_task = asyncio.create_task(self._source_watch_loop())
            self.proactive_source_task = asyncio.create_task(self._proactive_source_loop())
            
            # Start the proactive conversation engine
            await self.proactive_engine.start()
            
            # Start the processing loop
            await self.processing_hub.start_processing_loop()
            
        except Exception as e:
            logger.error(f"Error starting main orchestrator: {e}")
            raise
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the entire orchestrator system."""
        if not self.running:
            return

        logger.info("Stopping main orchestrator system...")
        self.running = False

        # Stop processing hub
        self.processing_hub.stop_processing_loop()
        if self.source_watch_task:
            self.source_watch_task.cancel()
            await asyncio.gather(self.source_watch_task, return_exceptions=True)
            self.source_watch_task = None
        if self.proactive_source_task:
            self.proactive_source_task.cancel()
            await asyncio.gather(self.proactive_source_task, return_exceptions=True)
            self.proactive_source_task = None
        if self.steward_task:
            self.steward_task.cancel()
            await asyncio.gather(self.steward_task, return_exceptions=True)
            self.steward_task = None
        
        # Stop proactive conversation engine
        if self.proactive_engine:
            await self.proactive_engine.stop()
        
        # Stop NFT and eligibility services
        if self.eligibility_service:
            await self.eligibility_service.stop()
        
        # Disconnect all integrations
        await self.integration_manager.disconnect_all()
        
        # Clean up integration manager resources
        await self.integration_manager.cleanup()
        
        # Stop external observers (legacy compatibility)
        if self.matrix_observer:
            await self.matrix_observer.stop()

        if self.farcaster_observer:
            await self.farcaster_observer.stop()

        if self.telegram_observer:
            await self.telegram_observer.stop()

        if self.discord_observer:
            await self.discord_observer.stop()

        if hasattr(self, "telegram_operator_queue") and self.telegram_operator_queue:
            await self.telegram_operator_queue.stop()

        logger.info("Main orchestrator system stopped")

    def _setup_processing_components(self):
        """Set up processing components for the processing hub."""
        # Create traditional processor wrapper
        traditional_processor = TraditionalProcessor(
            ai_engine=self.ai_engine,
            tool_registry=self.tool_registry,
            rate_limiter=self.rate_limiter,
            context_manager=self.context_manager,
            action_context=self.action_context,
            capability_policy=self.capability_policy,
        )
        
        self.processing_hub.set_traditional_processor(traditional_processor)
        
        from ..node_system.processor import NodeProcessor
        node_processor = NodeProcessor(
            self.world_state, self.payload_builder, traditional_processor, self.config.db_path,
            research_store=self.research_store, awareness_store=self.awareness_store,
            task_service=self.task_service,
        )
        from ..node_system.source_watches import WatchStore, SourceWatchService
        from ..node_system.watch_delivery import WatchDelivery
        allowed_channels = {"discord": self.capability_policy.approved_discord_channel_ids,
                            "matrix": self.capability_policy.approved_matrix_room_ids}
        owner_ids = {"discord": {value.strip() for value in settings.DISCORD_OWNER_USER_IDS.split(",") if value.strip()},
                     "matrix": self.capability_policy.operator_user_ids}
        watch_store = WatchStore(self.config.db_path, owner_ids=owner_ids, allowed_channels=allowed_channels)
        delivery = WatchDelivery(self.action_context, allowed_channels)
        self.source_watch_service = SourceWatchService(watch_store, send=delivery.send,
            reconcile=delivery.reconcile, action_context=self.action_context,
            daily_lookup_budget=settings.SOURCE_WATCH_DAILY_LOOKUP_BUDGET)
        self.action_context.source_watch_service = self.source_watch_service
        node_processor.watch_service = self.source_watch_service
        from ..node_system.proactive_sources import ProactiveStore, ProactiveSourceService
        proactive_channels = {value.strip() for value in settings.PROACTIVE_DISCORD_CHANNEL_IDS.split(",") if value.strip()} & self.capability_policy.approved_discord_channel_ids
        proactive_store = ProactiveStore(self.config.db_path, owner_ids["discord"], proactive_channels, settings.PROACTIVE_TIMEZONE)
        for channel_id in proactive_channels:
            proactive_store.seed(channel_id, enabled=settings.PROACTIVE_PUBLIC_SOURCES_ENABLED)
        self.proactive_source_service = ProactiveSourceService(proactive_store, self.ai_engine, delivery, self.action_context)
        self.action_context.proactive_source_service = self.proactive_source_service
        self.processing_hub.set_node_processor(node_processor)

    async def _source_watch_loop(self):
        while self.running:
            try:
                await self.source_watch_service.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Source watch check needs another attempt")
            await asyncio.sleep(60)

    async def _proactive_source_loop(self):
        while self.running:
            try:
                await self.proactive_source_service.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Proactive source check needs another attempt")
            await asyncio.sleep(60)

    async def _initialize_nft_services(self) -> None:
        """Initialize NFT and blockchain services if credentials are available."""
        try:
            # Initialize Base NFT service
            if (settings.BASE_RPC_URL and 
                settings.NFT_DEV_WALLET_PRIVATE_KEY and 
                settings.NFT_COLLECTION_ADDRESS_BASE):
                
                self.base_nft_service = BaseNFTService()
                
                # Initialize the service
                if await self.base_nft_service.initialize():
                    logger.info("Base NFT service initialized successfully")
                    
                    # Initialize eligibility service if we have Farcaster observer
                    if (settings.ECOSYSTEM_TOKEN_CONTRACT_ADDRESS and 
                        hasattr(self, 'farcaster_observer') and 
                        self.farcaster_observer and 
                        hasattr(self.farcaster_observer, 'neynar_api_client')):
                        
                        self.eligibility_service = UserEligibilityService(
                            neynar_api_client=self.farcaster_observer.neynar_api_client,
                            base_nft_service=self.base_nft_service,
                            world_state_manager=self.world_state
                        )
                        await self.eligibility_service.start()
                        logger.info("User eligibility service started")
                        
                        # Update action context with NFT services
                        self.action_context.base_nft_service = self.base_nft_service
                        self.action_context.eligibility_service = self.eligibility_service
                    else:
                        logger.info("Eligibility service not started - missing dependencies")
                        
                else:
                    logger.warning("Failed to initialize Base NFT service")
                    self.base_nft_service = None
                
            else:
                logger.info("NFT service configuration incomplete - NFT features disabled")
                
        except Exception as e:
            logger.error(f"Failed to initialize NFT services: {e}")
            logger.info("Continuing without NFT integration")

    async def _initialize_observers(self) -> None:
        """Initialize available observers based on environment configuration."""
        # Initialize Matrix observer if credentials available
        if matrix_auth_is_configured(settings):
            try:
                self.matrix_observer = MatrixObserver(self.world_state, self.arweave_client)
                self.matrix_observer.awareness_store = self.awareness_store
                room_id = settings.MATRIX_ROOM_ID
                self.matrix_observer.add_channel(room_id, "Robot Laboratory")
                await self.matrix_observer.start()
                
                # Connect state change notifications
                self.matrix_observer.on_state_change = self.processing_hub.trigger_state_change
                
                # Connect proactive conversation engine to state changes
                self.matrix_observer.on_state_change = self._on_world_state_change
                
                logger.info("Matrix observer initialized and started")
            except Exception as e:
                logger.error(f"Failed to initialize Matrix observer: {e}")
                logger.info("Continuing without Matrix integration")

        # Initialize Farcaster observer if credentials available
        if settings.NEYNAR_API_KEY:
            try:
                self.farcaster_observer = FarcasterObserver(
                    settings.NEYNAR_API_KEY,
                    settings.FARCASTER_BOT_SIGNER_UUID,
                    settings.FARCASTER_BOT_FID,
                    world_state_manager=self.world_state,
                )
                await self.farcaster_observer.start()
                
                # Connect state change notifications
                self.farcaster_observer.on_state_change = self.processing_hub.trigger_state_change
                
                # Connect proactive conversation engine to state changes  
                self.farcaster_observer.on_state_change = self._on_world_state_change
                
                self.world_state.update_system_status({"farcaster_connected": True})
                logger.info("Farcaster observer initialized and started")
            except Exception as e:
                logger.error(f"Failed to initialize Farcaster observer: {e}")
                logger.info("Continuing without Farcaster integration")

        # Initialize Telegram observer if token available
        if settings.TELEGRAM_BOT_TOKEN:
            try:
                self.telegram_observer = TelegramObserver(
                    world_state_manager=self.world_state
                )
                await self.telegram_observer.start()
                self.telegram_operator_queue = OperatorQueueBridge(
                    self.telegram_observer
                )
                self.telegram_observer.operator_queue_bridge = (
                    self.telegram_operator_queue
                )
                await self.telegram_operator_queue.start()
                self.world_state.update_system_status(
                    {"telegram_connected": True}
                )
                logger.info("Telegram observer initialized and started")
            except Exception as e:
                logger.error(f"Failed to initialize Telegram observer: {e}")
                if getattr(self, "telegram_operator_queue", None):
                    await self.telegram_operator_queue.stop()
                    self.telegram_operator_queue = None
                if self.telegram_observer:
                    await self.telegram_observer.stop()
                    self.telegram_observer = None
                logger.info("Continuing without Telegram integration")

        if settings.DISCORD_BOT_TOKEN:
            try:
                self.discord_observer = DiscordObserver(self.world_state)
                self.discord_observer.awareness_store = self.awareness_store
                self.discord_observer.on_state_change = self.processing_hub.trigger_state_change
                await self.discord_observer.start()
                logger.info("Discord observer initialized and started")
            except Exception:
                logger.error("Discord setup needs a valid token, server IDs, and channel IDs")
                if self.discord_observer:
                    await self.discord_observer.stop()
                self.discord_observer = None

        # Update action context with initialized observers
        self.action_context.matrix_observer = self.matrix_observer
        self.action_context.farcaster_observer = self.farcaster_observer
        self.action_context.telegram_observer = self.telegram_observer
        self.action_context.discord_observer = self.discord_observer
        
        # Configure critical node pinning based on active integrations
        self._configure_critical_node_pinning()

    async def _update_action_context_integrations(self) -> None:
        """Update action context with properly connected integrations from IntegrationManager."""
        # Update action context with initialized observers
        active_integrations = self.integration_manager.get_active_integrations()
        
        # Find Matrix and Farcaster integrations
        matrix_integration = None
        farcaster_integration = None
        
        for integration_id, integration in active_integrations.items():
            if hasattr(integration, 'integration_type') and integration.integration_type == 'matrix':
                matrix_integration = integration
            elif hasattr(integration, 'integration_type') and integration.integration_type == 'farcaster':
                farcaster_integration = integration
        
        # Update action context and maintain legacy properties
        self.action_context.matrix_observer = matrix_integration or self.matrix_observer
        self.action_context.farcaster_observer = farcaster_integration or self.farcaster_observer
        
        # Debug logging to track which observer is being used
        if farcaster_integration:
            logger.info(f"✓ Using Farcaster integration from IntegrationManager (ID: {farcaster_integration.integration_id})")
            logger.info(f"  API client initialized: {farcaster_integration.api_client is not None}")
        elif self.farcaster_observer:
            logger.info(f"⚠ Using legacy Farcaster observer (fallback)")
            logger.info(f"  API client initialized: {self.farcaster_observer.api_client is not None}")
        else:
            logger.info("ℹ No Farcaster observer available")

    def _configure_critical_node_pinning(self):
        """Configure critical node paths for pinning based on active integrations."""
        critical_pins = []
        
        # Add Matrix room if available
        if self.matrix_observer and settings.MATRIX_ROOM_ID:
            critical_pins.append(f"channels.matrix.{settings.MATRIX_ROOM_ID}")
            logger.info(f"Added Matrix room to critical pins: channels.matrix.{settings.MATRIX_ROOM_ID}")
        
        # Add Farcaster feeds if available
        if self.farcaster_observer:
            critical_pins.extend([
                "farcaster.feeds.home",
                "farcaster.feeds.notifications"
            ])
            logger.info("Added Farcaster feeds to critical pins: home, notifications")
        
        # Try to apply critical pins to available node managers
        node_manager = None
        
        # First, check if PayloadBuilder has a node_manager
        if hasattr(self.payload_builder, 'node_manager') and self.payload_builder.node_manager:
            node_manager = self.payload_builder.node_manager
            logger.debug("Using PayloadBuilder's NodeManager for critical pinning")
        # Next, check if ProcessingHub's node_processor has a node_manager
        elif (self.processing_hub.node_processor and 
              hasattr(self.processing_hub.node_processor, 'node_manager') and 
              self.processing_hub.node_processor.node_manager):
            node_manager = self.processing_hub.node_processor.node_manager
            logger.debug("Using ProcessingHub's node_processor NodeManager for critical pinning")
        
        if node_manager:
            for pin_path in critical_pins:
                node_manager.get_node_metadata(pin_path).is_pinned = True
                node_manager._log_system_event(
                    "integration_pin",
                    f"Node '{pin_path}' pinned as critical integration point.",
                    [pin_path]
                )
            logger.info(f"Configured {len(critical_pins)} critical node pins")
        else:
            if critical_pins:  # Only warn if there are actually pins to configure
                logger.warning("NodeManager not available for critical pinning")
            else:
                logger.debug("No critical pins to configure")

    def trigger_state_change(self):
        """Trigger immediate processing when world state changes."""
        self.processing_hub.trigger_state_change()
    
    def _on_world_state_change(self):
        """Handle world state changes for both processing and proactive conversations."""
        # Trigger normal processing
        self.processing_hub.trigger_state_change()
        
        # Trigger proactive conversation opportunity detection
        if self.proactive_engine:
            asyncio.create_task(self.proactive_engine.on_world_state_change())

    async def get_system_status(self) -> Dict[str, Any]:
        """
        Get comprehensive system status for the management UI.
        
        Returns:
            Dictionary containing system status information
        """
        try:
            # Get processing hub status
            processing_status = await self.processing_hub.get_processing_status()
            
            # Get world state metrics
            world_state_metrics = {
                "channels_count": len(self.world_state.state.channels),
                "total_messages": sum(len(ch.messages) for ch in self.world_state.state.channels.values()),
                "action_history_count": len(self.world_state.state.action_history.actions),
                "pending_invites": len(self.world_state.get_pending_matrix_invites()),
                "generated_media_count": len(self.world_state.state.generated_media_library),
                "research_entries": len(self.world_state.state.research_database.entries)
            }
            
            # Get tool stats
            tool_stats = self.tool_registry.get_tool_stats()
            
            # Get rate limiter status
            rate_limit_status = self.rate_limiter.get_status()
            
            # Get integration status
            integrations = {
                "matrix": {
                    "connected": self.matrix_observer is not None and getattr(self.matrix_observer.client, 'logged_in', False) if self.matrix_observer else False,
                    "monitored_rooms": getattr(self.matrix_observer, 'channels_to_monitor', []) if self.matrix_observer else [],
                    "pending_invites": len(self.world_state.get_pending_matrix_invites())
                },
                "farcaster": {
                    "connected": self.farcaster_observer is not None,
                    "bot_fid": settings.FARCASTER_BOT_FID,
                    "post_queue_size": getattr(self.farcaster_observer.scheduler.post_queue, 'qsize', lambda: 0)() if self.farcaster_observer and hasattr(self.farcaster_observer, 'scheduler') else 0,
                    "reply_queue_size": getattr(self.farcaster_observer.scheduler.reply_queue, 'qsize', lambda: 0)() if self.farcaster_observer and hasattr(self.farcaster_observer, 'scheduler') else 0
                }
            }
            
            return {
                "system_running": self.running,
                "cycle_count": self.cycle_count,
                "processing": processing_status,
                "world_state": world_state_metrics,
                "tools": tool_stats,
                "rate_limits": rate_limit_status,
                "integrations": integrations,
                "config": {
                    "ai_model": self.config.ai_model,
                    "processing_mode": "node_based" if self.config.processing_config.enable_node_based_processing else "traditional",
                    "observation_interval": self.config.processing_config.observation_interval,
                    "max_cycles_per_hour": self.config.processing_config.max_cycles_per_hour
                }
            }
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {
                "system_running": self.running,
                "error": str(e)
            }

    async def force_processing_mode(self, enable_node_based: bool) -> bool:
        """
        Force the processing mode to a specific type.
        
        Args:
            enable_node_based: True to force node-based processing, False for traditional
        """
        return await self.processing_hub.force_processing_mode(
            "node_based" if enable_node_based else "traditional"
        )

    def reset_processing_mode(self) -> None:
        """Reset processing mode to automatic determination."""
        self.processing_hub.reset_processing_mode()
        logger.info("Processing mode reset to automatic determination")

    def get_tool_registry(self) -> ToolRegistry:
        """Get the tool registry instance."""
        return self.tool_registry

    def get_ai_engine(self) -> AIDecisionEngine:
        """Get the AI engine instance."""
        return self.ai_engine

    def get_world_state_manager(self) -> WorldStateManager:
        """Get the world state manager instance."""
        return self.world_state

    def get_processing_hub(self) -> ProcessingHub:
        """Get the processing hub instance.""" 
        return self.processing_hub

    def increment_cycle_count(self) -> None:
        """Increment the processing cycle counter."""
        self.cycle_count += 1

    # Additional API methods for test compatibility and external usage
    async def process_payload(self, payload: Dict[str, Any], active_channels: list) -> None:
        """Process a payload directly using the traditional processor."""
        if hasattr(self.processing_hub, 'traditional_processor') and self.processing_hub.traditional_processor:
            await self.processing_hub.traditional_processor.process_payload(payload, active_channels)
        else:
            logger.warning("No traditional processor available")

    async def _execute_action(self, action) -> None:
        """Execute a single action - wrapper for test compatibility."""
        denial_reason = self.capability_policy.denial_reason(
            action.action_type, action.parameters
        )
        if denial_reason:
            logger.warning(denial_reason)
            return
        if hasattr(self.processing_hub, 'traditional_processor') and self.processing_hub.traditional_processor:
            await self.processing_hub.traditional_processor._execute_action(action)
        else:
            logger.warning("No traditional processor available, executing action directly")
            # For test compatibility, execute matrix actions directly
            if action.action_type in ["send_matrix_reply", "send_matrix_message"]:
                await self._execute_matrix_action_directly(action)
            else:
                logger.warning(f"Cannot execute action type {action.action_type} without traditional processor")
    
    async def _execute_matrix_action_directly(self, action) -> None:
        """Execute matrix actions directly for test compatibility."""
        try:
            from ...tools.matrix_tools import SendMatrixReplyTool, SendMatrixMessageTool
            
            # Get matrix observer from integration manager
            active_integrations = self.integration_manager.get_active_integrations()
            matrix_integration = None
            for integration_id, integration in active_integrations.items():
                if hasattr(integration, 'name') and integration.name == 'matrix':
                    matrix_integration = integration
                    break
            
            # Update action context with required components
            self.action_context.matrix_observer = matrix_integration or self.matrix_observer
            self.action_context.world_state_manager = self.world_state
            self.action_context.context_manager = self.context_manager
            
            if action.action_type == "send_matrix_reply":
                tool = SendMatrixReplyTool()
            elif action.action_type == "send_matrix_message":
                tool = SendMatrixMessageTool()
            else:
                logger.error(f"Unknown matrix action type: {action.action_type}")
                return
                
            result = await tool.execute(action.parameters, self.action_context)
            logger.info(f"Direct matrix action execution result: {result}")
            
        except Exception as e:
            logger.error(f"Error executing matrix action directly: {str(e)}")
            logger.exception(e)

    async def _process_channel(self, channel_id: str) -> None:
        """Process a specific channel - simplified implementation for tests."""
        try:
            # Get conversation messages
            messages = await self.context_manager.get_conversation_messages(channel_id)
            
            # Build world state payload
            payload = self.world_state.to_dict()
            payload['messages'] = messages
            
            # Process using traditional processor
            await self.process_payload(payload, [channel_id])
            
        except Exception as e:
            logger.error(f"Error processing channel {channel_id}: {e}")

    async def add_user_message(self, channel_id: str, message_data: Dict[str, Any]) -> None:
        """Add a user message to the context."""
        await self.context_manager.add_user_message(channel_id, message_data)

    async def get_context_summary(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Get context summary for a channel."""
        return await self.context_manager.get_context_summary(channel_id)

    async def clear_context(self, channel_id: str) -> None:
        """Clear context for a channel."""
        await self.context_manager.clear_context(channel_id)

    async def _ensure_media_gallery_exists(self) -> None:
        """Check for, create, and configure the media gallery room."""
        if self.config.capability_profile != "operator":
            return
        if settings.MATRIX_MEDIA_GALLERY_ROOM_ID:
            logger.info(f"Matrix media gallery is configured: {settings.MATRIX_MEDIA_GALLERY_ROOM_ID}")
            return

        config_path = Path("data/config.json")
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config_data = json.load(f)
                    gallery_id = config_data.get("MATRIX_MEDIA_GALLERY_ROOM_ID")
                    if gallery_id:
                        settings.MATRIX_MEDIA_GALLERY_ROOM_ID = gallery_id
                        logger.info(f"Loaded Matrix media gallery from config.json: {gallery_id}")
                        return
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not read gallery ID from config.json: {e}")

        logger.info("MATRIX_MEDIA_GALLERY_ROOM_ID not found. Attempting to create a new gallery room...")
        if not self.action_context.matrix_observer:
            logger.error("Cannot create gallery room: Matrix observer is not available.")
            return

        try:
            from nio import RoomCreateResponse, RoomVisibility
            response = await self.action_context.matrix_observer.client.room_create(
                visibility=RoomVisibility.public,
                name="AI Media Gallery",
                topic="A collection of media generated by the AI agent.",
                initial_state=[{"type": "m.room.guest_access", "state_key": "", "content": {"guest_access": "can_join"}}],
            )
            if isinstance(response, RoomCreateResponse) and response.room_id:
                new_room_id = response.room_id
                logger.info(f"Successfully created new Matrix media gallery: {new_room_id}")
                settings.MATRIX_MEDIA_GALLERY_ROOM_ID = new_room_id

                # Persist the new room ID to config.json
                config_data = {}
                if config_path.exists():
                    with open(config_path, 'r') as f: 
                        config_data = json.load(f)
                config_data["MATRIX_MEDIA_GALLERY_ROOM_ID"] = new_room_id
                config_path.parent.mkdir(parents=True, exist_ok=True)
                with open(config_path, 'w') as f: 
                    json.dump(config_data, f, indent=2)
                logger.info(f"Saved new gallery room ID to {config_path}")
            else:
                logger.error(f"Failed to create gallery room. Response: {response}")
        except Exception as e:
            logger.error(f"Exception during gallery room creation: {e}", exc_info=True)

    async def _register_integrations_from_env(self) -> None:
        """Register integrations from environment variables if they don't exist."""
        logger.info("Checking for integrations to register from environment variables...")
        
        # Get existing integrations
        existing_integrations = await self.integration_manager.list_integrations()
        
        # Check for Farcaster integration
        if (settings.NEYNAR_API_KEY and 
            settings.FARCASTER_BOT_FID and 
            settings.FARCASTER_BOT_SIGNER_UUID):
            
            farcaster_exists = any(
                integration.get('integration_type') == 'farcaster' 
                for integration in existing_integrations
            )
            
            if not farcaster_exists:
                logger.info("Registering Farcaster integration from environment variables...")
                try:
                    await self.integration_manager.add_integration(
                        integration_type='farcaster',
                        display_name='Farcaster Bot',
                        config={
                            'username': settings.FARCASTER_BOT_USERNAME or 'farcaster_bot'
                        },
                        credentials={
                            'api_key': settings.NEYNAR_API_KEY,
                            'bot_fid': settings.FARCASTER_BOT_FID,
                            'signer_uuid': settings.FARCASTER_BOT_SIGNER_UUID
                        }
                    )
                    logger.info("✓ Farcaster integration registered successfully")
                except Exception as e:
                    logger.error(f"Failed to register Farcaster integration: {e}")
            else:
                logger.info("Farcaster integration already exists, updating credentials from environment...")
                # Update credentials for existing integration
                farcaster_integration = next(
                    (integration for integration in existing_integrations 
                     if integration.get('integration_type') == 'farcaster'), None
                )
                if farcaster_integration:
                    try:
                        # Clean up any invalid credentials first
                        await self.integration_manager.clean_invalid_credentials(farcaster_integration['integration_id'])
                        
                        # Update credentials from environment
                        await self.integration_manager.update_credentials(
                            farcaster_integration['integration_id'],
                            {
                                'api_key': settings.NEYNAR_API_KEY,
                                'bot_fid': settings.FARCASTER_BOT_FID,
                                'signer_uuid': settings.FARCASTER_BOT_SIGNER_UUID
                            }
                        )
                        logger.info("✓ Farcaster credentials updated from environment variables")
                    except Exception as e:
                        logger.error(f"Failed to update Farcaster credentials: {e}")
        else:
            logger.debug("Farcaster environment variables not fully configured, skipping auto-registration")
            # If environment variables aren't set but integration exists, remove it
            farcaster_integration = next(
                (integration for integration in existing_integrations 
                 if integration.get('integration_type') == 'farcaster'), None
            )
            if farcaster_integration:
                logger.info("Removing Farcaster integration since environment variables are not configured")
                try:
                    await self.integration_manager.remove_integration(farcaster_integration['integration_id'])
                    logger.info("✓ Farcaster integration removed successfully")
                except Exception as e:
                    logger.error(f"Failed to remove Farcaster integration: {e}")
        
        # Check for Matrix integration
        if matrix_auth_is_configured(settings):
            matrix_credentials = {
                "homeserver": settings.MATRIX_HOMESERVER,
                "user_id": settings.MATRIX_USER_ID,
            }
            if settings.MATRIX_PASSWORD:
                matrix_credentials["password"] = settings.MATRIX_PASSWORD
            if settings.MATRIX_ACCESS_TOKEN:
                matrix_credentials["access_token"] = settings.MATRIX_ACCESS_TOKEN
                matrix_credentials["device_id"] = settings.MATRIX_DEVICE_ID
            
            matrix_exists = any(
                integration.get('integration_type') == 'matrix' 
                for integration in existing_integrations
            )
            
            if not matrix_exists:
                logger.info("Registering Matrix integration from environment variables...")
                try:
                    await self.integration_manager.add_integration(
                        integration_type='matrix',
                        display_name='Matrix Bot',
                        config={
                            'room_id': settings.MATRIX_ROOM_ID,
                            'device_name': settings.DEVICE_NAME
                        },
                        credentials=matrix_credentials,
                    )
                    logger.info("✓ Matrix integration registered successfully")
                except Exception as e:
                    logger.error(f"Failed to register Matrix integration: {e}")
            else:
                logger.info("Matrix integration already exists, updating credentials from environment...")
                # Update credentials for existing integration
                matrix_integration = next(
                    (integration for integration in existing_integrations 
                     if integration.get('integration_type') == 'matrix'), None
                )
                if matrix_integration:
                    try:
                        # Clean up any invalid credentials first
                        await self.integration_manager.clean_invalid_credentials(matrix_integration['integration_id'])
                        
                        # Update credentials from environment
                        await self.integration_manager.update_credentials(
                            matrix_integration['integration_id'],
                            matrix_credentials,
                        )
                        logger.info("✓ Matrix credentials updated from environment variables")
                    except Exception as e:
                        logger.error(f"Failed to update Matrix credentials: {e}")
        else:
            logger.debug("Matrix environment variables not fully configured, skipping auto-registration")
            # If environment variables aren't set but integration exists, remove it
            matrix_integration = next(
                (integration for integration in existing_integrations 
                 if integration.get('integration_type') == 'matrix'), None
            )
            if matrix_integration:
                logger.info("Removing Matrix integration since environment variables are not configured")
                try:
                    await self.integration_manager.remove_integration(matrix_integration['integration_id'])
                    logger.info("✓ Matrix integration removed successfully")
                except Exception as e:
                    logger.error(f"Failed to remove Matrix integration: {e}")
