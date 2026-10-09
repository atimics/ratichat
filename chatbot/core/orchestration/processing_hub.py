"""
Processing Hub

Central hub for handling different processing strategies (traditional vs node-based)
and managing the main event loop logic.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..world_state.manager import WorldStateManager
    from ..world_state.payload_builder import PayloadBuilder
    from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


@dataclass
class ProcessingConfig:
    """Configuration for processing strategy selection."""
    
    # Processing mode settings
    enable_node_based_processing: bool = True
    force_traditional_fallback: bool = False
    max_traditional_payload_size: int = 80000  # Bytes
    
    # Observation settings
    observation_interval: float = 2.0
    max_cycles_per_hour: int = 300
    
    # AI Model settings
    traditional_ai_model: str = "openai/gpt-4o-mini"
    node_based_ai_model: str = "openai/gpt-4o-mini"


class ProcessingHub:
    """
    Central processing hub that coordinates different processing strategies.
    
    This class is responsible for:
    1. Managing the main event loop
    2. Selecting between traditional and node-based processing
    3. Coordinating rate limiting and state change detection
    4. Providing unified status and metrics
    """
    
    def __init__(
        self,
        world_state_manager: "WorldStateManager",
        payload_builder: "PayloadBuilder", 
        rate_limiter: "RateLimiter",
        config: Optional[ProcessingConfig] = None
    ):
        self.world_state = world_state_manager
        self.payload_builder = payload_builder
        self.rate_limiter = rate_limiter
        self.config = config or ProcessingConfig()
        
        # Processing state
        self.running = False
        self.cycle_count = 0
        self.last_cycle_time = 0
        self.current_processing_mode = "traditional"
        self.payload_size_history: List[int] = []
        
        # Event coordination
        self.state_changed_event = asyncio.Event()
        
        # Component availability tracking
        self.traditional_processor = None
        self.node_processor = None
        
    def set_traditional_processor(self, processor):
        """Set the traditional AI processing component."""
        self.traditional_processor = processor
        
    def set_node_processor(self, processor):
        """Set the node-based processing component."""
        self.node_processor = processor
        self.node_manager = processor.node_manager
        self.current_processing_mode = self._determine_processing_mode([])
        
    async def start_processing_loop(self) -> None:
        """Start the main processing event loop."""
        if self.running:
            logger.warning("Processing hub already running")
            return

        logger.info("Starting processing hub...")
        self.running = True
        
        try:
            await self._main_event_loop()
        except Exception as e:
            logger.error(f"Error in processing hub: {e}")
            raise
        finally:
            self.running = False
            logger.info("Processing hub stopped")

    def stop_processing_loop(self):
        """Stop the processing loop."""
        self.running = False
        
    def trigger_state_change(self):
        """Trigger immediate processing when world state changes."""
        if self.state_changed_event and not self.state_changed_event.is_set():
            self.state_changed_event.set()
            logger.debug("State change event triggered")

    async def _main_event_loop(self) -> None:
        """Main event loop for processing world state changes."""
        logger.info("Starting main event loop...")
        last_state_hash = None

        while self.running:
            try:
                # Wait for state change event or timeout
                try:
                    await asyncio.wait_for(
                        self.state_changed_event.wait(),
                        timeout=self.config.observation_interval,
                    )
                    self.state_changed_event.clear()
                    logger.info("State change event triggered")
                except asyncio.TimeoutError:
                    # Periodic check even if no events
                    pass

                cycle_start = time.time()

                # Check rate limiting
                can_process, wait_time = self.rate_limiter.can_process_cycle(cycle_start)

                if not can_process:
                    if wait_time > 0:
                        logger.debug(f"Rate limiting: waiting {wait_time:.2f}s before next cycle")
                        await asyncio.sleep(min(wait_time, self.config.observation_interval))
                    continue

                # Get current world state
                current_state = self.world_state.to_dict()
                current_hash = self._hash_state(current_state)
                pending_channels = []
                if self.node_processor and hasattr(self.node_processor, "pending_channels"):
                    pending = self.node_processor.pending_channels()
                    if isinstance(pending, list):
                        pending_channels = pending

                # Check if state has changed
                if current_hash != last_state_hash or pending_channels:
                    # Idle polling keeps the processing budget available for chat.
                    self.rate_limiter.record_cycle(cycle_start)
                    logger.info(f"World state changed, processing cycle {self.cycle_count}")

                    # Get active channels to determine primary focus
                    active_channels = self._get_active_channels(current_state)
                    active_channels = list(dict.fromkeys(active_channels + pending_channels))

                    # Process using selected strategy
                    await self._process_world_state(active_channels)

                    # Update tracking
                    last_state_hash = current_hash
                    self.cycle_count += 1
                    self.last_cycle_time = cycle_start

                    cycle_duration = time.time() - cycle_start
                    logger.info(f"Cycle {self.cycle_count} completed in {cycle_duration:.2f}s")

                    # Log rate limiting status every 10 cycles for monitoring
                    if self.cycle_count % 10 == 0:
                        self._log_rate_limit_status()

            except Exception as e:
                logger.error(f"Error in event loop cycle {self.cycle_count}: {e}")
                await asyncio.sleep(5)

    async def _process_world_state(self, active_channels: List[str]) -> None:
        """
        Process world state using the appropriate strategy.
        """
        try:
            # Determine processing mode
            processing_mode = self._determine_processing_mode(active_channels)
            
            if processing_mode == "node_based" and self.node_processor:
                await self._process_with_node_based_strategy(active_channels)
            else:
                await self._process_with_traditional_strategy(active_channels)
                
        except Exception as e:
            logger.error(f"Error in world state processing: {e}")
            # Mode fallback happens before a turn starts. A failed turn may
            # already have sent an external action, so keep its claimed scope.

    def _determine_processing_mode(self, active_channels: List[str]) -> str:
        """
        Determine whether to use traditional or node-based processing.
        
        Returns: "traditional" or "node_based"
        """
        # Force traditional fallback if configured
        if self.config.force_traditional_fallback:
            return "traditional"
        
        # Use traditional if node processor is not available
        if not self.config.enable_node_based_processing or not self.node_processor:
            return "traditional"
        
        # Node processing is the configured default, including small requests.
        try:
            estimated_size = self.payload_builder.estimate_payload_size(
                self.world_state.get_state_data()
            )
            
            # Track payload size history
            self.payload_size_history.append(estimated_size)
            if len(self.payload_size_history) > 10:
                self.payload_size_history.pop(0)
            
        except Exception as e:
            logger.error(f"Error estimating payload size: {e}")
        return "node_based"

    async def _process_with_traditional_strategy(self, active_channels: List[str]) -> None:
        """Process using the traditional full payload approach."""
        self.current_processing_mode = "traditional"
        
        if not self.traditional_processor:
            logger.error("Traditional processor not available")
            return
            
        try:
            if self.node_processor and hasattr(self.node_processor, "pending_channels"):
                saved_channels = self.node_processor.pending_channels()
                if isinstance(saved_channels, list):
                    scoped_channels = list(dict.fromkeys(saved_channels + [
                        channel_id for channel_id in active_channels
                        if (self.world_state.get_channel(channel_id)
                            and self.world_state.get_channel(channel_id).type in {"discord", "matrix"})
                    ]))
                    for channel_id in scoped_channels:
                        await self.node_processor.process_cycle(
                            cycle_id=f"cycle_{self.cycle_count}", primary_channel_id=channel_id,
                            context={"processing_mode": "traditional"},
                        )
                    active_channels = [channel_id for channel_id in active_channels if channel_id not in scoped_channels]
                    if not active_channels:
                        return
            # Determine primary channel
            primary_channel_id = self._get_primary_channel(active_channels)
            
            # Build full payload with optimized configuration for smaller size
            from ...config import settings
            config = {
                "optimize_for_size": True,
                "include_detailed_user_info": settings.AI_INCLUDE_DETAILED_USER_INFO,
                "max_messages_per_channel": settings.AI_CONVERSATION_HISTORY_LENGTH,
                "max_action_history": settings.AI_ACTION_HISTORY_LENGTH,
                "max_thread_messages": settings.AI_THREAD_HISTORY_LENGTH,
                "max_other_channels": settings.AI_OTHER_CHANNELS_SUMMARY_COUNT,
                "message_snippet_length": settings.AI_OTHER_CHANNELS_MESSAGE_SNIPPET_LENGTH,
                "bot_fid": settings.FARCASTER_BOT_FID,
                "bot_username": settings.FARCASTER_BOT_USERNAME,
            }
            
            payload = self.payload_builder.build_full_payload(
                world_state_data=self.world_state.get_state_data(),
                primary_channel_id=primary_channel_id,
                config=config
            )
            
            # Process with traditional approach
            await self.traditional_processor.process_payload(payload, active_channels)
            
            logger.debug("Processed with traditional approach")
            
        except Exception as e:
            logger.error(f"Error in traditional processing: {e}")
            raise

    async def _process_with_node_based_strategy(self, active_channels: List[str]) -> None:
        """Process using the node-based interactive exploration approach."""
        self.current_processing_mode = "node_based"
        
        if not self.node_processor:
            logger.error("Node processor not available")
            return
            
        try:
            # Determine primary channel
            primary_channel_id = self._get_primary_channel(active_channels)
            
            # Process with node-based approach
            cycle_id = f"cycle_{self.cycle_count}"
            result = {"actions_executed": 0}
            # Every active channel gets its own scoped request. A busy channel
            # cannot hide another channel's pending request.
            channel_ids = [primary_channel_id] + [channel for channel in active_channels if channel != primary_channel_id]
            for channel_id in channel_ids:
                turn = await self.node_processor.process_cycle(
                    cycle_id=cycle_id, primary_channel_id=channel_id,
                    context={"processing_mode": "node_based"},
                )
                result["actions_executed"] += turn.get("actions_executed", 0)
            
            if result.get("actions_executed", 0) > 0:
                logger.info(f"Node processor executed {result['actions_executed']} actions")
            else:
                logger.debug("Node processor found no actions to execute")
                
        except Exception as e:
            logger.error(f"Error in node-based processing: {e}")
            raise

    def _get_primary_channel(self, active_channels: List[str]) -> Optional[str]:
        """Get the primary (most recently active) channel."""
        if not active_channels:
            return None
            
        # Sort by recent activity to get most active channel as primary
        channel_activity = []
        for channel_id in active_channels:
            channel_data = self.world_state.get_channel(channel_id)
            if channel_data and channel_data.recent_messages:
                last_msg_time = channel_data.recent_messages[-1].timestamp
                channel_activity.append((channel_id, last_msg_time))

        if channel_activity:
            # Primary channel is the one with most recent activity
            channel_activity.sort(key=lambda x: x[1], reverse=True)
            return channel_activity[0][0]
            
        return None

    def _get_active_channels(self, world_state_dict: Dict[str, Any]) -> List[str]:
        """Extract active channels from world state."""
        active_channels = []
        
        channels = world_state_dict.get("channels", {})
        current_time = time.time()
        
        for channel_id, channel_data in channels.items():
            # Consider channels with recent activity (last hour)
            recent_messages = channel_data.get("recent_messages", [])
            if recent_messages:
                last_message_time = recent_messages[-1].get("timestamp", 0)
                if current_time - last_message_time < 3600:  # 1 hour
                    active_channels.append(channel_id)
        
        return active_channels

    def _hash_state(self, state_dict: Dict[str, Any]) -> str:
        """Generate a stable hash of state that can change a bot decision.

        ``recent_activity`` is a derived view. It includes the wall clock time on
        every call. The observation counters also change while an AI cycle is
        being built. Including either value makes an idle system look active and
        can trigger a paid model request on every polling interval.
        """
        import hashlib
        import json

        stable_state = {
            key: value
            for key, value in state_dict.items()
            if key != "recent_activity"
        }
        system_status = stable_state.get("system_status")
        if isinstance(system_status, dict):
            stable_state["system_status"] = {
                key: value
                for key, value in system_status.items()
                if key not in {"last_observation_cycle", "total_cycles"}
            }

        # Create a deterministic representation
        state_str = json.dumps(stable_state, sort_keys=True, default=str)
        return hashlib.sha256(state_str.encode()).hexdigest()

    def _log_rate_limit_status(self):
        """Log current rate limiting status."""
        try:
            current_time = time.time()
            status = self.rate_limiter.get_rate_limit_status(current_time)
            logger.info(f"Rate limit status: {status['cycles_per_hour']}/{status['max_cycles_per_hour']} cycles/hour")
        except Exception as e:
            logger.error(f"Error logging rate limit status: {e}")

    def get_processing_status(self) -> Dict[str, Any]:
        """Get comprehensive processing status."""
        return {
            "running": self.running,
            "current_mode": self.current_processing_mode,
            "cycle_count": self.cycle_count,
            "last_cycle_time": self.last_cycle_time,
            "payload_size_history": self.payload_size_history[-5:],  # Last 5 estimates
            "traditional_processor_available": self.traditional_processor is not None,
            "node_processor_available": self.node_processor is not None,
            "node_request": self.node_processor.last_result if self.node_processor else {},
            "config": {
                "enable_node_based_processing": self.config.enable_node_based_processing,
                "force_traditional_fallback": self.config.force_traditional_fallback,
                "max_traditional_payload_size": self.config.max_traditional_payload_size,
                "observation_interval": self.config.observation_interval,
            }
        }

    def get_rate_limit_status(self) -> Dict[str, Any]:
        """Get current rate limiting status."""
        current_time = time.time()
        return self.rate_limiter.get_rate_limit_status(current_time)

    async def force_processing_mode(self, mode: str) -> bool:
        """
        Force a specific processing mode.
        
        Args:
            mode: "traditional" or "node_based"
            
        Returns:
            bool: True if mode was set successfully
        """
        if mode not in ["traditional", "node_based"]:
            logger.error(f"Invalid processing mode: {mode}")
            return False
        
        if mode == "node_based" and not self.node_processor:
            logger.error("Cannot force node-based mode: processor not available")
            return False
        
        # Update configuration
        if mode == "traditional":
            self.config.force_traditional_fallback = True
        else:
            self.config.force_traditional_fallback = False
            self.config.enable_node_based_processing = True
        self.current_processing_mode = mode
        
        logger.info(f"Forced processing mode to: {mode}")
        return True

    def reset_processing_mode(self):
        """Reset to automatic processing mode selection."""
        self.config.force_traditional_fallback = False
        self.current_processing_mode = self._determine_processing_mode([])
        logger.info("Reset to automatic processing mode selection")
