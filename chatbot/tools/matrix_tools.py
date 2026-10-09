"""
Matrix platform-specific tools.
"""
import logging
import time
from typing import Any, Dict

from ..config import settings
from ..utils.markdown_utils import format_for_matrix
from .base import ActionContext, ToolInterface

logger = logging.getLogger(__name__)


class SendMatrixReplyTool(ToolInterface):
    """
    Tool for sending replies to specific messages in Matrix channels.
    """

    @property
    def name(self) -> str:
        return "send_matrix_reply"

    @property
    def description(self) -> str:
        return ("Reply to a specific message in a Matrix channel. If reply_to_id is not provided, will send as a regular message to the channel. "
                "Recently generated media (within 5 minutes) will be automatically attached as a separate image message if no explicit image_url is provided.")

    @property
    def parameters_schema(self) -> Dict[str, Any]:
        return {
            "channel_id": "string (Matrix room ID) - The room where the reply should be sent",
            "content": "string - The message content to send as a reply (supports markdown formatting)",
            "reply_to_id": "string (optional) - The event ID of the message to reply to. If not provided, sends as regular message",
            "format_as_markdown": "boolean (optional, default: true) - Whether to format the content as markdown",
            "image_url": "string (optional) - URL of an image to attach. If not provided, recently generated media will be auto-attached",
        }

    async def execute(
        self, params: Dict[str, Any], context: ActionContext
    ) -> Dict[str, Any]:
        """
        Execute the Matrix reply action.
        """
        logger.info(f"Executing tool '{self.name}' with params: {params}")

        # Check if Matrix integration is available
        if not context.matrix_observer:
            error_msg = "Matrix integration (observer) not configured."
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        # Extract and validate parameters
        room_id = params.get("channel_id")
        content = params.get("content")
        reply_to_event_id = params.get("reply_to_id")
        format_as_markdown = params.get("format_as_markdown", True)
        image_url = None if params.get("delivery_id") else params.get("image_url")

        missing_params = []
        if not room_id:
            missing_params.append("channel_id")
        if not content:
            missing_params.append("content")

        if missing_params:
            error_msg = f"Missing required parameters for Matrix reply: {', '.join(missing_params)}"
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        # Auto-attachment: Check for recently generated media if no image_url provided
        if not params.get("delivery_id") and not image_url and context.world_state_manager:
            recent_media_url = context.world_state_manager.get_last_generated_media_url()
            if recent_media_url:
                # Check if the media was generated recently (within last 5 minutes)
                if hasattr(context.world_state_manager.state, 'generated_media_library'):
                    media_library = context.world_state_manager.state.generated_media_library
                    if media_library:
                        last_media = media_library[-1]
                        media_age = time.time() - last_media.get('timestamp', 0)
                        if media_age <= 300:  # 5 minutes
                            image_url = recent_media_url
                            logger.info(f"Auto-attaching recently generated media to Matrix reply: {image_url}")

        # Deduplication check: prevent replying to events we've already replied to
        if reply_to_event_id and context.world_state_manager:
            if context.world_state_manager.has_bot_replied_to_matrix_event(reply_to_event_id):
                warning_msg = f"Bot has already replied to Matrix event {reply_to_event_id}, skipping to prevent feedback loop"
                logger.warning(warning_msg)
                return {
                    "status": "skipped",
                    "message": warning_msg,
                    "event_id": reply_to_event_id,
                    "room_id": room_id,
                    "reason": "already_replied",
                    "timestamp": time.time(),
                }

        # If reply_to_id is missing but we have channel_id and content, fall back to regular message
        if not reply_to_event_id:
            logger.info(
                f"reply_to_id missing, falling back to regular message in {room_id}"
            )
            try:
                # Format content if markdown is enabled
                if format_as_markdown:
                    formatted = format_for_matrix(content)
                    result = await context.matrix_observer.send_formatted_message(
                        room_id, formatted["plain"], formatted["html"]
                    )
                else:
                    result = await context.matrix_observer.send_message(
                        room_id, content
                    )

                logger.info(f"Matrix observer send_message returned: {result}")

                if result.get("success"):
                    event_id = result.get("event_id", "unknown")
                    success_msg = f"Sent Matrix message (fallback from reply) to {room_id} (event: {event_id})"
                    logger.info(success_msg)

                    # Record the sent message in world state for AI blindness fix
                    if context.world_state_manager:
                        from ..core.world_state.structures import Message
                        bot_message = Message(
                            id=event_id,
                            channel_id=room_id,
                            channel_type="matrix",
                            sender=settings.MATRIX_USER_ID,
                            content=content,
                            timestamp=time.time(),
                            reply_to=None  # This is a fallback message, not a reply
                        )
                        context.world_state_manager.add_message(room_id, bot_message)
                        logger.debug(f"Recorded sent Matrix fallback message in world state: {event_id}")

                    # Record the sent message in context manager for AI blindness fix
                    if context.context_manager:
                        assistant_message = {
                            "event_id": event_id,
                            "sender": settings.MATRIX_USER_ID,
                            "content": content,
                            "timestamp": time.time(),
                            "type": "assistant"
                        }
                        await context.context_manager.add_assistant_message(room_id, assistant_message)
                        logger.debug(f"Recorded sent Matrix fallback message in context manager: {event_id}")

                    # Send auto-attached image if available (fallback case)
                    image_event_id = None
                    if image_url:
                        try:
                            image_result = await context.matrix_observer.send_image(
                                room_id, image_url, None, None
                            )
                            if image_result.get("success"):
                                image_event_id = image_result.get("event_id", "unknown")
                                logger.info(f"Auto-attached image to Matrix fallback message: {image_event_id}")
                            else:
                                logger.warning(f"Failed to auto-attach image to fallback: {image_result.get('error', 'unknown error')}")
                        except Exception as e:
                            logger.warning(f"Error auto-attaching image to fallback: {e}")

                    return {
                        "status": "success",
                        "message": success_msg,
                        "event_id": event_id,
                        "room_id": room_id,
                        "sent_content": content,
                        "fallback_to_message": True,  # Indicate this was a fallback
                        "auto_attached_image": image_url if image_url else None,
                        "image_event_id": image_event_id,
                        "timestamp": time.time(),
                    }
                else:
                    error_msg = f"Failed to send Matrix message (fallback) via observer: {result.get('error', 'unknown error')}"
                    logger.error(error_msg)
                    return {
                        "status": "failure",
                        "error": error_msg,
                        "timestamp": time.time(),
                    }

            except Exception as e:
                error_msg = (
                    f"Error executing fallback message for {self.name}: {str(e)}"
                )
                logger.exception(error_msg)
                return {
                    "status": "failure",
                    "error": error_msg,
                    "timestamp": time.time(),
                }

        try:
            # Format content if markdown is enabled
            if format_as_markdown:
                formatted = format_for_matrix(content)
                result = await context.matrix_observer.send_formatted_reply(
                    room_id, formatted["plain"], formatted["html"], reply_to_event_id,
                    **({"tx_id": params["delivery_id"]} if params.get("delivery_id") else {}),
                )
            else:
                result = await context.matrix_observer.send_reply(
                    room_id, content, reply_to_event_id,
                    **({"tx_id": params["delivery_id"]} if params.get("delivery_id") else {}),
                )
            logger.info(f"Matrix observer send_reply returned: {result}")

            if result.get("success"):
                event_id = result.get("event_id", "unknown")
                success_msg = f"Sent Matrix reply to {room_id} (event: {event_id})"
                logger.info(success_msg)

                # Record the sent message in world state for AI blindness fix
                if context.world_state_manager:
                    from ..core.world_state.structures import Message
                    bot_message = Message(
                        id=event_id,
                        channel_id=room_id,
                        channel_type="matrix",
                        sender=settings.MATRIX_USER_ID,  # Use bot user ID from settings
                        content=content,
                        timestamp=time.time(),
                        reply_to=reply_to_event_id
                    )
                    context.world_state_manager.add_message(room_id, bot_message)
                    logger.debug(f"Recorded sent Matrix reply in world state: {event_id}")

                # Record the sent message in context manager for AI blindness fix
                if context.context_manager:
                    assistant_message = {
                        "event_id": event_id,
                        "sender": settings.MATRIX_USER_ID,
                        "content": content,
                        "timestamp": time.time(),
                        "type": "assistant"
                    }
                    await context.context_manager.add_assistant_message(room_id, assistant_message)
                    logger.debug(f"Recorded sent Matrix reply in context manager: {event_id}")

                # Send auto-attached image if available
                image_event_id = None
                if image_url:
                    try:
                        image_result = await context.matrix_observer.send_image(
                            room_id, image_url, None, None
                        )
                        if image_result.get("success"):
                            image_event_id = image_result.get("event_id", "unknown")
                            logger.info(f"Auto-attached image to Matrix reply: {image_event_id}")
                        else:
                            logger.warning(f"Failed to auto-attach image to reply: {image_result.get('error', 'unknown error')}")
                    except Exception as e:
                        logger.warning(f"Error auto-attaching image to reply: {e}")

                return {
                    "status": "success",
                    "message": success_msg,
                    "event_id": event_id,
                    "room_id": room_id,
                    "reply_to_event_id": reply_to_event_id,
                    "sent_content": content,  # For AI Blindness Fix
                    "auto_attached_image": image_url if image_url else None,
                    "image_event_id": image_event_id,
                    "timestamp": time.time(),
                }
            else:
                error_msg = f"Failed to send Matrix reply via observer: {result.get('error', 'unknown error')}"
                logger.error(error_msg)
                return {
                    "status": result.get("status", "failure"),
                    "error": error_msg,
                    "timestamp": time.time(),
                }

        except Exception as e:
            error_msg = f"Error executing {self.name}: {str(e)}"
            logger.exception(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}


class SendMatrixMessageTool(ToolInterface):
    """
    Tool for sending new messages to Matrix channels.
    """

    @property
    def name(self) -> str:
        return "send_matrix_message"

    @property
    def description(self) -> str:
        return ("Send a new message to a Matrix channel. Use this when you want to start a new conversation or make an announcement. "
                "Recently generated media (within 5 minutes) will be automatically attached as a separate image message if no explicit image_url is provided.")

    @property
    def parameters_schema(self) -> Dict[str, Any]:
        return {
            "channel_id": "string (Matrix room ID) - The room where the message should be sent",
            "content": "string - The message content to send (supports markdown formatting)",
            "format_as_markdown": "boolean (optional, default: true) - Whether to format the content as markdown",
            "image_url": "string (optional) - URL of an image to attach. If not provided, recently generated media will be auto-attached",
        }

    async def execute(
        self, params: Dict[str, Any], context: ActionContext
    ) -> Dict[str, Any]:
        """
        Execute the Matrix message action.
        """
        logger.info(f"Executing tool '{self.name}' with params: {params}")

        # Check if Matrix integration is available
        if not context.matrix_observer:
            error_msg = "Matrix integration (observer) not configured."
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        # Extract and validate parameters
        room_id = params.get("channel_id")
        content = params.get("content")
        format_as_markdown = params.get("format_as_markdown", True)
        image_url = params.get("image_url")

        missing_params = []
        if not room_id:
            missing_params.append("channel_id")
        if not content:
            missing_params.append("content")

        if missing_params:
            error_msg = f"Missing required parameters for Matrix message: {', '.join(missing_params)}"
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        # Auto-attachment: Check for recently generated media if no image_url provided
        if not image_url and context.world_state_manager:
            recent_media_url = context.world_state_manager.get_last_generated_media_url()
            if recent_media_url:
                # Check if the media was generated recently (within last 5 minutes)
                if hasattr(context.world_state_manager.state, 'generated_media_library'):
                    media_library = context.world_state_manager.state.generated_media_library
                    if media_library:
                        last_media = media_library[-1]
                        media_age = time.time() - last_media.get('timestamp', 0)
                        if media_age <= 300:  # 5 minutes
                            image_url = recent_media_url
                            logger.info(f"Auto-attaching recently generated media to Matrix message: {image_url}")

        try:
            # Send the text message first
            if format_as_markdown:
                formatted = format_for_matrix(content)
                result = await context.matrix_observer.send_formatted_message(
                    room_id, formatted["plain"], formatted["html"]
                )
            else:
                result = await context.matrix_observer.send_message(room_id, content)

            logger.info(f"Matrix observer send_message returned: {result}")

            if result.get("success"):
                event_id = result.get("event_id", "unknown")
                success_msg = f"Sent Matrix message to {room_id} (event: {event_id})"
                logger.info(success_msg)

                # Record the sent message in world state for AI blindness fix
                if context.world_state_manager:
                    from ..core.world_state.structures import Message
                    bot_message = Message(
                        id=event_id,
                        channel_id=room_id,
                        channel_type="matrix",
                        sender=settings.MATRIX_USER_ID,
                        content=content,
                        timestamp=time.time(),
                        reply_to=None  # This is a regular message, not a reply
                    )
                    context.world_state_manager.add_message(room_id, bot_message)
                    logger.debug(f"Recorded sent Matrix message in world state: {event_id}")

                # Record the sent message in context manager for AI blindness fix
                if context.context_manager:
                    assistant_message = {
                        "content": content,
                        "sender": settings.MATRIX_USER_ID,
                        "timestamp": time.time(),
                        "event_id": event_id,
                        "channel_type": "matrix"
                    }
                    try:
                        await context.context_manager.add_assistant_message(room_id, assistant_message)
                        logger.debug(f"Recorded sent Matrix message in context manager: {event_id}")
                    except Exception as e:
                        logger.warning(f"Failed to record message in context manager: {e}")

                # Send auto-attached image if available
                image_event_id = None
                if image_url:
                    try:
                        image_result = await context.matrix_observer.send_image(
                            room_id, image_url, None, None
                        )
                        if image_result.get("success"):
                            image_event_id = image_result.get("event_id", "unknown")
                            logger.info(f"Auto-attached image to Matrix message: {image_event_id}")
                        else:
                            logger.warning(f"Failed to auto-attach image: {image_result.get('error', 'unknown error')}")
                    except Exception as e:
                        logger.warning(f"Error auto-attaching image: {e}")

                return {
                    "status": "success",
                    "message": success_msg,
                    "event_id": event_id,
                    "room_id": room_id,
                    "sent_content": content,  # For AI Blindness Fix
                    "auto_attached_image": image_url if image_url else None,
                    "image_event_id": image_event_id,
                    "timestamp": time.time(),
                }
            else:
                error_msg = f"Failed to send Matrix message via observer: {result.get('error', 'unknown error')}"
                logger.error(error_msg)
                return {
                    "status": "failure",
                    "error": error_msg,
                    "timestamp": time.time(),
                }

        except Exception as e:
            error_msg = f"Error executing {self.name}: {str(e)}"
            logger.exception(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}


class JoinMatrixRoomTool(ToolInterface):
    """
    Tool for joining Matrix rooms by room ID or alias.
    """

    @property
    def name(self) -> str:
        return "join_matrix_room"

    @property
    def description(self) -> str:
        return "Join a Matrix room by room ID or alias. Use this when you want to join a new room that you're not currently in."

    @property
    def parameters_schema(self) -> Dict[str, Any]:
        return {
            "room_identifier": "string - The room ID (!room:server.com) or alias (#room:server.com) to join",
        }

    async def execute(
        self, params: Dict[str, Any], context: ActionContext
    ) -> Dict[str, Any]:
        """
        Execute the Matrix room join action.
        """
        logger.info(f"Executing tool '{self.name}' with params: {params}")

        # Check if Matrix integration is available
        if not context.matrix_observer:
            error_msg = "Matrix integration (observer) not configured."
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        # Extract and validate parameters
        room_identifier = params.get("room_identifier")

        if not room_identifier:
            error_msg = "Missing required parameter: room_identifier"
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        try:
            # Use the observer's join_room method
            result = await context.matrix_observer.join_room(room_identifier)
            logger.info(f"Matrix observer join_room returned: {result}")

            if result.get("success"):
                room_id = result.get("room_id", room_identifier)
                success_msg = f"Successfully joined Matrix room {room_id}"
                logger.info(success_msg)

                return {
                    "status": "success",
                    "message": success_msg,
                    "room_id": room_id,
                    "room_identifier": room_identifier,
                    "timestamp": time.time(),
                }
            else:
                error_msg = f"Failed to join Matrix room: {result.get('error', 'unknown error')}"
                logger.error(error_msg)
                return {
                    "status": "failure",
                    "error": error_msg,
                    "timestamp": time.time(),
                }

        except Exception as e:
            error_msg = f"Error executing {self.name}: {str(e)}"
            logger.exception(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}


class LeaveMatrixRoomTool(ToolInterface):
    """
    Tool for leaving Matrix rooms.
    """

    @property
    def name(self) -> str:
        return "leave_matrix_room"

    @property
    def description(self) -> str:
        return "Leave a Matrix room. Use this when you want to stop participating in a room."

    @property
    def parameters_schema(self) -> Dict[str, Any]:
        return {
            "room_id": "string - The room ID to leave",
            "reason": "string (optional) - Reason for leaving the room",
        }

    async def execute(
        self, params: Dict[str, Any], context: ActionContext
    ) -> Dict[str, Any]:
        """
        Execute the Matrix room leave action.
        """
        logger.info(f"Executing tool '{self.name}' with params: {params}")

        # Check if Matrix integration is available
        if not context.matrix_observer:
            error_msg = "Matrix integration (observer) not configured."
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        # Extract and validate parameters
        room_id = params.get("room_id")
        reason = params.get("reason", "Leaving room")

        if not room_id:
            error_msg = "Missing required parameter: room_id"
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        try:
            # Use the observer's leave_room method
            result = await context.matrix_observer.leave_room(room_id, reason)
            logger.info(f"Matrix observer leave_room returned: {result}")

            if result.get("success"):
                success_msg = f"Successfully left Matrix room {room_id}"
                logger.info(success_msg)

                return {
                    "status": "success",
                    "message": success_msg,
                    "room_id": room_id,
                    "reason": reason,
                    "timestamp": time.time(),
                }
            else:
                error_msg = f"Failed to leave Matrix room: {result.get('error', 'unknown error')}"
                logger.error(error_msg)
                return {
                    "status": "failure",
                    "error": error_msg,
                    "timestamp": time.time(),
                }

        except Exception as e:
            error_msg = f"Error executing {self.name}: {str(e)}"
            logger.exception(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}


class AcceptMatrixInviteTool(ToolInterface):
    """
    Tool for accepting Matrix room invitations.
    """

    @property
    def name(self) -> str:
        return "accept_matrix_invite"

    @property
    def description(self) -> str:
        return "Accept a pending Matrix room invitation and join the room. Use this when you want to join a room you've been invited to. You can see pending invites in the world state."

    @property
    def parameters_schema(self) -> Dict[str, Any]:
        return {
            "room_id": "string - The room ID of the invitation to accept (e.g., !xmpqAkRnpDKKtcUWrC:chat.ratimics.com)",
        }

    async def execute(
        self, params: Dict[str, Any], context: ActionContext
    ) -> Dict[str, Any]:
        """
        Execute the Matrix invite acceptance action.
        """
        logger.info(f"Executing tool '{self.name}' with params: {params}")

        # Check if Matrix integration is available
        if not context.matrix_observer:
            error_msg = "Matrix integration (observer) not configured."
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        # Extract and validate parameters
        room_id = params.get("room_id")

        if not room_id:
            error_msg = "Missing required parameter: room_id"
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        try:
            # Use the observer's accept_invite method
            result = await context.matrix_observer.accept_invite(room_id)
            logger.info(f"Matrix observer accept_invite returned: {result}")

            if result.get("success"):
                success_msg = (
                    f"Successfully accepted Matrix room invitation for {room_id}"
                )
                logger.info(success_msg)

                return {
                    "status": "success",
                    "message": success_msg,
                    "room_id": room_id,
                    "timestamp": time.time(),
                }
            else:
                error_msg = f"Failed to accept Matrix room invitation: {result.get('error', 'unknown error')}"
                logger.error(error_msg)
                return {
                    "status": "failure",
                    "error": error_msg,
                    "timestamp": time.time(),
                }

        except Exception as e:
            error_msg = f"Error executing {self.name}: {str(e)}"
            logger.exception(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}


class IgnoreMatrixInviteTool(ToolInterface):
    """
    Tool for ignoring/declining Matrix room invitations.
    """

    @property
    def name(self) -> str:
        return "ignore_matrix_invite"

    @property
    def description(self) -> str:
        return "Ignore or decline a pending Matrix room invitation. Use this when you don't want to join a room you've been invited to."

    @property
    def parameters_schema(self) -> Dict[str, Any]:
        return {
            "room_id": "string - The room ID of the invitation to ignore/decline",
            "reason": "string (optional) - Optional reason for declining the invite"
        }

    async def execute(
        self, params: Dict[str, Any], context: ActionContext
    ) -> Dict[str, Any]:
        """
        Execute the Matrix invite ignoring action.
        """
        logger.info(f"Executing tool '{self.name}' with params: {params}")

        # Check if Matrix integration is available
        if not context.matrix_observer:
            error_msg = "Matrix integration (observer) not configured."
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        # Extract and validate parameters
        room_id = params.get("room_id")
        reason = params.get("reason", "No reason provided")

        if not room_id:
            error_msg = "Missing required parameter: room_id"
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        try:
            # Simply remove the invite from world state without accepting
            if hasattr(context, 'world_state_manager') and context.world_state_manager:
                removed = context.world_state_manager.remove_pending_matrix_invite(room_id)
                if removed:
                    success_msg = f"Successfully ignored Matrix room invitation for {room_id}"
                    logger.info(f"{success_msg}. Reason: {reason}")
                    
                    return {
                        "status": "success",
                        "message": success_msg,
                        "room_id": room_id,
                        "reason": reason,
                        "timestamp": time.time(),
                    }
                else:
                    error_msg = f"No pending invitation found for room {room_id}"
                    logger.warning(error_msg)
                    return {
                        "status": "failure", 
                        "error": error_msg,
                        "timestamp": time.time(),
                    }
            else:
                error_msg = "World state manager not available"
                logger.error(error_msg)
                return {
                    "status": "failure",
                    "error": error_msg,
                    "timestamp": time.time(),
                }

        except Exception as e:
            error_msg = f"Error executing {self.name}: {str(e)}"
            logger.exception(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}


class ReactToMatrixMessageTool(ToolInterface):
    """
    Tool for reacting to Matrix messages with emoji.
    """

    @property
    def name(self) -> str:
        return "react_to_matrix_message"

    @property
    def description(self) -> str:
        return "React to a Matrix message with an emoji. Use this to express emotions or acknowledgments without sending a full message."

    @property
    def parameters_schema(self) -> Dict[str, Any]:
        return {
            "room_id": "string - The room ID where the message is located",
            "event_id": "string - The event ID of the message to react to",
            "emoji": "string - The emoji to react with (e.g., '👍', '❤️', '😀')",
        }

    async def execute(
        self, params: Dict[str, Any], context: ActionContext
    ) -> Dict[str, Any]:
        """
        Execute the Matrix reaction action.
        """
        logger.info(f"Executing tool '{self.name}' with params: {params}")

        # Check if Matrix integration is available
        if not context.matrix_observer:
            error_msg = "Matrix integration (observer) not configured."
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        # Extract and validate parameters
        room_id = params.get("room_id")
        event_id = params.get("event_id")
        emoji = params.get("emoji")

        missing_params = []
        if not room_id:
            missing_params.append("room_id")
        if not event_id:
            missing_params.append("event_id")
        if not emoji:
            missing_params.append("emoji")

        if missing_params:
            error_msg = f"Missing required parameters: {', '.join(missing_params)}"
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        try:
            # Use the observer's react_to_message method
            result = await context.matrix_observer.react_to_message(
                room_id, event_id, emoji
            )
            logger.info(f"Matrix observer react_to_message returned: {result}")

            if result.get("success"):
                success_msg = f"Successfully reacted to message {event_id} with {emoji}"
                logger.info(success_msg)

                # Record this action in world state
                if context.world_state_manager:
                    context.world_state_manager.add_action_result(
                        action_type=self.name,
                        parameters={
                            "room_id": room_id,
                            "event_id": event_id,
                            "emoji": emoji,
                        },
                        result="success",
                    )

                return {
                    "status": "success",
                    "message": success_msg,
                    "room_id": room_id,
                    "event_id": event_id,
                    "emoji": emoji,
                    "timestamp": time.time(),
                }
            else:
                error_msg = f"Failed to react to Matrix message: {result.get('error', 'unknown error')}"
                logger.error(error_msg)

                # Record this action failure in world state
                if context.world_state_manager:
                    context.world_state_manager.add_action_result(
                        action_type=self.name,
                        parameters={
                            "room_id": room_id,
                            "event_id": event_id,
                            "emoji": emoji,
                        },
                        result=f"failure: {result.get('error', 'unknown error')}",
                    )

                return {
                    "status": "failure",
                    "error": error_msg,
                    "timestamp": time.time(),
                }

        except Exception as e:
            error_msg = f"Error executing {self.name}: {str(e)}"
            logger.exception(error_msg)

            # Record this action failure in world state
            if context.world_state_manager:
                context.world_state_manager.add_action_result(
                    action_type=self.name,
                    parameters={
                        "room_id": room_id,
                        "event_id": event_id,
                        "emoji": emoji,
                    },
                    result=f"failure: {str(e)}",
                )

            return {"status": "failure", "error": error_msg, "timestamp": time.time()}


class SendMatrixImageTool(ToolInterface):
    """
    Tool for sending images to Matrix channels.
    """

    @property
    def name(self) -> str:
        return "send_matrix_image"

    @property
    def description(self) -> str:
        return "Send an image to a Matrix room. Use this to share generated images or other images with Matrix users."

    @property
    def parameters_schema(self) -> Dict[str, Any]:
        return {
            "channel_id": "string (Matrix room ID) - The room where the image should be sent",
            "image_url": "string - The URL of the image to send (must be publicly accessible, such as Arweave URLs from image generation)",
            "caption": "string (optional) - Optional text caption or description for the image",
            "filename": "string (optional) - Optional filename for the image (will be auto-detected if not provided)",
        }

    async def execute(
        self, params: Dict[str, Any], context: ActionContext
    ) -> Dict[str, Any]:
        """
        Execute the Matrix image sending action.
        """
        logger.info(f"Executing tool '{self.name}' with params: {params}")

        # Check if Matrix integration is available
        if not context.matrix_observer:
            error_msg = "Matrix integration (observer) not configured."
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        # Extract and validate parameters
        room_id = params.get("channel_id")
        image_url = params.get("image_url")
        caption = params.get("caption")
        filename = params.get("filename")

        missing_params = []
        if not room_id:
            missing_params.append("channel_id")
        if not image_url:
            missing_params.append("image_url")

        if missing_params:
            error_msg = f"Missing required parameters for Matrix image: {', '.join(missing_params)}"
            logger.error(error_msg)
            return {"status": "failure", "error": error_msg, "timestamp": time.time()}

        try:
            # Use the observer's send_image method
            result = await context.matrix_observer.send_image(
                room_id, image_url, filename, caption
            )
            logger.info(f"Matrix observer send_image returned: {result}")

            if result.get("success"):
                event_id = result.get("event_id", "unknown")
                success_msg = f"Sent Matrix image to {room_id} (event: {event_id})"
                logger.info(success_msg)

                # Record this action in world state
                if context.world_state_manager:
                    context.world_state_manager.add_action_result(
                        action_type=self.name,
                        parameters={
                            "room_id": room_id,
                            "image_url": image_url,
                            "caption": caption,
                        },
                        result="success",
                    )

                return {
                    "status": "success",
                    "message": success_msg,
                    "event_id": event_id,
                    "room_id": room_id,
                    "image_url": image_url,
                    "filename": result.get("filename"),
                    "timestamp": time.time(),
                }
            else:
                error_msg = f"Failed to send Matrix image via observer: {result.get('error', 'unknown error')}"
                logger.error(error_msg)

                # Record this action failure in world state
                if context.world_state_manager:
                    context.world_state_manager.add_action_result(
                        action_type=self.name,
                        parameters={
                            "room_id": room_id,
                            "image_url": image_url,
                        },
                        result=f"failure: {result.get('error', 'unknown error')}",
                    )

                return {
                    "status": "failure",
                    "error": error_msg,
                    "timestamp": time.time(),
                }

        except Exception as e:
            error_msg = f"Error executing {self.name}: {str(e)}"
            logger.exception(error_msg)

            # Record this action failure in world state
            if context.world_state_manager:
                context.world_state_manager.add_action_result(
                    action_type=self.name,
                    parameters={
                        "room_id": room_id,
                        "image_url": image_url,
                    },
                    result=f"failure: {str(e)}",
                )

            return {"status": "failure", "error": error_msg, "timestamp": time.time()}


class SendMatrixVideoTool(ToolInterface):
    """
    Tool for sending video files to Matrix channels.
    """

    @property
    def name(self) -> str:
        return "send_matrix_video"

    @property
    def description(self) -> str:
        return "Uploads a video from a URL and sends it to a Matrix room. Use this for sharing generated videos."

    @property
    def parameters_schema(self) -> Dict[str, Any]:
        return {
            "channel_id": "string (Matrix room ID) - The room where the video should be sent",
            "video_url": "string - The public URL of the video to send",
            "caption": "string (optional) - Optional text caption for the video",
            "filename": "string (optional) - Optional filename for the video",
        }

    async def execute(self, params: Dict[str, Any], context: ActionContext) -> Dict[str, Any]:
        logger.info(f"Executing tool '{self.name}' with params: {params}")
        if not context.matrix_observer:
            return {"status": "failure", "error": "Matrix integration not configured."}

        room_id = params.get("channel_id")
        video_url = params.get("video_url")
        caption = params.get("caption")
        filename = params.get("filename", "video.mp4")

        if not room_id or not video_url:
            return {"status": "failure", "error": "Missing required parameters: channel_id and video_url"}

        try:
            # Download the video data from the URL
            import httpx
            
            # Define browser-like headers to bypass WAF rules
            browser_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }
            
            async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
                response = await client.get(video_url, headers=browser_headers)
                response.raise_for_status()
                video_data = response.content
                
                # Get content type from HTTP response headers
                response_content_type = response.headers.get('content-type', '').split(';')[0].strip()

            # Determine MIME type for the video file
            import mimetypes
            mime_type, _ = mimetypes.guess_type(filename)
            
            # Prefer HTTP response content type if it's a video type
            if response_content_type and response_content_type.startswith('video/'):
                mime_type = response_content_type
                logger.info(f"Using content-type from HTTP response: {mime_type}")
            elif not mime_type or not mime_type.startswith('video/'):
                # Fallback to file extension detection
                lower_filename = filename.lower()
                if lower_filename.endswith('.webm'):
                    mime_type = "video/webm"
                elif lower_filename.endswith('.mov'):
                    mime_type = "video/quicktime"
                elif lower_filename.endswith('.avi'):
                    mime_type = "video/avi"
                elif lower_filename.endswith('.mkv'):
                    mime_type = "video/x-matroska"
                else:
                    mime_type = "video/mp4"  # Default fallback
            
            logger.info(f"Final detected video MIME type: {mime_type} for file: {filename}")

            # Upload the video to Matrix media repository
            from nio import UploadResponse, UploadError
            upload_response = await context.matrix_observer.client.upload(
                data_provider=lambda _, __: video_data,
                content_type=mime_type,
                filename=filename,
                filesize=len(video_data)
            )

            if isinstance(upload_response, UploadError):
                raise Exception(f"Failed to upload video to Matrix: {upload_response.message}")
            if not isinstance(upload_response, UploadResponse):
                raise Exception(f"Unexpected upload response type: {type(upload_response)}")

            # Send the video message
            content = {
                "body": caption or filename,
                "msgtype": "m.video",
                "url": upload_response.content_uri,
                "info": {
                    "mimetype": mime_type,
                    "size": len(video_data),
                    # Future enhancement: Add duration, thumbnail_url, w, h
                }
            }
            send_response = await context.matrix_observer.client.room_send(
                room_id=room_id,
                message_type="m.room.message",
                content=content
            )
            
            from nio import RoomSendResponse, RoomSendError
            if isinstance(send_response, RoomSendResponse):
                # Record this action success in world state
                if context.world_state_manager:
                    context.world_state_manager.add_action_result(
                        action_type=self.name,
                        parameters={
                            "room_id": room_id,
                            "video_url": video_url,
                            "caption": caption,
                        },
                        result="success",
                    )

                return {
                    "status": "success", 
                    "event_id": send_response.event_id,
                    "message": f"Successfully sent video to Matrix room {room_id}",
                    "timestamp": time.time()
                }
            else:
                raise Exception(f"Failed to send video message: {send_response}")

        except Exception as e:
            logger.error(f"Error sending Matrix video: {e}", exc_info=True)
            
            # Record this action failure in world state
            if context.world_state_manager:
                context.world_state_manager.add_action_result(
                    action_type=self.name,
                    parameters={
                        "room_id": room_id,
                        "video_url": video_url,
                    },
                    result=f"failure: {str(e)}",
                )
            
            return {"status": "failure", "error": str(e), "timestamp": time.time()}
