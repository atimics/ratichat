"""Capability profiles for AI-selected tool execution."""

from dataclasses import dataclass
from typing import Any, Collection, Mapping


READ_ONLY_SOURCE_TOOLS = frozenset({"web_search", "read_webpage", "read_feed", "read_news", "list_public_sources", "search_social", "read_bluesky_feed"})
WATCH_READ_TOOLS = frozenset({"list_source_watches", "get_source_digest"})
WATCH_WRITE_TOOLS = frozenset({"create_source_watch", "remove_source_watch"})
SOURCE_WATCH_TOOLS = WATCH_READ_TOOLS | WATCH_WRITE_TOOLS
PROACTIVE_READ_TOOLS = frozenset({"get_proactive_status"})
PROACTIVE_WRITE_TOOLS = frozenset({"configure_proactive"})
PROACTIVE_SOURCE_TOOLS = PROACTIVE_READ_TOOLS | PROACTIVE_WRITE_TOOLS
STATE_TOOLS = SOURCE_WATCH_TOOLS | PROACTIVE_SOURCE_TOOLS

PUBLIC_BOT_ALLOWED_TOOLS = READ_ONLY_SOURCE_TOOLS | STATE_TOOLS | frozenset(
    {
        "wait",
        "send_matrix_reply",
        "react_to_matrix_message",
        "send_farcaster_reply",
        "like_farcaster_post",
        "send_discord_reply",
    }
)

PUBLIC_BOT_BLOCKED_PARAMETERS = {
    "send_matrix_reply": frozenset({"image_url"}),
}

MATRIX_MANAGEMENT_TOOLS = frozenset({"manage_matrix_room", "manage_matrix_server"})
MATRIX_STEWARD_TOOLS = PUBLIC_BOT_ALLOWED_TOOLS | MATRIX_MANAGEMENT_TOOLS | {"matrix_server_status"}


@dataclass(frozen=True)
class ExecutionScope:
    """Destinations and source messages available in one processing cycle."""

    channel_id: str | None
    channel_type: str | None
    message_ids: frozenset[str]
    latest_event_id: str | None = None
    latest_sender_id: str | None = None


class CapabilityPolicy:
    """Apply a hard capability ceiling to model-selected actions."""

    SUPPORTED_PROFILES = frozenset({"public", "operator", "matrix_steward"})

    def __init__(
        self,
        profile: str = "public",
        approved_matrix_room_ids: Collection[str] = (),
        control_room_id: str = "",
        operator_user_ids: Collection[str] = (),
        managed_room_ids: Collection[str] = (),
        approved_discord_channel_ids: Collection[str] = (),
        discord_owner_user_ids: Collection[str] = (),
    ) -> None:
        normalized_profile = profile.strip().lower()
        if normalized_profile not in self.SUPPORTED_PROFILES:
            supported = ", ".join(sorted(self.SUPPORTED_PROFILES))
            raise ValueError(
                f"Unknown bot capability profile '{profile}'. Supported profiles: {supported}"
            )
        self.profile = normalized_profile
        self.approved_discord_channel_ids = frozenset(
            value.strip() for value in approved_discord_channel_ids if value.strip()
        )
        self.discord_owner_user_ids = frozenset(value.strip() for value in discord_owner_user_ids if value.strip())
        self.control_room_id = control_room_id
        self.operator_user_ids = frozenset(value.strip() for value in operator_user_ids if value.strip())
        self.managed_room_ids = frozenset(value.strip() for value in managed_room_ids if value.strip())
        self.approved_matrix_room_ids = frozenset(
            room_id.strip()
            for room_id in approved_matrix_room_ids
            if room_id and room_id.strip()
        )

    def allows(self, tool_name: str) -> bool:
        """Return whether this profile permits a model-selected tool."""
        if self.profile == "operator":
            return True
        if self.profile == "matrix_steward":
            return tool_name in MATRIX_STEWARD_TOOLS
        return tool_name in PUBLIC_BOT_ALLOWED_TOOLS

    def filter_tool_names(self, tool_names: Collection[str], execution_scope=None) -> set[str]:
        """Return the names that may be shown to and used by the model."""
        if execution_scope and execution_scope.channel_type == "discord":
            tool_names = set(tool_names) & ({"wait", "send_discord_reply"} | READ_ONLY_SOURCE_TOOLS | STATE_TOOLS)
        return {
            name for name in tool_names
            if self.allows(name) and (
                self.profile != "matrix_steward"
                or name not in MATRIX_MANAGEMENT_TOOLS
                or self._operator_scope(execution_scope)
            ) and (name not in STATE_TOOLS or self._watch_scope(execution_scope))
            and (name not in PROACTIVE_SOURCE_TOOLS or execution_scope.channel_type == "discord")
            and (name not in WATCH_WRITE_TOOLS | PROACTIVE_WRITE_TOOLS or self._watch_owner_scope(execution_scope))
        }

    def _watch_scope(self, scope: ExecutionScope | None) -> bool:
        return bool(scope and scope.latest_event_id and scope.latest_sender_id and (
            scope.channel_type == "discord" and scope.channel_id in self.approved_discord_channel_ids
            or scope.channel_type == "matrix" and scope.channel_id in self.approved_matrix_room_ids
        ))

    def _watch_owner_scope(self, scope: ExecutionScope | None) -> bool:
        if not self._watch_scope(scope):
            return False
        owners = self.discord_owner_user_ids if scope.channel_type == "discord" else self.operator_user_ids
        return scope.latest_sender_id in owners

    def _operator_scope(self, scope: ExecutionScope | None) -> bool:
        return bool(
            scope and scope.channel_type == "matrix"
            and self.control_room_id
            and scope.channel_id == self.control_room_id
            and scope.latest_sender_id in self.operator_user_ids
        )

    def scope_from_payload(self, payload: Mapping[str, Any]) -> ExecutionScope:
        """Build the public action scope from the current source channel."""
        channel_id = payload.get("current_processing_channel_id")
        channels = payload.get("channels")
        if not isinstance(channel_id, str) or not isinstance(channels, Mapping):
            return ExecutionScope(None, None, frozenset())

        channel = channels.get(channel_id)
        if not isinstance(channel, Mapping):
            return ExecutionScope(None, None, frozenset())

        channel_type = channel.get("type")
        if not isinstance(channel_type, str):
            channel_type = None

        recent_messages = channel.get("recent_messages")
        if not isinstance(recent_messages, list):
            recent_messages = []
        message_ids = frozenset(
            message["id"]
            for message in recent_messages
            if isinstance(message, Mapping)
            and isinstance(message.get("id"), str)
            and message["id"]
        )
        latest = recent_messages[-1] if recent_messages else {}
        if not isinstance(latest, Mapping):
            latest = {}
        # The observer sets sender from the Matrix event, and the compact
        # payload preserves it as sender_id. Display names carry no authority.
        sender_id = latest.get("sender_id", latest.get("sender"))
        return ExecutionScope(
            channel_id, channel_type, message_ids,
            latest.get("id"), sender_id if isinstance(sender_id, str) else None,
        )

    def denial_reason(
        self,
        tool_name: str,
        parameters: dict,
        execution_scope: ExecutionScope | None = None,
    ) -> str | None:
        """Return a reason when an action exceeds the active profile."""
        if not self.allows(tool_name):
            return (
                f"Tool '{tool_name}' is blocked by the "
                f"'{self.profile}' capability profile"
            )
        if tool_name in STATE_TOOLS:
            if not self._watch_scope(execution_scope):
                return "Use a current request in a configured Discord or Matrix channel"
            if tool_name in PROACTIVE_SOURCE_TOOLS and execution_scope.channel_type != "discord":
                return "Use a configured Discord channel for proactive speaking"
            if tool_name in WATCH_WRITE_TOOLS | PROACTIVE_WRITE_TOOLS and not self._watch_owner_scope(execution_scope):
                return "The bot owner can change watch and proactive settings"
            if set(parameters) & {"channel_id", "channel_type", "sender_id", "event_id", "source_event_id", "is_owner"}:
                return "Use the sender and channel from the current request"
            return None
        if execution_scope and execution_scope.channel_type == "discord" and tool_name not in ({"wait", "send_discord_reply"} | READ_ONLY_SOURCE_TOOLS):
            return "Use a Discord reply for this request"
        if tool_name in READ_ONLY_SOURCE_TOOLS:
            if not execution_scope or not execution_scope.latest_event_id:
                return "A current chat request is required"
            if execution_scope.channel_type == "discord" and execution_scope.channel_id not in self.approved_discord_channel_ids:
                return "Use a configured Discord channel"
            if execution_scope.channel_type == "matrix" and execution_scope.channel_id not in self.approved_matrix_room_ids:
                return "Use an approved Matrix room"
            if execution_scope.channel_type not in {"discord", "matrix", "farcaster"}:
                return "Choose a supported chat source"
            return None
        if tool_name == "send_discord_reply":
            if not execution_scope or execution_scope.channel_type != "discord":
                return "A current Discord mention is required"
            channel_id = parameters.get("channel_id")
            if channel_id not in self.approved_discord_channel_ids or channel_id != execution_scope.channel_id:
                return "Choose the current configured Discord channel"
            if not execution_scope.latest_event_id or parameters.get("reply_to_id") != execution_scope.latest_event_id:
                return "Reply to the latest Discord mention"
            return None
        if self.profile == "operator":
            return None

        if tool_name in MATRIX_MANAGEMENT_TOOLS | {"matrix_server_status"}:
            if not execution_scope or execution_scope.channel_type != "matrix":
                return "A current Matrix request is required"
            if execution_scope.channel_id not in self.approved_matrix_room_ids:
                return "Use an approved Matrix room"
            if not execution_scope.latest_event_id or parameters.get("source_event_id") != execution_scope.latest_event_id:
                return "Use the latest Matrix request event"
            if tool_name in MATRIX_MANAGEMENT_TOOLS and not self._operator_scope(execution_scope):
                return "Management requires an operator request in the control room"
            if tool_name == "manage_matrix_room" and parameters.get("room_id") not in self.managed_room_ids:
                return "Choose a configured managed room"
            return None

        blocked_parameters = PUBLIC_BOT_BLOCKED_PARAMETERS.get(tool_name, frozenset())
        supplied_blocked = sorted(
            name for name in blocked_parameters if parameters.get(name) is not None
        )
        if supplied_blocked:
            names = ", ".join(supplied_blocked)
            return (
                f"Tool '{tool_name}' parameters are blocked by the "
                f"'{self.profile}' capability profile: {names}"
            )

        if tool_name == "wait":
            return None

        if tool_name in {"send_matrix_reply", "react_to_matrix_message"}:
            return self._matrix_denial_reason(
                tool_name, parameters, execution_scope
            )

        if tool_name in {"send_farcaster_reply", "like_farcaster_post"}:
            return self._farcaster_denial_reason(
                tool_name, parameters, execution_scope
            )
        return None

    def _matrix_denial_reason(
        self,
        tool_name: str,
        parameters: dict,
        execution_scope: ExecutionScope | None,
    ) -> str | None:
        room_parameter = (
            "channel_id" if tool_name == "send_matrix_reply" else "room_id"
        )
        event_parameter = (
            "reply_to_id" if tool_name == "send_matrix_reply" else "event_id"
        )
        room_id = parameters.get(room_parameter)
        event_id = parameters.get(event_parameter)

        if not room_id or not event_id:
            return (
                f"Tool '{tool_name}' requires an explicit Matrix room and "
                "source event in the public capability profile"
            )
        if execution_scope is None or execution_scope.channel_type != "matrix":
            return f"Tool '{tool_name}' requires a current Matrix source context"
        if not self.approved_matrix_room_ids:
            return "The public capability profile has no approved Matrix rooms"
        if room_id not in self.approved_matrix_room_ids:
            return f"Matrix room '{room_id}' is outside the approved public rooms"
        if room_id != execution_scope.channel_id:
            return f"Matrix room '{room_id}' is outside the current source context"
        if event_id not in execution_scope.message_ids:
            return f"Matrix event '{event_id}' is outside the current source context"
        return None

    def _farcaster_denial_reason(
        self,
        tool_name: str,
        parameters: dict,
        execution_scope: ExecutionScope | None,
    ) -> str | None:
        target_parameter = (
            "reply_to_hash" if tool_name == "send_farcaster_reply" else "cast_hash"
        )
        target_hash = parameters.get(target_parameter)

        if not target_hash:
            return f"Tool '{tool_name}' requires an explicit Farcaster target"
        if execution_scope is None or execution_scope.channel_type != "farcaster":
            return f"Tool '{tool_name}' requires a current Farcaster source context"
        if target_hash not in execution_scope.message_ids:
            return (
                f"Farcaster target '{target_hash}' is outside the current "
                "source context"
            )
        return None
