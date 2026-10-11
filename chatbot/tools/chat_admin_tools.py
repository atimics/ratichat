"""Admin tools tied to the current Matrix operator or Discord owner request."""

import discord
import httpx

from .base import ToolInterface


class ChatAdminTool(ToolInterface):
    async def execute(self, params, context):
        policy = getattr(context, "capability_policy", None)
        scope = getattr(context, "execution_scope", None)
        if not policy or not scope:
            return {"status": "failure", "message": "Use a current admin request"}
        denial = policy.denial_reason(self.name, params, scope)
        if denial:
            return {"status": "blocked", "message": denial}
        service = getattr(context, self.platform + "_admin", None)
        observer = getattr(context, self.platform + "_observer", None)
        if not service:
            return {"status": "failure", "message": f"Configure {self.platform.title()} admin tools first"}
        try:
            return await service.execute(self.name, params, scope, observer)
        except ValueError as error:
            return {"status": "failure", "message": str(error)}
        except (httpx.HTTPError, discord.HTTPException):
            return {"status": "failure", "message": "Check the current source, target, and bot permissions"}


MATRIX_SOURCE = {
    "source_event_id": "string - Current operator request event ID",
    "room_id": "string - Configured managed Matrix room ID",
}
DISCORD_SOURCE = {"source_event_id": "string - Current owner request message ID"}
PAGING = {"offset": "integer - First result index, default 0", "limit": "integer - Page size from 1 to 50, default 25"}
REASON = {"reason": "string - Reason from the user's request, up to 300 characters"}


class InspectMatrixRoomTool(ChatAdminTool):
    platform = "matrix"
    name = "inspect_matrix_room"
    description = "Read a managed Matrix room's settings or members for an operator in the control room. Use exact IDs from results for later admin requests."
    parameters_schema = {**MATRIX_SOURCE, "view": "string - summary or members, default summary", **PAGING}


class ModerateMatrixMemberTool(ChatAdminTool):
    platform = "matrix"
    name = "moderate_matrix_member"
    description = "Invite, kick, ban, or unban one Matrix member after an explicit operator request in the control room. Use the exact room and user IDs requested."
    parameters_schema = {**MATRIX_SOURCE, "operation": "string - invite, kick, ban, or unban", "user_id": "string - Full Matrix user ID", **REASON}


class ManageMatrixMessageTool(ChatAdminTool):
    platform = "matrix"
    name = "manage_matrix_message"
    description = "Pin, unpin, or redact one message in a managed Matrix room after an explicit operator request. Redact removes the message content."
    parameters_schema = {**MATRIX_SOURCE, "operation": "string - pin, unpin, or redact", "event_id": "string - Target message event ID", **REASON}


class InspectDiscordServerTool(ChatAdminTool):
    platform = "discord"
    name = "inspect_discord_server"
    description = "Read current Discord channel settings, configured channels in this server, roles, or one member after an owner request. Use exact returned IDs for later admin requests."
    parameters_schema = {**DISCORD_SOURCE, "view": "string - summary, channels, roles, or member; default summary", "user_id": "string - User ID for member view", **PAGING}


class ManageDiscordChannelTool(ChatAdminTool):
    platform = "discord"
    name = "manage_discord_channel"
    description = "Change the current Discord text channel's name, topic, or slowmode after an explicit owner request that mentions the bot."
    parameters_schema = {**DISCORD_SOURCE, "operation": "string - name, topic, or slowmode", "value": "string - New name or topic; an empty topic clears it", "seconds": "integer - Slowmode from 0 to 21600 seconds", **REASON}


class ModerateDiscordMemberTool(ChatAdminTool):
    platform = "discord"
    name = "moderate_discord_member"
    description = "Timeout, clear timeout, kick, ban, or unban one member in the current Discord server after an explicit owner request that mentions the bot. Use the exact user ID requested."
    parameters_schema = {**DISCORD_SOURCE, "operation": "string - timeout, clear_timeout, kick, ban, or unban", "user_id": "string - Target Discord user ID", "seconds": "integer - Timeout from 1 to 2419200 seconds", **REASON}


class ManageDiscordRoleTool(ChatAdminTool):
    platform = "discord"
    name = "manage_discord_role"
    description = "Add or remove a configured community role for one Discord member after an explicit owner request that mentions the bot. Use exact user and role IDs."
    parameters_schema = {**DISCORD_SOURCE, "operation": "string - add or remove", "user_id": "string - Target Discord user ID", "role_id": "string - Configured managed role ID", **REASON}


class ManageDiscordMessageTool(ChatAdminTool):
    platform = "discord"
    name = "manage_discord_message"
    description = "Pin, unpin, or delete one message in the current Discord channel after an explicit owner request that mentions the bot. Delete removes the message."
    parameters_schema = {**DISCORD_SOURCE, "operation": "string - pin, unpin, or delete", "message_id": "string - Target Discord message ID", **REASON}


CHAT_ADMIN_TOOL_CLASSES = (
    InspectMatrixRoomTool, ModerateMatrixMemberTool, ManageMatrixMessageTool,
    InspectDiscordServerTool, ManageDiscordChannelTool, ModerateDiscordMemberTool,
    ManageDiscordRoleTool, ManageDiscordMessageTool,
)
