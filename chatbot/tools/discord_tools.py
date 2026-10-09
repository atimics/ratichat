"""Reply to an accepted Discord bot mention."""

from .base import ToolInterface


class SendDiscordReplyTool(ToolInterface):
    @property
    def name(self):
        return "send_discord_reply"

    @property
    def description(self):
        return "Reply to a user's Discord mention in the current channel. Use their message ID as reply_to_id. Keep text within 2000 characters."

    @property
    def parameters_schema(self):
        return {
            "channel_id": "string — current Discord text channel ID",
            "reply_to_id": "string — source Discord message ID",
            "content": "string — reply text, up to 2000 characters",
        }

    async def execute(self, params, context):
        observer = getattr(context, "discord_observer", None)
        if observer is None:
            return {"status": "failure", "error": "Connect Discord first"}
        return await observer.send_reply(
            params.get("channel_id"), params.get("content"), params.get("reply_to_id"),
            **({"delivery_id": params["delivery_id"]} if params.get("delivery_id") else {}),
        )
