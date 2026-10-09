"""Deliver saved owner-created watch updates to their fixed channel."""

import time

from ..world_state.structures import Message


class WatchDelivery:
    def __init__(self, action_context, allowed_channels):
        self.context = action_context
        self.allowed_channels = allowed_channels

    async def send(self, watch_id, platform, channel_id, text, delivery_key):
        if channel_id not in self.allowed_channels.get(platform, ()):
            return {"status": "failure", "error": "Use a configured watch channel"}
        observer = getattr(self.context, platform + "_observer", None)
        if not observer:
            return {"status": "failure", "error": "Connect the watch channel first"}
        if platform == "discord":
            result = await observer.send_digest(channel_id, text, delivery_key)
        elif platform == "matrix":
            result = await observer.send_message(channel_id, text, tx_id="watch:" + delivery_key)
            result = {**result, "status": "success" if result.get("success") else result.get("status", "failure"),
                      "message_id": result.get("event_id")}
        else:
            return {"status": "failure", "error": "Use Discord or Matrix for source watches"}
        if result.get("status") == "success" and result.get("message_id"):
            world = getattr(self.context, "world_state_manager", None)
            if world and world.get_channel(channel_id):
                world.add_message(channel_id, Message(str(result["message_id"]), platform, "ratichat", text,
                    time.time(), channel_id=channel_id, metadata={"is_bot": True, "watch_id": watch_id}))
        return result

    async def reconcile(self, watch_id, platform, channel_id, text, delivery_key):
        if channel_id not in self.allowed_channels.get(platform, ()):
            return {"status": "unknown"}
        observer = getattr(self.context, platform + "_observer", None)
        if not observer:
            return {"status": "unknown"}
        if platform == "discord":
            return await observer.reconcile_digest(channel_id, text, delivery_key)
        if platform == "matrix":
            return await self.send(watch_id, platform, channel_id, text, delivery_key)
        return {"status": "unknown"}
