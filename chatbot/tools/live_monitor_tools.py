"""Monitoring tools with a fixed owner and update channel."""

from .source_watch_tools import SourceWatchTool


class LiveMonitorTool(SourceWatchTool):
    async def execute(self, params, context):
        service = getattr(context, "live_monitor_service", None)
        scope = getattr(context, "execution_scope", None)
        if not service or not scope or not scope.latest_event_id or not scope.latest_sender_id:
            return {"status": "blocked", "message": "Use a current request in a configured chat channel."}
        if not isinstance(params, dict) or set(params) - set(self.parameters_schema):
            return {"status": "blocked", "message": "Use the listed monitor parameters."}
        trusted = {"channel_type": scope.channel_type, "channel_id": scope.channel_id,
                   "sender_id": scope.latest_sender_id, "event_id": scope.latest_event_id}
        return await service.execute_tool(self.name, params, trusted,
            is_owner=scope.latest_sender_id in service.store.owner_ids.get(scope.channel_type, ()))


class CreateLiveMonitorTool(LiveMonitorTool):
    name = "create_live_monitor"
    description = "When the owner asks to watch wallet activity or keep them updated, save a live monitor for these complete addresses. It runs between chats and survives restart. New confirmed transactions and coverage changes are posted in this channel. The first successful check saves a quiet baseline. Confirm the saved ID and interval from the result."
    parameters_schema = {"targets": "array of objects with address and network (auto, bitcoin, tron, ethereum, base, arbitrum, optimism, polygon); up to 12",
        "label": "string - Short name for these addresses; default Address activity", "interval_minutes": "integer - Check every 1 to 1440 minutes; default 5"}


class ListLiveMonitorsTool(LiveMonitorTool):
    name = "list_live_monitors"
    description = "Read this channel's saved monitors, exact targets, check times, per-network coverage, delivery state and recent transaction links."
    parameters_schema = {}


class StopLiveMonitorTool(LiveMonitorTool):
    name = "stop_live_monitor"
    description = "Stop a saved live monitor when the owner asks. Use the monitor ID returned by list_live_monitors."
    parameters_schema = {"monitor_id": "string - Saved monitor ID in this channel"}
