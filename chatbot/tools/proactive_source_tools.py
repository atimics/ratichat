"""Owner controls for proactive public-source posts in the current channel."""

from .base import ToolInterface


class ProactiveSourceTool(ToolInterface):
    async def execute(self, params, context):
        service = getattr(context, "proactive_source_service", None)
        scope = getattr(context, "execution_scope", None)
        if service is None:
            return {"status": "failure", "message": "Proactive speaking needs configuration."}
        if not scope or not scope.latest_event_id or not scope.latest_sender_id:
            return {"status": "blocked", "message": "Use a current Discord request for proactive settings."}
        if not isinstance(params, dict) or set(params) - set(self.parameters_schema):
            return {"status": "blocked", "message": "Use the listed parameters. The current request supplies the sender and channel."}
        trusted_scope = {"channel_type": scope.channel_type, "channel_id": scope.channel_id,
                         "sender_id": scope.latest_sender_id, "event_id": scope.latest_event_id}
        return await service.execute_tool(self.name, params, trusted_scope)


class ConfigureProactiveTool(ProactiveSourceTool):
    name = "configure_proactive"
    description = "Enable, pause, or change proactive speaking when the owner asks. Settings apply to this Discord channel. Posts use public sources, source links, saved receipts, daytime hours, and a three-hour gap."
    parameters_schema = {"enabled": "boolean - true to enable or false to pause",
                         "topics": "array of strings - Optional topics: tech, ai, developer, crypto, reddit, social, world",
                         "max_daily_posts": "integer - Optional maximum posts per day, from 1 to 4",
                         "interval_minutes": "integer - Optional minutes between source checks, from 60 to 1440"}


class GetProactiveStatusTool(ProactiveSourceTool):
    name = "get_proactive_status"
    description = "Read this Discord channel's proactive topics, enabled status, daytime hours, post limits, and today's posts."
    parameters_schema = {}
