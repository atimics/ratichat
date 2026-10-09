"""Source-watch tools that use the current request's saved sender and channel."""

from .base import ToolInterface


class SourceWatchTool(ToolInterface):
    async def execute(self, params, context):
        service = getattr(context, "source_watch_service", None)
        scope = getattr(context, "execution_scope", None)
        if service is None:
            return {"status": "failure", "message": "Source watches need configuration."}
        if not scope or not scope.latest_event_id or not scope.latest_sender_id:
            return {"status": "blocked", "message": "Use a current chat request for source watches."}
        if not isinstance(params, dict) or set(params) - set(self.parameters_schema):
            return {"status": "blocked", "message": "Use the listed tool parameters. The current request supplies the sender and channel."}
        trusted_scope = {"channel_type": scope.channel_type, "channel_id": scope.channel_id,
                         "sender_id": scope.latest_sender_id, "event_id": scope.latest_event_id}
        owners = service.store.owner_ids.get(scope.channel_type, ())
        return await service.execute_tool(self.name, params, trusted_scope,
                                          is_owner=scope.latest_sender_id in owners)


class CreateSourceWatchTool(SourceWatchTool):
    name = "create_source_watch"
    description = "Track a public RSS, Atom, or GitHub release feed when the owner asks for updates in this channel. The first check saves a quiet baseline; new entries are posted later."
    parameters_schema = {
        "url": "string - Public RSS or Atom feed URL",
        "interval_minutes": "integer - Minutes between checks, from 15 to 1440; default 60",
    }


class ListSourceWatchesTool(SourceWatchTool):
    name = "list_source_watches"
    description = "Read the current channel's saved feed watches, IDs, check intervals, and status. Use this to find a watch before changing it."
    parameters_schema = {}


class RemoveSourceWatchTool(SourceWatchTool):
    name = "remove_source_watch"
    description = "Remove a watch from the current channel when the owner asks to stop its updates. Use an ID from list_source_watches."
    parameters_schema = {"watch_id": "string - ID of a watch in the current channel"}


class GetSourceDigestTool(SourceWatchTool):
    name = "get_source_digest"
    description = "Read the latest saved feed entries and source links for this channel. Choose a watch ID for its full digest, or omit it for a short overview."
    parameters_schema = {"watch_id": "string - Optional watch ID from the current channel"}
