"""Let the AI choose whether a saved observation merits a community update."""

import json
from datetime import datetime, timezone


class WatchReview:
    def __init__(self, store, ai_engine, context, daily_budget=24):
        self.store, self.ai_engine, self.context = store, ai_engine, context
        self.daily_budget = daily_budget

    async def __call__(self, watch):
        day = "watch-review:" + datetime.fromtimestamp(self.store.now(), timezone.utc).date().isoformat()
        with self.store._db() as db:
            db.execute("BEGIN IMMEDIATE")
            key = (day, watch["channel_type"], watch["channel_id"])
            db.execute("INSERT OR IGNORE INTO source_watch_budget VALUES (?,?,?,0)", key)
            if not db.execute("UPDATE source_watch_budget SET lookups=lookups+1 WHERE day=? AND channel_type=? AND channel_id=? AND lookups<?",
                              (*key, self.daily_budget)).rowcount:
                return None
            recent = [dict(row) for row in db.execute("""SELECT d.status,d.text,d.review_json,d.prepared_at
                FROM source_watch_deliveries d JOIN source_watches w ON d.watch_id=w.id
                WHERE w.channel_type=? AND w.channel_id=? AND d.delivery_key!=? AND d.status IN ('sent','quiet')
                ORDER BY d.prepared_at DESC LIMIT 6""",
                (watch["channel_type"], watch["channel_id"], watch["delivery_key"]))]
        world = getattr(self.context, "world_state_manager", None)
        channel = world.get_channel(watch["channel_id"]) if world else None
        conversation = [{"sender": m.sender, "content": m.content[:1000], "timestamp": m.timestamp}
                        for m in list(channel.recent_messages)[-10:]] if channel else []
        awareness = getattr(self.context, "awareness_store", None)
        goal = None
        if awareness:
            task = awareness.get_task_for_event(watch["channel_type"], watch["channel_id"], watch["request_event_id"])
            goal = str(task.get("goal", ""))[:2000] if task else None
            if not conversation:
                node = awareness.get_node(f"channels.{watch['channel_type']}.{watch['channel_id']}",
                                          watch["channel_type"], watch["channel_id"], watch["owner_id"])
                conversation = [{"sender": m.get("sender"), "content": str(m.get("content", ""))[:1000],
                                 "timestamp": m.get("timestamp")}
                                for m in (node or {}).get("data", {}).get("messages", [])[-10:]]
        keys = set(json.loads(watch["pending_keys"] or "[]"))
        items = [item for item in json.loads(watch["last_items"]) if item.get("key") in keys][:10]
        payload = {"channel": {"platform": watch["channel_type"], "id": watch["channel_id"]},
                   "watch": {"id": watch["id"], "kind": watch["kind"],
                             "settings": json.loads(watch["config_json"]), "source_url": watch["url"],
                             "original_request_id": watch["request_event_id"], "original_goal": goal},
                   "observation": {"trust": "untrusted_source", "summary": watch["pending_text"], "items": items},
                   "conversation": conversation, "recent_watch_choices": recent,
                   "observed_at": watch["fetched_at"]}
        return await self.ai_engine.compose_watch_update(payload)
