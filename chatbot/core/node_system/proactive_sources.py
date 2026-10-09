"""Share useful public stories in approved channels with saved post receipts."""

import asyncio
from contextlib import contextmanager
from datetime import datetime
from email.utils import parsedate_to_datetime
import hashlib
import json
import re
import sqlite3
import time
import uuid
from zoneinfo import ZoneInfo

from .source_watches import _scope, _safe_text, _feed_url
from ...tools.public_source_tools import ReadNewsTool, SearchSocialTool
from ...tools.web_tools import public_url

TOPICS = ("tech", "ai", "developer", "crypto", "reddit", "social", "world")
POST_GAP = 3 * 3600


def configuration(params):
    if not isinstance(params, dict) or set(params) - {"enabled", "topics", "max_daily_posts", "interval_minutes"}:
        raise ValueError("Use the listed proactive settings")
    result = dict(params)
    if type(result.get("enabled")) is not bool:
        raise ValueError("Set enabled to true or false")
    if "topics" in result:
        topics = result["topics"]
        if not isinstance(topics, list) or not topics or len(topics) > len(TOPICS) or any(topic not in TOPICS for topic in topics):
            raise ValueError("Choose topics from tech, ai, developer, crypto, reddit, social, and world")
        result["topics"] = sorted(set(topics))
    for field, lower, upper in (("max_daily_posts", 1, 4), ("interval_minutes", 60, 1440)):
        if field in result and (type(result[field]) is not int or not lower <= result[field] <= upper):
            raise ValueError(f"Choose {field} from {lower} to {upper}")
    return result


class ProactiveStore:
    def __init__(self, db_path, owner_ids, allowed_channels, timezone="America/Vancouver", now=time.time):
        self.db_path = str(db_path)
        self.owner_ids, self.allowed_channels = set(owner_ids), set(allowed_channels)
        self.timezone, self.now = ZoneInfo(timezone), now
        self._memory = sqlite3.connect(":memory:") if self.db_path == ":memory:" else None
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS proactive_settings (
                    channel_id TEXT PRIMARY KEY, enabled INTEGER NOT NULL, topics_json TEXT NOT NULL,
                    max_daily_posts INTEGER NOT NULL DEFAULT 4, interval_seconds INTEGER NOT NULL DEFAULT 3600,
                    generation INTEGER NOT NULL DEFAULT 1, cursor INTEGER NOT NULL DEFAULT 0,
                    next_check REAL NOT NULL DEFAULT 0, last_post REAL NOT NULL DEFAULT 0,
                    lease_token TEXT, lease_until REAL NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS proactive_seen (
                    channel_id TEXT NOT NULL, url TEXT NOT NULL, seen_at REAL NOT NULL,
                    PRIMARY KEY(channel_id,url)
                );
                CREATE TABLE IF NOT EXISTS proactive_posts (
                    id TEXT PRIMARY KEY, channel_id TEXT NOT NULL, generation INTEGER NOT NULL,
                    text TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', local_day TEXT NOT NULL,
                    created_at REAL NOT NULL, sent_at REAL, message_id TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
                    lease_token TEXT, lease_until REAL NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS proactive_usage (
                    channel_id TEXT NOT NULL, local_day TEXT NOT NULL,
                    lookups INTEGER NOT NULL DEFAULT 0, decisions INTEGER NOT NULL DEFAULT 0,
                    web_searches INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(channel_id,local_day)
                );
                CREATE TABLE IF NOT EXISTS proactive_tool_receipts (
                    channel_id TEXT NOT NULL, event_id TEXT NOT NULL, sender_id TEXT NOT NULL,
                    params_hash TEXT NOT NULL, result_json TEXT NOT NULL, created_at REAL NOT NULL,
                    PRIMARY KEY(channel_id,event_id,params_hash)
                );
            """)

    @contextmanager
    def _db(self):
        db = self._memory or sqlite3.connect(self.db_path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            if self._memory is None:
                db.close()

    def _check_scope(self, scope, write=False):
        value = _scope(scope)
        if value["channel_type"] != "discord" or value["channel_id"] not in self.allowed_channels:
            raise ValueError("Use a configured proactive Discord channel")
        if write and (value["sender_id"] not in self.owner_ids or not value["event_id"]):
            raise ValueError("The bot owner can change proactive settings")
        return value

    def seed(self, channel_id, enabled=False, topics=TOPICS):
        if channel_id not in self.allowed_channels or not self.owner_ids:
            return
        with self._db() as db:
            db.execute("INSERT OR IGNORE INTO proactive_settings(channel_id,enabled,topics_json) VALUES(?,?,?)",
                       (channel_id, bool(enabled), json.dumps(list(topics))))

    def _local(self):
        return datetime.fromtimestamp(self.now(), self.timezone)

    def _daytime(self):
        return 8 <= self._local().hour < 22

    def _public(self, db, row):
        if row is None:
            return {"enabled": False, "topics": [], "max_daily_posts": 4, "interval_minutes": 60,
                    "timezone": str(self.timezone), "daytime_hours": "08:00 to 22:00", "posts_today": 0}
        count = db.execute("SELECT count(*) FROM proactive_posts WHERE channel_id=? AND local_day=? AND status IN ('pending','sending','unknown','sent')",
                           (row["channel_id"], self._local().date().isoformat())).fetchone()[0]
        return {"enabled": bool(row["enabled"]), "topics": json.loads(row["topics_json"]),
                "max_daily_posts": row["max_daily_posts"], "interval_minutes": row["interval_seconds"] // 60,
                "timezone": str(self.timezone), "daytime_hours": "08:00 to 22:00",
                "minimum_post_gap_minutes": POST_GAP // 60, "posts_today": count}

    def status(self, scope):
        value = self._check_scope(scope)
        with self._db() as db:
            return self._public(db, db.execute("SELECT * FROM proactive_settings WHERE channel_id=?", (value["channel_id"],)).fetchone())

    def configure(self, params, scope):
        value = self._check_scope(scope, write=True)
        params = configuration(params)
        digest = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()
        key = (value["channel_id"], value["event_id"], digest)
        self.seed(value["channel_id"])
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            receipt = db.execute("SELECT result_json FROM proactive_tool_receipts WHERE channel_id=? AND event_id=? AND params_hash=?", key).fetchone()
            if receipt:
                return json.loads(receipt[0])
            row = db.execute("SELECT * FROM proactive_settings WHERE channel_id=?", (value["channel_id"],)).fetchone()
            topics = params.get("topics", json.loads(row["topics_json"]))
            db.execute("""UPDATE proactive_settings SET enabled=?,topics_json=?,max_daily_posts=?,interval_seconds=?,
                generation=generation+1,next_check=0,lease_token=NULL,lease_until=0 WHERE channel_id=?""",
                (params["enabled"], json.dumps(topics), params.get("max_daily_posts", row["max_daily_posts"]),
                 params.get("interval_minutes", row["interval_seconds"] // 60) * 60, value["channel_id"]))
            db.execute("UPDATE proactive_posts SET status='cancelled' WHERE channel_id=? AND status='pending'", (value["channel_id"],))
            current = db.execute("SELECT * FROM proactive_settings WHERE channel_id=?", (value["channel_id"],)).fetchone()
            result = {"status": "success", "message": "Proactive speaking is enabled." if params["enabled"] else "Proactive speaking is paused.",
                      "settings": self._public(db, current)}
            db.execute("INSERT INTO proactive_tool_receipts VALUES(?,?,?,?,?,?)",
                       (value["channel_id"], value["event_id"], value["sender_id"], digest, json.dumps(result), self.now()))
            return result

    def tool_results(self, scope):
        value = self._check_scope(scope)
        with self._db() as db:
            rows = db.execute("""SELECT result_json FROM proactive_tool_receipts WHERE channel_id=? AND event_id=? AND sender_id=?
                ORDER BY created_at DESC,rowid DESC LIMIT 15""", (value["channel_id"], value["event_id"], value["sender_id"])).fetchall()
            return [{"tool": "configure_proactive", "trust": "untrusted_source", **json.loads(row[0])} for row in reversed(rows)]

    def claim(self):
        if not self._daytime() or not self.owner_ids:
            return None
        now, day = self.now(), self._local().date().isoformat()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM proactive_seen WHERE seen_at<?", (now - 30 * 86400,))
            for row in db.execute("SELECT * FROM proactive_settings WHERE enabled=1 AND next_check<=? AND lease_until<=? ORDER BY next_check,channel_id", (now, now)).fetchall():
                channel = row["channel_id"]
                if channel not in self.allowed_channels or row["last_post"] and now - row["last_post"] < POST_GAP:
                    continue
                if db.execute("SELECT 1 FROM proactive_posts WHERE channel_id=? AND status IN ('pending','sending','unknown')", (channel,)).fetchone():
                    continue
                posts = db.execute("SELECT count(*) FROM proactive_posts WHERE channel_id=? AND local_day=?", (channel, day)).fetchone()[0]
                if posts >= row["max_daily_posts"]:
                    continue
                db.execute("INSERT OR IGNORE INTO proactive_usage(channel_id,local_day) VALUES(?,?)", (channel, day))
                usage = db.execute("SELECT * FROM proactive_usage WHERE channel_id=? AND local_day=?", (channel, day)).fetchone()
                if usage["lookups"] >= 24 or usage["decisions"] >= 8:
                    continue
                topics = json.loads(row["topics_json"])
                topic = topics[row["cursor"] % len(topics)]
                paid = topic in {"ai", "reddit", "social"}
                if paid and usage["web_searches"] >= 3:
                    topic, paid = "developer", False
                token = uuid.uuid4().hex
                db.execute("UPDATE proactive_settings SET lease_token=?,lease_until=?,next_check=?,cursor=cursor+1 WHERE channel_id=?",
                           (token, now + 300, now + row["interval_seconds"], channel))
                db.execute("UPDATE proactive_usage SET lookups=lookups+1,decisions=decisions+1,web_searches=web_searches+? WHERE channel_id=? AND local_day=?", (int(paid), channel, day))
                return {**dict(row), "lease_token": token, "topic": topic, "day": day}
        return None

    def fresh_items(self, channel_id, items):
        with self._db() as db:
            seen = {row[0] for row in db.execute("SELECT url FROM proactive_seen WHERE channel_id=?", (channel_id,))}
        fresh = []
        for raw in items[:10]:
            try:
                url = _feed_url(raw.get("url"))
                if len(url) > 600 or url in seen:
                    continue
                published = raw.get("published")
                if published:
                    try:
                        date = datetime.fromisoformat(published.replace("Z", "+00:00"))
                    except ValueError:
                        date = parsedate_to_datetime(published)
                    if self.now() - date.timestamp() > 48 * 3600 or date.timestamp() > self.now() + 3600:
                        continue
                fresh.append({"id": hashlib.sha256(url.encode()).hexdigest()[:16], "url": url,
                              "title": str(raw.get("title", ""))[:200], "summary": str(raw.get("summary", ""))[:500],
                              "published": published or None, "access": raw.get("access", "public_feed"),
                              "publisher": str(raw.get("publisher", "Public source"))[:100]})
                seen.add(url)
            except (ValueError, TypeError, AttributeError, OverflowError):
                continue
        return fresh[:6]

    def finish(self, claim, items, draft):
        now = self.now()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute("SELECT * FROM proactive_settings WHERE channel_id=? AND lease_token=? AND generation=? AND lease_until>?",
                                 (claim["channel_id"], claim["lease_token"], claim["generation"], now)).fetchone()
            if current is None:
                return
            db.execute("UPDATE proactive_settings SET lease_token=NULL,lease_until=0 WHERE channel_id=?", (claim["channel_id"],))
            if not isinstance(draft, dict) or type(draft.get("publish")) is not bool:
                return
            if draft["publish"] and current["enabled"]:
                chosen = next((item for item in items if item["id"] == draft.get("item_id")), None)
                content = draft.get("content")
                if not chosen or not isinstance(content, str) or not content.strip() or len(content) > 850:
                    return
                links = re.findall(r"https?://[^\s<>\]\)]+", content)
                if any(link != chosen["url"] for link in links):
                    return
                post_id = uuid.uuid4().hex
                text = content.strip().replace("@", "＠") + "\n\n" + _safe_text(chosen["publisher"], 60) + ": [" + _safe_text(chosen["title"], 100) + "](" + chosen["url"].replace("(", "%28").replace(")", "%29") + ")"
                text += "\n\nReceipt: proactive:" + post_id[:12]
                if len(text) > 1900:
                    return
                db.execute("INSERT INTO proactive_posts(id,channel_id,generation,text,local_day,created_at) VALUES(?,?,?,?,?,?)",
                           (post_id, claim["channel_id"], claim["generation"], text, self._local().date().isoformat(), now))
            for item in items:
                db.execute("INSERT OR IGNORE INTO proactive_seen VALUES(?,?,?)", (claim["channel_id"], item["url"], now))

    def deliveries(self):
        with self._db() as db:
            db.execute("UPDATE proactive_posts SET status='unknown',lease_token=NULL,lease_until=0 WHERE status='sending' AND lease_until<=?", (self.now(),))
            return [dict(row) for row in db.execute("SELECT * FROM proactive_posts WHERE status IN ('pending','unknown') AND next_attempt<=? ORDER BY created_at LIMIT 5", (self.now(),))]

    def claim_delivery(self, post):
        now, day = self.now(), self._local().date().isoformat()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM proactive_posts WHERE id=? AND status='pending' AND next_attempt<=?", (post["id"], now)).fetchone()
            settings = db.execute("SELECT * FROM proactive_settings WHERE channel_id=?", (post["channel_id"],)).fetchone()
            if row is None or settings is None:
                return None
            if post["channel_id"] not in self.allowed_channels or not self.owner_ids or not settings["enabled"] or settings["generation"] != post["generation"] or now - post["created_at"] > 48 * 3600:
                db.execute("UPDATE proactive_posts SET status='cancelled' WHERE id=?", (post["id"],))
                return None
            if not self._daytime() or settings["last_post"] and now - settings["last_post"] < POST_GAP:
                return None
            count = db.execute("SELECT count(*) FROM proactive_posts WHERE channel_id=? AND local_day=? AND id<>?", (post["channel_id"], day, post["id"])).fetchone()[0]
            if count >= settings["max_daily_posts"]:
                return None
            token = uuid.uuid4().hex
            db.execute("UPDATE proactive_posts SET status='sending',attempts=attempts+1,local_day=?,lease_token=?,lease_until=? WHERE id=?",
                       (day, token, now + 120, post["id"]))
            return {**post, "lease_token": token}

    def record_delivery(self, post, result):
        now = self.now()
        status = result.get("status", "unknown") if isinstance(result, dict) else "unknown"
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM proactive_posts WHERE id=? AND lease_token=? AND status='sending'", (post["id"], post["lease_token"])).fetchone()
            if row is None:
                return
            if status == "success":
                db.execute("UPDATE proactive_posts SET status='sent',sent_at=?,message_id=?,lease_token=NULL,lease_until=0 WHERE id=?", (now, result.get("message_id"), post["id"]))
                db.execute("UPDATE proactive_settings SET last_post=? WHERE channel_id=?", (now, post["channel_id"]))
            else:
                following = "failed" if status == "failure" and row["attempts"] >= 3 else "pending" if status == "failure" else "unknown"
                db.execute("UPDATE proactive_posts SET status=?,next_attempt=?,lease_token=NULL,lease_until=0 WHERE id=?", (following, now + 60 * row["attempts"], post["id"]))

    def reconcile(self, post, result):
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            if result.get("status") == "success":
                changed = db.execute("UPDATE proactive_posts SET status='sent',sent_at=?,message_id=? WHERE id=? AND status='unknown'", (self.now(), result.get("message_id"), post["id"])).rowcount
                if changed:
                    db.execute("UPDATE proactive_settings SET last_post=? WHERE channel_id=?", (self.now(), post["channel_id"]))
            else:
                db.execute("UPDATE proactive_posts SET next_attempt=? WHERE id=? AND status='unknown'", (self.now() + 300, post["id"]))


class ProactiveSourceService:
    def __init__(self, store, ai_engine, delivery, action_context=None, fetch=None, callback_timeout=30):
        self.store, self.ai_engine, self.delivery = store, ai_engine, delivery
        self.action_context, self.fetch = action_context, fetch or self._fetch
        self.callback_timeout = callback_timeout
        self._lock = asyncio.Lock()

    async def execute_tool(self, name, params, scope):
        try:
            if name == "configure_proactive":
                return self.store.configure(params, scope)
            if name != "get_proactive_status" or params:
                raise ValueError("Use the listed proactive tools and parameters")
            return {"status": "success", "settings": self.store.status(scope)}
        except (ValueError, TypeError) as error:
            return {"status": "failure", "message": str(error)}
        except sqlite3.Error:
            return {"status": "failure", "message": "Proactive settings need another save attempt."}

    async def _fetch(self, topic):
        news = {"tech": "bbc_technology", "developer": "hacker_news", "crypto": "coindesk", "world": "bbc_world"}
        if topic in news:
            result = await ReadNewsTool().execute({"source": news[topic]}, self.action_context)
        else:
            platform = "reddit" if topic in {"ai", "reddit"} else "farcaster"
            query = {"ai": "artificial intelligence research and tools this week", "reddit": "technology crypto interesting discussions this week", "social": "developer AI crypto discussions this week"}[topic]
            result = await SearchSocialTool().execute({"platform": platform, "query": query}, self.action_context)
        items = result.get("items") or [{"url": item["url"], "title": item.get("title", ""), "summary": item.get("content", "")} for item in result.get("sources", [])]
        return [{**item, "publisher": result.get("publisher", result.get("platform", "Public source")), "access": result.get("access", "public_feed")} for item in items] if result.get("status") == "success" else []

    async def tick(self):
        async with self._lock:
            for post in self.store.deliveries():
                try:
                    if post["status"] == "unknown":
                        result = await asyncio.wait_for(self.delivery.reconcile("proactive", "discord", post["channel_id"], post["text"], "proactive:" + post["id"]), self.callback_timeout)
                        self.store.reconcile(post, result)
                        self._remember_post(post, result)
                        continue
                    claimed = self.store.claim_delivery(post)
                    if claimed:
                        try:
                            result = await asyncio.wait_for(self.delivery.send("proactive", "discord", post["channel_id"], post["text"], "proactive:" + post["id"]), self.callback_timeout)
                        except asyncio.CancelledError:
                            self.store.record_delivery(claimed, {"status": "unknown"})
                            raise
                        except Exception:
                            result = {"status": "unknown"}
                        self.store.record_delivery(claimed, result)
                        self._remember_post(post, result)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.store.reconcile(post, {"status": "unknown"})
            if not self.ai_engine.api_key:
                return
            claim = self.store.claim()
            if claim:
                try:
                    service = getattr(self.action_context, "task_service", None)
                    binding = None
                    if service:
                        from ..world_state.structures import Channel, Message
                        event_id = f"proactive:{claim['channel_id']}:{claim['generation']}:{claim['day']}:{claim['cursor']}"
                        channel = Channel(id=claim["channel_id"], type="discord", name="community")
                        source = Message(id=event_id, channel_type="discord", sender="ratichat",
                            content=f"Choose a useful {claim['topic']} story for this community.", timestamp=self.store.now())
                        binding = await service.prepare(channel, source, proactive=True)
                    items, draft = await self._compose(claim, binding, service)
                    self.store.finish(claim, items, draft)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.store.finish(claim, [], None)

    async def _compose(self, claim, binding, service):
        from contextlib import nullcontext
        with service.activate(binding) if binding else nullcontext():
            attempt = None
            if binding and claim["topic"] in {"ai", "reddit", "social"}:
                attempt = service.store.reserve_attempt(binding.task["id"], 0.01,
                    request_key="proactive-source:" + binding.event_id, input_versions=binding.input_versions)
                if not attempt.get("reserved"):
                    raise ValueError("Use a fresh proactive source snapshot")
            items = self.store.fresh_items(claim["channel_id"], await asyncio.wait_for(self.fetch(claim["topic"]), 60))
            if attempt:
                service.store.record_result(binding.task["id"], {"kind": "source_read", "topic": claim["topic"]},
                    attempt_id=attempt["id"], cost_usd=attempt["reserved_usd"], status="active")
            payload = {"processing_mode": "node_based", "topics": json.loads(claim["topics_json"]),
                "expanded_nodes": {"sources.public": {"trust": "untrusted_source", "topic": claim["topic"], "items": items}},
                "fetched_at": self.store.now()}
            if binding:
                payload["task_route"] = binding.route
                payload["community_nodes"] = {k: v for k, v in binding.nodes.items()
                    if k in binding.route.get("expanded_nodes", [])}
                payload["current_processing_channel_id"] = claim["channel_id"]
                payload["community"] = {"platform": "discord", "channel_id": claim["channel_id"]}
            return items, await self.ai_engine.compose_proactive(payload) if items else None

    def _remember_post(self, post, result):
        store = getattr(self.action_context, "awareness_store", None)
        if store and result.get("status") == "success" and result.get("message_id"):
            store.ingest_message("discord", post["channel_id"], {"id": result["message_id"],
                "sender": "ratichat", "content": post["text"], "timestamp": self.store.now(),
                "metadata": {"is_bot": True, "confirmed": True, "proactive": True}})
