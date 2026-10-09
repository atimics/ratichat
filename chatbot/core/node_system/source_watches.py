"""Owner-managed public feed watches with saved updates and delivery receipts."""

import asyncio
import hashlib
import json
import re
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from contextlib import contextmanager

from ...tools.web_tools import ReadFeedTool, page_text, public_url


HELP = "Use `watch <public RSS/Atom URL> [every 1h|60m]`, `watches`, `unwatch <id>`, or `digest [id]`. Intervals range from 15 minutes to 24 hours."


def _scope(value):
    def field(name):
        result = value.get(name) if isinstance(value, dict) else getattr(value, name, None)
        return str(result or "")
    return {name: field(name) for name in ("channel_type", "channel_id", "sender_id", "event_id")}


def _allowed(config, platform, identity):
    values = config.get(platform, ()) if isinstance(config, dict) else (config or ())
    return identity in {str(value) for value in values}


def _link_url(value):
    return str(value).replace("(", "%28").replace(")", "%29")


def _feed_url(value):
    url = public_url(value)
    if len(_link_url(url)) > 1000 or url.host.lower() in {"localhost", "localhost.localdomain"} or url.host.lower().endswith((".localhost", ".local")):
        raise ValueError("Choose a public feed URL of at most 1000 characters")
    return str(url)


def _safe_text(value, limit):
    text = page_text(str(value or ""))[:limit].replace("@", "＠")
    return re.sub(r"([\\`*_\[\]<>|])", r"\\\1", text)


def _items(result, feed_url):
    canonical_url = _feed_url(result.get("url") or feed_url)
    items = []
    keys = set()
    for raw in result.get("items", [])[:10]:
        if not isinstance(raw, dict):
            continue
        try:
            url = _feed_url(raw.get("url") or canonical_url)
        except (ValueError, TypeError):
            continue
        title = page_text(str(raw.get("title") or "Feed update"))[:200]
        published = str(raw.get("published") or "")[:200]
        identity = str(raw.get("id") or raw.get("guid") or "")
        if not identity:
            identity = url if url != canonical_url else f"{published}:{title}"
        key = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        if key in keys:
            continue
        keys.add(key)
        items.append({"key": key, "title": title, "url": url,
                      "published": published, "summary": page_text(str(raw.get("summary") or ""))[:500]})
    if not items:
        raise ValueError("Choose an RSS or Atom feed with public entry links")
    return items


def _digest(watch, items, latest=False):
    heading = f"{'Latest entries' if latest else 'New entries'} ({len(items)}) · watch {watch['id']}"
    lines = [heading]
    feed_url = _link_url(_feed_url(watch["url"]))
    footer = f"[Read all {len(items)} entries]({feed_url})"
    body_limit = min(1750, 1850 - len(footer))
    for item in items:
        url = _link_url(_feed_url(item["url"]))
        entry = f"• [{_safe_text(item['title'], 140)}]({url})"
        summary = _safe_text(item.get("summary"), 160)
        if summary:
            entry += f"\n  {summary}"
        if len("\n".join(lines)) + len(entry) + 2 > body_limit:
            break
        lines.append(entry)
    if len(lines) - 1 < len(items):
        lines.append(footer)
    return "\n".join(lines)


class WatchStore:
    """SQLite watch state. Writes require a configured owner and channel."""

    def __init__(self, db_path, owner_ids=None, allowed_channels=None, now=time.time):
        self.db_path = str(db_path)
        self.owner_ids = owner_ids or {}
        self.allowed_channels = allowed_channels or {}
        self.now = now
        self._memory = sqlite3.connect(":memory:") if self.db_path == ":memory:" else None
        with self._db() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS source_watches (
                    id TEXT PRIMARY KEY, channel_type TEXT NOT NULL, channel_id TEXT NOT NULL,
                    owner_id TEXT NOT NULL, request_event_id TEXT NOT NULL DEFAULT '', url TEXT NOT NULL, interval_seconds INTEGER NOT NULL,
                    created_at REAL NOT NULL, next_due REAL NOT NULL, fetched_at REAL,
                    baseline INTEGER NOT NULL DEFAULT 0, last_hash TEXT, last_items TEXT NOT NULL DEFAULT '[]',
                    pending_text TEXT, pending_keys TEXT, delivery_key TEXT, pending_status TEXT,
                    delivery_message_id TEXT, last_digest TEXT,
                    lease_token TEXT, lease_until REAL NOT NULL DEFAULT 0,
                    reconcile_after REAL NOT NULL DEFAULT 0, reconcile_attempts INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(channel_type, channel_id, url)
                );
                CREATE UNIQUE INDEX IF NOT EXISTS source_watch_request_event
                    ON source_watches(channel_type,channel_id,request_event_id) WHERE request_event_id<>'';
                CREATE TABLE IF NOT EXISTS source_watch_items (
                    watch_id TEXT NOT NULL, item_key TEXT NOT NULL,
                    PRIMARY KEY(watch_id, item_key),
                    FOREIGN KEY(watch_id) REFERENCES source_watches(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS source_watch_deliveries (
                    delivery_key TEXT PRIMARY KEY, watch_id TEXT NOT NULL, status TEXT NOT NULL,
                    text TEXT NOT NULL, item_keys TEXT NOT NULL,
                    prepared_at REAL NOT NULL, message_id TEXT, sent_at REAL
                );
                CREATE TABLE IF NOT EXISTS source_watch_budget (
                    day TEXT NOT NULL, channel_type TEXT NOT NULL, channel_id TEXT NOT NULL,
                    lookups INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(day,channel_type,channel_id)
                );
                CREATE TABLE IF NOT EXISTS source_watch_commands (
                    channel_type TEXT NOT NULL, channel_id TEXT NOT NULL, event_id TEXT NOT NULL,
                    response TEXT NOT NULL, PRIMARY KEY(channel_type, channel_id, event_id)
                );
            """)
            columns = {row[1] for row in db.execute("PRAGMA table_info(source_watches)")}
            for name, declaration in (("reconcile_after", "REAL NOT NULL DEFAULT 0"),
                                      ("reconcile_attempts", "INTEGER NOT NULL DEFAULT 0")):
                if name not in columns:
                    db.execute(f"ALTER TABLE source_watches ADD COLUMN {name} {declaration}")

    @contextmanager
    def _db(self):
        db = self._memory or sqlite3.connect(self.db_path, timeout=5)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        try:
            with db:
                yield db
        finally:
            if self._memory is None:
                db.close()

    def _check_scope(self, scope, is_owner=False, write=False):
        scope = _scope(scope)
        if not _allowed(self.allowed_channels, scope["channel_type"], scope["channel_id"]):
            raise ValueError("Use a configured channel for source watches")
        if write and (not is_owner or not _allowed(self.owner_ids, scope["channel_type"], scope["sender_id"])):
            raise ValueError("The bot owner can add or remove source watches")
        return scope

    def create(self, scope, url, interval_seconds=3600, is_owner=False):
        scope = self._check_scope(scope, is_owner, write=True)
        url = _feed_url(url)
        if type(interval_seconds) is not int or not 900 <= interval_seconds <= 86400:
            raise ValueError("Choose an interval from 15 minutes to 24 hours")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT * FROM source_watches WHERE channel_type=? AND channel_id=? AND (url=? OR (request_event_id<>'' AND request_event_id=?))",
                                  (scope["channel_type"], scope["channel_id"], url, scope["event_id"])).fetchone()
            if existing:
                return dict(existing)
            count = db.execute("SELECT COUNT(*) FROM source_watches WHERE channel_type=? AND channel_id=?",
                               (scope["channel_type"], scope["channel_id"])).fetchone()[0]
            if count >= 5:
                raise ValueError("This channel has five watches. Remove a watch to add another")
            watch_id = uuid.uuid4().hex[:12]
            now = self.now()
            db.execute("INSERT INTO source_watches (id,channel_type,channel_id,owner_id,request_event_id,url,interval_seconds,created_at,next_due) VALUES (?,?,?,?,?,?,?,?,?)",
                       (watch_id, scope["channel_type"], scope["channel_id"], scope["sender_id"], scope["event_id"], url, interval_seconds, now, now))
            return dict(db.execute("SELECT * FROM source_watches WHERE id=?", (watch_id,)).fetchone())

    def list(self, scope):
        scope = self._check_scope(scope)
        with self._db() as db:
            return [dict(row) for row in db.execute("SELECT * FROM source_watches WHERE channel_type=? AND channel_id=? ORDER BY created_at,id",
                                                   (scope["channel_type"], scope["channel_id"]))]

    def remove(self, scope, watch_id, is_owner=False):
        scope = self._check_scope(scope, is_owner, write=True)
        with self._db() as db:
            return db.execute("DELETE FROM source_watches WHERE id=? AND channel_type=? AND channel_id=?",
                              (watch_id, scope["channel_type"], scope["channel_id"])).rowcount == 1

    def command_response(self, scope):
        scope = _scope(scope)
        if not scope["event_id"]:
            return None
        with self._db() as db:
            row = db.execute("SELECT response FROM source_watch_commands WHERE channel_type=? AND channel_id=? AND event_id=?",
                             (scope["channel_type"], scope["channel_id"], scope["event_id"])).fetchone()
            return row[0] if row else None

    def save_command_response(self, scope, text):
        scope = _scope(scope)
        if scope["event_id"]:
            with self._db() as db:
                db.execute("INSERT OR IGNORE INTO source_watch_commands VALUES (?,?,?,?)",
                           (scope["channel_type"], scope["channel_id"], scope["event_id"], text))

    def claim(self, pending=False, lease_seconds=180):
        now = self.now()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            # A process may have stopped after sending. Hold the saved update for reconciliation.
            db.execute("UPDATE source_watches SET pending_status='unknown',lease_token=NULL,lease_until=0,reconcile_after=0,reconcile_attempts=0 WHERE pending_status='sending' AND lease_until<=?", (now,))
            db.execute("UPDATE source_watch_deliveries SET status='unknown' WHERE status='sending' AND delivery_key IN (SELECT delivery_key FROM source_watches WHERE pending_status='unknown')")
            state = "pending_status='pending'" if pending else "pending_status IS NULL"
            rows = db.execute(f"SELECT * FROM source_watches WHERE {state} AND next_due<=? AND lease_until<=? ORDER BY next_due,id", (now, now)).fetchall()
            row = next((row for row in rows if _allowed(self.allowed_channels, row["channel_type"], row["channel_id"])
                        and _allowed(self.owner_ids, row["channel_type"], row["owner_id"])), None)
            if row is None:
                return None
            token = uuid.uuid4().hex
            db.execute("UPDATE source_watches SET lease_token=?,lease_until=? WHERE id=?", (token, now + lease_seconds, row["id"]))
            result = dict(row)
            result.update(lease_token=token, lease_until=now + lease_seconds)
            return result

    def reserve_lookup(self, scope, limit):
        scope = self._check_scope(scope)
        day = datetime.fromtimestamp(self.now(), timezone.utc).date().isoformat()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            key = (day, scope["channel_type"], scope["channel_id"])
            db.execute("INSERT OR IGNORE INTO source_watch_budget(day,channel_type,channel_id,lookups) VALUES (?,?,?,0)", key)
            return db.execute("UPDATE source_watch_budget SET lookups=lookups+1 WHERE day=? AND channel_type=? AND channel_id=? AND lookups<?", (*key, max(0, int(limit)))).rowcount == 1

    def defer(self, watch, delay=60):
        with self._db() as db:
            db.execute("UPDATE source_watches SET next_due=?,lease_token=NULL,lease_until=0 WHERE id=? AND lease_token=?",
                       (self.now() + delay, watch["id"], watch["lease_token"]))

    def save_feed(self, watch, items):
        now = self.now()
        feed_hash = hashlib.sha256(json.dumps(sorted(items, key=lambda item: item["key"]), sort_keys=True).encode()).hexdigest()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM source_watches WHERE id=? AND lease_token=?", (watch["id"], watch["lease_token"])).fetchone()
            if row is None:
                return None
            seen = {row[0] for row in db.execute("SELECT item_key FROM source_watch_items WHERE watch_id=?", (watch["id"],))}
            new_items = [item for item in items if item["key"] not in seen]
            latest = _digest(watch, items, latest=True)
            if not row["baseline"]:
                db.executemany("INSERT OR IGNORE INTO source_watch_items VALUES (?,?)", [(watch["id"], item["key"]) for item in items])
                new_items = []
            db.execute("UPDATE source_watches SET baseline=1,last_hash=?,last_items=?,fetched_at=?,last_digest=?,next_due=? WHERE id=?",
                       (feed_hash, json.dumps(items), now, latest, now + watch["interval_seconds"], watch["id"]))
            if not new_items:
                db.execute("UPDATE source_watches SET lease_token=NULL,lease_until=0 WHERE id=?", (watch["id"],))
                return None
            keys = sorted(item["key"] for item in new_items)
            delivery_key = hashlib.sha256((watch["id"] + ":" + ":".join(keys)).encode()).hexdigest()
            text = _digest(watch, new_items)
            db.execute("INSERT INTO source_watch_deliveries(delivery_key,watch_id,status,text,item_keys,prepared_at) VALUES (?,?,?,?,?,?)",
                       (delivery_key, watch["id"], "pending", text, json.dumps(keys), now))
            db.execute("UPDATE source_watches SET pending_text=?,pending_keys=?,delivery_key=?,pending_status='pending',next_due=? WHERE id=?",
                       (text, json.dumps(keys), delivery_key, now, watch["id"]))
            return dict(db.execute("SELECT * FROM source_watches WHERE id=?", (watch["id"],)).fetchone())

    def begin_delivery(self, watch):
        with self._db() as db:
            changed = db.execute("UPDATE source_watches SET pending_status='sending' WHERE id=? AND lease_token=? AND lease_until>? AND pending_status='pending'",
                                 (watch["id"], watch["lease_token"], self.now())).rowcount
            if changed:
                db.execute("UPDATE source_watch_deliveries SET status='sending' WHERE delivery_key=?", (watch["delivery_key"],))
            return changed == 1

    def reconcile_delivery(self, delivery_key, status, message_id=None):
        if status not in {"success", "sent", "failure", "unknown"}:
            raise ValueError("Use a confirmed delivery status")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM source_watches WHERE delivery_key=?", (delivery_key,)).fetchone()
            if row is None:
                return False
            if status in {"success", "sent"}:
                if not message_id:
                    status = "unknown"
                else:
                    db.executemany("INSERT OR IGNORE INTO source_watch_items VALUES (?,?)",
                                   [(row["id"], key) for key in json.loads(row["pending_keys"] or "[]")])
                    db.execute("UPDATE source_watch_deliveries SET status='sent',message_id=?,sent_at=? WHERE delivery_key=?", (str(message_id), self.now(), delivery_key))
                    db.execute("UPDATE source_watches SET pending_text=NULL,pending_keys=NULL,pending_status=NULL,delivery_key=NULL,delivery_message_id=?,lease_token=NULL,lease_until=0,reconcile_after=0,reconcile_attempts=0,next_due=? WHERE id=?",
                               (str(message_id), max(self.now(), (row["fetched_at"] or self.now()) + row["interval_seconds"]), row["id"]))
                    return True
            stored_status = "pending" if status == "failure" else "unknown"
            db.execute("UPDATE source_watch_deliveries SET status=? WHERE delivery_key=?", (stored_status, delivery_key))
            db.execute("UPDATE source_watches SET pending_status=?,lease_token=NULL,lease_until=0,next_due=? WHERE id=?",
                       (stored_status, self.now() + 60, row["id"]))
            if stored_status == "pending":
                db.execute("UPDATE source_watches SET reconcile_after=0,reconcile_attempts=0 WHERE id=?", (row["id"],))
            return True

    def uncertain_deliveries(self):
        with self._db() as db:
            return [dict(row) for row in db.execute("SELECT * FROM source_watches WHERE pending_status='unknown' ORDER BY reconcile_after,id")
                    if _allowed(self.allowed_channels, row["channel_type"], row["channel_id"])
                    and _allowed(self.owner_ids, row["channel_type"], row["owner_id"])]


    def claim_uncertain(self, limit=5, minimum_delay=60):
        now = self.now()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute("SELECT * FROM source_watches WHERE pending_status='unknown' AND reconcile_after<=? ORDER BY reconcile_after,id", (now,)).fetchall()
            claimed = []
            for row in rows:
                if not _allowed(self.allowed_channels, row["channel_type"], row["channel_id"]) or not _allowed(self.owner_ids, row["channel_type"], row["owner_id"]):
                    continue
                delay = max(minimum_delay, min(900, 60 * 2 ** min(row["reconcile_attempts"], 4)))
                db.execute("UPDATE source_watches SET reconcile_after=?,reconcile_attempts=reconcile_attempts+1 WHERE id=?", (now + delay, row["id"]))
                claimed.append(dict(row))
                if len(claimed) >= limit:
                    break
            return claimed


class SourceWatchService:
    """Poll feeds within a daily budget and send saved source-linked updates."""

    def __init__(self, store, fetch=None, send=None, action_context=None, daily_lookup_budget=24, now=time.time, reconcile=None, callback_timeout=30):
        self.store = store
        self.fetch = fetch or self._fetch
        self.send = send
        self.action_context = action_context
        self.daily_lookup_budget = daily_lookup_budget
        self.now = now
        self.reconcile = reconcile
        self.callback_timeout = callback_timeout
        self._tick_lock = asyncio.Lock()

    async def _fetch(self, url):
        return await ReadFeedTool().execute({"url": url}, self.action_context)

    async def manage(self, command, scope, is_owner=False):
        text = str(command or "").strip()
        verb = text.split(maxsplit=1)[0].lower() if text else ""
        if verb not in {"watch", "watches", "unwatch", "digest"}:
            return None
        try:
            self.store._check_scope(scope, is_owner, write=verb in {"watch", "unwatch"})
            prior = self.store.command_response(scope)
            if prior:
                return prior
            if verb == "watch":
                match = re.fullmatch(r"watch\s+(\S+)(?:\s+every\s+(\d+)([mh]))?", text, re.IGNORECASE)
                if not match:
                    return HELP
                interval = int(match[2]) * (3600 if match[3].lower() == "h" else 60) if match[2] else 3600
                watch = self.store.create(scope, match[1], interval, is_owner)
                response = f"Watch {watch['id']} is set for every {watch['interval_seconds'] // 60} minutes. The first check saves a quiet baseline.\n{watch['url']}"
            elif verb == "unwatch":
                match = re.fullmatch(r"unwatch\s+([a-f0-9]{12})", text, re.IGNORECASE)
                if not match:
                    return HELP
                response = f"Removed watch {match[1]}." if self.store.remove(scope, match[1].lower(), is_owner) else "Choose an ID from this channel's `watches` list."
            else:
                match = re.fullmatch(r"watches" if verb == "watches" else r"digest(?:\s+([a-f0-9]{12}))?", text, re.IGNORECASE)
                if not match:
                    return HELP
                watches = self.store.list(scope)
                if verb == "watches":
                    lines = []
                    for watch in watches:
                        url = watch["url"].replace("`", "%60")
                        preview = url[:260] + ("…" if len(url) > 260 else "")
                        lines.append(f"{watch['id']} · every {watch['interval_seconds'] // 60}m · `{preview}`")
                    response = "\n".join(lines) or "This channel has zero watches. " + HELP
                else:
                    requested = match[1].lower() if match[1] else None
                    selected = [watch for watch in watches if requested is None or watch["id"] == requested]
                    if len(selected) > 1:
                        lines = []
                        for watch in selected:
                            items = json.loads(watch["last_items"])
                            if not items:
                                lines.append(f"Watch {watch['id']} is waiting for its first check.")
                                continue
                            item = items[0]
                            title = _safe_text(item["title"], 90)
                            line = f"Watch {watch['id']} · [{title}]({_link_url(item['url'])})"
                            if len(line) > 350:
                                line = f"Watch {watch['id']} · {title}. Read `digest {watch['id']}` for its source links."
                            lines.append(line)
                        response = "Latest cached entries:\n" + "\n".join(lines)
                    else:
                        response = "\n\n".join(watch["last_digest"] or f"Watch {watch['id']} is waiting for its first check." for watch in selected) or "Choose an ID from this channel's `watches` list."
            self.store.save_command_response(scope, response)
            return response
        except (ValueError, TypeError) as error:
            return str(error)

    async def _deliver(self, watch):
        if self.send is None:
            self.store.defer(watch)
            return
        if not self.store.begin_delivery(watch):
            return
        try:
            result = await asyncio.wait_for(self.send(watch["id"], watch["channel_type"], watch["channel_id"], watch["pending_text"], watch["delivery_key"]), timeout=self.callback_timeout)
        except asyncio.CancelledError:
            self.store.reconcile_delivery(watch["delivery_key"], "unknown")
            raise
        except Exception:
            self.store.reconcile_delivery(watch["delivery_key"], "unknown")
            return
        status = result.get("status", "unknown") if isinstance(result, dict) else "unknown"
        self.store.reconcile_delivery(watch["delivery_key"], status if status in {"success", "sent", "failure"} else "unknown",
                                      result.get("message_id") if isinstance(result, dict) else None)

    async def tick(self):
        async with self._tick_lock:
            # Bound each wake even when several watches are due after a restart.
            for _ in range(5):
                watch = self.store.claim(pending=True)
                if watch is None:
                    break
                await self._deliver(watch)
            if self.reconcile:
                for _ in range(5):
                    claimed = self.store.claim_uncertain(limit=1, minimum_delay=max(60, self.callback_timeout + 5))
                    if not claimed:
                        break
                    watch = claimed[0]
                    try:
                        result = await asyncio.wait_for(self.reconcile(watch["id"], watch["channel_type"], watch["channel_id"], watch["pending_text"], watch["delivery_key"]), timeout=self.callback_timeout)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        continue
                    if isinstance(result, dict):
                        status = result.get("status", "unknown")
                        if status in {"success", "sent", "failure"}:
                            self.store.reconcile_delivery(watch["delivery_key"], status, result.get("message_id"))
            for _ in range(5):
                watch = self.store.claim()
                if watch is None:
                    break
                if not self.store.reserve_lookup(watch, self.daily_lookup_budget):
                    tomorrow = (int(self.now()) // 86400 + 1) * 86400
                    self.store.defer(watch, max(60, tomorrow - self.now()))
                    continue
                try:
                    result = await self.fetch(watch["url"])
                    if not isinstance(result, dict) or result.get("status") != "success":
                        self.store.defer(watch, min(900, watch["interval_seconds"]))
                        continue
                    prepared = self.store.save_feed(watch, _items(result, watch["url"]))
                except asyncio.CancelledError:
                    self.store.defer(watch)
                    raise
                except Exception:
                    self.store.defer(watch, min(900, watch["interval_seconds"]))
                    continue
                if prepared:
                    await self._deliver(prepared)
