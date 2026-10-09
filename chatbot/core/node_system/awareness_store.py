"""Persistent awareness, independent channel views, and saved task spending.

Adapters supply trusted channel and sender IDs. Reader scope comes from those
IDs. Model text supplies content, summaries, and routes within that scope.
"""

from contextlib import contextmanager
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import hashlib
import json
from pathlib import Path
import secrets
import sqlite3
import threading
import time
import uuid


def _json(value):
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    if len(encoded.encode()) > 100_000:
        raise ValueError("Use a bounded awareness record")
    return encoded


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _message_hash(message):
    stable = dict(message)
    stable.pop("timestamp", None)
    metadata = dict(stable.get("metadata", {}))
    metadata.pop("historical", None)
    stable["metadata"] = metadata
    return _hash(stable)


def _money(value):
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0 or amount > 1000:
            raise ValueError("Use a finite cost between zero and 1000 dollars")
        return int((amount * 1_000_000).to_integral_value(rounding=ROUND_CEILING))
    except (InvalidOperation, TypeError):
        raise ValueError("Use a valid dollar amount") from None


class AwarenessStore:
    def __init__(self, db_path, *, allowed_channels=None, community_channels=None,
                 clock=time.time, max_task_budget_usd=1):
        self.clock = clock
        self.max_budget = _money(max_task_budget_usd)
        self.allowed = {str(p): {str(c) for c in channels} for p, channels in (allowed_channels or {}).items()}
        self.communities = {}
        for platform, channels in (community_channels or {}).items():
            pairs = channels.items() if isinstance(channels, dict) else ((c, "shared") for c in channels)
            for channel, community in pairs:
                if str(channel) in self.allowed.get(str(platform), set()):
                    self.communities[(str(platform), str(channel))] = str(community)
        if str(db_path) != ":memory:":
            Path(db_path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS awareness_actors (id TEXT PRIMARY KEY, canonical_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS awareness_accounts (
                platform TEXT, account_id TEXT, actor_id TEXT NOT NULL, PRIMARY KEY(platform,account_id));
            CREATE TABLE IF NOT EXISTS awareness_messages (
                event_key TEXT PRIMARY KEY, platform TEXT, channel_id TEXT, event_id TEXT, sender_id TEXT,
                actor_id TEXT, revision INTEGER, content_hash TEXT, data_json TEXT, deleted INTEGER,
                timestamp REAL, updated_at REAL, UNIQUE(platform,channel_id,event_id));
            CREATE TABLE IF NOT EXISTS awareness_events (
                position INTEGER PRIMARY KEY AUTOINCREMENT, event_key TEXT, revision INTEGER, kind TEXT,
                data_json TEXT, created_at REAL, UNIQUE(event_key,revision));
            CREATE TABLE IF NOT EXISTS awareness_nodes (
                id TEXT PRIMARY KEY, kind TEXT, data_json TEXT, summary TEXT DEFAULT '',
                version INTEGER DEFAULT 1, summary_version INTEGER DEFAULT 0, valid INTEGER DEFAULT 1,
                audience_kind TEXT, audience_key TEXT, evidence_json TEXT DEFAULT '[]',
                created_at REAL, updated_at REAL);
            CREATE TABLE IF NOT EXISTS awareness_dependencies (
                node_id TEXT, ref_kind TEXT, ref_id TEXT, ref_version INTEGER,
                PRIMARY KEY(node_id,ref_kind,ref_id));
            CREATE TABLE IF NOT EXISTS awareness_views (
                id TEXT PRIMARY KEY, platform TEXT, channel_id TEXT, actor_id TEXT,
                state_json TEXT, revision INTEGER, updated_at REAL);
            CREATE TABLE IF NOT EXISTS awareness_tasks (
                id TEXT PRIMARY KEY, actor_id TEXT, platform TEXT, channel_id TEXT,
                audience_kind TEXT, audience_key TEXT, goal TEXT, topic TEXT, persona_id TEXT,
                status TEXT DEFAULT 'queued', parent_id TEXT, budget INTEGER, spent INTEGER DEFAULT 0,
                reserved INTEGER DEFAULT 0, input_json TEXT DEFAULT '{}', route_id TEXT,
                result_json TEXT, created_at REAL, updated_at REAL);
            CREATE TABLE IF NOT EXISTS awareness_task_requests (
                platform TEXT, channel_id TEXT, event_id TEXT, task_id TEXT,
                PRIMARY KEY(platform,channel_id,event_id));
            CREATE TABLE IF NOT EXISTS awareness_routes (
                id TEXT PRIMARY KEY, task_id TEXT, request_key TEXT, data_json TEXT,
                input_json TEXT, created_at REAL, UNIQUE(task_id,request_key));
            CREATE TABLE IF NOT EXISTS awareness_attempts (
                id TEXT PRIMARY KEY, task_id TEXT, request_key TEXT, route_id TEXT, status TEXT,
                reserved INTEGER, cost INTEGER DEFAULT 0, input_json TEXT, result_json TEXT,
                created_at REAL, updated_at REAL, UNIQUE(task_id,request_key));
            CREATE TABLE IF NOT EXISTS awareness_links (
                id TEXT PRIMARY KEY, source_actor TEXT, source_platform TEXT, source_account TEXT,
                target_platform TEXT, target_account TEXT, target_actor TEXT, token_hash TEXT,
                stage TEXT, expires_at REAL, created_at REAL);
        """)

    @contextmanager
    def _transaction(self):
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield self._db
                self._db.commit()
            except BaseException:
                self._db.rollback()
                raise

    def _scope(self, platform, channel_id, sender_id=""):
        platform, channel_id, sender_id = str(platform), str(channel_id), str(sender_id)
        if channel_id not in self.allowed.get(platform, set()):
            raise PermissionError("Use an approved awareness channel")
        return platform, channel_id, sender_id

    def _actor(self, db, platform, sender_id):
        if not sender_id:
            raise ValueError("A trusted sender ID is required")
        actor = "actor:" + _hash([platform, sender_id])[:24]
        db.execute("INSERT OR IGNORE INTO awareness_actors VALUES(?,?)", (actor, actor))
        db.execute("INSERT OR IGNORE INTO awareness_accounts VALUES(?,?,?)", (platform, sender_id, actor))
        row = db.execute("SELECT actor_id FROM awareness_accounts WHERE platform=? AND account_id=?", (platform, sender_id)).fetchone()
        return self._canonical(db, row[0])

    @staticmethod
    def _canonical(db, actor_id):
        for _ in range(32):
            row = db.execute("SELECT canonical_id FROM awareness_actors WHERE id=?", (actor_id,)).fetchone()
            if not row or row[0] == actor_id:
                return actor_id
            actor_id = row[0]
        raise ValueError("Actor links need a valid canonical ID")

    def actor_id(self, platform, channel_id, sender_id):
        platform, _, sender_id = self._scope(platform, channel_id, sender_id)
        with self._transaction() as db:
            return self._actor(db, platform, sender_id)

    def _audience(self, platform, channel_id, actor_id, audience=None):
        if audience == "personal":
            return "personal", actor_id
        if audience not in (None, "channel", "community"):
            raise ValueError("Choose channel, community, or personal audience")
        community = self.communities.get((platform, channel_id))
        if audience == "community" and not community:
            raise PermissionError("Use a configured shared community")
        return ("community", community) if community and audience != "channel" else ("channel", _json([platform, channel_id]))

    def _can_read(self, db, row, platform, channel_id, actor_id):
        kind, key = row["audience_kind"], row["audience_key"]
        if kind == "community":
            return key == self.communities.get((platform, channel_id))
        if kind == "personal":
            return actor_id == self._canonical(db, key)
        return key == _json([platform, channel_id])

    def _can_derive(self, db, audience, row):
        if audience == (row["audience_kind"], row["audience_key"]):
            return True
        kind, key = audience
        if row["audience_kind"] == "community":
            if kind == "channel":
                return self.communities.get(tuple(json.loads(key))) == row["audience_key"]
            return kind == "personal"
        return kind == "personal" and row["audience_kind"] == "personal" and self._canonical(db, key) == self._canonical(db, row["audience_key"])

    @staticmethod
    def _node(row):
        return {"kind": row["kind"], "data": json.loads(row["data_json"]), "summary": row["summary"],
                "version": row["version"], "summary_version": row["summary_version"], "valid": bool(row["valid"]),
                "audience": {"kind": row["audience_kind"], "key": row["audience_key"]},
                "evidence": json.loads(row["evidence_json"]), "updated_at": row["updated_at"]}

    def _invalidate(self, db, ref_kind, ref_id):
        queue, seen = [(ref_kind, ref_id)], set()
        while queue:
            kind, identity = queue.pop()
            for row in db.execute("SELECT node_id FROM awareness_dependencies WHERE ref_kind=? AND ref_id=?", (kind, identity)).fetchall():
                node = row[0]
                if node in seen:
                    continue
                seen.add(node)
                db.execute("UPDATE awareness_nodes SET valid=0,summary='',summary_version=0,version=version+1,updated_at=? WHERE id=?", (self.clock(), node))
                if node.startswith("tasks."):
                    task_id = node[len("tasks."):]
                    db.execute("UPDATE awareness_tasks SET status='needs_refresh',result_json=NULL,updated_at=? WHERE id=?", (self.clock(), task_id))
                    db.execute("UPDATE awareness_attempts SET status='stale' WHERE task_id=? AND status IN ('active','complete')", (task_id,))
                queue.append(("node", node))
        return seen

    def _write_node(self, db, node_id, kind, data, audience, evidence=()):
        previous = db.execute("SELECT * FROM awareness_nodes WHERE id=?", (node_id,)).fetchone()
        encoded = _json(data)
        encoded_evidence = _json(list(evidence))
        if previous and previous["valid"] and previous["data_json"] == encoded and previous["evidence_json"] == encoded_evidence:
            return self._node(previous)
        if previous:
            self._invalidate(db, "node", node_id)
        version = previous["version"] + 1 if previous else 1
        now = self.clock()
        db.execute("""INSERT INTO awareness_nodes
            (id,kind,data_json,summary,version,summary_version,valid,audience_kind,audience_key,evidence_json,created_at,updated_at)
            VALUES(?,?,?,'',?,0,1,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
            kind=excluded.kind,data_json=excluded.data_json,summary='',version=excluded.version,summary_version=0,valid=1,
            audience_kind=excluded.audience_kind,audience_key=excluded.audience_key,evidence_json=excluded.evidence_json,updated_at=excluded.updated_at""",
            (node_id, kind, encoded, version, *audience, encoded_evidence, previous["created_at"] if previous else now, now))
        db.execute("DELETE FROM awareness_dependencies WHERE node_id=?", (node_id,))
        for item in evidence:
            db.execute("INSERT OR REPLACE INTO awareness_dependencies VALUES(?,?,?,?)", (node_id, item["kind"], item["id"], item["version"]))
        return self._node(db.execute("SELECT * FROM awareness_nodes WHERE id=?", (node_id,)).fetchone())

    def ingest_message(self, platform, channel_id, message_dict):
        platform, channel_id, _ = self._scope(platform, channel_id)
        message = dict(message_dict)
        event_id, sender_id = str(message.get("id", "")), str(message.get("sender", message.get("sender_id", "")))
        if not event_id or len(event_id) > 500:
            raise ValueError("A bounded source event ID is required")
        with self._transaction() as db:
            key = _hash([platform, channel_id, event_id])
            previous = db.execute("SELECT * FROM awareness_messages WHERE event_key=?", (key,)).fetchone()
            sender_id = sender_id or (previous["sender_id"] if previous else "")
            if previous and not previous["deleted"] and sender_id != previous["sender_id"]:
                raise PermissionError("Keep the original source sender")
            deleted = bool(message.get("deleted") or message.get("metadata", {}).get("deleted"))
            # A saved deletion survives older event replay.
            if previous and previous["deleted"] and not deleted:
                return {"event_id": event_id, "revision": previous["revision"], "changed": False,
                        "node_id": f"channels.{platform}.{channel_id}", "deleted": True}
            sender_id = sender_id or "source:unknown"
            actor = self._actor(db, platform, sender_id)
            if deleted:
                message = {"id": event_id, "sender": sender_id, "deleted": True}
            else:
                message["id"], message["sender"] = event_id, sender_id
                message["content"] = str(message.get("content", ""))[:12000]
            fingerprint = _message_hash(message)
            if previous and previous["content_hash"] == fingerprint:
                return {"event_id": event_id, "revision": previous["revision"], "changed": False,
                        "node_id": f"channels.{platform}.{channel_id}", "deleted": deleted}
            # Adapters can supply source revisions to fence older edit replay.
            source_revision = message.get("source_revision")
            old_message = json.loads(previous["data_json"]) if previous else {}
            historical_match = previous and source_revision is None and any(_message_hash(json.loads(row[0])) == fingerprint for row in db.execute("SELECT data_json FROM awareness_events WHERE event_key=?", (key,)))
            if previous and not deleted and source_revision is None and ("source_revision" in old_message or historical_match):
                return {"event_id": event_id, "revision": previous["revision"], "changed": False,
                        "node_id": f"channels.{platform}.{channel_id}", "deleted": bool(previous["deleted"])}
            if source_revision is not None and (isinstance(source_revision, bool) or not isinstance(source_revision, (int, float)) or not float("-inf") < source_revision < float("inf")):
                raise ValueError("Use a finite source revision")
            edit_id = str(message.get("metadata", {}).get("edit_event_id", ""))
            old_edit_id = str(old_message.get("metadata", {}).get("edit_event_id", ""))
            if not deleted and source_revision is not None and previous and (source_revision, edit_id) <= (old_message.get("source_revision", -1), old_edit_id):
                return {"event_id": event_id, "revision": previous["revision"], "changed": False,
                        "node_id": f"channels.{platform}.{channel_id}", "deleted": bool(previous["deleted"])}
            revision = previous["revision"] + 1 if previous else 1
            timestamp = message.get("timestamp", previous["timestamp"] if previous else self.clock())
            if not isinstance(timestamp, (int, float)) or not float("-inf") < timestamp < float("inf"):
                raise ValueError("Use a finite source timestamp")
            db.execute("""INSERT INTO awareness_messages VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(event_key) DO UPDATE SET revision=excluded.revision,content_hash=excluded.content_hash,
                data_json=excluded.data_json,deleted=excluded.deleted,updated_at=excluded.updated_at""",
                (key, platform, channel_id, event_id, sender_id, actor, revision, fingerprint, _json(message), int(deleted), timestamp, self.clock()))
            db.execute("INSERT INTO awareness_events(event_key,revision,kind,data_json,created_at) VALUES(?,?,?,?,?)",
                       (key, revision, "delete" if deleted else "edit" if previous else "message", _json(message), self.clock()))
            self._invalidate(db, "event", key)
            rows = db.execute("SELECT * FROM awareness_messages WHERE platform=? AND channel_id=? AND deleted=0 ORDER BY timestamp DESC,event_key LIMIT 10", (platform, channel_id)).fetchall()
            projected = []
            for row in reversed(rows):
                source = json.loads(row["data_json"])
                item = {key: source[key] for key in ("id", "sender", "sender_id", "sender_display_name", "timestamp", "reply_to", "source_revision") if key in source}
                item["content"] = str(source.get("content", ""))[:1000]
                projected.append(item)
            data = {"platform": platform, "channel_id": channel_id,
                    "messages": projected}
            refs = [{"kind": "event", "id": row["event_key"], "version": row["revision"]} for row in rows]
            node_id = f"channels.{platform}.{channel_id}"
            self._write_node(db, node_id, "conversation", data, self._audience(platform, channel_id, actor), refs)
            return {"event_id": event_id, "revision": revision, "changed": True, "node_id": node_id, "deleted": deleted}

    def edit_message(self, platform, channel_id, event_id, message_dict):
        self._scope(platform, channel_id)
        with self._lock:
            previous = self._db.execute("SELECT data_json FROM awareness_messages WHERE platform=? AND channel_id=? AND event_id=?", (platform, channel_id, event_id)).fetchone()
            if not previous:
                raise ValueError("Edit a saved original event")
            saved = json.loads(previous[0])
            message = {**saved, **message_dict, "id": event_id}
            message["reply_to"] = saved.get("reply_to")
            message["timestamp"] = saved.get("timestamp", message.get("timestamp", self.clock()))
            message["metadata"] = {**saved.get("metadata", {}), **message_dict.get("metadata", {})}
            return self.ingest_message(platform, channel_id, message)

    def delete_message(self, platform, channel_id, event_id, sender_id=None, timestamp=None):
        self._scope(platform, channel_id)
        with self._lock:
            previous = self._db.execute("SELECT sender_id FROM awareness_messages WHERE platform=? AND channel_id=? AND event_id=?", (platform, channel_id, event_id)).fetchone()
            message = {"id": event_id, "sender": previous[0] if previous else sender_id or "source:unknown", "deleted": True}
            if timestamp is not None:
                message["timestamp"] = timestamp
            return self.ingest_message(platform, channel_id, message)

    def request_current(self, platform, channel_id, event_id, content):
        self._scope(platform, channel_id)
        with self._lock:
            row = self._db.execute("SELECT data_json,deleted FROM awareness_messages WHERE platform=? AND channel_id=? AND event_id=?", (platform, channel_id, event_id)).fetchone()
            return bool(row and not row["deleted"] and json.loads(row["data_json"]).get("content", "") == content)

    def put_node(self, node_id, kind, data, platform, channel_id, sender_id, *, audience=None, evidence=()):
        platform, channel_id, sender_id = self._scope(platform, channel_id, sender_id)
        if not node_id or len(node_id) > 600:
            raise ValueError("Use a bounded node ID")
        with self._transaction() as db:
            actor = self._actor(db, platform, sender_id)
            target = self._audience(platform, channel_id, actor, audience)
            existing = db.execute("SELECT * FROM awareness_nodes WHERE id=?", (node_id,)).fetchone()
            if existing and (not self._can_read(db, existing, platform, channel_id, actor) or target != (existing["audience_kind"], existing["audience_key"])):
                raise PermissionError("Keep the saved node audience")
            for ref in evidence:
                if ref["kind"] == "node":
                    source = db.execute("SELECT * FROM awareness_nodes WHERE id=? AND valid=1", (ref["id"],)).fetchone()
                    if not source or source["version"] != ref["version"] or not self._can_read(db, source, platform, channel_id, actor) or not self._can_derive(db, target, source):
                        raise PermissionError("Use current evidence within the node audience")
                elif ref["kind"] == "event":
                    source = db.execute("SELECT * FROM awareness_messages WHERE event_key=? AND deleted=0", (ref["id"],)).fetchone()
                    if not source or source["revision"] != ref["version"]:
                        raise ValueError("Use current source evidence")
                    event_audience = self._audience(source["platform"], source["channel_id"], source["actor_id"])
                    if not self._can_derive(db, target, {"audience_kind": event_audience[0], "audience_key": event_audience[1]}):
                        raise PermissionError("Keep source evidence within its audience")
                else:
                    raise ValueError("Choose node or event evidence")
            return self._write_node(db, node_id, kind, data, target, evidence)

    def source_result(self, platform, channel_id, sender_id, tool, params, result, *, audience=None):
        platform, channel_id, sender_id = self._scope(platform, channel_id, sender_id)
        actor = self.actor_id(platform, channel_id, sender_id)
        scope = self._audience(platform, channel_id, actor, audience)
        node_id = "sources." + _hash([scope, tool, params])[:24]
        node = self.put_node(node_id, "source", {"tool": tool, "params": params, "result": result,
            "fetched_at": self.clock(), "trust_label": "untrusted"}, platform, channel_id, sender_id, audience=audience)
        return {"node_id": node_id, **node}

    def publish_summary(self, node_id, content_version, summary):
        with self._transaction() as db:
            updated = db.execute("UPDATE awareness_nodes SET summary=?,summary_version=? WHERE id=? AND version=? AND valid=1",
                                 (str(summary)[:1200], content_version, node_id, content_version))
            return {"updated": bool(updated.rowcount), "conflict": not bool(updated.rowcount)}

    def snapshot_versions(self, platform, channel_id, sender_id, nodes, *, event_id=None):
        """Pin source events so later conversation activity keeps task inputs stable."""
        platform, channel_id, sender_id = self._scope(platform, channel_id, sender_id)
        with self._transaction() as db:
            actor = self._actor(db, platform, sender_id)
            versions = {}
            if event_id:
                event = db.execute("SELECT * FROM awareness_messages WHERE platform=? AND channel_id=? AND event_id=? AND deleted=0",
                    (platform, channel_id, event_id)).fetchone()
                if not event or self._canonical(db, event["actor_id"]) != actor:
                    raise PermissionError("Snapshot the current sender's source event")
                versions["event:" + event["event_key"]] = event["revision"]
            for node_id, value in nodes.items():
                row = db.execute("SELECT * FROM awareness_nodes WHERE id=? AND valid=1", (node_id,)).fetchone()
                if not row or row["version"] != value.get("version") or not self._can_read(db, row, platform, channel_id, actor):
                    raise ValueError("Snapshot current nodes from your allowed catalog")
                if row["kind"] == "task":
                    continue
                if row["kind"] == "conversation":
                    for ref in json.loads(row["evidence_json"]):
                        if ref["kind"] == "event":
                            versions["event:" + ref["id"]] = ref["version"]
                else:
                    versions[node_id] = row["version"]
            return versions

    def catalog(self, platform, channel_id, sender_id, query="", *, limit=24, max_chars=16000):
        platform, channel_id, sender_id = self._scope(platform, channel_id, sender_id)
        limit, max_chars = max(0, min(int(limit), 100)), max(0, min(int(max_chars), 100000))
        with self._transaction() as db:
            actor = self._actor(db, platform, sender_id)
            rows = db.execute("SELECT * FROM awareness_nodes WHERE valid=1 ORDER BY updated_at DESC,id").fetchall()
            accessible = [row for row in rows if self._can_read(db, row, platform, channel_id, actor)]
            terms = str(query).lower().split()[:20]
            if terms:
                accessible.sort(key=lambda row: sum(t in (row["id"] + row["summary"] + row["data_json"]).lower() for t in terms), reverse=True)
            catalog = {}
            for row in accessible:
                if len(catalog) >= limit:
                    break
                value = self._node(row)
                candidate = {**catalog, row["id"]: value}
                if len(json.dumps(candidate, ensure_ascii=False)) <= max_chars:
                    catalog = candidate
            return catalog

    def load_view(self, view_id, platform, channel_id, sender_id):
        platform, channel_id, sender_id = self._scope(platform, channel_id, sender_id)
        with self._transaction() as db:
            actor = self._actor(db, platform, sender_id)
            row = db.execute("SELECT * FROM awareness_views WHERE id=?", (view_id,)).fetchone()
            if row and (row["platform"] != platform or row["channel_id"] != channel_id or self._canonical(db, row["actor_id"]) != actor):
                raise PermissionError("Use your conversation view")
            return {**(json.loads(row["state_json"]) if row else {"expanded": [], "collapsed": [], "pins": []}), "revision": row["revision"] if row else 0}

    def save_view(self, view_id, platform, channel_id, sender_id, *, expanded=(), collapsed=(), pins=(), expected_revision=0):
        platform, channel_id, sender_id = self._scope(platform, channel_id, sender_id)
        state = {"expanded": sorted(set(expanded)), "collapsed": sorted(set(collapsed)), "pins": sorted(set(pins))}
        if any(len(values) > 100 for values in state.values()) or set(state["expanded"]) & set(state["collapsed"]):
            raise ValueError("Use a bounded view with distinct expansions")
        with self._transaction() as db:
            actor = self._actor(db, platform, sender_id)
            row = db.execute("SELECT * FROM awareness_views WHERE id=?", (view_id,)).fetchone()
            if row and (row["platform"] != platform or row["channel_id"] != channel_id or self._canonical(db, row["actor_id"]) != actor):
                raise PermissionError("Use your conversation view")
            current = row["revision"] if row else 0
            if current != expected_revision:
                return {"saved": False, "conflict": True, "revision": current}
            for node_id in set().union(*map(set, state.values())):
                node = db.execute("SELECT * FROM awareness_nodes WHERE id=? AND valid=1", (node_id,)).fetchone()
                if not node or not self._can_read(db, node, platform, channel_id, actor):
                    raise PermissionError("Choose current nodes from your allowed catalog")
            db.execute("INSERT OR REPLACE INTO awareness_views VALUES(?,?,?,?,?,?,?)", (view_id, platform, channel_id, actor, _json(state), current + 1, self.clock()))
            return {"saved": True, "conflict": False, "revision": current + 1, **state}

    def _task(self, db, row):
        if not row:
            return None
        task = dict(row)
        for name in ("budget", "spent", "reserved"):
            task[name + "_usd"] = task.pop(name) / 1_000_000
        task["input_versions"] = json.loads(task.pop("input_json"))
        result = task.pop("result_json")
        task["result"] = json.loads(result) if result else None
        route = db.execute("SELECT data_json FROM awareness_routes WHERE id=?", (task["route_id"],)).fetchone()
        task["route"] = json.loads(route[0]) if route else None
        task["actor_id"] = self._canonical(db, task["actor_id"])
        task["root_task_id"] = self._chain(db, task["id"])[-1]["id"]
        task["remaining_usd"] = max(0, task["budget_usd"] - task["spent_usd"] - task["reserved_usd"])
        attempts = db.execute("SELECT * FROM awareness_attempts WHERE task_id=? ORDER BY created_at DESC,id LIMIT 10", (task["id"],)).fetchall()
        task["attempts"] = [self._attempt(item) for item in reversed(attempts)]
        task["results"] = [{"attempt_id": item["id"], "status": item["status"], "result": json.loads(item["result_json"])} for item in reversed(attempts) if item["status"] in {"active", "complete"} and item["result_json"]]
        return task

    def get_task(self, task_id, platform=None, channel_id=None, sender_id=None):
        """Internal reads can omit scope; adapter reads supply all trusted IDs."""
        with self._transaction() as db:
            row = db.execute("SELECT * FROM awareness_tasks WHERE id=?", (task_id,)).fetchone()
            if platform is not None:
                platform, channel_id, sender_id = self._scope(platform, channel_id, sender_id)
                actor = self._actor(db, platform, sender_id)
                if row and not self._task_access(db, row, platform, channel_id, actor):
                    raise PermissionError("Use a task within your audience")
            return self._task(db, row)

    def _task_access(self, db, task, platform, channel_id, actor):
        return actor == self._canonical(db, task["actor_id"]) or self._can_read(db, task, platform, channel_id, actor)

    def task_for_request(self, platform, channel_id, sender_id, event_id, goal="", topic="", *,
                         persona_id="ratichat", budget_usd=0.05, parent_id=None, audience=None):
        platform, channel_id, sender_id = self._scope(platform, channel_id, sender_id)
        if not event_id:
            raise ValueError("A trusted request event ID is required")
        budget = _money(budget_usd)
        if not 0 < budget <= self.max_budget:
            raise ValueError("Use a task budget within the configured limit")
        with self._transaction() as db:
            actor = self._actor(db, platform, sender_id)
            previous = db.execute("SELECT t.* FROM awareness_tasks t JOIN awareness_task_requests r ON r.task_id=t.id WHERE r.platform=? AND r.channel_id=? AND r.event_id=?", (platform, channel_id, event_id)).fetchone()
            if previous:
                if not self._task_access(db, previous, platform, channel_id, actor):
                    raise PermissionError("Use the saved request actor")
                return self._task(db, previous)
            target = self._audience(platform, channel_id, actor, audience)
            if parent_id:
                parent = db.execute("SELECT * FROM awareness_tasks WHERE id=?", (parent_id,)).fetchone()
                if not parent or not self._task_access(db, parent, platform, channel_id, actor):
                    raise PermissionError("Use an allowed parent task")
                if parent["parent_id"] or db.execute("SELECT COUNT(*) FROM awareness_tasks WHERE parent_id=?", (parent_id,)).fetchone()[0] >= 3:
                    raise ValueError("Use at most three children under a root task")
                if budget > parent["budget"] - parent["spent"] - parent["reserved"]:
                    raise ValueError("Use the parent task's remaining budget")
                target = parent["audience_kind"], parent["audience_key"]
            identity = "task:" + uuid.uuid4().hex
            now = self.clock()
            db.execute("""INSERT INTO awareness_tasks(id,actor_id,platform,channel_id,audience_kind,audience_key,goal,topic,persona_id,parent_id,budget,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", (identity, actor, platform, channel_id, *target, str(goal)[:2000], str(topic)[:100], str(persona_id)[:100], parent_id, budget, now, now))
            db.execute("INSERT INTO awareness_task_requests VALUES(?,?,?,?)", (platform, channel_id, event_id, identity))
            self._task_node(db, identity)
            return self._task(db, db.execute("SELECT * FROM awareness_tasks WHERE id=?", (identity,)).fetchone())

    create_task = task_for_request

    def get_task_for_event(self, platform, channel_id, event_id):
        self._scope(platform, channel_id)
        with self._transaction() as db:
            row = db.execute("SELECT t.* FROM awareness_tasks t JOIN awareness_task_requests r ON r.task_id=t.id WHERE r.platform=? AND r.channel_id=? AND r.event_id=?", (platform, channel_id, event_id)).fetchone()
            return self._task(db, row)

    def list_tasks(self, platform, channel_id, sender_id, *, limit=20):
        platform, channel_id, sender_id = self._scope(platform, channel_id, sender_id)
        with self._transaction() as db:
            actor = self._actor(db, platform, sender_id)
            rows = db.execute("SELECT * FROM awareness_tasks ORDER BY updated_at DESC,id").fetchall()
            return [self._task(db, row) for row in rows if self._task_access(db, row, platform, channel_id, actor)][:max(0, min(100, int(limit)))]

    def create_child(self, parent_task_id, request_key, goal="", topic="", *, persona_id="ratichat", budget_usd=None):
        if not request_key:
            raise ValueError("Use a stable child task key")
        child_id = "task:" + _hash([parent_task_id, request_key])[:32]
        with self._transaction() as db:
            parent = db.execute("SELECT * FROM awareness_tasks WHERE id=?", (parent_task_id,)).fetchone()
            if not parent or parent["parent_id"]:
                raise ValueError("Create children under a saved root task")
            previous = db.execute("SELECT * FROM awareness_tasks WHERE id=?", (child_id,)).fetchone()
            if previous:
                return self._task(db, previous)
            if db.execute("SELECT COUNT(*) FROM awareness_tasks WHERE parent_id=?", (parent_task_id,)).fetchone()[0] >= 3:
                raise ValueError("Use at most three child tasks")
            remaining = parent["budget"] - parent["spent"] - parent["reserved"]
            budget = remaining if budget_usd is None else _money(budget_usd)
            if not 0 < budget <= remaining:
                raise ValueError("Use the root task's remaining budget")
            now = self.clock()
            db.execute("""INSERT INTO awareness_tasks(id,actor_id,platform,channel_id,audience_kind,audience_key,goal,topic,persona_id,parent_id,budget,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", (child_id, parent["actor_id"], parent["platform"], parent["channel_id"], parent["audience_kind"], parent["audience_key"], str(goal)[:2000], str(topic)[:100], str(persona_id)[:100], parent_task_id, budget, now, now))
            self._task_node(db, child_id)
            return self._task(db, db.execute("SELECT * FROM awareness_tasks WHERE id=?", (child_id,)).fetchone())

    def continue_task(self, task_id, platform, channel_id, sender_id, event_id, *, replace_task_id=None):
        platform, channel_id, sender_id = self._scope(platform, channel_id, sender_id)
        if not event_id:
            raise ValueError("A trusted continuation event ID is required")
        with self._transaction() as db:
            actor = self._actor(db, platform, sender_id)
            row = db.execute("SELECT * FROM awareness_tasks WHERE id=?", (task_id,)).fetchone()
            if not row or not self._task_access(db, row, platform, channel_id, actor):
                raise PermissionError("Continue an allowed task")
            previous = db.execute("SELECT task_id FROM awareness_task_requests WHERE platform=? AND channel_id=? AND event_id=?", (platform, channel_id, event_id)).fetchone()
            if previous and previous[0] != task_id:
                if not replace_task_id or previous[0] != replace_task_id:
                    raise ValueError("This request already refers to another task")
                placeholder = db.execute("SELECT * FROM awareness_tasks WHERE id=?", (replace_task_id,)).fetchone()
                if (not placeholder or not self._task_access(db, placeholder, platform, channel_id, actor)
                        or self._canonical(db, placeholder["actor_id"]) != actor or placeholder["parent_id"]
                        or placeholder["route_id"] or placeholder["status"] not in {"queued", "active"}
                        or db.execute("SELECT 1 FROM awareness_tasks WHERE parent_id=?", (replace_task_id,)).fetchone()
                        or db.execute("SELECT COUNT(*) FROM awareness_task_requests WHERE task_id=?", (replace_task_id,)).fetchone()[0] != 1):
                    raise PermissionError("Replace a fresh routing placeholder from this request")
                attempts = db.execute("SELECT * FROM awareness_attempts WHERE task_id=?", (replace_task_id,)).fetchall()
                for attempt in attempts:
                    route_result = json.loads(attempt["result_json"]) if attempt["result_json"] else {}
                    if not (attempt["status"] == "active" and route_result.get("kind") == "route"
                            or attempt["status"] == "reserved" and attempt["request_key"].startswith("route:")):
                        raise ValueError("Transfer only the request's routing attempts")
                for target in self._chain(db, task_id):
                    if target["spent"] + target["reserved"] + placeholder["spent"] + placeholder["reserved"] > target["budget"]:
                        raise ValueError("Use the continued task's remaining budget")
                    db.execute("UPDATE awareness_tasks SET spent=spent+?,reserved=reserved+?,updated_at=? WHERE id=?",
                               (placeholder["spent"], placeholder["reserved"], self.clock(), target["id"]))
                for attempt in attempts:
                    db.execute("UPDATE awareness_attempts SET task_id=?,request_key=? WHERE id=?",
                               (task_id, "moved:" + replace_task_id + ":" + attempt["request_key"], attempt["id"]))
                db.execute("UPDATE awareness_task_requests SET task_id=? WHERE platform=? AND channel_id=? AND event_id=?", (task_id, platform, channel_id, event_id))
                db.execute("UPDATE awareness_tasks SET status='retired',spent=0,reserved=0,result_json=NULL,updated_at=? WHERE id=?", (self.clock(), replace_task_id))
                self._invalidate(db, "node", "tasks." + replace_task_id)
                db.execute("UPDATE awareness_nodes SET valid=0,summary='',summary_version=0 WHERE id=?", ("tasks." + replace_task_id,))
            db.execute("INSERT OR IGNORE INTO awareness_task_requests VALUES(?,?,?,?)", (platform, channel_id, event_id, task_id))
            if not previous or previous[0] != task_id:
                db.execute("UPDATE awareness_tasks SET status='active',updated_at=? WHERE id=?", (self.clock(), task_id))
                self._task_node(db, task_id)
            return self._task(db, db.execute("SELECT * FROM awareness_tasks WHERE id=?", (task_id,)).fetchone())

    def _versions(self, db, versions, task=None):
        for node_id, version in (versions or {}).items():
            if task and node_id == "tasks." + task["id"]:
                raise ValueError("Use evidence nodes outside the task's own projection")
            if node_id.startswith("event:"):
                event = db.execute("SELECT * FROM awareness_messages WHERE event_key=? AND deleted=0", (node_id[len("event:"):],)).fetchone()
                if not event or event["revision"] != version:
                    return False
                if task:
                    audience = self._audience(event["platform"], event["channel_id"], event["actor_id"])
                    local_personal = task["audience_kind"] == "personal" and (event["platform"], event["channel_id"]) == (task["platform"], task["channel_id"])
                    if not local_personal and not self._can_derive(db, (task["audience_kind"], task["audience_key"]), {"audience_kind": audience[0], "audience_key": audience[1]}):
                        raise PermissionError("Keep source event inputs within the task audience")
                continue
            node = db.execute("SELECT * FROM awareness_nodes WHERE id=? AND valid=1", (node_id,)).fetchone()
            if not node or node["version"] != version:
                return False
            if task and not self._can_derive(db, (task["audience_kind"], task["audience_key"]), node):
                raise PermissionError("Keep task inputs within its audience")
        return True

    def save_route(self, task_id, route, *, input_versions=None, request_key=None):
        route = dict(route)
        if not isinstance(route.get("model"), str) or not route["model"] or len(route["model"]) > 200:
            raise ValueError("Choose a bounded model ID")
        if any(key.lower() in {"api_key", "authorization", "token", "secret"} for key in route):
            raise ValueError("Save route metadata and credential references separately")
        inputs = input_versions or {}
        request_key = request_key or _hash([route, inputs])
        with self._transaction() as db:
            task = db.execute("SELECT * FROM awareness_tasks WHERE id=?", (task_id,)).fetchone()
            if not task:
                raise ValueError("Choose a saved task")
            if not self._versions(db, inputs, task):
                return {"saved": False, "conflict": True}
            previous = db.execute("SELECT * FROM awareness_routes WHERE task_id=? AND request_key=?", (task_id, request_key)).fetchone()
            if previous:
                return {"saved": True, "conflict": False, "id": previous["id"], "route": json.loads(previous["data_json"])}
            identity = "route:" + uuid.uuid4().hex
            db.execute("INSERT INTO awareness_routes VALUES(?,?,?,?,?,?)", (identity, task_id, request_key, _json(route), _json(inputs), self.clock()))
            db.execute("UPDATE awareness_tasks SET route_id=?,input_json=?,topic=?,persona_id=?,status='ready',updated_at=? WHERE id=?",
                (identity, _json(inputs), str(route.get("topic", task["topic"]))[:100],
                 str(route.get("persona", task["persona_id"]))[:100], self.clock(), task_id))
            self._task_node(db, task_id)
            return {"saved": True, "conflict": False, "id": identity, "route": route}

    @staticmethod
    def _chain(db, task_id):
        chain, seen = [], set()
        while task_id:
            if task_id in seen:
                raise ValueError("Use an acyclic parent task")
            seen.add(task_id)
            task = db.execute("SELECT * FROM awareness_tasks WHERE id=?", (task_id,)).fetchone()
            if not task:
                raise ValueError("Choose a saved task")
            chain.append(task)
            task_id = task["parent_id"]
        return chain

    def reserve_attempt(self, task_id, estimated_cost_usd, request_key, *, route_id=None, input_versions=None):
        amount = _money(estimated_cost_usd)
        if amount <= 0 or not request_key:
            raise ValueError("Reserve a positive amount with a stable attempt key")
        with self._transaction() as db:
            previous = db.execute("SELECT * FROM awareness_attempts WHERE task_id=? AND request_key=?", (task_id, request_key)).fetchone()
            if previous:
                return self._attempt(previous)
            chain = self._chain(db, task_id)
            task = chain[0]
            route_id = route_id or task["route_id"]
            if route_id and not db.execute("SELECT 1 FROM awareness_routes WHERE id=? AND task_id=?", (route_id, task_id)).fetchone():
                raise ValueError("Use this task's saved route")
            inputs = input_versions if input_versions is not None else json.loads(task["input_json"])
            if not self._versions(db, inputs, task):
                return {"reserved": False, "conflict": True}
            if any(row["spent"] + row["reserved"] + amount > row["budget"] for row in chain):
                raise ValueError("Use the remaining task budget")
            identity = "attempt:" + uuid.uuid4().hex
            now = self.clock()
            db.execute("INSERT INTO awareness_attempts VALUES(?,?,?,?,?,?,0,?,NULL,?,?)", (identity, task_id, request_key, route_id, "reserved", amount, _json(inputs), now, now))
            for row in chain:
                db.execute("UPDATE awareness_tasks SET reserved=reserved+?,updated_at=? WHERE id=?", (amount, now, row["id"]))
            return self._attempt(db.execute("SELECT * FROM awareness_attempts WHERE id=?", (identity,)).fetchone())

    @staticmethod
    def _attempt(row):
        return {"id": row["id"], "task_id": row["task_id"], "request_key": row["request_key"],
                "route_id": row["route_id"], "status": row["status"], "reserved": True,
                "reserved_usd": row["reserved"] / 1_000_000, "cost_usd": row["cost"] / 1_000_000,
                "input_versions": json.loads(row["input_json"])}

    def record_result(self, task_id, result, *, attempt_id=None, cost_usd=0, input_versions=None, success=True, status="complete"):
        if status not in {"complete", "active"}:
            raise ValueError("Choose complete or active result status")
        cost = _money(cost_usd)
        encoded = _json(result)
        with self._transaction() as db:
            if attempt_id is None:
                if cost != 0:
                    raise ValueError("Paid results need a saved reservation")
                task = db.execute("SELECT * FROM awareness_tasks WHERE id=?", (task_id,)).fetchone()
                if not task:
                    raise ValueError("Choose a saved task")
                inputs = input_versions if input_versions is not None else json.loads(task["input_json"])
                free_key = "free:" + _hash([result, inputs, status, success])
                previous = db.execute("SELECT id FROM awareness_attempts WHERE task_id=? AND request_key=?", (task_id, free_key)).fetchone()
                attempt_id = previous[0] if previous else "result:" + uuid.uuid4().hex
                if not previous:
                    now = self.clock()
                    db.execute("INSERT INTO awareness_attempts VALUES(?,?,?,?,?,0,0,?,NULL,?,?)", (attempt_id, task_id, free_key, task["route_id"], "reserved", _json(inputs), now, now))
            attempt = db.execute("SELECT * FROM awareness_attempts WHERE id=? AND task_id=?", (attempt_id, task_id)).fetchone()
            if not attempt:
                raise ValueError("Choose this task's reserved attempt")
            if attempt["status"] != "reserved":
                return {"saved": True, "duplicate": True, "conflict": attempt["status"] == "stale", "status": attempt["status"], "accepted": attempt["status"] in {"active", "complete"}}
            chain = self._chain(db, task_id)
            task = chain[0]
            saved_inputs = json.loads(attempt["input_json"])
            if input_versions is not None and input_versions != saved_inputs:
                raise ValueError("Use the attempt's saved input versions")
            current = self._versions(db, saved_inputs, task)
            over_budget = any(row["spent"] + cost + row["reserved"] - attempt["reserved"] > row["budget"] for row in chain)
            status = "budget_exceeded" if over_budget else "stale" if not current else status if success else "failed"
            now = self.clock()
            db.execute("UPDATE awareness_attempts SET status=?,cost=?,result_json=?,updated_at=? WHERE id=?", (status, cost, encoded, now, attempt_id))
            for row in chain:
                db.execute("UPDATE awareness_tasks SET spent=spent+?,reserved=reserved-?,updated_at=? WHERE id=?", (cost, attempt["reserved"], now, row["id"]))
            db.execute("UPDATE awareness_tasks SET status=?,result_json=?,updated_at=? WHERE id=?",
                       ("needs_refresh" if status == "stale" else status, encoded if status in {"complete", "active"} else None, now, task_id))
            self._task_node(db, task_id)
            return {"saved": True, "duplicate": False, "conflict": status == "stale", "status": status,
                    "budget_exceeded": over_budget, "accepted": status in {"complete", "active"}}

    def _task_node(self, db, task_id):
        task = db.execute("SELECT * FROM awareness_tasks WHERE id=?", (task_id,)).fetchone()
        data = {"task_id": task_id, "goal": task["goal"], "topic": task["topic"], "persona_id": task["persona_id"],
                "status": task["status"], "result": json.loads(task["result_json"]) if task["result_json"] else None}
        refs = [{"kind": "event" if node.startswith("event:") else "node", "id": node[len("event:"):] if node.startswith("event:") else node, "version": version} for node, version in json.loads(task["input_json"]).items()]
        self._write_node(db, "tasks." + task_id, "task", data, (task["audience_kind"], task["audience_key"]), refs)

    def begin_link(self, platform, channel_id, sender_id, target_platform, target_account_id):
        """Stage one: the source account requests an exact target account."""
        platform, _, sender_id = self._scope(platform, channel_id, sender_id)
        if target_platform not in self.allowed or not target_account_id:
            raise ValueError("Choose a supported target account")
        with self._transaction() as db:
            actor = self._actor(db, platform, sender_id)
            identity, token = uuid.uuid4().hex, secrets.token_urlsafe(24)
            expires = self.clock() + 600
            db.execute("INSERT INTO awareness_links VALUES(?,?,?,?,?,?,NULL,?,'requested',?,?)",
                       (identity, actor, platform, sender_id, target_platform, str(target_account_id), _hash(token), expires, self.clock()))
            return {"link_id": identity, "code": token, "expires_at": expires, "stage": "requested"}

    def prove_link(self, link_id, code, platform, channel_id, sender_id):
        """Stage two: the trusted target account presents the source's code."""
        platform, _, sender_id = self._scope(platform, channel_id, sender_id)
        with self._transaction() as db:
            row = db.execute("SELECT * FROM awareness_links WHERE id=?", (link_id,)).fetchone()
            if not row or row["expires_at"] <= self.clock() or row["stage"] not in {"requested", "proved"} or (platform, sender_id) != (row["target_platform"], row["target_account"]) or not secrets.compare_digest(_hash(code), row["token_hash"]):
                raise PermissionError("Use the current code from the target account")
            actor = self._actor(db, platform, sender_id)
            db.execute("UPDATE awareness_links SET stage='proved',target_actor=? WHERE id=?", (actor, link_id))
            return {"link_id": link_id, "stage": "proved"}

    def confirm_link(self, link_id, platform, channel_id, sender_id):
        """Stage three: the original source account confirms the proved target."""
        platform, _, sender_id = self._scope(platform, channel_id, sender_id)
        with self._transaction() as db:
            row = db.execute("SELECT * FROM awareness_links WHERE id=?", (link_id,)).fetchone()
            if not row or row["expires_at"] <= self.clock() or row["stage"] not in {"proved", "complete"} or (platform, sender_id) != (row["source_platform"], row["source_account"]):
                raise PermissionError("Confirm from the original source account")
            source, target = self._canonical(db, row["source_actor"]), self._canonical(db, row["target_actor"])
            if target != source:
                db.execute("UPDATE awareness_actors SET canonical_id=? WHERE id=?", (source, target))
            db.execute("UPDATE awareness_links SET stage='complete',token_hash='' WHERE id=?", (link_id,))
            return {"link_id": link_id, "stage": "complete", "actor_id": source}

    def cleanup(self, *, raw_retention_days=7):
        """Remove old raw edit bodies while retaining current projections and tombstones."""
        cutoff = self.clock() - max(0, float(raw_retention_days)) * 86400
        with self._transaction() as db:
            events = db.execute("DELETE FROM awareness_events WHERE created_at<? AND revision<(SELECT revision FROM awareness_messages WHERE awareness_messages.event_key=awareness_events.event_key)", (cutoff,)).rowcount
            links = db.execute("DELETE FROM awareness_links WHERE expires_at<=? AND stage!='complete'", (self.clock(),)).rowcount
            return {"events": events, "links": links}

    def close(self):
        with self._lock:
            self._db.close()
