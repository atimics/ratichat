"""Saved research turns, source results, and reply receipts for one channel."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time
from typing import Any, Callable
import uuid


class ResearchStore:
    """Keep accepted requests and reply delivery separate.

    Each channel processes its active turns in intake order. Known failures can
    retry. An expired send stays uncertain so its reply can be checked before
    another send. Later requests can continue while that receipt is checked.
    """

    def __init__(
        self,
        db_path: str,
        *,
        clock: Callable[[], float] = time.time,
        retention_days: float = 7,
        max_attempts: int = 3,
    ):
        self.clock = clock
        self.retention_seconds = max(0.0, float(retention_days)) * 86400
        self.max_attempts = max(1, int(max_attempts))
        self._lock = threading.RLock()
        if str(db_path) != ":memory:":
            Path(db_path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(db_path), isolation_level=None, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS research_turns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                snapshot_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                completed_at REAL,
                attempts INTEGER NOT NULL DEFAULT 0,
                delivery_attempts INTEGER NOT NULL DEFAULT 0,
                lease_token TEXT,
                delivery_token TEXT,
                lease_until REAL,
                retry_at REAL NOT NULL DEFAULT 0,
                reply TEXT,
                source_results_json TEXT NOT NULL DEFAULT '[]',
                receipt_json TEXT,
                last_error TEXT,
                UNIQUE(platform, channel_id, event_id)
            );
            CREATE INDEX IF NOT EXISTS research_turns_channel
                ON research_turns(platform, channel_id, id);
            CREATE INDEX IF NOT EXISTS research_turns_status ON research_turns(status, retry_at);
            CREATE TABLE IF NOT EXISTS research_event_receipts (
                platform TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                PRIMARY KEY(platform, channel_id, event_id)
            );
            CREATE TABLE IF NOT EXISTS research_legacy_receipts (
                channel_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                PRIMARY KEY(channel_id, event_id)
            );
            CREATE TABLE IF NOT EXISTS research_sources (
                platform TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                tool TEXT NOT NULL,
                params_hash TEXT NOT NULL,
                params_json TEXT NOT NULL,
                result_json TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                trust_label TEXT NOT NULL,
                fetched_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                PRIMARY KEY(platform, channel_id, tool, params_hash)
            );
            CREATE INDEX IF NOT EXISTS research_sources_expiry ON research_sources(expires_at);
        """)
        with self._transaction() as db:
            old = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='node_request_receipts'").fetchone()
            if old:
                db.execute("INSERT OR IGNORE INTO research_legacy_receipts SELECT channel_id,event_id FROM node_request_receipts")

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

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)

    @staticmethod
    def _scope(platform: str, channel_id: str) -> tuple[str, str]:
        platform, channel_id = str(platform).strip(), str(channel_id).strip()
        if not platform or not channel_id:
            raise ValueError("Platform and channel are required")
        return platform, channel_id

    @staticmethod
    def _turn(row) -> dict | None:
        if row is None:
            return None
        value = dict(row)
        value["snapshot"] = json.loads(value.pop("snapshot_json"))
        value["sources"] = json.loads(value.pop("source_results_json"))
        receipt = value.pop("receipt_json")
        value["receipt"] = json.loads(receipt) if receipt else None
        return value

    @staticmethod
    def _source(row) -> dict | None:
        if row is None:
            return None
        value = dict(row)
        value["params"] = json.loads(value.pop("params_json"))
        value["result"] = json.loads(value.pop("result_json"))
        return value

    def enqueue(self, platform: str, channel_id: str, event_id: str, snapshot: dict, *, kind: str = "request") -> dict:
        """Save the accepted snapshot once. Duplicate intake returns that snapshot."""
        platform, channel_id = self._scope(platform, channel_id)
        event_id = str(event_id).strip()
        if not event_id or not isinstance(snapshot, dict):
            raise ValueError("An event ID and a request snapshot are required")
        encoded = self._json(snapshot)
        now = self.clock()
        with self._transaction() as db:
            legacy = db.execute("SELECT 1 FROM research_legacy_receipts WHERE channel_id=? AND event_id=?", (channel_id, event_id)).fetchone()
            if not legacy:
                legacy = db.execute("SELECT 1 FROM research_event_receipts WHERE platform=? AND channel_id=? AND event_id=?", (platform, channel_id, event_id)).fetchone()
            status = "legacy" if legacy else "queued"
            db.execute("""INSERT OR IGNORE INTO research_turns
                (platform,channel_id,event_id,kind,snapshot_json,status,created_at,updated_at,completed_at)
                VALUES(?,?,?,?,?,?,?,?,?)""", (platform, channel_id, event_id, str(kind), encoded, status, now, now, now if legacy else None))
            return self._turn(db.execute("SELECT * FROM research_turns WHERE platform=? AND channel_id=? AND event_id=?", (platform, channel_id, event_id)).fetchone())

    def get(self, turn_id: int) -> dict | None:
        with self._lock:
            return self._turn(self._db.execute("SELECT * FROM research_turns WHERE id=?", (turn_id,)).fetchone())

    def _recover_expired(self, db, now: float):
        db.execute("""UPDATE research_turns SET
            status=CASE WHEN attempts<? THEN 'queued' ELSE 'failed' END,
            lease_token=NULL,lease_until=NULL,retry_at=0,updated_at=?,
            completed_at=CASE WHEN attempts>=? THEN ? ELSE NULL END,
            last_error='Processing lease expired'
            WHERE status='running' AND lease_until<=?""", (self.max_attempts, now, self.max_attempts, now, now))
        db.execute("""UPDATE research_turns SET status='uncertain',delivery_token=NULL,
            lease_until=NULL,completed_at=?,updated_at=?,last_error='Delivery receipt needs a check'
            WHERE status='sending' AND lease_until<=?""", (now, now, now))
        db.execute("""UPDATE research_turns SET status='uncertain',lease_token=NULL,
            lease_until=NULL,completed_at=?,updated_at=?,last_error='Action receipt needs a check'
            WHERE status='effect_started' AND lease_until<=?""", (now, now, now))

    @staticmethod
    def _first_active(db, platform: str, channel_id: str):
        return db.execute("""SELECT * FROM research_turns WHERE platform=? AND channel_id=?
            AND status IN ('queued','running','effect_started','ready','sending') ORDER BY id LIMIT 1""", (platform, channel_id)).fetchone()

    def claim(self, platform: str, channel_id: str, *, lease_seconds: float = 120) -> dict | None:
        """Claim the oldest request with a new token and a limited lease."""
        platform, channel_id = self._scope(platform, channel_id)
        now = self.clock()
        with self._transaction() as db:
            self._recover_expired(db, now)
            row = self._first_active(db, platform, channel_id)
            if row is None or row["status"] != "queued" or row["retry_at"] > now:
                return None
            token = uuid.uuid4().hex
            db.execute("""UPDATE research_turns SET status='running',attempts=attempts+1,
                lease_token=?,lease_until=?,updated_at=? WHERE id=?""", (token, now + max(1, lease_seconds), now, row["id"]))
            return self._turn(db.execute("SELECT * FROM research_turns WHERE id=?", (row["id"],)).fetchone())

    def heartbeat(self, turn_id: int, token: str, *, lease_seconds: float = 120) -> bool:
        now = self.clock()
        with self._transaction() as db:
            changed = db.execute("""UPDATE research_turns SET lease_until=?,updated_at=? WHERE id=?
                AND lease_until>? AND ((status IN ('running','effect_started') AND lease_token=?) OR (status='sending' AND delivery_token=?))""",
                (now + max(1, lease_seconds), now, turn_id, now, token, token)).rowcount
            return bool(changed)

    def mark_effect(self, turn_id: int, lease_token: str) -> bool:
        """Save the external-action boundary before a management action starts."""
        now = self.clock()
        with self._transaction() as db:
            return bool(db.execute("""UPDATE research_turns SET status='effect_started',updated_at=?
                WHERE id=? AND status='running' AND lease_token=? AND lease_until>?""",
                (now, turn_id, lease_token, now)).rowcount)

    def finish_without_reply(self, turn_id: int, lease_token: str) -> bool:
        now = self.clock()
        with self._transaction() as db:
            return bool(db.execute("""UPDATE research_turns SET status='complete',lease_token=NULL,
                lease_until=NULL,completed_at=?,updated_at=?
                WHERE id=? AND status='running' AND lease_token=? AND lease_until>?""",
                (now, now, turn_id, lease_token, now)).rowcount)

    complete = finish_without_reply

    def ready(self, turn_id: int, lease_token: str, reply: str, *, sources: list | None = None) -> bool:
        """Save the finished reply before any delivery starts."""
        reply = str(reply).strip()
        if not reply:
            raise ValueError("A prepared reply is required")
        encoded = self._json(sources or [])
        now = self.clock()
        with self._transaction() as db:
            changed = db.execute("""UPDATE research_turns SET status='ready',reply=?,source_results_json=?,
                lease_token=NULL,lease_until=NULL,retry_at=0,last_error=NULL,updated_at=?
                WHERE id=? AND status IN ('running','effect_started') AND lease_token=? AND lease_until>?""",
                (reply, encoded, now, turn_id, lease_token, now)).rowcount
            return bool(changed)

    def claim_delivery(self, platform: str, channel_id: str, *, lease_seconds: float = 120) -> dict | None:
        """Claim the prepared reply. A send has its own lease token."""
        platform, channel_id = self._scope(platform, channel_id)
        now = self.clock()
        with self._transaction() as db:
            self._recover_expired(db, now)
            row = self._first_active(db, platform, channel_id)
            if row is None or row["status"] != "ready" or row["retry_at"] > now:
                return None
            token = uuid.uuid4().hex
            db.execute("""UPDATE research_turns SET status='sending',delivery_attempts=delivery_attempts+1,
                delivery_token=?,lease_until=?,updated_at=? WHERE id=?""", (token, now + max(1, lease_seconds), now, row["id"]))
            return self._turn(db.execute("SELECT * FROM research_turns WHERE id=?", (row["id"],)).fetchone())

    def sent(self, turn_id: int, delivery_token: str, receipt: dict) -> bool:
        if not isinstance(receipt, dict) or not receipt:
            raise ValueError("A delivery receipt is required")
        encoded = self._json(receipt)
        now = self.clock()
        with self._transaction() as db:
            changed = db.execute("""UPDATE research_turns SET status='sent',receipt_json=?,
                delivery_token=NULL,lease_until=NULL,completed_at=?,updated_at=?,last_error=NULL
                WHERE id=? AND status='sending' AND delivery_token=? AND lease_until>?""",
                (encoded, now, now, turn_id, delivery_token, now)).rowcount
            return bool(changed)

    def fail(self, turn_id: int, token: str, error: str, *, retry_after: float = 0,
             phase: str = "processing", retryable: bool = True) -> bool:
        """Retry a known failure within its attempt limit, or finish it as failed."""
        if phase not in {"processing", "delivery"}:
            raise ValueError("Choose processing or delivery")
        now = self.clock()
        token_column, attempts_column = ("lease_token", "attempts") if phase == "processing" else ("delivery_token", "delivery_attempts")
        with self._transaction() as db:
            states = ("running", "effect_started") if phase == "processing" else ("sending", "sending")
            row = db.execute(f"SELECT * FROM research_turns WHERE id=? AND status IN (?,?) AND {token_column}=? AND lease_until>?", (turn_id, *states, token, now)).fetchone()
            if row is None:
                return False
            retry = bool(retryable) and row[attempts_column] < self.max_attempts
            next_state = ("queued" if phase == "processing" else "ready") if retry else "failed"
            if row["status"] == "effect_started":
                next_state, retry = "uncertain", False
            db.execute("""UPDATE research_turns SET status=?,lease_token=NULL,delivery_token=NULL,
                lease_until=NULL,retry_at=?,updated_at=?,completed_at=?,last_error=? WHERE id=?""",
                (next_state, now + max(0, retry_after) if retry else 0, now, None if retry else now, str(error)[:1000], turn_id))
            return True

    def uncertain(self, turn_id: int, delivery_token: str, error: str = "Delivery receipt needs a check") -> bool:
        now = self.clock()
        with self._transaction() as db:
            changed = db.execute("""UPDATE research_turns SET status='uncertain',delivery_token=NULL,
                lease_until=NULL,completed_at=?,updated_at=?,last_error=?
                WHERE id=? AND status='sending' AND delivery_token=? AND lease_until>?""",
                (now, now, str(error)[:1000], turn_id, delivery_token, now)).rowcount
            return bool(changed)

    def uncertain_deliveries(self) -> list[dict]:
        with self._transaction() as db:
            self._recover_expired(db, self.clock())
            rows = db.execute("SELECT * FROM research_turns WHERE status='uncertain' AND reply IS NOT NULL ORDER BY id LIMIT 100").fetchall()
            return [self._turn(row) for row in rows]

    def resolve_delivery(self, turn_id: int, receipt: dict) -> bool:
        return self.reconcile(turn_id, receipt=receipt)

    def state_counts(self) -> dict:
        with self._lock:
            return {row["status"]: row["count"] for row in self._db.execute("SELECT status,COUNT(*) AS count FROM research_turns GROUP BY status")}

    def reconcile(self, turn_id: int, *, receipt: dict | None = None, retry: bool = False) -> bool:
        """Settle an uncertain send after checking the remote channel.

        Supply its receipt when the reply exists. A confirmed missing reply may
        retry when its delivery attempt budget has room.
        """
        if receipt is not None and (not isinstance(receipt, dict) or not receipt):
            raise ValueError("A delivery receipt is required")
        now = self.clock()
        with self._transaction() as db:
            row = db.execute("SELECT * FROM research_turns WHERE id=? AND status='uncertain' AND reply IS NOT NULL", (turn_id,)).fetchone()
            if row is None:
                return False
            status = "sent" if receipt else ("ready" if retry and row["delivery_attempts"] < self.max_attempts else "failed")
            db.execute("""UPDATE research_turns SET status=?,receipt_json=?,completed_at=?,updated_at=?,
                retry_at=0,last_error=? WHERE id=?""", (status, self._json(receipt) if receipt else None,
                None if status == "ready" else now, now, None if status == "sent" else "Delivery checked", turn_id))
            return True

    def pending_channels(self, *, include_processing: bool = True) -> list[dict]:
        now = self.clock()
        with self._transaction() as db:
            self._recover_expired(db, now)
            rows = db.execute("""SELECT t.platform,t.channel_id FROM research_turns t
                WHERE t.status IN ('queued','ready') AND t.retry_at<=?
                AND (t.status='ready' OR t.kind='command' OR ?)
                AND t.id=(SELECT MIN(first.id) FROM research_turns first
                    WHERE first.platform=t.platform AND first.channel_id=t.channel_id
                    AND first.status IN ('queued','running','effect_started','ready','sending')) ORDER BY t.id""", (now, include_processing)).fetchall()
            return [dict(row) for row in rows]

    def cached_source(self, platform: str, channel_id: str, tool: str, params: dict) -> dict | None:
        platform, channel_id = self._scope(platform, channel_id)
        params_hash = hashlib.sha256(self._json(params).encode()).hexdigest()
        with self._lock:
            return self._source(self._db.execute("""SELECT * FROM research_sources
                WHERE platform=? AND channel_id=? AND tool=? AND params_hash=? AND expires_at>?""",
                (platform, channel_id, tool, params_hash, self.clock())).fetchone())

    def save_source(self, platform: str, channel_id: str, tool: str, params: dict, result: dict,
                    *, ttl_seconds: float = 300, trust_label: str = "untrusted") -> dict:
        platform, channel_id = self._scope(platform, channel_id)
        params_json, result_json = self._json(params), self._json(result)
        params_hash = hashlib.sha256(params_json.encode()).hexdigest()
        content_hash = hashlib.sha256(result_json.encode()).hexdigest()
        now = self.clock()
        with self._transaction() as db:
            db.execute("""INSERT INTO research_sources
                (platform,channel_id,tool,params_hash,params_json,result_json,content_hash,trust_label,fetched_at,expires_at)
                VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(platform,channel_id,tool,params_hash)
                DO UPDATE SET result_json=excluded.result_json,content_hash=excluded.content_hash,
                trust_label=excluded.trust_label,fetched_at=excluded.fetched_at,expires_at=excluded.expires_at""",
                (platform, channel_id, str(tool), params_hash, params_json, result_json, content_hash,
                 str(trust_label)[:100], now, now + max(0, ttl_seconds)))
            return self._source(db.execute("""SELECT * FROM research_sources WHERE
                platform=? AND channel_id=? AND tool=? AND params_hash=?""", (platform, channel_id, str(tool), params_hash)).fetchone())

    @classmethod
    def _bounded(cls, value: Any, string_limit: int = 800) -> Any:
        if isinstance(value, str):
            return value[:string_limit]
        if isinstance(value, dict):
            return {str(k): cls._bounded(v, string_limit) for k, v in list(value.items())[:30]}
        if isinstance(value, list):
            return [cls._bounded(v, string_limit) for v in value[-7:]]
        return value

    def memory_node(self, platform: str, channel_id: str, *, limit: int = 5, max_chars: int = 6000) -> dict:
        """Return a small reference node with confirmed complete turns in this channel."""
        platform, channel_id = self._scope(platform, channel_id)
        now = self.clock()
        cutoff = now - self.retention_seconds
        max_chars = max(256, int(max_chars))
        with self._lock:
            rows = self._db.execute("""SELECT * FROM research_turns WHERE platform=? AND channel_id=?
                AND status='sent' AND completed_at>=? ORDER BY id DESC LIMIT ?""", (platform, channel_id, cutoff, max(0, min(20, int(limit))))).fetchall()
            sources = self._db.execute("""SELECT * FROM research_sources WHERE platform=? AND channel_id=?
                AND expires_at>? AND fetched_at>=? ORDER BY fetched_at DESC LIMIT 3""", (platform, channel_id, now, cutoff)).fetchall()
        node = {"kind": "memory", "platform": platform, "channel_id": channel_id,
                "trust_label": "reference", "turns": [], "sources": []}
        for row in rows:
            turn = self._turn(row)
            request = turn["snapshot"].get("source", turn["snapshot"])
            entry = self._bounded({"event_id": turn["event_id"], "request": request,
                "reply": turn["reply"], "receipt": turn["receipt"], "sources": turn["sources"],
                "completed_at": turn["completed_at"]})
            node["turns"].insert(0, entry)
            if len(self._json(node)) > max_chars:
                node["turns"].pop(0)
        for row in sources:
            source = self._source(row)
            entry = self._bounded({k: source[k] for k in ("tool", "params", "result", "content_hash", "trust_label", "fetched_at", "expires_at")})
            node["sources"].append(entry)
            if len(self._json(node)) > max_chars:
                node["sources"].pop()
        return node

    def cleanup(self) -> dict:
        """Expire source results and old settled turn content. Keep pending work."""
        now = self.clock()
        with self._transaction() as db:
            self._recover_expired(db, now)
            db.execute("""INSERT OR IGNORE INTO research_event_receipts
                SELECT platform,channel_id,event_id FROM research_turns
                WHERE status IN ('sent','failed','legacy','complete') AND completed_at<?""", (now - self.retention_seconds,))
            turns = db.execute("""DELETE FROM research_turns WHERE status IN ('sent','failed','legacy','complete')
                AND completed_at<?""", (now - self.retention_seconds,)).rowcount
            sources = db.execute("DELETE FROM research_sources WHERE expires_at<=? OR fetched_at<?", (now, now - self.retention_seconds)).rowcount
            return {"turns": turns, "sources": sources}

    def close(self):
        with self._lock:
            self._db.close()
