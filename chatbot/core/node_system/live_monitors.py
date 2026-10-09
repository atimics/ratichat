"""Saved blockchain checks with the feed watch delivery and restart guarantees."""

import asyncio
import hashlib
import json
import re
import sqlite3

from ...tools.onchain_tools import LIMITS, OnchainReader, normalize_targets
from .source_watches import WatchStore, SourceWatchService, _safe_text


def public_monitor(row):
    config = json.loads(row["config_json"])
    state = json.loads(row["check_state"])
    return {"monitor_id": row["id"], **config, "interval_minutes": row["interval_seconds"] // 60,
            "checked_at": row["fetched_at"], "next_due": row["next_due"],
            "delivery_status": row["pending_status"] or "idle",
            "coverage": [{k: v for k, v in stream.items() if k != "keys"} for stream in state.values()],
            "recent_events": json.loads(row["last_items"])[:20], "limits": LIMITS}


class MonitorStore(WatchStore):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, kind="chain")

    def tool_results(self, scope):
        return [r for r in super().tool_results(scope) if r["tool"] in {"create_live_monitor", "stop_live_monitor"}]

    def execute_mutation(self, name, params, scope, is_owner=False):
        scope = self._check_scope(scope, is_owner, write=True)
        if not scope["event_id"]:
            raise ValueError("Use a saved owner request to change a monitor.")
        if name == "create_live_monitor":
            targets = normalize_targets(params.get("targets"))
            minutes = params.get("interval_minutes", 5)
            if type(minutes) is not int or not 1 <= minutes <= 1440:
                raise ValueError("Choose a check interval from 1 to 1440 minutes.")
            label = params.get("label", "Address activity")
            if not isinstance(label, str) or not label.strip() or len(label) > 120:
                raise ValueError("Give this monitor a short label of up to 120 characters.")
            params = {"targets": targets, "label": label.strip(), "interval_minutes": minutes}
        elif name == "stop_live_monitor":
            if not re.fullmatch(r"[a-f0-9]{12}", str(params.get("monitor_id", ""))):
                raise ValueError("Choose a monitor ID from this channel.")
        else:
            raise ValueError("Choose a listed monitor change tool.")
        digest = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()
        key = (scope["channel_type"], scope["channel_id"], scope["event_id"], name, digest)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute("SELECT result_json FROM source_watch_tool_receipts WHERE channel_type=? AND channel_id=? AND event_id=? AND tool_name=? AND params_hash=?", key).fetchone()
            if prior:
                return json.loads(prior[0])
            if name == "create_live_monitor":
                identity = "chain:" + hashlib.sha256(json.dumps(targets, sort_keys=True).encode()).hexdigest()
                watch, created = self._create(db, scope, identity, minutes * 60)
                if created:
                    config = {"targets": targets, "label": label.strip()}
                    db.execute("UPDATE source_watches SET config_json=? WHERE id=?", (json.dumps(config), watch["id"]))
                    watch["config_json"] = json.dumps(config)
                result = {"status": "success", "created": created, "monitor": public_monitor(watch),
                          "message": "The monitor is saved. The first successful check saves a quiet baseline. Later checks post new activity and coverage changes here."}
            else:
                removed = db.execute("DELETE FROM source_watches WHERE id=? AND channel_type=? AND channel_id=? AND kind='chain'",
                    (params["monitor_id"], scope["channel_type"], scope["channel_id"])).rowcount == 1
                result = {"status": "success" if removed else "failure", "removed": removed,
                          "monitor_id": params["monitor_id"], "message": "The monitor has stopped." if removed else "Choose a saved monitor from this channel."}
            self._save_tool_receipt(db, key, scope["sender_id"], result)
            return result

    def save_check(self, watch, result):
        """Baseline each stream separately and prepare one receipt for new transactions."""
        now = self.now()
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM source_watches WHERE id=? AND lease_token=? AND lease_until>? AND kind='chain'",
                             (watch["id"], watch["lease_token"], now)).fetchone()
            if row is None:
                return None
            previous = json.loads(row["check_state"])
            state, baseline_keys, active_keys, notices = {}, set(), set(), []
            for stream in result["streams"]:
                identity = stream["id"]
                prior = previous.get(identity, {})
                status = stream["status"]
                good = status in {"ok", "limited"}
                failures = 0 if status == "ok" else prior.get("failures", 0) + 1
                reported = prior.get("reported", "ok")
                if failures >= 2 and status != reported:
                    notices.append(f"{stream['network']} {stream['stream']}: {stream.get('message') or stream.get('coverage')}")
                    reported = status
                elif status == "ok" and reported != "ok":
                    notices.append(f"{stream['network']} {stream['stream']}: coverage recovered")
                    reported = "ok"
                keys = stream.get("keys", [])
                if good:
                    (active_keys if prior.get("baseline") else baseline_keys).update(keys)
                state[identity] = {**{k: v for k, v in stream.items() if k != "events"},
                    # Keep the last complete checkpoint when a busy stream hits
                    # the page limit. Later polls must retain the visible gap.
                    "keys": prior.get("keys", []) if status == "error" or (status == "limited" and prior.get("baseline")) else keys,
                    "baseline": good or prior.get("baseline", False), "failures": failures, "reported": reported,
                    "last_success_at": now if good else prior.get("last_success_at")}
            db.executemany("INSERT OR IGNORE INTO source_watch_items VALUES (?,?)", [(watch["id"], key) for key in baseline_keys])
            seen = {r[0] for r in db.execute("SELECT item_key FROM source_watch_items WHERE watch_id=?", (watch["id"],))}
            events = [event for event in result["events"] if event["key"] in active_keys and event["key"] not in seen]
            db.execute("UPDATE source_watches SET baseline=?,check_state=?,last_items=?,fetched_at=?,next_due=? WHERE id=?",
                (int(any(s["baseline"] for s in state.values())), json.dumps(state), json.dumps(result["events"]), now, now + watch["interval_seconds"], watch["id"]))
            if not events and not notices:
                db.execute("UPDATE source_watches SET lease_token=NULL,lease_until=0 WHERE id=?", (watch["id"],))
                return None
            label = json.loads(row["config_json"])["label"]
            lines = [f"{_safe_text(label, 120)} · monitor {watch['id']}"]
            if events:
                lines.append(f"{len(events)} new confirmed transaction(s) observed.")
                for event in events[:8]:
                    line = f"• [{event['network']} {event['transaction_hash'][:12]}…]({event['url']})"
                    if len("\n".join(lines)) + len(line) < 1400:
                        lines.append(line)
                lines.append("These are observed movements. Theft attribution and loss totals need separate evidence.")
            if notices:
                lines.append("Coverage update: " + _safe_text("; ".join(sorted(set(notices))), 350))
            keys = sorted(e["key"] for e in events)
            delivery_key = hashlib.sha256((watch["id"] + json.dumps([keys, notices, now])).encode()).hexdigest()
            text = "\n".join(lines) + "\nReceipt: " + delivery_key[:12]
            db.execute("INSERT INTO source_watch_deliveries(delivery_key,watch_id,status,text,item_keys,prepared_at) VALUES (?,?,?,?,?,?)",
                       (delivery_key, watch["id"], "pending", text, json.dumps(keys), now))
            db.execute("UPDATE source_watches SET pending_text=?,pending_keys=?,delivery_key=?,pending_status='pending',next_due=? WHERE id=?",
                       (text, json.dumps(keys), delivery_key, now, watch["id"]))
            return dict(db.execute("SELECT * FROM source_watches WHERE id=?", (watch["id"],)).fetchone())


class LiveMonitorService(SourceWatchService):
    def __init__(self, store, reader=None, **kwargs):
        super().__init__(store, **kwargs)
        self.reader = reader or OnchainReader()

    async def execute_tool(self, name, params, scope, is_owner=False):
        try:
            if name in {"create_live_monitor", "stop_live_monitor"}:
                return self.store.execute_mutation(name, params, scope, is_owner)
            monitors = self.store.list(scope)
            return {"status": "success", "monitors": [public_monitor(m) for m in monitors]}
        except (ValueError, TypeError, sqlite3.Error) as error:
            return {"status": "failure", "message": str(error) if isinstance(error, ValueError) else "Monitor storage needs another attempt."}

    async def poll_due(self):
        for _ in range(5):
            watch = self.store.claim(lease_seconds=180)
            if watch is None:
                break
            if not self.store.reserve_lookup(watch, self.daily_lookup_budget):
                self.store.defer(watch, max(60, (int(self.now()) // 86400 + 1) * 86400 - self.now()))
                continue
            try:
                result = await asyncio.wait_for(self.reader.check(json.loads(watch["config_json"])["targets"],
                    json.loads(watch["check_state"])), timeout=150)
                prepared = self.store.save_check(watch, result)
            except asyncio.CancelledError:
                self.store.defer(watch)
                raise
            except Exception:
                self.store.defer(watch, 60)
                continue
            if prepared:
                await self._deliver(prepared)
