"""Save one admin change per source request, including uncertain outcomes."""

import hashlib
import json
import sqlite3
import time
from pathlib import Path


class AdminReceipts:
    def __init__(self, db_path):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path, timeout=10) as db:
            db.execute("""CREATE TABLE IF NOT EXISTS chat_admin_receipts (
                receipt_id TEXT PRIMARY KEY, action TEXT NOT NULL,
                created_at REAL NOT NULL, result TEXT NOT NULL
            )""")

    def previous(self, platform, channel_id, source_id, tool, params):
        receipt_id = hashlib.sha256(json.dumps([platform, channel_id, source_id]).encode()).hexdigest()
        action = json.dumps([tool, params], sort_keys=True)
        with sqlite3.connect(self.db_path, timeout=10) as db:
            row = db.execute("SELECT action, result FROM chat_admin_receipts WHERE receipt_id=?", (receipt_id,)).fetchone()
        if row is None:
            return None
        if row[0] != action:
            return {"status": "failure", "message": "This request already has an admin action. Send a fresh request for another change.",
                    "receipt_id": receipt_id, "replayed": True}
        return {**json.loads(row[1]), "receipt_id": receipt_id, "replayed": True}

    def reserve(self, platform, channel_id, source_id, tool, params):
        receipt_id = hashlib.sha256(json.dumps([platform, channel_id, source_id]).encode()).hexdigest()
        action = json.dumps([tool, params], sort_keys=True)
        pending = {"status": "uncertain", "message": "This request has an admin receipt. Check the target before sending a fresh request."}
        with sqlite3.connect(self.db_path, timeout=10) as db:
            inserted = db.execute(
                "INSERT OR IGNORE INTO chat_admin_receipts VALUES (?, ?, ?, ?)",
                (receipt_id, action, time.time(), json.dumps(pending)),
            ).rowcount
            if inserted:
                return receipt_id, None
            saved_action, result = db.execute(
                "SELECT action, result FROM chat_admin_receipts WHERE receipt_id=?", (receipt_id,),
            ).fetchone()
        if saved_action != action:
            result = json.dumps({"status": "failure", "message": "This request already has an admin action. Send a fresh request for another change."})
        return receipt_id, {**json.loads(result), "receipt_id": receipt_id, "replayed": True}

    def finish(self, receipt_id, result):
        result = {**result, "receipt_id": receipt_id}
        with sqlite3.connect(self.db_path, timeout=10) as db:
            db.execute("UPDATE chat_admin_receipts SET result=? WHERE receipt_id=?", (json.dumps(result), receipt_id))
        return result


def text_value(value, label, maximum, *, empty=False):
    if not isinstance(value, str) or len(value) > maximum or (not empty and not value.strip()):
        raise ValueError(f"Supply {label} text up to {maximum} characters")
    return value


def integer_value(value, label, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"Supply {label} as a whole number from {minimum} to {maximum}")
    return value


def result_message(data):
    message = json.dumps(data, ensure_ascii=False)
    if len(message) > 1800:
        return message[:1700] + "… Choose a smaller page limit for more detail."
    return message
