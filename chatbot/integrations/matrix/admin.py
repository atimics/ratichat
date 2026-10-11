"""Room inspection and moderation through the Matrix client API."""

import asyncio
from urllib.parse import quote

import httpx

from ..admin_actions import AdminReceipts, integer_value, text_value, result_message


class MatrixRoomAdmin:
    def __init__(self, steward):
        self.steward = steward
        self.config = steward.config
        self.receipts = AdminReceipts(steward.db_path)
        self.lock = asyncio.Lock()

    async def execute(self, tool, params, scope, observer):
        room_id = params.get("room_id")
        allowed = {room.strip() for room in self.config.MATRIX_MANAGED_ROOM_IDS.split(",") if room.strip()}
        if room_id not in allowed or room_id in {self.config.MATRIX_CONTROL_ROOM_ID, self.config.MATRIX_ADMIN_ROOM_ID}:
            raise ValueError("Choose a configured public managed room")
        if not observer or not observer.client or not observer.client.access_token:
            raise ValueError("Connect the Matrix bot first")
        operators = {user.strip() for user in self.config.MATRIX_OPERATOR_USER_IDS.split(",") if user.strip()}
        if (not scope or scope.channel_type != "matrix" or scope.channel_id != self.config.MATRIX_CONTROL_ROOM_ID
                or scope.latest_sender_id not in operators or not scope.latest_event_id
                or params.get("source_event_id") != scope.latest_event_id):
            raise ValueError("Use a current operator request in the Matrix control room")
        room = "/_matrix/client/v3/rooms/" + quote(room_id, safe="")
        async with self.lock, self.steward._client(observer.client.access_token) as client:
            source = await client.get(
                "/_matrix/client/v3/rooms/" + quote(scope.channel_id, safe="")
                + "/event/" + quote(scope.latest_event_id, safe=""))
            source.raise_for_status()
            event = source.json()
            if (event.get("sender") != scope.latest_sender_id or event.get("type") != "m.room.message"
                    or not event.get("content", {}).get("body")):
                raise ValueError("Use a current operator message in the control room")
            if tool == "inspect_matrix_room":
                return await self.inspect(client, room, room_id, params)
            previous = self.receipts.previous("matrix", scope.channel_id, scope.latest_event_id, tool, params)
            if previous:
                return previous
            method, path, body = await self.prepare(client, room, tool, params, operators, observer)
            receipt_id, previous = self.receipts.reserve("matrix", scope.channel_id, scope.latest_event_id, tool, params)
            if previous:
                return previous
            try:
                response = await client.request(method, path.replace("{receipt}", receipt_id), json=body)
                response.raise_for_status()
                result = {"status": "success", "message": f"Matrix {params['operation']} completed for {room_id}.",
                          "room_id": room_id, "operation": params["operation"],
                          "event_id": response.json().get("event_id")}
            except httpx.HTTPStatusError as error:
                result = {"status": "failure" if error.response.status_code < 500 else "uncertain",
                          "message": "Check Matrix room permissions and the target before a fresh request.",
                          "http_status": error.response.status_code}
            except httpx.RequestError:
                result = {"status": "uncertain", "message": "Check the Matrix target to confirm this change before a fresh request."}
            return self.receipts.finish(receipt_id, result)

    async def inspect(self, client, room, room_id, params):
        view = params.get("view", "summary")
        if view not in {"summary", "members"}:
            raise ValueError("Choose summary or members")
        offset = integer_value(params.get("offset", 0), "offset", 0, 1000000)
        limit = integer_value(params.get("limit", 25), "limit", 1, 50)
        response = await client.get(room + "/state")
        response.raise_for_status()
        state = response.json()
        members = sorted(({"user_id": event.get("state_key"),
                           "membership": event.get("content", {}).get("membership"),
                           "display_name": str(event.get("content", {}).get("displayname", ""))[:100]}
                          for event in state if event.get("type") == "m.room.member"), key=lambda item: item["user_id"])
        if view == "members":
            data = {"members": members[offset:offset + limit], "total": len(members),
                    "next_offset": offset + limit if offset + limit < len(members) else None}
        else:
            fields = {"m.room.name": "name", "m.room.topic": "topic", "m.room.join_rules": "join_rule",
                      "m.room.history_visibility": "history_visibility", "m.room.pinned_events": "pinned"}
            data = {fields[event["type"]]: event.get("content", {}).get(fields[event["type"]])
                    for event in state if event.get("type") in fields and event.get("state_key", "") == ""}
            data["member_counts"] = {status: sum(member["membership"] == status for member in members)
                                     for status in ("join", "invite", "leave", "ban")}
        return {"status": "success", "room_id": room_id, **data,
                "message": result_message({"room_id": room_id, **data})}

    async def prepare(self, client, room, tool, params, operators, observer):
        operation = params.get("operation")
        reason = text_value(params.get("reason"), "reason", 300)
        if tool == "moderate_matrix_member":
            if operation not in {"invite", "kick", "ban", "unban"}:
                raise ValueError("Choose invite, kick, ban, or unban")
            user = params.get("user_id")
            if not isinstance(user, str) or not user.startswith("@") or ":" not in user or len(user) > 255:
                raise ValueError("Supply a full Matrix user ID")
            protected = operators | {self.config.MATRIX_USER_ID, getattr(observer.client, "user_id", None)}
            if operation in {"kick", "ban"} and user in protected:
                raise ValueError("Choose a community member for this action")
            return "POST", room + "/" + operation, {"user_id": user, "reason": reason}
        if tool != "manage_matrix_message" or operation not in {"pin", "unpin", "redact"}:
            raise ValueError("Choose pin, unpin, or redact")
        event_id = params.get("event_id")
        if not isinstance(event_id, str) or not event_id.startswith("$") or len(event_id) > 255:
            raise ValueError("Supply a Matrix message event ID")
        event = await client.get(room + "/event/" + quote(event_id, safe=""))
        event.raise_for_status()
        if event.json().get("type") != "m.room.message":
            raise ValueError("Choose a Matrix message event")
        if operation == "redact":
            return "PUT", room + "/redact/" + quote(event_id, safe="") + "/{receipt}", {"reason": reason}
        response = await client.get(room + "/state/m.room.pinned_events")
        if response.status_code == 404 and response.json().get("errcode") == "M_NOT_FOUND":
            pins = []
        else:
            response.raise_for_status()
            pins = response.json().get("pinned", [])
        if not isinstance(pins, list) or any(not isinstance(pin, str) for pin in pins):
            raise ValueError("Check the room's pinned message state")
        pins = list(pins)
        if operation == "pin" and event_id not in pins:
            pins.append(event_id)
        if operation == "unpin":
            pins = [pin for pin in pins if pin != event_id]
        return "PUT", room + "/state/m.room.pinned_events", {"pinned": pins}
