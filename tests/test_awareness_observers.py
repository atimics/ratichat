"""Platform events reach shared awareness with their real source identity."""

import asyncio
import copy
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import nio
import pytest

from chatbot.config import AppConfig, settings
from chatbot.core.world_state.manager import WorldStateManager
from chatbot.integrations.discord.observer import DiscordObserver
from chatbot.integrations.matrix.observer import MatrixObserver


class MemoryStore:
    """Small fake for the shared store's source event contract."""

    def __init__(self):
        self.messages, self.deleted, self.edits = {}, set(), []

    def ingest_message(self, platform, channel, message):
        key = platform, channel, message["id"]
        if key not in self.deleted:
            self.messages.setdefault(key, copy.deepcopy(message))
        return {"deleted": key in self.deleted}

    def edit_message(self, platform, channel, original, message):
        key = platform, channel, original
        saved = self.messages.get(key)
        if key in self.deleted or not saved or saved["sender"] != message["sender"]:
            return {"changed": False}
        revision = message["source_revision"]
        if revision <= saved.get("source_revision", saved["timestamp"]):
            return {"changed": False}
        self.edits.append(copy.deepcopy(message))
        saved.update(content=message["content"], source_revision=revision)
        if "image_urls" in message:
            saved["image_urls"] = message["image_urls"]
        saved["metadata"].update(message["metadata"])
        return {"changed": True}

    def delete_message(self, platform, channel, original, sender_id=None, timestamp=None):
        key = platform, channel, original
        self.deleted.add(key)
        self.messages.pop(key, None)


@pytest.fixture
def discord_observer():
    config = AppConfig(_env_file=None, DISCORD_BOT_TOKEN="test",
        DISCORD_ALLOWED_GUILD_IDS="10", DISCORD_ALLOWED_CHANNEL_IDS="20")
    observer = DiscordObserver(WorldStateManager(), config)
    observer.client = SimpleNamespace(user=SimpleNamespace(id=30), is_ready=lambda: True)
    observer.awareness_store = MemoryStore()
    observer.on_state_change = Mock()
    return observer


def discord_message(**changes):
    value = dict(id=40, guild=SimpleNamespace(id=10), channel=SimpleNamespace(id=20, name="general"),
        author=SimpleNamespace(id=50, bot=False, name="owner", display_name="Owner"),
        webhook_id=None, mentions=[], content="A useful chat message", created_at=datetime.now(timezone.utc),
        reference=SimpleNamespace(message_id=39))
    value.update(changes)
    return SimpleNamespace(**value)


def discord_edit(**changes):
    value = dict(guild_id=10, channel_id=20, message_id=40, cached_message=None,
        data={"author": {"id": "50", "bot": False}, "content": "A corrected message",
              "edited_timestamp": datetime.fromtimestamp(time.time() + 1, timezone.utc).isoformat()})
    value.update(changes)
    return SimpleNamespace(**value)


@pytest.mark.asyncio
async def test_discord_passive_message_keeps_reply_and_source_time(discord_observer):
    source = discord_message()
    await discord_observer._handle_message(source)
    await discord_observer._handle_message(source)
    saved = discord_observer.awareness_store.messages["discord", "20", "40"]
    assert saved["reply_to"] == "39"
    assert saved["timestamp"] == source.created_at.timestamp()
    assert saved["sender"] == "50"
    assert saved["metadata"]["bot_mentioned"] is False
    assert len(discord_observer.awareness_store.messages) == 1
    assert not discord_observer.can_reply("20", "40")
    assert discord_observer.world_state.get_channel("20") is None
    discord_observer.on_state_change.assert_not_called()


@pytest.mark.asyncio
async def test_discord_mention_keeps_request_acceptance(discord_observer):
    source = discord_message(mentions=[SimpleNamespace(id=30)])
    await discord_observer._handle_message(source)
    await discord_observer._handle_message(source)
    assert discord_observer.can_reply("20", "40")
    assert len(discord_observer.world_state.get_channel("20").recent_messages) == 1
    discord_observer.on_state_change.assert_called_once()


@pytest.mark.asyncio
async def test_discord_historical_message_is_saved_as_context(discord_observer):
    await discord_observer._handle_message(discord_message(
        mentions=[SimpleNamespace(id=30)], created_at=datetime.fromtimestamp(100, timezone.utc)))
    saved = discord_observer.awareness_store.messages["discord", "20", "40"]
    assert saved["metadata"]["historical"] is True
    assert not discord_observer.can_reply("20", "40")


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [
    {"guild": None}, {"guild": SimpleNamespace(id=99)},
    {"channel": SimpleNamespace(id=99, name="other")},
    {"author": SimpleNamespace(id=50, bot=True)}, {"webhook_id": 2}, {"content": ""},
])
async def test_discord_awareness_keeps_configured_human_scope(discord_observer, changes):
    await discord_observer._handle_message(discord_message(**changes))
    assert not discord_observer.awareness_store.messages


@pytest.mark.asyncio
async def test_discord_edit_preserves_original_id_author_time_and_reply(discord_observer):
    await discord_observer._handle_message(discord_message(mentions=[SimpleNamespace(id=30)]))
    before = copy.deepcopy(discord_observer.awareness_store.messages["discord", "20", "40"])
    await discord_observer._handle_message_edit(discord_edit())
    saved = discord_observer.awareness_store.messages["discord", "20", "40"]
    assert saved["content"] == "A corrected message"
    cached = discord_observer.world_state.get_channel("20").recent_messages[0]
    cached.image_urls = ["https://example.test/old-image.png"]
    discord_observer._handle_message_delete(SimpleNamespace(guild_id=10, channel_id=20, message_id=40))
    assert cached.content == ""
    assert cached.image_urls == []
    assert (saved["id"], saved["sender"], saved["timestamp"], saved["reply_to"]) == (
        before["id"], before["sender"], before["timestamp"], before["reply_to"])
    assert not discord_observer.can_reply("20", "40")
    await discord_observer._handle_message_edit(discord_edit(data={
        "author": {"id": "60"}, "content": "Wrong author", "edited_timestamp": datetime.now(timezone.utc).isoformat()}))
    assert saved["content"] == "A corrected message"


@pytest.mark.asyncio
async def test_discord_edit_handles_partial_and_out_of_scope_events(discord_observer):
    await discord_observer._handle_message(discord_message())
    await discord_observer._handle_message_edit(discord_edit(data={"embeds": []}))
    await discord_observer._handle_message_edit(discord_edit(channel_id=99))
    await discord_observer._handle_message_edit(discord_edit(cached_message=discord_message(
        author=SimpleNamespace(id=60))))
    assert not discord_observer.awareness_store.edits


@pytest.mark.asyncio
async def test_discord_delete_before_intake_keeps_replay_deleted(discord_observer):
    discord_observer._handle_message_delete(SimpleNamespace(guild_id=10, channel_id=20, message_id=40))
    await discord_observer._handle_message(discord_message(mentions=[SimpleNamespace(id=30)]))
    assert not discord_observer.awareness_store.messages
    assert ("discord", "20", "40") in discord_observer.awareness_store.deleted
    assert not discord_observer.can_reply("20", "40")
    discord_observer._handle_message_delete(SimpleNamespace(guild_id=99, channel_id=20, message_id=41))
    assert ("discord", "20", "41") not in discord_observer.awareness_store.deleted


@pytest.mark.asyncio
async def test_discord_gateway_registers_raw_changes_and_optional_content_intent(monkeypatch, discord_observer):
    class Client:
        def __init__(self, **options):
            self.options, self.callbacks, self.closed = options, {}, asyncio.Event()

        def event(self, callback):
            self.callbacks[callback.__name__] = callback
            return callback

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            await self.close()

        async def start(self, *args, **kwargs):
            await self.callbacks["on_ready"]()
            await self.closed.wait()

        async def close(self):
            self.closed.set()

    discord_observer.message_content_enabled = True
    monkeypatch.setattr("chatbot.integrations.discord.observer.discord.Client", Client)
    await discord_observer.connect()
    client = discord_observer.client
    assert client.options["intents"].message_content
    assert {"on_raw_message_edit", "on_raw_message_delete", "on_raw_bulk_message_delete"} <= set(client.callbacks)
    await client.callbacks["on_raw_bulk_message_delete"](SimpleNamespace(guild_id=10, channel_id=20, message_ids={40, 41}))
    assert len(discord_observer.awareness_store.deleted) == 2
    await discord_observer.disconnect()


@pytest.fixture
def matrix_observer(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(settings, "BOT_CAPABILITY_PROFILE", "matrix_steward")
    monkeypatch.setattr(settings, "PUBLIC_MATRIX_ROOM_IDS", "!general:example.test")
    monkeypatch.setattr(settings, "MATRIX_ADMIN_ROOM_ID", "!admin:example.test")
    observer = MatrixObserver(world_state_manager=WorldStateManager())
    observer.user_id = "@bot:example.test"
    observer.awareness_store = MemoryStore()
    return observer


def matrix_event(event_id="$original", sender="@alice:example.test", content=None, stamp=None, **changes):
    source = {"type": "m.room.message", "event_id": event_id, "sender": sender,
        "origin_server_ts": stamp if stamp is not None else int((time.time() + 1) * 1000),
        "content": content or {"msgtype": "m.text", "body": "A useful chat message",
            "m.relates_to": {"m.in_reply_to": {"event_id": "$parent"}}}}
    source.update(changes)
    return nio.Event.parse_event(source)


@pytest.mark.asyncio
async def test_matrix_intake_preserves_server_time_reply_and_historical_context(matrix_observer):
    room = nio.MatrixRoom("!general:example.test", matrix_observer.user_id)
    event = matrix_event(stamp=100_000)
    await matrix_observer._on_message(room, event)
    await matrix_observer._on_message(room, event)
    saved = matrix_observer.awareness_store.messages["matrix", room.room_id, "$original"]
    assert saved["timestamp"] == 100
    assert saved["reply_to"] == "$parent"
    assert saved["metadata"]["historical"] is True
    assert len(matrix_observer.awareness_store.messages) == 1


@pytest.mark.asyncio
async def test_matrix_edit_updates_original_and_preserves_reply(matrix_observer):
    room = nio.MatrixRoom("!general:example.test", matrix_observer.user_id)
    await matrix_observer._on_message(room, matrix_event(stamp=100_000))
    edited = matrix_event(event_id="$edit", stamp=101_000, content={"msgtype": "m.text", "body": "* corrected",
        "m.new_content": {"msgtype": "m.text", "body": "Corrected", "m.relates_to": {"m.in_reply_to": {"event_id": "$forged"}}},
        "m.relates_to": {"rel_type": "m.replace", "event_id": "$original"}})
    await matrix_observer._on_message(room, edited)
    await matrix_observer._on_message(room, edited)
    saved = matrix_observer.awareness_store.messages["matrix", room.room_id, "$original"]
    assert saved["content"] == "Corrected"
    assert saved["metadata"]["edit_event_id"] == "$edit"
    assert saved["reply_to"] == "$parent"
    assert saved["timestamp"] == 100
    assert len(matrix_observer.world_state.get_channel(room.room_id).recent_messages) == 1
    assert matrix_observer.world_state.get_channel(room.room_id).recent_messages[0].content == "Corrected"


@pytest.mark.asyncio
async def test_matrix_edits_require_original_author_and_valid_new_content(matrix_observer):
    room = nio.MatrixRoom("!general:example.test", matrix_observer.user_id)
    await matrix_observer._on_message(room, matrix_event(stamp=100_000))
    replacement = {"msgtype": "m.text", "body": "* changed",
        "m.new_content": {"body": "Wrong author"},
        "m.relates_to": {"rel_type": "m.replace", "event_id": "$original"}}
    await matrix_observer._on_message(room, matrix_event(event_id="$edit", sender="@other:example.test", content=replacement))
    replacement.pop("m.new_content")
    await matrix_observer._on_message(room, matrix_event(event_id="$bad", content=replacement))
    assert not matrix_observer.awareness_store.edits
    assert len(matrix_observer.world_state.get_channel(room.room_id).recent_messages) == 1


@pytest.mark.asyncio
async def test_matrix_replacement_retires_previous_image_evidence(matrix_observer):
    room = nio.MatrixRoom("!general:example.test", matrix_observer.user_id)
    await matrix_observer._on_message(room, matrix_event(stamp=100_000))
    saved = matrix_observer.awareness_store.messages["matrix", room.room_id, "$original"]
    saved["image_urls"] = ["https://example.test/old-image.png"]
    saved["metadata"]["original_filename"] = "old-image.png"
    cached = matrix_observer.world_state.get_channel(room.room_id).recent_messages[0]
    cached.image_urls = list(saved["image_urls"])
    cached.metadata["original_filename"] = "old-image.png"
    await matrix_observer._on_message(room, matrix_event(event_id="$edit", stamp=101_000,
        content={"msgtype": "m.text", "body": "* Changed",
                 "m.new_content": {"msgtype": "m.text", "body": "Changed"},
                 "m.relates_to": {"rel_type": "m.replace", "event_id": "$original"}}))
    assert saved["image_urls"] == []
    assert saved["metadata"]["original_filename"] is None
    assert cached.image_urls == []
    assert "original_filename" not in cached.metadata
    cached.image_urls = ["https://example.test/old-image.png"]
    await matrix_observer._on_redaction(room, nio.Event.parse_event({
        "type": "m.room.redaction", "event_id": "$redaction", "sender": "@moderator:example.test",
        "origin_server_ts": 102_000, "content": {}, "redacts": "$original"}))
    assert cached.content == ""
    assert cached.image_urls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("modern", [False, True])
async def test_matrix_redaction_before_intake_keeps_replay_deleted(matrix_observer, modern):
    room = nio.MatrixRoom("!general:example.test", matrix_observer.user_id)
    source = {"type": "m.room.redaction", "event_id": "$redaction", "sender": "@moderator:example.test",
        "origin_server_ts": 200_000, "content": {"redacts": "$original"} if modern else {},
        **({} if modern else {"redacts": "$original"})}
    event = nio.Event.parse_event(source)
    assert isinstance(event, (nio.RedactionEvent, nio.BadEvent))
    await matrix_observer._on_redaction(room, event)
    await matrix_observer._on_message(room, matrix_event())
    assert not matrix_observer.awareness_store.messages
    assert ("matrix", room.room_id, "$original") in matrix_observer.awareness_store.deleted


@pytest.mark.asyncio
async def test_matrix_awareness_keeps_approved_room_scope(matrix_observer):
    room = nio.MatrixRoom("!other:example.test", matrix_observer.user_id)
    await matrix_observer._on_message(room, matrix_event())
    await matrix_observer._on_redaction(room, nio.Event.parse_event({
        "type": "m.room.redaction", "event_id": "$redaction", "sender": "@moderator:example.test",
        "origin_server_ts": 200_000, "content": {}, "redacts": "$original"}))
    assert not matrix_observer.awareness_store.messages
    assert not matrix_observer.awareness_store.deleted
