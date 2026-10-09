"""Discord intake, reply scope, and channel privacy checks."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from chatbot.config import AppConfig
from chatbot.core.orchestration.capability_policy import CapabilityPolicy, ExecutionScope
from chatbot.core.orchestration.main_orchestrator import TraditionalProcessor
from chatbot.core.world_state.manager import WorldStateManager
from chatbot.integrations.base import IntegrationConnectionError
from chatbot.integrations.discord import DiscordObserver
from chatbot.tools.discord_tools import SendDiscordReplyTool
from chatbot.tools.registry import ToolRegistry


@pytest.fixture
def observer():
    config = AppConfig(
        _env_file=None, DISCORD_BOT_TOKEN="test-secret",
        DISCORD_ALLOWED_GUILD_IDS="10", DISCORD_ALLOWED_CHANNEL_IDS="20",
    )
    connection = DiscordObserver(WorldStateManager(), config)
    connection.client = SimpleNamespace(user=SimpleNamespace(id=30), is_ready=lambda: True)
    connection.on_state_change = Mock()
    return connection


def mention(**overrides):
    values = dict(
        id=40, guild=SimpleNamespace(id=10),
        channel=SimpleNamespace(id=20, name="general"),
        author=SimpleNamespace(id=50, bot=False, name="owner", display_name="Owner"),
        webhook_id=None, mentions=[SimpleNamespace(id=30)],
        content="<@30> hello", created_at=datetime.now(timezone.utc),
    )
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.asyncio
async def test_mention_reaches_world_state_once(observer):
    await observer._handle_message(mention())
    await observer._handle_message(mention())
    channel = observer.world_state.get_channel("20")
    assert channel.type == "discord"
    assert len(channel.recent_messages) == 1
    assert channel.recent_messages[0].sender == "50"
    assert observer.can_reply("20", "40")
    observer.on_state_change.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [
    {"guild": None}, {"guild": SimpleNamespace(id=99)},
    {"channel": SimpleNamespace(id=99, name="private")},
    {"author": SimpleNamespace(id=50, bot=True)},
    {"webhook_id": 60}, {"mentions": []}, {"content": "a" * 4001},
])
async def test_intake_requires_configured_human_mention(observer, overrides):
    await observer._handle_message(mention(**overrides))
    assert observer.world_state.get_channel("20") is None
    assert not observer._requests
    observer.on_state_change.assert_not_called()


@pytest.mark.asyncio
async def test_rate_limit_bounds_paid_intake(observer):
    for message_id in range(40, 55):
        await observer._handle_message(mention(id=message_id))
    assert len(observer.world_state.get_channel("20").recent_messages) == 10


@pytest.mark.asyncio
async def test_reply_checks_source_and_sends_once_without_pings(observer):
    message = mention()
    await observer._handle_message(message)
    channel = SimpleNamespace(
        guild=message.guild, fetch_message=AsyncMock(return_value=message),
        send=AsyncMock(return_value=SimpleNamespace(id=70)),
    )
    observer.client.get_channel = Mock(return_value=channel)
    result = await observer.send_reply("20", "@everyone " + "a" * 2500, "40")
    assert result["status"] == "success"
    sent = channel.send.await_args
    assert len(sent.args[0]) == 2000
    assert sent.kwargs["allowed_mentions"].to_dict() == {"parse": []}
    assert sent.kwargs["mention_author"] is False
    assert sent.kwargs["reference"] is message
    repeated = await observer.send_reply("20", "again", "40")
    assert repeated["duplicate"] is True
    channel.send.assert_awaited_once()
    assert not observer.can_reply("20", "40")


@pytest.mark.asyncio
async def test_reply_requires_an_accepted_source_in_same_channel(observer):
    await observer._handle_message(mention())
    observer.client.get_channel = Mock()
    for channel, message_id in [("99", "40"), ("20", "99")]:
        result = await observer.send_reply(channel, "hello", message_id)
        assert result["status"] == "failure"
    observer.client.get_channel.assert_not_called()


@pytest.mark.asyncio
async def test_source_edit_requires_fresh_mention(observer):
    await observer._handle_message(mention())
    channel = SimpleNamespace(
        guild=SimpleNamespace(id=10),
        fetch_message=AsyncMock(return_value=mention(content="changed")), send=AsyncMock(),
    )
    observer.client.get_channel = Mock(return_value=channel)
    result = await observer.send_reply("20", "reply", "40")
    assert result["status"] == "failure"
    channel.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_watch_digest_has_fixed_channel_and_saved_nonce(observer):
    channel = SimpleNamespace(guild=SimpleNamespace(id=10), send=AsyncMock(return_value=SimpleNamespace(id=91)))
    observer.client.get_channel = Mock(return_value=channel)
    text = "New entries · watch abc\nReceipt: unique"
    first = await observer.send_digest("20", text, "saved-key")
    assert first["message_id"] == "91"
    assert (await observer.send_digest("20", text, "saved-key"))["message_id"] == "91"
    channel.send.assert_awaited_once()
    assert channel.send.await_args.kwargs["allowed_mentions"].to_dict() == {"parse": []}
    assert len(channel.send.await_args.kwargs["nonce"]) == 24
    assert (await observer.send_digest("99", text, "other-key"))["status"] == "failure"
    channel.guild.id = 99
    assert (await observer.send_digest("20", text, "other-key"))["status"] == "failure"


@pytest.mark.asyncio
async def test_watch_digest_recovers_accepted_send_after_lost_response(observer):
    text = "New entries · watch abc\nReceipt: unique"
    accepted = SimpleNamespace(id=91, content=text, author=SimpleNamespace(id=30))
    async def history(**kwargs):
        yield SimpleNamespace(id=90, content=text, author=SimpleNamespace(id=50))
        yield accepted
    channel = SimpleNamespace(guild=SimpleNamespace(id=10), history=history,
                              send=AsyncMock(side_effect=TimeoutError("lost response")))
    observer.client.get_channel = Mock(return_value=channel)
    assert (await observer.send_digest("20", text, "saved-key"))["status"] == "unknown"
    receipt = await observer.reconcile_digest("20", text, "saved-key")
    assert receipt["message_id"] == "91"
    assert (await observer.send_digest("20", text, "saved-key"))["message_id"] == "91"
    channel.send.assert_awaited_once()


@pytest.mark.parametrize("profile", ["public", "matrix_steward", "operator"])
def test_discord_policy_checks_destination_latest_source_and_platform(profile):
    policy = CapabilityPolicy(profile, approved_discord_channel_ids=["20"])
    scope = ExecutionScope("20", "discord", frozenset({"39", "40"}), "40", "50")
    params = {"channel_id": "20", "reply_to_id": "40", "content": "hello"}
    assert policy.denial_reason("send_discord_reply", params, scope) is None
    assert policy.denial_reason("send_discord_reply", {**params, "channel_id": "99"}, scope)
    assert policy.denial_reason("send_discord_reply", {**params, "reply_to_id": "39"}, scope)
    assert policy.denial_reason("send_discord_reply", params, None)
    assert policy.denial_reason("manage_matrix_room", {}, scope)
    assert policy.filter_tool_names({"wait", "send_discord_reply", "manage_matrix_room"}, scope) == {"wait", "send_discord_reply"}


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["public", "matrix_steward", "operator"])
async def test_discord_ai_payload_keeps_current_channel_only(observer, profile):
    await observer._handle_message(mention())
    registry = ToolRegistry()
    registry.register_tool(SendDiscordReplyTool())
    ai = SimpleNamespace(api_key="test", make_decision=AsyncMock(
        return_value=SimpleNamespace(selected_actions=[]),
    ))
    policy = CapabilityPolicy(profile, approved_discord_channel_ids=["20"])
    processor = TraditionalProcessor(
        ai, registry, Mock(), AsyncMock(), SimpleNamespace(discord_observer=observer), policy,
    )
    await processor.process_payload({
        "current_processing_channel_id": "20",
        "channels": {
            "20": {"type": "discord", "recent_messages": [{"id": "40", "content": "hello"}]},
            "private": {"type": "matrix", "recent_messages": [{"content": "private data"}]},
        },
        "action_history": "private data", "user_profiles": "private data",
        "other_channels_summary": "private data",
    }, ["20"])
    payload = ai.make_decision.await_args.args[0]
    assert set(payload["channels"]) == {"20"}
    assert "private data" not in str(payload)
    assert "matrix_management" not in payload
    assert "send_discord_reply" in str(payload["available_tools"])


@pytest.mark.asyncio
async def test_startup_and_shutdown_close_gateway(monkeypatch, observer):
    class Client:
        user = SimpleNamespace(id=30)

        def __init__(self, **kwargs):
            self.options = kwargs
            self.closed = asyncio.Event()

        def event(self, callback):
            setattr(self, callback.__name__, callback)
            return callback

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            await self.close()

        async def start(self, token, reconnect):
            await self.on_ready()
            await self.closed.wait()

        async def close(self):
            self.closed.set()

        def is_ready(self):
            return not self.closed.is_set()

    monkeypatch.setattr("chatbot.integrations.discord.observer.discord.Client", Client)
    await observer.connect()
    client = observer.client
    assert (await observer.get_status())["connected"]
    assert client.options["intents"].guild_messages
    assert not client.options["intents"].message_content
    await observer.stop()
    assert client.closed.is_set()
    assert observer._task is None


@pytest.mark.asyncio
async def test_config_requires_token_and_both_id_lists():
    observer = DiscordObserver(WorldStateManager(), AppConfig(_env_file=None))
    with pytest.raises(IntegrationConnectionError):
        await observer.start()
