import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import discord
import httpx
import pytest

from chatbot.config import AppConfig
from chatbot.core.ai_engine import ActionPlan, DecisionResult
from chatbot.core.orchestration.capability_policy import (
    CapabilityPolicy, ExecutionScope, MATRIX_ROOM_ADMIN_TOOLS, DISCORD_ADMIN_TOOLS,
)
from chatbot.core.orchestration.main_orchestrator import TraditionalProcessor
from chatbot.integrations.discord.admin import DiscordAdmin
from chatbot.integrations.discord.observer import DiscordObserver
from chatbot.integrations.matrix.admin import MatrixRoomAdmin
from chatbot.integrations.matrix.steward import MatrixSteward
from chatbot.tools.base import ActionContext
from chatbot.tools.chat_admin_tools import CHAT_ADMIN_TOOL_CLASSES, ManageDiscordChannelTool
from chatbot.tools.registry import ToolRegistry

ROOM, CONTROL, OWNER = "!room:test", "!control:test", "@owner:test"
MATRIX_SCOPE = ExecutionScope(CONTROL, "matrix", frozenset({"$request"}), "$request", OWNER)
DISCORD_SCOPE = ExecutionScope("20", "discord", frozenset({"40"}), "40", "50")


def policy(profile="matrix_steward", enabled=True):
    return CapabilityPolicy(profile, [ROOM, CONTROL], CONTROL, [OWNER], [ROOM], ["20"], ["50"], enabled)


@pytest.mark.parametrize("profile", ["public", "matrix_steward", "operator"])
@pytest.mark.parametrize("tool", sorted(DISCORD_ADMIN_TOOLS))
def test_discord_admin_policy(profile, tool):
    p = policy(profile)
    params = {"source_event_id": "40"}
    assert p.denial_reason(tool, params, DISCORD_SCOPE) is None
    assert tool in p.filter_tool_names({tool}, DISCORD_SCOPE)
    for scope in (None, MATRIX_SCOPE, replace(DISCORD_SCOPE, latest_sender_id="99"),
                  replace(DISCORD_SCOPE, channel_id="99"), replace(DISCORD_SCOPE, latest_event_id=None)):
        assert p.denial_reason(tool, params, scope)
        assert tool not in p.filter_tool_names({tool}, scope)
    for changed in ({"source_event_id": "39"}, {**params, "guild_id": "99"}, {**params, "channel_id": "99"}):
        assert p.denial_reason(tool, changed, DISCORD_SCOPE)
    assert policy(profile, False).denial_reason(tool, params, DISCORD_SCOPE)
    assert tool not in policy(profile, False).filter_tool_names({tool}, DISCORD_SCOPE)


@pytest.mark.parametrize("profile", ["matrix_steward", "operator"])
@pytest.mark.parametrize("tool", sorted(MATRIX_ROOM_ADMIN_TOOLS))
def test_matrix_admin_policy(profile, tool):
    p = policy(profile)
    params = {"source_event_id": "$request", "room_id": ROOM}
    assert p.denial_reason(tool, params, MATRIX_SCOPE) is None
    for scope in (None, DISCORD_SCOPE, replace(MATRIX_SCOPE, latest_sender_id="@visitor:test"),
                  replace(MATRIX_SCOPE, channel_id=ROOM)):
        assert p.denial_reason(tool, params, scope)
        assert tool not in p.filter_tool_names({tool}, scope)
    for changed in ({**params, "source_event_id": "$old"}, {**params, "room_id": CONTROL}):
        assert p.denial_reason(tool, changed, MATRIX_SCOPE)
    assert policy("public").denial_reason(tool, params, MATRIX_SCOPE)


@pytest.mark.asyncio
@pytest.mark.parametrize("tool_class", CHAT_ADMIN_TOOL_CLASSES)
async def test_tools_require_trusted_context(tool_class):
    context = SimpleNamespace(discord_admin=AsyncMock(), matrix_admin=AsyncMock())
    assert (await tool_class().execute({}, context))["status"] == "failure"
    context.discord_admin.execute.assert_not_awaited()
    context.matrix_admin.execute.assert_not_awaited()


@pytest.fixture
def matrix(tmp_path):
    cfg = AppConfig(_env_file=None, MATRIX_HOMESERVER="https://matrix.test", MATRIX_USER_ID="@bot:test",
                    MATRIX_MANAGED_ROOM_IDS=ROOM, MATRIX_CONTROL_ROOM_ID=CONTROL, MATRIX_OPERATOR_USER_IDS=OWNER)
    calls = []
    state = {"pins": ["$old"], "source_sender": OWNER, "write_status": 200, "timeout": False}

    def handle(request):
        calls.append(request)
        assert request.headers["Authorization"] == "Bearer bot-token"
        if request.method != "GET":
            if state["timeout"]:
                raise httpx.ReadTimeout("lost response", request=request)
            return httpx.Response(state["write_status"], json={"event_id": "$result"})
        if request.url.path.endswith("/event/$request"):
            return httpx.Response(200, json={"type": "m.room.message", "sender": state["source_sender"], "content": {"body": "Please manage this room"}})
        if "/event/" in request.url.path:
            return httpx.Response(200, json={"type": "m.room.message"})
        if request.url.path.endswith("/state/m.room.pinned_events"):
            return httpx.Response(200, json={"pinned": state["pins"]})
        return httpx.Response(200, json=[
            {"type": "m.room.name", "state_key": "", "content": {"name": "Community"}},
            *[{"type": "m.room.member", "state_key": f"@user{i}:test", "content": {"membership": "join"}} for i in range(3)],
        ])

    steward = MatrixSteward(cfg, str(tmp_path / "matrix.db"), httpx.MockTransport(handle))
    observer = SimpleNamespace(client=SimpleNamespace(access_token="bot-token", user_id="@bot:test"))
    return MatrixRoomAdmin(steward), observer, calls, state


def matrix_params(**kwargs):
    return {"source_event_id": "$request", "room_id": ROOM, "reason": "User requested cleanup", **kwargs}


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["invite", "kick", "ban", "unban"])
async def test_matrix_members_use_client_api_and_durable_receipts(matrix, operation):
    admin, observer, calls, state = matrix
    params = matrix_params(operation=operation, user_id="@member:test")
    result = await admin.execute("moderate_matrix_member", params, MATRIX_SCOPE, observer)
    assert result["status"] == "success"
    write = next(call for call in calls if call.method == "POST")
    assert write.url.path.endswith("/" + operation)
    assert json.loads(write.content) == {"user_id": "@member:test", "reason": params["reason"]}
    replay = await MatrixRoomAdmin(admin.steward).execute("moderate_matrix_member", params, MATRIX_SCOPE, observer)
    assert replay["replayed"] and replay["receipt_id"] == result["receipt_id"]
    assert sum(call.method == "POST" for call in calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("operation,pins", [("pin", ["$old", "$target"]), ("unpin", ["$old"]), ("redact", None)])
async def test_matrix_message_actions_preserve_other_pins(matrix, operation, pins):
    admin, observer, calls, state = matrix
    state["pins"] = ["$old", "$target"] if operation == "unpin" else ["$old"]
    params = matrix_params(operation=operation, event_id="$target")
    result = await admin.execute("manage_matrix_message", params, MATRIX_SCOPE, observer)
    assert result["status"] == "success"
    write = next(call for call in calls if call.method == "PUT")
    if pins is not None:
        assert json.loads(write.content) == {"pinned": pins}
    else:
        assert write.url.path.endswith("/redact/$target/" + result["receipt_id"])


@pytest.mark.asyncio
async def test_matrix_inspection_pages_members(matrix):
    admin, observer, calls, state = matrix
    result = await admin.execute("inspect_matrix_room", matrix_params(view="members", limit=2), MATRIX_SCOPE, observer)
    assert len(result["members"]) == 2 and result["next_offset"] == 2
    summary = await admin.execute("inspect_matrix_room", matrix_params(), MATRIX_SCOPE, observer)
    assert summary["name"] == "Community" and summary["member_counts"]["join"] == 3
    assert all(call.method == "GET" for call in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"user_id": OWNER}, {"user_id": "@bot:test"}, {"room_id": CONTROL}, {"room_id": "!other:test"},
    {"operation": "shutdown"}, {"reason": ""}, {"source_event_id": "$old"},
])
async def test_matrix_invalid_target_has_no_write(matrix, change):
    admin, observer, calls, state = matrix
    with pytest.raises(ValueError):
        await admin.execute("moderate_matrix_member", matrix_params(operation="ban", user_id="@member:test") | change, MATRIX_SCOPE, observer)
    assert all(call.method == "GET" for call in calls)


@pytest.mark.asyncio
async def test_matrix_live_sender_and_uncertain_receipt(matrix):
    admin, observer, calls, state = matrix
    params = matrix_params(operation="ban", user_id="@member:test")
    state["source_sender"] = "@visitor:test"
    with pytest.raises(ValueError):
        await admin.execute("moderate_matrix_member", params, MATRIX_SCOPE, observer)
    state["source_sender"] = OWNER
    state["timeout"] = True
    assert (await admin.execute("moderate_matrix_member", params, MATRIX_SCOPE, observer))["status"] == "uncertain"
    assert (await MatrixRoomAdmin(admin.steward).execute("moderate_matrix_member", params, MATRIX_SCOPE, observer))["status"] == "uncertain"
    assert sum(call.method == "POST" for call in calls) == 1


class Role:
    def __init__(self, role_id, position):
        self.id, self.position = role_id, position
        self.permissions = discord.Permissions.none()
        self.managed = False
        self.name = "Community"

    def __ge__(self, other):
        return self.position >= other.position

    def is_default(self):
        return False


@pytest.fixture
def discord_admin(tmp_path):
    cfg = AppConfig(_env_file=None, DISCORD_ADMIN_ENABLED=True, DISCORD_OWNER_USER_IDS="50",
                    DISCORD_MANAGED_ROLE_IDS="80")
    permissions = discord.Permissions.all()
    actor = SimpleNamespace(id=50, top_role=Role(51, 10), guild_permissions=permissions)
    bot = SimpleNamespace(id=30, top_role=Role(31, 9), guild_permissions=permissions)
    member = SimpleNamespace(id=60, top_role=Role(61, 2), guild_permissions=discord.Permissions.none(),
                             timeout=AsyncMock(), kick=AsyncMock(), add_roles=AsyncMock(), remove_roles=AsyncMock(),
                             roles=[], display_name="Member", timed_out_until=None)
    role = Role(80, 3)
    target = SimpleNamespace(id=70, pin=AsyncMock(), unpin=AsyncMock(), delete=AsyncMock())
    source = SimpleNamespace(id=40, author=SimpleNamespace(id=50, bot=False), content="<@30> admin request",
                             embeds=[], webhook_id=None, mentions=[SimpleNamespace(id=30)])
    guild = SimpleNamespace(id=10, owner_id=99, name="Community", fetch_member=AsyncMock(side_effect=lambda user_id: {50: actor, 30: bot, 60: member}[user_id]),
                            fetch_roles=AsyncMock(return_value=[role]), ban=AsyncMock(), unban=AsyncMock())
    channel = Mock(spec=discord.TextChannel)
    channel.id, channel.guild = 20, guild
    channel.name, channel.topic, channel.slowmode_delay = "general", "Welcome", 0
    channel.permissions_for = Mock(side_effect=lambda user: user.guild_permissions)
    channel.fetch_message = AsyncMock(side_effect=lambda message_id: {40: source, 70: target}[message_id])
    channel.edit = AsyncMock()
    guild.fetch_channel = AsyncMock(return_value=channel)
    guild.fetch_channels = AsyncMock(return_value=[channel])
    observer = SimpleNamespace(client=SimpleNamespace(is_ready=lambda: True, user=SimpleNamespace(id=30),
                                  fetch_channel=AsyncMock(return_value=channel), fetch_guild=AsyncMock(return_value=guild)),
                               allowed_channel_ids={"20"}, allowed_guild_ids={"10"},
                               _requests={"40": ("20", "50", source.content)}, _content=DiscordObserver._content)
    return SimpleNamespace(admin=DiscordAdmin(cfg, str(tmp_path / "discord.db")), observer=observer, guild=guild,
                           channel=channel, actor=actor, bot=bot, member=member, role=role, target=target, source=source)


def discord_params(**kwargs):
    return {"source_event_id": "40", "reason": "Owner requested cleanup", **kwargs}


@pytest.mark.asyncio
@pytest.mark.parametrize("operation,value,expected", [("name", "updates", {"name": "updates"}), ("topic", "", {"topic": ""}), ("slowmode", 30, {"slowmode_delay": 30})])
async def test_discord_channel_edits_and_replay(discord_admin, operation, value, expected):
    d = discord_admin
    params = discord_params(operation=operation, **({"seconds": value} if operation == "slowmode" else {"value": value}))
    result = await d.admin.execute("manage_discord_channel", params, DISCORD_SCOPE, d.observer)
    assert result["status"] == "success"
    assert d.channel.edit.await_args.kwargs == {**expected, "reason": "RatiChat request 40 by 50: Owner requested cleanup"}
    replay = await DiscordAdmin(d.admin.config, d.admin.receipts.db_path).execute("manage_discord_channel", params, DISCORD_SCOPE, d.observer)
    assert replay["replayed"]
    d.channel.edit.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["timeout", "clear_timeout", "kick", "ban", "unban"])
async def test_discord_moderation(discord_admin, operation):
    d = discord_admin
    result = await d.admin.execute("moderate_discord_member", discord_params(operation=operation, user_id="60", seconds=60), DISCORD_SCOPE, d.observer)
    assert result["status"] == "success"
    if operation == "ban":
        assert d.guild.ban.await_args.kwargs["delete_message_seconds"] == 0
    elif operation == "unban":
        assert d.guild.unban.await_args.args[0].id == 60
    elif operation == "clear_timeout":
        assert d.member.timeout.await_args.args == (None,)
    elif operation == "timeout":
        assert 55 < (d.member.timeout.await_args.args[0] - discord.utils.utcnow()).total_seconds() <= 60
    else:
        d.member.kick.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["add", "remove"])
async def test_discord_role_actions(discord_admin, operation):
    d = discord_admin
    result = await d.admin.execute("manage_discord_role", discord_params(operation=operation, user_id="60", role_id="80"), DISCORD_SCOPE, d.observer)
    assert result["status"] == "success"
    method = d.member.add_roles if operation == "add" else d.member.remove_roles
    assert method.await_args.args == (d.role,)
    assert method.await_args.kwargs["atomic"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["pin", "unpin", "delete"])
async def test_discord_message_actions(discord_admin, operation):
    d = discord_admin
    result = await d.admin.execute("manage_discord_message", discord_params(operation=operation, message_id="70"), DISCORD_SCOPE, d.observer)
    assert result["status"] == "success"
    getattr(d.target, operation).assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("view", ["summary", "channels", "roles", "member"])
async def test_discord_inspection(discord_admin, view):
    d = discord_admin
    result = await d.admin.execute("inspect_discord_server", discord_params(view=view, user_id="60"), DISCORD_SCOPE, d.observer)
    assert result["status"] == "success"
    assert len(result["message"]) <= 1800
    d.channel.edit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["sender", "content", "mention", "webhook", "channel", "guild", "disabled", "permission"])
async def test_discord_source_and_permissions_checked_live(discord_admin, change):
    d = discord_admin
    if change == "sender": d.source.author.id = 90
    if change == "content": d.source.content = "edited"
    if change == "mention": d.source.mentions = []
    if change == "webhook": d.source.webhook_id = 100
    if change == "channel": d.observer.allowed_channel_ids = {"99"}
    if change == "guild": d.guild.id = 99
    if change == "disabled": d.admin.config.DISCORD_ADMIN_ENABLED = False
    if change == "permission": d.actor.guild_permissions = discord.Permissions.none()
    with pytest.raises(ValueError):
        await d.admin.execute("manage_discord_channel", discord_params(operation="topic", value="New topic"), DISCORD_SCOPE, d.observer)
    d.channel.edit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("user_id", ["50", "30", "99", "bad", "-1", str(2**64)])
async def test_discord_protected_member_ids(discord_admin, user_id):
    d = discord_admin
    with pytest.raises(ValueError):
        await d.admin.execute("moderate_discord_member", discord_params(operation="ban", user_id=user_id), DISCORD_SCOPE, d.observer)
    d.guild.ban.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["member_order", "role_order", "managed", "allowlist", "permissions"])
async def test_discord_role_constraints(discord_admin, change):
    d = discord_admin
    if change == "member_order": d.member.top_role.position = 20
    if change == "role_order": d.role.position = 20
    if change == "managed": d.role.managed = True
    if change == "allowlist": d.admin.config.DISCORD_MANAGED_ROLE_IDS = "81"
    if change == "permissions":
        d.actor.guild_permissions = discord.Permissions(manage_roles=True)
        d.role.permissions = discord.Permissions(administrator=True)
    with pytest.raises(ValueError):
        await d.admin.execute("manage_discord_role", discord_params(operation="add", user_id="60", role_id="80"), DISCORD_SCOPE, d.observer)
    d.member.add_roles.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [-1, 21601, True, "60", None])
async def test_discord_slowmode_validation(discord_admin, seconds):
    d = discord_admin
    with pytest.raises(ValueError):
        await d.admin.execute("manage_discord_channel", discord_params(operation="slowmode", seconds=seconds), DISCORD_SCOPE, d.observer)
    d.channel.edit.assert_not_awaited()


@pytest.mark.asyncio
async def test_discord_uncertain_action_and_conflicting_request(discord_admin):
    d = discord_admin
    d.channel.edit.side_effect = TimeoutError("lost response")
    params = discord_params(operation="topic", value="Welcome")
    assert (await d.admin.execute("manage_discord_channel", params, DISCORD_SCOPE, d.observer))["status"] == "uncertain"
    restarted = DiscordAdmin(d.admin.config, d.admin.receipts.db_path)
    assert (await restarted.execute("manage_discord_channel", params, DISCORD_SCOPE, d.observer))["status"] == "uncertain"
    other = await restarted.execute("manage_discord_channel", {**params, "value": "Different"}, DISCORD_SCOPE, d.observer)
    assert other["status"] == "failure" and other["replayed"]
    d.channel.edit.assert_awaited_once()


@pytest.mark.asyncio
async def test_discord_concurrent_requests_share_receipt(discord_admin):
    d = discord_admin
    params = discord_params(operation="topic", value="Welcome")
    results = await asyncio.gather(*(d.admin.execute("manage_discord_channel", params, DISCORD_SCOPE, d.observer) for _ in range(2)))
    assert all(result["status"] == "success" for result in results)
    d.channel.edit.assert_awaited_once()


@pytest.mark.asyncio
async def test_traditional_processor_reports_actual_discord_result():
    registry = ToolRegistry()
    registry.register_tool(ManageDiscordChannelTool())
    reply = Mock(name="reply", parameters_schema={})
    reply.name = "send_discord_reply"
    reply.execute = AsyncMock(return_value={"status": "success"})
    registry.register_tool(reply)
    context = ActionContext()
    context.discord_observer = SimpleNamespace(can_reply=lambda *args: True)
    context.discord_admin = SimpleNamespace(execute=AsyncMock(return_value={"status": "success", "message": "Topic updated", "receipt_id": "receipt123456789"}))
    engine = AsyncMock()
    engine.make_decision.return_value = DecisionResult([
        ActionPlan("send_discord_reply", {"content": "guessed result"}, "test", 1),
        ActionPlan("manage_discord_channel", discord_params(operation="topic", value="Welcome"), "test", 5),
    ], "", "", "cycle")
    processor = TraditionalProcessor(engine, registry, Mock(), AsyncMock(), context, policy())
    await processor.process_payload({"current_processing_channel_id": "20", "channels": {"20": {
        "type": "discord", "recent_messages": [{"id": "40", "sender_id": "50", "content": "Update topic"}],
    }}}, ["20"])
    context.discord_admin.execute.assert_awaited_once()
    assert reply.execute.await_args.args[0]["content"] == "Topic updated\n\nReceipt: receipt12345"
    assert reply.execute.await_args.args[0]["channel_id"] == "20"


def ai_policy(*, moderation=False):
    return CapabilityPolicy("matrix_steward", [ROOM, CONTROL], CONTROL, [OWNER], [ROOM], ["20"], ["50"],
                            discord_admin_enabled=True, ai_control_enabled=True, ai_moderation_enabled=moderation)


@pytest.mark.parametrize("tool,params,scope", [
    ("inspect_matrix_room", {"room_id": ROOM}, replace(MATRIX_SCOPE, channel_id=ROOM, latest_sender_id="@guest:test")),
    ("manage_matrix_room", {"room_id": ROOM, "operation": "topic"}, replace(MATRIX_SCOPE, channel_id=ROOM, latest_sender_id="@guest:test")),
    ("manage_matrix_message", {"room_id": ROOM, "operation": "pin"}, replace(MATRIX_SCOPE, channel_id=ROOM, latest_sender_id="@guest:test")),
    ("manage_discord_channel", {"operation": "topic"}, replace(DISCORD_SCOPE, latest_sender_id="60")),
    ("manage_discord_message", {"operation": "pin"}, replace(DISCORD_SCOPE, latest_sender_id="60")),
    ("inspect_discord_server", {}, replace(DISCORD_SCOPE, latest_sender_id="60")),
])
def test_ai_role_grants_routine_control_from_community_events(tool, params, scope):
    p = ai_policy()
    params = {**params, "source_event_id": scope.latest_event_id}
    assert tool in p.filter_tool_names({tool}, scope)
    assert p.denial_reason(tool, params, scope) is None
    assert p.ai_controls(tool, params, scope)
    assert p.denial_reason(tool, {**params, "source_event_id": "old"}, scope)
    assert p.denial_reason(tool, params, replace(scope, channel_id="other"))
    assert not p.ai_controls(tool, params, replace(scope, channel_id="other"))
    assert p.control_context(scope)["mode"] == "ai"


@pytest.mark.parametrize("tool,operation,scope", [
    ("moderate_matrix_member", "ban", replace(MATRIX_SCOPE, channel_id=ROOM, latest_sender_id="@guest:test")),
    ("manage_matrix_message", "redact", replace(MATRIX_SCOPE, channel_id=ROOM, latest_sender_id="@guest:test")),
    ("moderate_discord_member", "timeout", replace(DISCORD_SCOPE, latest_sender_id="60")),
    ("manage_discord_role", "add", replace(DISCORD_SCOPE, latest_sender_id="60")),
    ("manage_discord_message", "delete", replace(DISCORD_SCOPE, latest_sender_id="60")),
])
def test_ai_moderation_follows_its_configured_authority(tool, operation, scope):
    params = {"source_event_id": scope.latest_event_id, "room_id": ROOM, "operation": operation}
    assert ai_policy().denial_reason(tool, params, scope)
    assert ai_policy(moderation=True).denial_reason(tool, params, scope) is None


def test_matrix_ai_actions_stay_in_current_room():
    p = ai_policy()
    p.managed_room_ids |= {"!other:test"}
    scope = replace(MATRIX_SCOPE, channel_id=ROOM, latest_sender_id="@guest:test")
    assert p.denial_reason("manage_matrix_room", {"room_id": "!other:test", "source_event_id": "$request"}, scope)


@pytest.mark.asyncio
async def test_discord_ai_uses_bot_authority_without_owner_mention(discord_admin):
    d = discord_admin
    d.admin.config.AI_CHAT_CONTROL_ENABLED = True
    d.source.author.id, d.source.mentions = 60, []
    d.observer._requests["40"] = ("20", "60", d.source.content)
    scope = replace(DISCORD_SCOPE, latest_sender_id="60")
    result = await d.admin.execute("manage_discord_channel", discord_params(operation="topic", value="Community"), scope, d.observer, autonomous=True)
    assert result["status"] == "success"
    assert d.channel.edit.await_args.kwargs["reason"].startswith("RatiChat AI at event 40:")
    assert [call.args[0] for call in d.guild.fetch_member.await_args_list] == [30]


@pytest.mark.asyncio
async def test_discord_ai_still_checks_bot_permission(discord_admin):
    d = discord_admin
    d.admin.config.AI_CHAT_CONTROL_ENABLED = True
    d.bot.guild_permissions = discord.Permissions(view_channel=True)
    with pytest.raises(ValueError, match="manage_channels"):
        await d.admin.execute("manage_discord_channel", discord_params(operation="topic", value="Community"), DISCORD_SCOPE, d.observer, autonomous=True)
    d.channel.edit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_discord_ai_can_moderate_source_member_when_enabled(discord_admin, enabled):
    d = discord_admin
    d.admin.config.AI_CHAT_CONTROL_ENABLED = True
    d.admin.config.AI_CHAT_MODERATION_ENABLED = enabled
    d.source.author.id, d.source.mentions = 60, []
    d.observer._requests["40"] = ("20", "60", d.source.content)
    scope = replace(DISCORD_SCOPE, latest_sender_id="60")
    params = discord_params(operation="timeout", user_id="60", seconds=60)
    if enabled:
        result = await d.admin.execute("moderate_discord_member", params, scope, d.observer, autonomous=True)
        assert result["status"] == "success"
        d.member.timeout.assert_awaited_once()
    else:
        with pytest.raises(ValueError, match="owner request"):
            await d.admin.execute("moderate_discord_member", params, scope, d.observer, autonomous=True)
        d.member.timeout.assert_not_awaited()


@pytest.mark.asyncio
async def test_matrix_ai_inspects_and_pins_from_public_conversation(matrix):
    admin, observer, calls, state = matrix
    admin.config.AI_CHAT_CONTROL_ENABLED = True
    admin.config.PUBLIC_MATRIX_ROOM_IDS = ROOM
    state["source_sender"] = "@guest:test"
    scope = replace(MATRIX_SCOPE, channel_id=ROOM, latest_sender_id="@guest:test")
    assert (await admin.execute("inspect_matrix_room", matrix_params(), scope, observer, autonomous=True))["status"] == "success"
    assert (await admin.execute("manage_matrix_message", matrix_params(operation="pin", event_id="$target"), scope, observer, autonomous=True))["status"] == "success"
    assert json.loads(next(call.content for call in calls if call.method == "PUT")) == {"pinned": ["$old", "$target"]}


@pytest.mark.asyncio
async def test_matrix_ai_keeps_moderation_with_owner_in_routine_mode(matrix):
    admin, observer, calls, state = matrix
    admin.config.AI_CHAT_CONTROL_ENABLED = True
    with pytest.raises(ValueError, match="owner request"):
        await admin.execute("moderate_matrix_member", matrix_params(operation="ban", user_id="@guest:test"), MATRIX_SCOPE, observer, autonomous=True)
    assert calls == []
