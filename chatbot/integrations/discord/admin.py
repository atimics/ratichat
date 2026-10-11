"""Discord controls using the AI bot role or a verified owner request."""

import asyncio
from datetime import timedelta

import discord

from ..admin_actions import AdminReceipts, integer_value, text_value, result_message, check_ai_control


def snowflake(value, label):
    if not isinstance(value, str) or not value.isascii() or not value.isdigit() or not 0 < int(value) < 2**64:
        raise ValueError(f"Supply a Discord {label} ID")
    return int(value)


class DiscordAdmin:
    def __init__(self, config, db_path):
        self.config = config
        self.receipts = AdminReceipts(db_path)
        self.lock = asyncio.Lock()

    async def execute(self, tool, params, scope, observer, *, autonomous=False):
        owners = {value.strip() for value in self.config.DISCORD_OWNER_USER_IDS.split(",") if value.strip()}
        if autonomous:
            check_ai_control(self.config, tool, params)
        if (not (self.config.DISCORD_ADMIN_ENABLED or autonomous) or not scope or scope.channel_type != "discord"
                or (not autonomous and scope.latest_sender_id not in owners) or not scope.latest_event_id
                or params.get("source_event_id") != scope.latest_event_id):
            raise ValueError("Use a current owner request with Discord admin tools enabled")
        if not observer or not observer.client or not observer.client.is_ready():
            raise ValueError("Connect Discord first")
        if scope.channel_id not in observer.allowed_channel_ids:
            raise ValueError("Use a configured Discord channel")
        async with self.lock:
            channel = await observer.client.fetch_channel(snowflake(scope.channel_id, "channel"))
            if (not isinstance(channel, discord.TextChannel)
                    or str(channel.guild.id) not in observer.allowed_guild_ids):
                raise ValueError("Use a configured Discord text channel")
            guild = await observer.client.fetch_guild(channel.guild.id)
            channel = await guild.fetch_channel(channel.id)
            source = await channel.fetch_message(snowflake(scope.latest_event_id, "source message"))
            request = observer._requests.get(scope.latest_event_id)
            if (not request or request != (scope.channel_id, str(source.author.id),
                                           observer._content(source.content, source.embeds))
                    or str(source.author.id) != scope.latest_sender_id
                    or source.author.id == observer.client.user.id
                    or (not autonomous and (source.author.bot or source.webhook_id
                        or not any(user.id == observer.client.user.id for user in source.mentions)))):
                raise ValueError("Send a fresh owner request that mentions the bot")
            guild = channel.guild
            bot = await guild.fetch_member(observer.client.user.id)
            actor = bot if autonomous else await guild.fetch_member(source.author.id)
            self.require_permission(actor, bot, "view_channel", channel)
            if tool == "inspect_discord_server":
                return await self.inspect(guild, channel, actor, bot, params, observer)
            previous = self.receipts.previous("discord", scope.channel_id, scope.latest_event_id, tool, params)
            if previous:
                return previous
            reason = text_value(params.get("reason"), "reason", 300)
            audit_reason = (f"RatiChat AI at event {source.id}: {reason}" if autonomous
                            else f"RatiChat request {source.id} by {actor.id}: {reason}")
            operation = params.get("operation")
            action = await self.prepare(tool, params, guild, channel, actor, bot, owners, audit_reason)
            receipt_id, previous = self.receipts.reserve("discord", scope.channel_id, scope.latest_event_id, tool, params)
            if previous:
                return previous
            try:
                await action()
                result = {"status": "success", "message": f"Discord {operation} completed.",
                          "guild_id": str(guild.id), "channel_id": str(channel.id), "operation": operation}
            except (discord.Forbidden, discord.NotFound):
                result = {"status": "failure", "message": "Check Discord permissions, role order, and the target before a fresh request."}
            except discord.HTTPException as error:
                result = {"status": "failure" if error.status < 500 else "uncertain",
                          "message": "Check the Discord target before a fresh request.", "http_status": error.status}
            except Exception:
                result = {"status": "uncertain", "message": "Check the Discord target to confirm this change before a fresh request."}
            return self.receipts.finish(receipt_id, result)

    @staticmethod
    def require_permission(actor, bot, permission, channel=None):
        for member in (actor, bot):
            permissions = channel.permissions_for(member) if channel else member.guild_permissions
            if not getattr(permissions, permission, False):
                raise ValueError(f"The requester and bot both need {permission} permission")

    @staticmethod
    def protect_member(member, actor, bot, guild, owners):
        if member.id in {actor.id, bot.id, guild.owner_id} or str(member.id) in owners:
            raise ValueError("Choose a community member for this action")
        if member.top_role >= bot.top_role or (actor.id != guild.owner_id and member.top_role >= actor.top_role):
            raise ValueError("The requester and bot both need a higher role than the target member")

    async def prepare(self, tool, params, guild, channel, actor, bot, owners, reason):
        operation = params.get("operation")
        if tool == "manage_discord_channel":
            self.require_permission(actor, bot, "manage_channels", channel)
            if operation == "name":
                changes = {"name": text_value(params.get("value"), "channel name", 100)}
            elif operation == "topic":
                changes = {"topic": text_value(params.get("value"), "channel topic", 1024, empty=True)}
            elif operation == "slowmode":
                changes = {"slowmode_delay": integer_value(params.get("seconds"), "slowmode seconds", 0, 21600)}
            else:
                raise ValueError("Choose name, topic, or slowmode")
            return lambda: channel.edit(**changes, reason=reason)
        if tool == "manage_discord_message":
            if operation not in {"pin", "unpin", "delete"}:
                raise ValueError("Choose pin, unpin, or delete")
            self.require_permission(actor, bot, "manage_messages" if operation == "delete" else "pin_messages", channel)
            message_id = snowflake(params.get("message_id"), "message")
            if str(message_id) == params.get("source_event_id"):
                raise ValueError("Choose a message separate from the current request")
            message = await channel.fetch_message(message_id)
            if operation == "delete":
                return lambda: message.delete()
            return lambda: getattr(message, operation)(reason=reason)
        if tool not in {"moderate_discord_member", "manage_discord_role"}:
            raise ValueError("Choose a supported Discord admin tool")
        user_id = snowflake(params.get("user_id"), "user")
        if user_id in {actor.id, bot.id, guild.owner_id} or str(user_id) in owners:
            raise ValueError("Choose a community member for this action")
        if tool == "moderate_discord_member":
            permissions = {"timeout": "moderate_members", "clear_timeout": "moderate_members",
                           "kick": "kick_members", "ban": "ban_members", "unban": "ban_members"}
            if operation not in permissions:
                raise ValueError("Choose timeout, clear_timeout, kick, ban, or unban")
            self.require_permission(actor, bot, permissions[operation])
            if operation == "unban":
                return lambda: guild.unban(discord.Object(id=user_id), reason=reason)
            member = await guild.fetch_member(user_id)
            self.protect_member(member, actor, bot, guild, owners)
            if operation in {"timeout", "clear_timeout"}:
                if member.guild_permissions.administrator:
                    raise ValueError("Choose a member with a community role for timeout changes")
                until = (discord.utils.utcnow() + timedelta(seconds=integer_value(params.get("seconds"), "timeout seconds", 1, 2419200))
                         if operation == "timeout" else None)
                return lambda: member.timeout(until, reason=reason)
            if operation == "kick":
                return lambda: member.kick(reason=reason)
            return lambda: guild.ban(member, reason=reason, delete_message_seconds=0)
        if operation not in {"add", "remove"}:
            raise ValueError("Choose add or remove")
        self.require_permission(actor, bot, "manage_roles")
        role_id = snowflake(params.get("role_id"), "role")
        allowed_roles = {value.strip() for value in self.config.DISCORD_MANAGED_ROLE_IDS.split(",") if value.strip()}
        if str(role_id) not in allowed_roles:
            raise ValueError("Choose a configured managed Discord role")
        roles = await guild.fetch_roles()
        role = next((item for item in roles if item.id == role_id), None)
        if not role or role.is_default() or role.managed:
            raise ValueError("Choose an editable community role")
        if role >= bot.top_role or (actor.id != guild.owner_id and role >= actor.top_role):
            raise ValueError("The requester and bot both need a higher role than the selected role")
        if role.permissions.value & ~actor.guild_permissions.value:
            raise ValueError("Choose a role covered by the requester's own permissions")
        member = await guild.fetch_member(user_id)
        self.protect_member(member, actor, bot, guild, owners)
        method = member.add_roles if operation == "add" else member.remove_roles
        return lambda: method(role, reason=reason, atomic=True)

    async def inspect(self, guild, channel, actor, bot, params, observer):
        self.require_permission(actor, bot, "view_channel", channel)
        view = params.get("view", "summary")
        offset = integer_value(params.get("offset", 0), "offset", 0, 1000000)
        limit = integer_value(params.get("limit", 25), "limit", 1, 50)
        if view == "summary":
            data = {"guild_id": str(guild.id), "server_name": guild.name, "channel_id": str(channel.id),
                    "channel_name": channel.name, "topic": channel.topic, "slowmode_seconds": channel.slowmode_delay,
                    "bot_permissions": [name for name, enabled in channel.permissions_for(bot) if enabled]}
        elif view == "channels":
            channels = await guild.fetch_channels()
            entries = [{"channel_id": str(item.id), "name": item.name, "type": str(item.type)} for item in channels
                       if str(item.id) in observer.allowed_channel_ids and item.permissions_for(actor).view_channel
                       and item.permissions_for(bot).view_channel]
            data = {"channels": entries[offset:offset + limit], "total": len(entries),
                    "next_offset": offset + limit if offset + limit < len(entries) else None}
        elif view == "roles":
            roles = await guild.fetch_roles()
            entries = [{"role_id": str(role.id), "name": role.name, "position": role.position, "managed": role.managed}
                       for role in roles]
            data = {"roles": entries[offset:offset + limit], "total": len(entries),
                    "next_offset": offset + limit if offset + limit < len(entries) else None}
        elif view == "member":
            member = await guild.fetch_member(snowflake(params.get("user_id"), "user"))
            data = {"user_id": str(member.id), "display_name": member.display_name,
                    "role_ids": [str(role.id) for role in member.roles],
                    "timeout_until": member.timed_out_until.isoformat() if member.timed_out_until else None}
        else:
            raise ValueError("Choose summary, channels, roles, or member")
        return {"status": "success", **data, "message": result_message(data)}
