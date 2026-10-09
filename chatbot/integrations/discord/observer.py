"""Observe configured Discord conversations and deliver selected replies."""

import asyncio
import hashlib
import logging
import time
from collections import OrderedDict
from dataclasses import asdict

import discord

from ...config import settings
from ...core.world_state.structures import Message
from ..base import Integration, IntegrationConnectionError
from ..telegram.observer import MessageRateLimiter, parse_id_allowlist

logger = logging.getLogger(__name__)


class DiscordObserver(Integration):
    def __init__(self, world_state_manager, config=None):
        super().__init__("discord", "Discord", {})
        config = config or settings
        self.world_state = world_state_manager
        self.awareness_store = None
        self.intake_started_at = time.time()
        self.message_content_enabled = getattr(config, "DISCORD_MESSAGE_CONTENT_ENABLED", False)
        self.participation_enabled = getattr(config, "CONVERSATION_PARTICIPATION_ENABLED", True)
        self.token = config.DISCORD_BOT_TOKEN
        self.allowed_guild_ids = parse_id_allowlist(config.DISCORD_ALLOWED_GUILD_IDS)
        self.allowed_channel_ids = parse_id_allowlist(config.DISCORD_ALLOWED_CHANNEL_IDS)
        self.max_message_chars = config.DISCORD_MAX_MESSAGE_CHARS
        if not 1 <= self.max_message_chars <= 4000:
            raise ValueError("DISCORD_MAX_MESSAGE_CHARS must be between 1 and 4000")
        self._rate_limiter = MessageRateLimiter(config.DISCORD_MESSAGE_RATE_LIMIT_PER_MINUTE)
        self.on_state_change = None
        self.client = None
        self._task = None
        self._ready = asyncio.Event()
        self._send_lock = asyncio.Lock()
        self._requests = OrderedDict()
        self._replies = OrderedDict()
        self._digest_receipts = OrderedDict()

    @property
    def integration_type(self):
        return "discord"

    @property
    def enabled(self):
        return bool(self.token and self.allowed_guild_ids and self.allowed_channel_ids)

    async def connect(self):
        if not self.enabled:
            raise IntegrationConnectionError("Configure the Discord token, server IDs, and channel IDs")
        if self._task:
            return
        intents = discord.Intents.none()
        intents.guilds = True
        intents.guild_messages = True
        intents.message_content = self.message_content_enabled
        self.client = discord.Client(
            intents=intents, allowed_mentions=discord.AllowedMentions.none(),
            max_messages=100, member_cache_flags=discord.MemberCacheFlags.none(),
        )

        @self.client.event
        async def on_ready():
            self._ready.set()
            self.world_state.update_system_status({"discord_connected": True})
            logger.info("Discord connected")

        @self.client.event
        async def on_disconnect():
            self.world_state.update_system_status({"discord_connected": False})

        @self.client.event
        async def on_message(message):
            await self._handle_message(message)

        @self.client.event
        async def on_raw_message_edit(payload):
            await self._handle_message_edit(payload)

        @self.client.event
        async def on_raw_message_delete(payload):
            self._handle_message_delete(payload)

        @self.client.event
        async def on_raw_bulk_message_delete(payload):
            for message_id in payload.message_ids:
                self._delete_observation(payload.guild_id, payload.channel_id, message_id)

        @self.client.event
        async def on_error(event, *args, **kwargs):
            logger.error("Discord event handler failed: %s", event)

        async def run_client():
            try:
                async with self.client:
                    await self.client.start(self.token, reconnect=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error("Discord connection failed; check the bot token and channel permissions")
            finally:
                self.world_state.update_system_status({"discord_connected": False})

        self._task = asyncio.create_task(run_client())
        ready_task = asyncio.create_task(self._ready.wait())
        try:
            await asyncio.wait(
                {self._task, ready_task}, timeout=30,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not self._ready.is_set() or self._task.done():
                await self.disconnect()
                raise IntegrationConnectionError("Discord connection needs a valid bot token and channel permissions")
        finally:
            ready_task.cancel()
            await asyncio.gather(ready_task, return_exceptions=True)

    async def disconnect(self):
        if self.client:
            await self.client.close()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        self.client = None
        self._ready.clear()
        self.world_state.update_system_status({"discord_connected": False})

    async def start(self):
        await self.connect()

    async def stop(self):
        await self.disconnect()

    async def get_status(self):
        return {
            "enabled": self.enabled,
            "connected": bool(self.client and self.client.is_ready()),
            "allowed_guild_ids": sorted(self.allowed_guild_ids),
            "allowed_channel_ids": sorted(self.allowed_channel_ids),
        }

    async def test_connection(self):
        return bool(self.client and self.client.is_ready())

    def can_reply(self, channel_id, message_id):
        request = self._requests.get(str(message_id))
        return bool(request and request[0] == str(channel_id) and str(message_id) not in self._replies)

    def _configured_source(self, guild_id, channel_id):
        return str(guild_id) in self.allowed_guild_ids and str(channel_id) in self.allowed_channel_ids

    @staticmethod
    def _content(text, embeds=()):
        parts = [text.strip()]
        for embed in list(embeds)[:3]:
            data = embed if isinstance(embed, dict) else embed.to_dict()
            parts.extend(str(data.get(key, ""))[:1500] for key in ("title", "description", "url"))
            for field in data.get("fields", [])[:6]:
                parts.append(str(field.get("name", ""))[:150] + ": " + str(field.get("value", ""))[:500])
        return "\n".join(part for part in parts if part).strip()[:4000]

    def _record_message(self, message, content, guild_id, channel_id, mentioned):
        reference = getattr(message, "reference", None)
        timestamp = message.created_at.timestamp()
        return Message(
            id=str(message.id), channel_id=channel_id, channel_type="discord",
            sender=str(message.author.id), sender_display_name=message.author.display_name,
            sender_username=message.author.name, content=content, timestamp=timestamp,
            reply_to=str(reference.message_id) if reference and reference.message_id else None,
            metadata={"guild_id": guild_id, "bot_mentioned": mentioned,
                      "is_bot": bool(message.author.bot or message.webhook_id), "is_self": False,
                      "webhook_id": str(message.webhook_id) if message.webhook_id else None,
                      "conversation_candidate": self.participation_enabled,
                      "request_content": content,
                      "raw_content": message.content[:4000],
                      "historical": bool(getattr(message, "historical", False) or timestamp < self.intake_started_at)},
        )

    def _save_observation(self, message):
        if self.awareness_store:
            try:
                return self.awareness_store.ingest_message("discord", message.channel_id, asdict(message))
            except Exception:
                logger.warning("Discord awareness intake needs another save attempt")

    async def _handle_message_edit(self, payload):
        if not self._configured_source(payload.guild_id, payload.channel_id):
            return
        data = payload.data
        if not isinstance(data, dict) or (not isinstance(data.get("content"), str) and "embeds" not in data):
            return
        author = data.get("author")
        message = getattr(payload, "message", None) or getattr(payload, "cached_message", None)
        if not isinstance(author, dict) or not author.get("id"):
            if not message or not getattr(message, "author", None):
                return
            author = {"id": str(message.author.id), "bot": message.author.bot}
        if self.client and self.client.user and str(author["id"]) == str(self.client.user.id):
            return
        original = getattr(payload, "cached_message", None)
        if original and str(original.author.id) != str(author["id"]):
            return
        saved_channel = self.world_state.get_channel(str(payload.channel_id))
        if saved_channel and any(item.id == str(payload.message_id) and item.sender != str(author["id"])
                                 for item in saved_channel.recent_messages):
            return
        edited_at = data.get("edited_timestamp")
        if isinstance(edited_at, str):
            from datetime import datetime
            try:
                revision = datetime.fromisoformat(edited_at.replace("Z", "+00:00")).timestamp()
            except ValueError:
                return
        else:
            stamp = getattr(message, "edited_at", None)
            if stamp is None:
                return
            revision = stamp.timestamp()
        if self.awareness_store:
            try:
                raw_content = data.get("content", getattr(message, "content", ""))
                content = self._content(raw_content, data.get("embeds", getattr(message, "embeds", [])))
                result = self.awareness_store.edit_message("discord", str(payload.channel_id), str(payload.message_id),
                    {"sender": str(author["id"]), "content": content,
                     "source_revision": revision, "metadata": {"edited_at": revision, "raw_content": raw_content[:4000],
                                                               "request_content": content}})
                if result.get("changed"):
                    self._requests.pop(str(payload.message_id), None)
                    if saved_channel:
                        for item in saved_channel.recent_messages:
                            if item.id == str(payload.message_id):
                                item.content = content
                                item.metadata.update(edited_at=revision, raw_content=raw_content[:4000], request_content=content)
            except Exception:
                logger.warning("Discord message edit needs another save attempt")

    def _delete_observation(self, guild_id, channel_id, message_id):
        if not self._configured_source(guild_id, channel_id):
            return
        self._requests.pop(str(message_id), None)
        channel = self.world_state.get_channel(str(channel_id))
        if channel:
            for item in channel.recent_messages:
                if item.id == str(message_id):
                    item.content = ""
                    item.image_urls = []
                    item.arweave_media_attachments = []
                    item.metadata = {"deleted": True, "historical": True}
        if self.awareness_store:
            try:
                self.awareness_store.delete_message("discord", str(channel_id), str(message_id), timestamp=time.time())
            except Exception:
                logger.warning("Discord message deletion needs another save attempt")

    def _handle_message_delete(self, payload):
        self._delete_observation(payload.guild_id, payload.channel_id, payload.message_id)

    async def _handle_message(self, message):
        if not self.client or not self.client.user or not message.guild:
            return
        channel_id, guild_id = str(message.channel.id), str(message.guild.id)
        if not self._configured_source(guild_id, channel_id):
            return
        if message.author.id == self.client.user.id:
            return
        if not self.participation_enabled and (message.author.bot or message.webhook_id):
            return
        content = self._content(message.content, getattr(message, "embeds", []))
        message_id = str(message.id)
        mentioned = any(user.id == self.client.user.id for user in message.mentions)
        record = self._record_message(message, content[:4000], guild_id, channel_id, mentioned)
        if content:
            saved = self._save_observation(record)
            if isinstance(saved, dict) and saved.get("deleted"):
                return
        if (not mentioned and not self.participation_enabled) or record.metadata["historical"]:
            return
        if not content or len(message.content) > self.max_message_chars or message_id in self._requests:
            return
        if not self._rate_limiter.allow(f"{channel_id}:{message.author.id}"):
            return
        self._requests[message_id] = (channel_id, str(message.author.id), content)
        while len(self._requests) > 1000:
            self._requests.popitem(last=False)
        if not self.world_state.get_channel(channel_id):
            self.world_state.add_channel(channel_id, "discord", message.channel.name)
        self.world_state.add_message(channel_id, record)
        if self.on_state_change:
            self.on_state_change()

    def restore_request(self, channel_id, source):
        """Restore an observed conversation event from its saved intake record."""
        if (channel_id in self.allowed_channel_ids
                and source.get("metadata", {}).get("guild_id") in self.allowed_guild_ids
                and (source.get("metadata", {}).get("bot_mentioned")
                     or source.get("metadata", {}).get("conversation_candidate"))):
            content = source["metadata"].get("request_content", source["metadata"].get("raw_content", source["content"]))
            self._requests[source["id"]] = (channel_id, source["sender"], content)

    async def reconcile_reply(self, channel_id, reply_to_id, content):
        """Find the actual bot reply after a send lost its response."""
        if reply_to_id in self._replies:
            return {"status": "success", "message_id": self._replies[reply_to_id]}
        if not self.client or not self.client.is_ready() or channel_id not in self.allowed_channel_ids:
            return {"status": "unknown"}
        channel = self.client.get_channel(int(channel_id))
        if not channel or str(channel.guild.id) not in self.allowed_guild_ids:
            return {"status": "unknown"}
        try:
            async for message in channel.history(limit=100, after=discord.Object(id=int(reply_to_id)), oldest_first=True):
                reference = getattr(message, "reference", None)
                if (message.author.id == self.client.user.id
                        and reference and str(reference.message_id) == reply_to_id
                        and message.content == content):
                    self._replies[reply_to_id] = str(message.id)
                    return {"status": "success", "message_id": str(message.id)}
        except Exception:
            logger.warning("Discord reply receipt needs another check")
        return {"status": "unknown"}

    async def send_digest(self, channel_id, content, delivery_id):
        """Send an owner-created watch update to its fixed channel."""
        channel_id = str(channel_id)
        if not self.client or not self.client.is_ready():
            return {"status": "failure", "error": "Connect Discord first"}
        if channel_id not in self.allowed_channel_ids:
            return {"status": "failure", "error": "Choose a configured watch channel"}
        if not isinstance(content, str) or not content.strip() or len(content) > 2000:
            return {"status": "failure", "error": "Use watch text up to 2000 characters"}
        async with self._send_lock:
            if delivery_id in self._digest_receipts:
                return {"status": "success", "message_id": self._digest_receipts[delivery_id]}
            channel = self.client.get_channel(int(channel_id))
            if channel is None or str(channel.guild.id) not in self.allowed_guild_ids:
                return {"status": "failure", "error": "Choose a configured server channel"}
            try:
                sent = await channel.send(content, allowed_mentions=discord.AllowedMentions.none(),
                    nonce=hashlib.sha256(delivery_id.encode()).hexdigest()[:24])
                self._digest_receipts[delivery_id] = str(sent.id)
                while len(self._digest_receipts) > 1000:
                    self._digest_receipts.popitem(last=False)
                return {"status": "success", "message_id": str(sent.id)}
            except (discord.Forbidden, discord.NotFound):
                return {"status": "failure", "error": "Check watch channel access"}
            except discord.HTTPException as error:
                return {"status": "failure" if error.status < 500 else "unknown"}
            except Exception:
                return {"status": "unknown"}

    async def reconcile_digest(self, channel_id, content, delivery_id):
        channel_id = str(channel_id)
        if delivery_id in self._digest_receipts:
            return {"status": "success", "message_id": self._digest_receipts[delivery_id]}
        if not self.client or not self.client.is_ready() or channel_id not in self.allowed_channel_ids:
            return {"status": "unknown"}
        channel = self.client.get_channel(int(channel_id))
        if channel is None or str(channel.guild.id) not in self.allowed_guild_ids:
            return {"status": "unknown"}
        try:
            async for message in channel.history(limit=100):
                if message.author.id == self.client.user.id and message.content == content:
                    self._digest_receipts[delivery_id] = str(message.id)
                    return {"status": "success", "message_id": str(message.id)}
        except Exception:
            logger.warning("Discord watch receipt needs another check")
        return {"status": "unknown"}

    async def send_reply(self, channel_id, content, reply_to_id, delivery_id=None):
        channel_id, reply_to_id = str(channel_id), str(reply_to_id)
        if not self.client or not self.client.is_ready():
            return {"status": "failure", "error": "Connect Discord first"}
        if not isinstance(content, str) or not content.strip():
            return {"status": "failure", "error": "Provide reply text"}
        async with self._send_lock:
            if reply_to_id in self._replies:
                return {"status": "success", "duplicate": True, "message_id": self._replies[reply_to_id]}
            if channel_id not in self.allowed_channel_ids or not self.can_reply(channel_id, reply_to_id):
                return {"status": "failure", "retryable": False, "error": "Choose an observed Discord message in this channel"}
            try:
                channel = self.client.get_channel(int(channel_id))
                if channel is None or str(channel.guild.id) not in self.allowed_guild_ids:
                    return {"status": "failure", "retryable": False, "error": "Choose a configured Discord server channel"}
                try:
                    source = await channel.fetch_message(int(reply_to_id))
                except (discord.Forbidden, discord.NotFound):
                    return {"status": "failure", "retryable": False, "error": "Check the source message and channel access"}
                except Exception:
                    return {"status": "failure", "retryable": True, "error": "The source message needs another fetch"}
                request = self._requests[reply_to_id]
                current_content = self._content(source.content, getattr(source, "embeds", []))
                if (str(source.author.id), current_content) != request[1:] or source.author.id == self.client.user.id:
                    return {"status": "failure", "retryable": False, "error": "Use the current source message"}
                content = content.strip()
                if len(content) > 2000:
                    content = content[:1999] + "…"
                sent = await channel.send(
                    content, reference=source,
                    allowed_mentions=discord.AllowedMentions.none(),
                    mention_author=False,
                    **({"nonce": hashlib.sha256(delivery_id.encode()).hexdigest()[:24]} if delivery_id else {}),
                )
            except (discord.Forbidden, discord.NotFound):
                return {"status": "failure", "retryable": False, "error": "Check the source message and channel access"}
            except discord.HTTPException as error:
                status = "failure" if error.status < 500 else "unknown"
                return {"status": status, "retryable": error.status == 429, "error": "Discord needs another delivery attempt"}
            except Exception:
                logger.error("Discord reply failed; check channel access")
                return {"status": "unknown", "error": "Discord delivery needs a receipt check"}
            self._replies[reply_to_id] = str(sent.id)
            while len(self._replies) > 1000:
                self._replies.popitem(last=False)
            return {"status": "success", "message_id": str(sent.id)}
