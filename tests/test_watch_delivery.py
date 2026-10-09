"""Saved watch delivery uses the approved platform and a stable receipt key."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from chatbot.core.node_system.watch_delivery import WatchDelivery
from chatbot.core.world_state import WorldStateManager


@pytest.mark.asyncio
async def test_matrix_watch_reuses_transaction_and_records_bot_context():
    world = WorldStateManager()
    world.add_channel("!room:server", "matrix", "room")
    observer = SimpleNamespace(send_message=AsyncMock(side_effect=[
        {"success": False, "status": "unknown"}, {"success": True, "event_id": "$saved"},
    ]))
    delivery = WatchDelivery(SimpleNamespace(matrix_observer=observer, world_state_manager=world),
                             {"matrix": {"!room:server"}})
    args = ("watch1", "matrix", "!room:server", "New entries", "key1")
    assert (await delivery.send(*args))["status"] == "unknown"
    assert (await delivery.reconcile(*args))["message_id"] == "$saved"
    assert {call.kwargs["tx_id"] for call in observer.send_message.await_args_list} == {"watch:key1"}
    saved = world.get_channel("!room:server").recent_messages[-1]
    assert saved.content == "New entries" and saved.metadata["is_bot"]


@pytest.mark.asyncio
async def test_removed_watch_channel_holds_receipt_and_skips_send():
    observer = SimpleNamespace(send_digest=AsyncMock(), reconcile_digest=AsyncMock())
    delivery = WatchDelivery(SimpleNamespace(discord_observer=observer), {"discord": {"20"}})
    args = ("watch1", "discord", "private", "New entries", "key1")
    assert (await delivery.send(*args))["status"] == "failure"
    assert (await delivery.reconcile(*args))["status"] == "unknown"
    observer.send_digest.assert_not_awaited()
    observer.reconcile_digest.assert_not_awaited()
