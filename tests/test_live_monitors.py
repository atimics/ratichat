"""Live explorer reads, saved checkpoints and receipt-based automatic updates."""

import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from chatbot.core.node_system.live_monitors import LiveMonitorService, MonitorStore
from chatbot.core.node_system.source_watches import WatchStore
from chatbot.core.orchestration.capability_policy import CapabilityPolicy, ExecutionScope
from chatbot.tools.live_monitor_tools import CreateLiveMonitorTool
from chatbot.tools.onchain_tools import ExplorerHTTPError, OnchainReader, normalize_targets
from chatbot.config import settings


BTC = "bc1q" + "a" * 38
TRON = "T" + "A" * 33
EVM = "0x" + "a" * 40
TARGETS = [{"address": BTC, "network": "bitcoin"}]
SCOPE = {"channel_type": "discord", "channel_id": "general", "sender_id": "owner", "event_id": "request"}
ALLOWED = {"discord": ["general"], "matrix": ["!public:test"]}
OWNERS = {"discord": ["owner"], "matrix": ["@owner:test"]}


def store_at(path, clock):
    return MonitorStore(path, owner_ids=OWNERS, allowed_channels=ALLOWED, now=lambda: clock[0])


def create(store, **params):
    return store.execute_mutation("create_live_monitor", {"targets": TARGETS, **params}, SCOPE, True)


def event(index, address=BTC, network="bitcoin"):
    txid = str(index) * 64
    return {"key": network + ":" + txid, "transaction_hash": txid, "network": network,
            "url": "https://blockstream.info/tx/" + txid, "addresses": [address], "transfers": [], "timestamp": 1}


def snapshot(events=(), *, status="ok", identity="bitcoin:address:transactions"):
    return {"status": "success", "events": list(events), "streams": [{"id": identity,
        "network": "bitcoin", "address": BTC, "stream": "transactions", "status": status,
        "keys": [e["key"] for e in events], "message": "Explorer HTTP 429" if status == "error" else None}]}


@pytest.mark.parametrize("targets", [[], [{"address": EVM, "network": "bitcoin"}],
    [{"address": BTC, "network": "ethereum"}], [{"address": "https://localhost", "network": "auto"}],
    [{"address": EVM, "network": "solana"}], [{"address": EVM, "url": "https://evil.example"}]])
def test_targets_require_complete_addresses_and_known_networks(targets):
    with pytest.raises(ValueError):
        normalize_targets(targets)


@pytest.mark.asyncio
async def test_live_reader_normalizes_three_chain_formats_and_deduplicates_wallet_hops():
    txid = "a" * 64
    async def fetch(url, headers):
        if "blockstream" in url:
            return [{"txid": txid, "status": {"confirmed": True, "block_time": 1700000000},
                "vin": [{"prevout": {"scriptpubkey_address": BTC, "value": 300}}],
                "vout": [{"scriptpubkey_address": BTC, "value": 100}]}]
        if "trongrid" in url:
            return {"success": True, "data": [{"transaction_id": txid, "type": "Transfer",
                "from": TRON, "to": TRON, "value": "123", "block_timestamp": 1700000000000,
                "token_info": {"symbol": "USDT", "decimals": 6}}] if "trc20" in url else [], "meta": {}}
        row = {"block_number": 123, "timestamp": "2023-11-14T22:13:20Z", "from": {"hash": EVM}, "to": {"hash": EVM}}
        if "token-transfers" in url:
            row.update(transaction_hash="0x" + txid, token={"symbol": "USDC", "address_hash": EVM}, total={"value": "500", "decimals": "6"}, log_index=1)
        else:
            row.update(hash="0x" + txid, value="1000000000000000000", status="ok")
        return {"items": [row], "next_page_params": None}
    result = await OnchainReader(fetch).check([*TARGETS, {"address": TRON}, {"address": EVM, "network": "ethereum"},
        {"address": "0x" + "b" * 40, "network": "ethereum"}])
    assert len(result["events"]) == 3
    btc = next(e for e in result["events"] if e["network"] == "bitcoin")
    assert btc["transfers"][0]["received_raw"] == "100"
    assert btc["transfers"][0]["spent_inputs_raw"] == "300"
    eth = next(e for e in result["events"] if e["network"] == "ethereum")
    assert len(eth["transfers"]) == 2
    assert len(eth["addresses"]) == 2
    assert {e["timestamp"] for e in result["events"]} == {1700000000}


@pytest.mark.asyncio
async def test_pending_and_approval_records_stay_out_of_confirmed_transfer_evidence():
    async def fetch(url, headers):
        if "trongrid" in url:
            return {"data": [{"type": "Approval", "transaction_id": "a" * 64}] if "trc20" in url else []}
        return {"items": [{"hash": "0x" + "a" * 64, "block_number": None}], "next_page_params": None}
    result = await OnchainReader(fetch).check([{"address": TRON}, {"address": EVM, "network": "ethereum"}])
    assert result["events"] == []


@pytest.mark.asyncio
async def test_unknown_evm_network_reports_every_probed_network_and_failures():
    async def fetch(url, headers):
        if "base.blockscout" in url:
            raise ValueError("Explorer HTTP 403")
        return {"items": [], "next_page_params": None}
    result = await OnchainReader(fetch).check([{"address": EVM}])
    assert {s["network"] for s in result["streams"]} == {"ethereum", "base", "arbitrum", "optimism", "polygon"}
    assert all(s["status"] == "error" for s in result["streams"] if s["network"] == "base")


@pytest.mark.asyncio
async def test_pagination_stops_at_saved_checkpoint_and_reports_bounded_gap():
    calls = []
    async def fetch(url, headers):
        calls.append(url)
        offset = len(calls)
        return [{"txid": f"{offset:064x}", "status": {"confirmed": True, "block_time": offset}}] * 25
    reader = OnchainReader(fetch)
    result = await reader.check(TARGETS)
    assert len(calls) == 3 and result["streams"][0]["status"] == "limited"
    calls.clear()
    identity = result["streams"][0]["id"]
    result = await reader.check(TARGETS, {identity: {"keys": ["bitcoin:" + f"{2:064x}"]}})
    assert len(calls) == 2 and result["streams"][0]["status"] == "ok"


@pytest.mark.asyncio
async def test_parallel_address_reads_share_host_pacing():
    starts = []
    async def fetch(url, headers):
        starts.append(time.monotonic())
        return {"items": [], "next_page_params": None}
    reader = OnchainReader(fetch, min_request_gap=0.02)
    result = await reader.check([{"address": EVM, "network": "ethereum"}, {"address": "0x" + "b" * 40, "network": "ethereum"}])
    assert all(s["status"] == "ok" for s in result["streams"])
    assert len(starts) == 4 and all(b - a >= 0.018 for a, b in zip(starts, starts[1:]))


@pytest.mark.asyncio
async def test_rate_limit_pauses_host_and_keeps_other_networks_available():
    calls = []
    async def fetch(url, headers):
        calls.append(url)
        if "eth.blockscout" in url:
            raise ExplorerHTTPError(429, retry_after=120)
        return {"items": [], "next_page_params": None}
    reader = OnchainReader(fetch)
    result = await reader.check([{"address": EVM, "network": "ethereum"}, {"address": EVM, "network": "base"}])
    assert len([u for u in calls if "eth.blockscout" in u]) == 1
    assert all(s["status"] == "ok" for s in result["streams"] if s["network"] == "base")
    assert reader._host_backoff["eth.blockscout.com"]["until"] - time.monotonic() > 119


@pytest.mark.asyncio
async def test_key_is_used_only_when_public_history_needs_access(monkeypatch):
    monkeypatch.setattr(settings, "BLOCKSCOUT_API_KEY", "private-test-key")
    calls = []
    async def fetch(url, headers):
        calls.append((url, headers))
        return {"items": [], "next_page_params": None}
    result = await OnchainReader(fetch).check([{"address": EVM, "network": "ethereum"}])
    assert all(s["status"] == "ok" for s in result["streams"])
    assert len(calls) == 2 and all("eth.blockscout.com" in url and not headers for url, headers in calls)


@pytest.mark.asyncio
async def test_paid_chain_access_error_keeps_free_chain_and_public_reads_working(monkeypatch):
    monkeypatch.setattr(settings, "BLOCKSCOUT_API_KEY", "private-test-key")
    calls = []
    async def fetch(url, headers):
        calls.append((url, headers))
        if "base.blockscout" in url or "arbitrum.blockscout" in url or "api.blockscout.com/8453/" in url:
            raise ExplorerHTTPError(403)
        return {"items": [], "next_page_params": None}
    reader = OnchainReader(fetch)
    result = await reader.check([{"address": EVM, "network": network} for network in ("ethereum", "base", "arbitrum")])
    assert all(s["status"] == ("error" if s["network"] == "base" else "ok") for s in result["streams"])
    assert "api.blockscout.com/8453" in reader._host_backoff and "api.blockscout.com" not in reader._host_backoff
    assert sum("api.blockscout.com/8453/" in url for url, _ in calls) == 1
    assert sum("api.blockscout.com/42161/" in url for url, _ in calls) == 2
    assert all(headers == {"Authorization": "Bearer private-test-key"} for url, headers in calls if "api.blockscout.com" in url)
    assert all(not headers for url, headers in calls if "api.blockscout.com" not in url)
    assert all("private-test-key" not in s.get("message", "") for s in result["streams"])


@pytest.mark.asyncio
async def test_key_quota_pauses_shared_api_and_preserves_public_reads(monkeypatch):
    monkeypatch.setattr(settings, "BLOCKSCOUT_API_KEY", "private-test-key")
    calls = []
    async def fetch(url, headers):
        calls.append(url)
        if "api.blockscout.com" in url:
            raise ExplorerHTTPError(429, 120)
        if "base.blockscout" in url or "arbitrum.blockscout" in url:
            raise ExplorerHTTPError(403)
        return {"items": [], "next_page_params": None}
    result = await OnchainReader(fetch).check([{"address": EVM}])
    assert all(s["status"] == "ok" for s in result["streams"] if s["network"] in {"ethereum", "optimism", "polygon"})
    assert sum("api.blockscout.com" in url for url in calls) == 1


@pytest.mark.asyncio
async def test_key_fallback_keeps_the_pagination_cursor(monkeypatch):
    monkeypatch.setattr(settings, "BLOCKSCOUT_API_KEY", "private-test-key")
    calls = []
    async def fetch(url, headers):
        calls.append(url)
        if "?" in url and "eth.blockscout.com" in url:
            raise ExplorerHTTPError(403)
        if "?" in url:
            assert "block_number=9" in url
            return {"items": [], "next_page_params": None}
        return {"items": [], "next_page_params": {"block_number": 9}}
    result = await OnchainReader(fetch).check([{"address": EVM, "network": "ethereum"}])
    assert all(s["status"] == "ok" for s in result["streams"])
    assert any("api.blockscout.com/1/" in url and "block_number=9" in url for url in calls)


@pytest.mark.asyncio
async def test_baseline_is_quiet_new_activity_is_sent_once_and_survives_restart(tmp_path):
    path, clock = str(tmp_path / "monitor.db"), [1000]
    store = store_at(path, clock)
    saved = create(store)
    send = AsyncMock(return_value={"status": "success", "message_id": "posted"})
    reader = SimpleNamespace(check=AsyncMock(side_effect=[snapshot([event(1)]), snapshot([event(2), event(1)])]))
    service = LiveMonitorService(store, reader, send=send, now=lambda: clock[0], daily_lookup_budget=100)
    await service.tick()
    send.assert_not_awaited()
    clock[0] += 301
    await service.tick()
    assert send.await_count == 1
    assert "1 new confirmed" in send.call_args.args[3]
    assert "bitcoin" in send.call_args.args[3]
    restarted = store_at(path, clock)
    assert create(restarted) == saved
    second = LiveMonitorService(restarted, SimpleNamespace(check=AsyncMock(return_value=snapshot([event(2), event(1)]))), send=send, now=lambda: clock[0], daily_lookup_budget=100)
    clock[0] += 301
    await second.tick()
    assert send.await_count == 1
    assert restarted.list(SCOPE)[0]["delivery_message_id"] == "posted"


@pytest.mark.asyncio
async def test_partial_baseline_sends_one_failure_then_recovery_without_old_funds(tmp_path):
    clock = [1000]
    store = store_at(str(tmp_path / "monitor.db"), clock)
    create(store)
    send = AsyncMock(return_value={"status": "success", "message_id": "posted"})
    reader = SimpleNamespace(check=AsyncMock(side_effect=[snapshot(status="error"), snapshot(status="error"), snapshot(status="error"), snapshot([event(1)]), snapshot([event(1)])]))
    service = LiveMonitorService(store, reader, send=send, now=lambda: clock[0], daily_lookup_budget=100)
    for _ in range(5):
        await service.tick()
        clock[0] += 301
    assert send.await_count == 2
    texts = [call.args[3] for call in send.call_args_list]
    assert "429" in texts[0] and "coverage recovered" in texts[1]
    assert all("new confirmed" not in text for text in texts)


@pytest.mark.asyncio
async def test_unknown_delivery_reconciles_after_restart_before_more_checks(tmp_path):
    path, clock = str(tmp_path / "monitor.db"), [1000]
    store = store_at(path, clock)
    create(store)
    send = AsyncMock(side_effect=TimeoutError)
    reader = SimpleNamespace(check=AsyncMock(side_effect=[snapshot(), snapshot([event(2)])]))
    service = LiveMonitorService(store, reader, send=send, now=lambda: clock[0], daily_lookup_budget=100)
    await service.tick()
    clock[0] += 301
    await service.tick()
    assert store.list(SCOPE)[0]["pending_status"] == "unknown"
    restart = store_at(path, clock)
    reconcile = AsyncMock(return_value={"status": "success", "message_id": "already-posted"})
    new_reader = SimpleNamespace(check=AsyncMock(return_value=snapshot([event(2)])))
    next_service = LiveMonitorService(restart, new_reader, send=send, reconcile=reconcile, now=lambda: clock[0], daily_lookup_budget=100)
    clock[0] += 60
    await next_service.tick()
    assert reconcile.await_count == 1 and send.await_count == 1
    assert restart.list(SCOPE)[0]["pending_status"] is None


def test_feed_and_chain_pollers_have_separate_claims_budgets_and_stop_tools(tmp_path):
    path, clock = str(tmp_path / "monitor.db"), [1000]
    chain = store_at(path, clock)
    monitor = create(chain)["monitor"]["monitor_id"]
    feeds = WatchStore(path, OWNERS, ALLOWED, now=lambda: clock[0])
    feed = feeds.create(SCOPE, "https://example.com/feed", is_owner=True)
    assert feeds.claim()["id"] == feed["id"]
    assert chain.claim()["id"] == monitor
    assert feeds.reserve_lookup(SCOPE, 1) and chain.reserve_lookup(SCOPE, 1)
    assert not feeds.reserve_lookup(SCOPE, 1) and not chain.reserve_lookup(SCOPE, 1)
    assert not feeds.remove(SCOPE, monitor, True)
    assert not chain.remove(SCOPE, feed["id"], True)


def test_stale_lease_and_stopped_monitor_cannot_prepare_delivery(tmp_path):
    clock = [1000]
    store = store_at(str(tmp_path / "monitor.db"), clock)
    identity = create(store)["monitor"]["monitor_id"]
    claimed = store.claim(lease_seconds=10)
    clock[0] += 11
    assert store.save_check(claimed, snapshot([event(1)])) is None
    assert store.remove(SCOPE, identity, True)
    assert store.save_check(claimed, snapshot([event(1)])) is None


def test_page_limit_retains_last_complete_checkpoint_and_visible_gap(tmp_path):
    clock = [1000]
    store = store_at(str(tmp_path / "monitor.db"), clock)
    create(store)
    first = store.claim()
    store.save_check(first, snapshot([event(1)]))
    clock[0] += 301
    second = store.claim()
    prepared = store.save_check(second, snapshot([event(2)], status="limited"))
    store.reconcile_delivery(prepared["delivery_key"], "success", "receipt")
    saved = json.loads(store.list(SCOPE)[0]["check_state"])
    assert next(iter(saved.values()))["keys"] == [event(1)["key"]]
    clock[0] += 301
    third = store.claim()
    limited = snapshot([event(2)], status="limited")
    limited["streams"][0]["coverage"] = "Read limit reached"
    prepared = store.save_check(third, limited)
    assert "Read limit reached" in prepared["pending_text"]


@pytest.mark.asyncio
async def test_owner_and_fixed_destination_are_required_for_monitor_changes(tmp_path):
    store = store_at(str(tmp_path / "monitor.db"), [1000])
    service = LiveMonitorService(store)
    tool = CreateLiveMonitorTool()
    scope = ExecutionScope("general", "discord", frozenset({"request"}), "request", "owner")
    context = SimpleNamespace(live_monitor_service=service, execution_scope=scope)
    assert (await tool.execute({"targets": TARGETS, "channel_id": "elsewhere"}, context))["status"] == "blocked"
    context.execution_scope = ExecutionScope("general", "discord", frozenset({"request"}), "request", "member")
    assert (await tool.execute({"targets": TARGETS}, context))["status"] == "failure"
    policy = CapabilityPolicy(approved_discord_channel_ids=["general"], discord_owner_user_ids=["owner"])
    assert "create_live_monitor" in policy.filter_tool_names(["create_live_monitor"], scope)
    assert "create_live_monitor" not in policy.filter_tool_names(["create_live_monitor"], context.execution_scope)
    context.execution_scope = scope
    result = await tool.execute({"targets": TARGETS}, context)
    assert result["monitor"]["interval_minutes"] == 5
    assert store.list(SCOPE)[0]["channel_id"] == "general"
