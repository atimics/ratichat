"""Bounded public explorer reads for confirmed address activity."""

import asyncio
import hashlib
import json
import re
import time
from datetime import datetime
from urllib.parse import urlencode
from urllib.parse import urlsplit

import aiohttp

from ..config import settings
from .base import ToolInterface
from .web_tools import PublicResolver


EVM_NETWORKS = {
    "ethereum": (1, "https://eth.blockscout.com", "ETH"),
    "base": (8453, "https://base.blockscout.com", "ETH"),
    "arbitrum": (42161, "https://arbitrum.blockscout.com", "ETH"),
    "optimism": (10, "https://explorer.optimism.io", "ETH"),
    "polygon": (137, "https://polygon.blockscout.com", "POL"),
}
NETWORKS = {"bitcoin", "tron", *EVM_NETWORKS}
ADDRESS_RE = re.compile(r"\b(?:0x[a-fA-F0-9]{40}|T[1-9A-HJ-NP-Za-km-z]{33}|bc1[ac-hj-np-z02-9]{25,80}|[13][a-km-zA-HJ-NP-Z1-9]{25,34})\b")
LIMITS = "Confirmed indexed activity; up to three pages per stream. EVM coverage: Ethereum, Base, Arbitrum, Optimism, Polygon. Explorer delays and coverage gaps are reported. Transaction movement needs separate evidence for theft, controller identity, or loss totals."


def normalize_targets(targets):
    if not isinstance(targets, list) or not 1 <= len(targets) <= 12:
        raise ValueError("Provide one to twelve address targets.")
    result = []
    for target in targets:
        if not isinstance(target, dict) or set(target) - {"address", "network"}:
            raise ValueError("Use address and network for each target.")
        address = target.get("address")
        if not isinstance(address, str) or not ADDRESS_RE.fullmatch(address):
            raise ValueError("Provide a complete Bitcoin, TRON, or EVM address.")
        family = "evm" if address.startswith("0x") else "tron" if address.startswith("T") else "bitcoin"
        network = target.get("network", "auto")
        if network == "auto":
            network = "auto" if family == "evm" else family
        if network not in NETWORKS | {"auto"} or (family == "evm" and network not in EVM_NETWORKS and network != "auto") or (family != "evm" and network != family):
            raise ValueError("Choose a network that matches the address format.")
        value = {"address": address.lower() if family == "evm" or address.startswith("bc1") else address, "network": network}
        if value not in result:
            result.append(value)
    return sorted(result, key=lambda t: (t["network"], t["address"]))


def _tron_address(value):
    if not isinstance(value, str) or not re.fullmatch(r"41[a-fA-F0-9]{40}", value):
        return str(value or "")[:100]
    raw = bytes.fromhex(value)
    raw += hashlib.sha256(hashlib.sha256(raw).digest()).digest()[:4]
    number, encoded = int.from_bytes(raw, "big"), ""
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    while number:
        number, index = divmod(number, 58)
        encoded = alphabet[index] + encoded
    return encoded


def _event(network, txid, url, timestamp, **fields):
    if not isinstance(txid, str) or not re.fullmatch(r"(?:0x)?[a-fA-F0-9]{64}", txid):
        raise ValueError("The explorer returned an invalid transaction ID.")
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()
    elif isinstance(timestamp, (float, int)) and timestamp > 1_000_000_000_000:
        timestamp /= 1000
    return {"key": network + ":" + txid.lower(), "network": network, "transaction_hash": txid,
            "url": url, "timestamp": timestamp, **fields}


class ExplorerHTTPError(ValueError):
    def __init__(self, status, retry_after=0, message=None):
        super().__init__(message or f"Explorer HTTP {status}. Check provider access and quota.")
        self.status = status
        try:
            self.retry_after = min(900, max(0, float(retry_after or 0)))
        except (ValueError, TypeError):
            self.retry_after = 0


class OnchainReader:
    def __init__(self, fetch=None, min_request_gap=None):
        self.fetch = fetch or self._fetch
        self._semaphore = asyncio.Semaphore(3)
        self.min_request_gap = (0 if fetch else settings.ONCHAIN_REQUEST_GAP_SECONDS) if min_request_gap is None else min_request_gap
        self._host_locks, self._host_next, self._host_backoff = {}, {}, {}

    async def _read(self, url, headers):
        parsed = urlsplit(url)
        host = parsed.hostname
        access_scope = host + "/" + parsed.path.split("/")[1] if host == "api.blockscout.com" else host
        lock = self._host_locks.setdefault(host, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            for scope in {host, access_scope}:
                backoff = self._host_backoff.get(scope, {})
                if backoff.get("until", 0) > now:
                    delay = int(backoff["until"] - now) + 1
                    raise ExplorerHTTPError(backoff["status"], delay,
                        f"Explorer HTTP {backoff['status']}; retry after {delay} seconds.")
            await asyncio.sleep(max(0, self._host_next.get(host, 0) - now))
            self._host_next[host] = time.monotonic() + max(0, self.min_request_gap)
            try:
                data = await self.fetch(url, headers)
                if isinstance(data, dict) and data.get("success") is False and data.get("statusCode") in {403, 429}:
                    raise ExplorerHTTPError(data["statusCode"])
            except ExplorerHTTPError as error:
                if error.status in {402, 403, 429}:
                    scope = host if error.status == 429 else access_scope
                    backoff = self._host_backoff.get(scope, {})
                    failures = min(5, backoff.get("failures", 0) + 1)
                    delay = min(900, max(error.retry_after, 60 * 2 ** (failures - 1)))
                    self._host_backoff[scope] = {"status": error.status, "failures": failures, "until": time.monotonic() + delay}
                raise
            self._host_backoff.pop(host, None)
            self._host_backoff.pop(access_scope, None)
            return data

    async def _history(self, network, address, kind, cursor):
        url, headers = self._request(network, address, kind, cursor)
        try:
            return await self._read(url, headers)
        except ExplorerHTTPError as public_error:
            if network not in EVM_NETWORKS or not settings.BLOCKSCOUT_API_KEY:
                raise
            url, headers = self._request(network, address, kind, cursor, use_pro=True)
            try:
                return await self._read(url, headers)
            except ExplorerHTTPError as key_error:
                raise ValueError(f"Public explorer HTTP {public_error.status}; key API HTTP {key_error.status}. Check provider access and quota.") from None

    async def _fetch(self, url, headers=None):
        connector = aiohttp.TCPConnector(resolver=PublicResolver(), use_dns_cache=False)
        async with aiohttp.ClientSession(connector=connector, trust_env=False,
            cookie_jar=aiohttp.DummyCookieJar(), timeout=aiohttp.ClientTimeout(total=15)) as client:
            async with client.get(url, headers=headers or {}, allow_redirects=False) as response:
                if response.status != 200:
                    raise ExplorerHTTPError(response.status, response.headers.get("Retry-After"))
                body = bytearray()
                async for chunk in response.content.iter_chunked(8192):
                    body.extend(chunk)
                    if len(body) > 2_000_000:
                        raise ValueError("The explorer response exceeds the read limit.")
                return json.loads(body)

    async def check(self, targets, checkpoints=None):
        targets = normalize_targets(targets)
        jobs = []
        for target in targets:
            networks = EVM_NETWORKS if target["network"] == "auto" else [target["network"]]
            for network in networks:
                kinds = ["transactions"] if network == "bitcoin" else ["transactions", "token-transfers"]
                for kind in kinds:
                    jobs.append(self._stream(network, target["address"], kind, checkpoints or {}))
        streams = await asyncio.gather(*jobs)
        events = {}
        for stream in streams:
            for event in stream.pop("events", []):
                if event["key"] not in events:
                    events[event["key"]] = event
                else:
                    prior = events[event["key"]]
                    prior["addresses"] = sorted(set(prior["addresses"] + event["addresses"]))
                    for transfer in event.get("transfers", []):
                        if transfer not in prior["transfers"]:
                            prior["transfers"].append(transfer)
        return {"status": "success" if any(s["status"] in {"ok", "limited"} for s in streams) else "failure",
                "checked_at": time.time(), "targets": targets, "streams": streams,
                "events": sorted(events.values(), key=lambda e: e.get("timestamp") or 0, reverse=True),
                "limits": LIMITS, "trust": "untrusted_source"}

    async def _stream(self, network, address, kind, checkpoints):
        identity = f"{network}:{address}:{kind}"
        base = {"id": identity, "network": network, "address": address, "stream": kind}
        previous = set(checkpoints.get(identity, {}).get("keys", []))
        events, pages, complete, cursor = [], 0, False, None
        try:
            async with self._semaphore:
                for page in range(3):
                    data = await self._history(network, address, kind, cursor)
                    rows, cursor = self._rows(network, kind, data)
                    if any(not isinstance(row, dict) for row in rows):
                        raise ValueError("The explorer returned invalid transaction records.")
                    pages += 1
                    for raw in rows:
                        event = self._normalize(network, address, kind, raw)
                        if event:
                            events.append(event)
                    if not cursor or previous.intersection(e["key"] for e in events):
                        complete = True
                        break
            return {**base, "status": "ok" if complete else "limited", "pages": pages,
                    "keys": list(dict.fromkeys(e["key"] for e in events)), "events": events,
                    "source_url": self._address_url(network, address),
                    "coverage": "Reached saved checkpoint or end of history" if complete else "Read limit reached; older activity needs a further scan"}
        except (ValueError, TypeError, KeyError, aiohttp.ClientError, asyncio.TimeoutError) as error:
            return {**base, "status": "error", "message": str(error)[:200] if isinstance(error, ValueError) else "Explorer read needs another attempt.",
                    "keys": [], "events": [], "source_url": self._address_url(network, address)}

    def _request(self, network, address, kind, cursor, *, use_pro=False):
        headers = {}
        if network == "bitcoin":
            suffix = "/txs" if cursor is None else "/txs/chain/" + cursor
            return "https://blockstream.info/api/address/" + address + suffix, headers
        if network == "tron":
            key = settings.TRONGRID_API_KEY
            if key:
                headers["TRON-PRO-API-KEY"] = key
            path = "/transactions" + ("/trc20" if kind == "token-transfers" else "")
            params = {"only_confirmed": "true", "limit": 50, "order_by": "block_timestamp,desc"}
            if cursor:
                params["fingerprint"] = cursor
            return "https://api.trongrid.io/v1/accounts/" + address + path + "?" + urlencode(params), headers
        chain_id, explorer, _ = EVM_NETWORKS[network]
        if use_pro and settings.BLOCKSCOUT_API_KEY:
            explorer = f"https://api.blockscout.com/{chain_id}"
            headers["Authorization"] = "Bearer " + settings.BLOCKSCOUT_API_KEY
        url = explorer + "/api/v2/addresses/" + address + "/" + kind
        if cursor:
            url += "?" + urlencode(cursor)
        return url, headers

    def _rows(self, network, kind, data):
        if network == "bitcoin":
            if not isinstance(data, list):
                raise ValueError("The Bitcoin explorer returned an invalid response.")
            confirmed = [r for r in data if r.get("status", {}).get("confirmed")]
            return confirmed, confirmed[-1]["txid"] if len(confirmed) >= 25 else None
        field = "data" if network == "tron" else "items"
        if not isinstance(data, dict) or not isinstance(data.get(field), list) or data.get("success") is False:
            raise ValueError("The explorer returned an invalid history response.")
        cursor = data.get("meta", {}).get("fingerprint") if network == "tron" else data.get("next_page_params")
        if cursor and ((network == "tron" and (not isinstance(cursor, str) or len(cursor) > 1000)) or
                       (network != "tron" and (not isinstance(cursor, dict) or len(json.dumps(cursor)) > 2000))):
            raise ValueError("The explorer returned an invalid page cursor.")
        return data[field], cursor

    @staticmethod
    def _address_url(network, address):
        if network == "bitcoin":
            return "https://blockstream.info/address/" + address
        if network == "tron":
            return "https://tronscan.org/#/address/" + address
        return EVM_NETWORKS[network][1] + "/address/" + address

    def _normalize(self, network, address, kind, row):
        if network == "bitcoin":
            txid = row["txid"]
            inputs = [r.get("prevout") or {} for r in row.get("vin", [])]
            outputs = row.get("vout", [])
            transfer = {"asset": "BTC", "decimals": 8,
                "received_raw": str(sum(int(r.get("value", 0)) for r in outputs if r.get("scriptpubkey_address") == address)),
                "spent_inputs_raw": str(sum(int(r.get("value", 0)) for r in inputs if r.get("scriptpubkey_address") == address)),
                "from": [r["scriptpubkey_address"] for r in inputs if r.get("scriptpubkey_address")][:16],
                "to": [r["scriptpubkey_address"] for r in outputs if r.get("scriptpubkey_address")][:16]}
            return _event(network, txid, "https://blockstream.info/tx/" + txid,
                row["status"].get("block_time"), addresses=[address], transfers=[transfer], confirmation="confirmed")
        if network == "tron":
            txid = row.get("transaction_id") or row.get("txID")
            if kind == "token-transfers":
                if row.get("type") != "Transfer":
                    return None
                token = row.get("token_info", {})
                transfers = [{"from": row.get("from"), "to": row.get("to"), "amount_raw": str(row.get("value", "")),
                    "asset": str(token.get("symbol", "TRC20"))[:40], "contract": token.get("address"), "decimals": token.get("decimals")}]
            else:
                transfers = []
                for contract in row.get("raw_data", {}).get("contract", [])[:10]:
                    value = contract.get("parameter", {}).get("value", {})
                    if contract.get("type") in {"TransferContract", "TransferAssetContract"}:
                        transfers.append({"from": _tron_address(value.get("owner_address")), "to": _tron_address(value.get("to_address")),
                            "amount_raw": str(value.get("amount", "")), "asset": "TRX" if contract["type"] == "TransferContract" else "TRC10",
                            "decimals": 6 if contract["type"] == "TransferContract" else None})
            return _event(network, txid, "https://tronscan.org/#/transaction/" + txid,
                row.get("block_timestamp"), addresses=[address], transfers=transfers, confirmation="confirmed")
        if row.get("block_number") is None:
            return None
        txid = row.get("transaction_hash") or row.get("hash")
        if kind == "token-transfers":
            token, total = row.get("token") or {}, row.get("total") or {}
            transfer = {"asset": str(token.get("symbol") or "token")[:40], "contract": token.get("address_hash"),
                "amount_raw": str(total.get("value", "")), "decimals": total.get("decimals", token.get("decimals")), "log_index": row.get("log_index")}
        else:
            transfer = {"asset": EVM_NETWORKS[network][2], "amount_raw": str(row.get("value", "")), "decimals": 18}
        transfer.update({key: (row.get(key) or {}).get("hash") for key in ("from", "to")})
        return _event(network, txid, EVM_NETWORKS[network][1] + "/tx/" + txid,
            row.get("timestamp"), addresses=[address], transfers=[transfer], confirmation="confirmed", execution_status=row.get("status"))


class CheckOnchainActivityTool(ToolInterface):
    name = "check_onchain_activity"
    description = "Read live confirmed Bitcoin, TRON and EVM address transactions and token transfers. Use complete addresses from the request. Return source links and per-network coverage. Use auto for EVM network discovery across the listed networks."
    parameters_schema = {"targets": "array of objects with address and network (auto, bitcoin, tron, ethereum, base, arbitrum, optimism, polygon); up to 12", "fresh": "boolean - fetch live evidence again"}

    async def execute(self, params, context):
        try:
            if not isinstance(params, dict) or set(params) - set(self.parameters_schema):
                raise ValueError("Use the listed address read parameters.")
            reader = getattr(context, "onchain_reader", None) or OnchainReader()
            result = await reader.check(params.get("targets"))
            count = len(result["events"])
            result["streams"] = [{k: v for k, v in stream.items() if k not in {"keys", "id"}} for stream in result["streams"]]
            result["events"] = [{**event, "transfers": event.get("transfers", [])[:8],
                "transfers_truncated": len(event.get("transfers", [])) > 8} for event in result["events"][:40]]
            while result["events"] and len(json.dumps(result).encode()) > 60_000:
                result["events"].pop()
            return {**result, "event_count": count, "truncated": count > len(result["events"])}
        except (ValueError, TypeError) as error:
            return {"status": "failure", "message": str(error)}
