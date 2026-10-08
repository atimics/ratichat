"""Public address guards, feed parsing, and linked search credentials."""

import asyncio
import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from chatbot.tools.web_tools import PublicResolver, ReadFeedTool, ReadWebpageTool, WebSearchTool, fetch_public_text, public_url


@pytest.mark.parametrize("url", [
    "http://127.0.0.1", "http://10.0.0.1", "http://169.254.169.254/latest",
    "http://[::1]", "http://[::ffff:127.0.0.1]", "http://100.64.0.1",
    "http://224.0.0.1", "file:///etc/passwd", "https://user:password@example.com",
    "http://example.com:8000",
])
def test_source_urls_require_public_standard_http(url):
    with pytest.raises(ValueError):
        public_url(url)


@pytest.mark.asyncio
@pytest.mark.parametrize("host", ["0x7f000001", "2130706433", "127.1"])
async def test_nonstandard_loopback_spelling_is_checked_at_resolution(host):
    with pytest.raises(ValueError):
        await PublicResolver().resolve(host, 80)


@pytest.mark.asyncio
@pytest.mark.parametrize("address", ["127.0.0.1", "10.0.0.1", "169.254.169.254", "::1"])
async def test_dns_address_used_by_connection_is_checked(monkeypatch, address):
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", AsyncMock(return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, 80))]))
    with pytest.raises(ValueError):
        await PublicResolver().resolve("example.com", 80)


@pytest.mark.asyncio
async def test_public_dns_resolution_returns_only_checked_addresses(monkeypatch):
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", AsyncMock(return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))]))
    addresses = await PublicResolver().resolve("example.com", 443)
    assert addresses[0]["host"] == "93.184.216.34"


class Response:
    def __init__(self, status=200, headers=None, body=b"page", kind="text/html"):
        self.status, self.headers = status, headers or {}
        self.content_type, self.charset = kind, "utf-8"
        self.content = SimpleNamespace(iter_chunked=self.chunks)
        self.body = body
    async def chunks(self, size):
        yield self.body
    def raise_for_status(self):
        pass
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        pass


class Session:
    def __init__(self, responses):
        self.get = Mock(side_effect=responses)
    async def __aenter__(self):
        return self
    async def __aexit__(self, *args):
        pass


@pytest.mark.asyncio
async def test_redirect_to_private_address_stops_before_second_request():
    session = Session([Response(302, {"Location": "http://169.254.169.254/latest"})])
    with patch("chatbot.tools.web_tools.aiohttp.ClientSession", return_value=session):
        with pytest.raises(ValueError):
            await fetch_public_text("https://example.com")
    session.get.assert_called_once()


@pytest.mark.asyncio
async def test_source_size_limit_is_enforced():
    session = Session([Response(body=b"x" * 512001)])
    with patch("chatbot.tools.web_tools.aiohttp.ClientSession", return_value=session):
        with pytest.raises(ValueError):
            await fetch_public_text("https://example.com")


@pytest.mark.asyncio
async def test_webpage_extracts_text_and_link_with_size_limit():
    html = "<title>Docs</title><script>hidden</script><p>Public docs " + "x" * 15000 + "</p>"
    with patch("chatbot.tools.web_tools.fetch_public_text", AsyncMock(return_value=("https://example.com/docs", html, "text/html"))):
        result = await ReadWebpageTool().execute({"url": "https://example.com/docs"}, None)
    assert result["title"] == "Docs"
    assert result["truncated"]
    assert len(result["result"]) == 12000
    assert "hidden" not in result["result"]
    assert result["trust"] == "untrusted_source"


@pytest.mark.asyncio
@pytest.mark.parametrize("xml", [
    '<rss><channel><item><title>Release</title><link>https://example.com/release</link><description>New version</description></item></channel></rss>',
    '<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Release</title><link href="https://example.com/release"/><summary>New version</summary></entry></feed>',
])
async def test_feed_supports_rss_and_atom_links(xml):
    with patch("chatbot.tools.web_tools.fetch_public_text", AsyncMock(return_value=("https://example.com/feed", xml, "application/xml"))):
        result = await ReadFeedTool().execute({"url": "https://example.com/feed"}, None)
    assert result["status"] == "success"
    assert result["items"][0]["url"] == "https://example.com/release"
    assert result["items"][0]["summary"] == "New version"


@pytest.mark.asyncio
async def test_feed_rejects_xml_entities():
    with patch("chatbot.tools.web_tools.fetch_public_text", AsyncMock(return_value=("https://example.com/feed", '<!DOCTYPE x [<!ENTITY a "x">]><rss/>', "application/xml"))):
        result = await ReadFeedTool().execute({"url": "https://example.com/feed"}, None)
    assert result["status"] == "failure"


@pytest.mark.asyncio
async def test_search_uses_live_linked_key_and_returns_citations():
    calls = []
    def respond(request):
        calls.append(request)
        return httpx.Response(200, json={"choices": [{"message": {
            "content": "public facts", "annotations": [{"type": "url_citation", "url_citation": {"url": "https://example.com/source", "title": "Source", "content": "evidence"}}],
        }}]})
    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    with patch("chatbot.tools.web_tools.httpx.AsyncClient", return_value=client):
        result = await WebSearchTool().execute({"query": "public topic"}, SimpleNamespace(ai_engine=SimpleNamespace(api_key="linked-key")))
    assert calls[0].headers["Authorization"] == "Bearer linked-key"
    assert result["sources"][0]["url"] == "https://example.com/source"
    assert "linked-key" not in str(result)


@pytest.mark.asyncio
async def test_search_without_source_links_reports_failure():
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"choices": [{"message": {"content": "unverified answer"}}]})))
    with patch("chatbot.tools.web_tools.httpx.AsyncClient", return_value=client):
        result = await WebSearchTool().execute({"query": "public topic"}, SimpleNamespace(ai_engine=SimpleNamespace(api_key="linked-key")))
    assert result["status"] == "failure"
