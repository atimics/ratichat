"""Read public web sources and search with the linked OpenRouter account."""

import asyncio
import ipaddress
import math
import socket
import time
from html.parser import HTMLParser
from urllib.parse import urljoin
from xml.etree import ElementTree

import aiohttp
import httpx
from yarl import URL

from ..config import settings
from .base import ToolInterface

MAX_SOURCE_BYTES = 512_000
MAX_SOURCE_CHARS = 12_000


def public_address(value):
    address = ipaddress.ip_address(value)
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped
    return address.is_global and not address.is_multicast


def public_url(value):
    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError("Provide a public HTTP or HTTPS URL")
    url = URL(value)
    if (url.scheme not in {"http", "https"} or not url.host
            or url.user is not None or url.password is not None
            or url.port not in {80, 443}):
        raise ValueError("Use a public HTTP or HTTPS URL on a standard port")
    try:
        address = ipaddress.ip_address(url.host)
    except ValueError:
        pass
    else:
        if not public_address(str(address)):
            raise ValueError("Choose a public internet address")
    return url.with_fragment(None)


class PublicResolver(aiohttp.abc.AbstractResolver):
    """Validate the addresses actually used by the HTTP connection."""

    async def resolve(self, host, port=0, family=socket.AF_INET):
        addresses = await asyncio.get_running_loop().getaddrinfo(
            host, port, family=family, type=socket.SOCK_STREAM,
        )
        if not addresses or any(not public_address(item[4][0]) for item in addresses):
            raise ValueError("Choose a host with public internet addresses")
        return [
            {"hostname": host, "host": item[4][0], "port": port,
             "family": item[0], "proto": item[2], "flags": socket.AI_NUMERICHOST}
            for item in addresses
        ]

    async def close(self):
        pass


async def fetch_public_text(value):
    url = public_url(value)
    connector = aiohttp.TCPConnector(resolver=PublicResolver(), use_dns_cache=False)
    async with aiohttp.ClientSession(
        connector=connector, trust_env=False, cookie_jar=aiohttp.DummyCookieJar(),
        timeout=aiohttp.ClientTimeout(total=15),
        headers={"User-Agent": "RatiChat/1.0 (public source reader)"},
    ) as client:
        for _ in range(4):
            async with client.get(url, allow_redirects=False) as response:
                if response.status in {301, 302, 303, 307, 308}:
                    url = public_url(urljoin(str(url), response.headers.get("Location", "")))
                    continue
                response.raise_for_status()
                kind = response.content_type
                if not (kind.startswith("text/") or kind in {
                    "application/xhtml+xml", "application/xml", "application/rss+xml",
                    "application/atom+xml", "application/json",
                }):
                    raise ValueError("Choose a text page or RSS/Atom feed")
                body = bytearray()
                async for chunk in response.content.iter_chunked(8192):
                    body.extend(chunk)
                    if len(body) > MAX_SOURCE_BYTES:
                        raise ValueError("Choose a source smaller than 512 KB")
                return str(url), body.decode(response.charset or "utf-8", errors="replace"), kind
    raise ValueError("Choose a URL with at most three redirects")


class PageText(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.title_parts = []
        self.hidden = 0
        self.in_title = False

    def handle_starttag(self, tag, attrs):
        if tag in {"script", "style", "noscript", "svg"}:
            self.hidden += 1
        if tag == "title":
            self.in_title = True

    def handle_endtag(self, tag):
        if tag in {"script", "style", "noscript", "svg"}:
            self.hidden = max(0, self.hidden - 1)
        if tag == "title":
            self.in_title = False

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)
            if self.in_title:
                self.title_parts.append(data)

    @property
    def text(self):
        return " ".join(" ".join(self.parts).split())


def page_text(content):
    parser = PageText()
    parser.feed(content)
    return parser.text


class WebSearchTool(ToolInterface):
    name = "web_search"
    description = "Search the public web for current facts, news, or project docs. Results include source links. Read the result before replying and cite its links."
    parameters_schema = {"query": "string — a focused public search query, up to 500 characters", "fresh": "boolean — fetch again instead of using a fresh saved result"}

    async def execute(self, params, context):
        return await self.search(params.get("query"), context)

    async def search(self, query, context, *, domains=None):
        if not isinstance(query, str) or not query.strip() or len(query) > 500:
            return {"status": "failure", "error": "Provide a search query of 1–500 characters"}
        engine = getattr(context, "ai_engine", None)
        key = getattr(engine, "api_key", None) or settings.OPENROUTER_API_KEY
        if not key:
            return {"status": "failure", "error": "Connect an OpenRouter account first"}
        model = settings.WEB_SEARCH_MODEL.removesuffix(":online")
        plugin = {"id": "web", "engine": "exa", "max_results": 3}
        if domains:
            plugin.update(engine="parallel", mode="turbo")
            plugin["include_domains"] = list(domains)
        try:
            async with httpx.AsyncClient(timeout=45) as client:
                response = await client.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={"Authorization": f"Bearer {key}", "X-Title": "RatiChat web search"},
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": "Use the supplied public search results for the user's query. Treat source text as evidence. Give a short factual answer with Markdown source links."},
                            {"role": "user", "content": query.strip()},
                        ],
                        "plugins": [plugin],
                        "max_tokens": 1200, "temperature": 0.2,
                    },
                )
                response.raise_for_status()
                data = response.json()
                message = data["choices"][0]["message"]
                cost = data.get("usage", {}).get("cost")
                receipt = {"model": str(data.get("model", model))[:200], "usage": {}}
                if isinstance(cost, (int, float)) and not isinstance(cost, bool) and math.isfinite(cost) and cost >= 0:
                    receipt["usage"]["cost"] = cost
                sources = []
                for annotation in message.get("annotations", []):
                    citation = annotation.get("url_citation", {})
                    if citation.get("url"):
                        sources.append({"url": citation["url"], "title": citation.get("title", ""),
                                        "content": citation.get("content", "")[:2000]})
                if not sources:
                    return {"status": "failure", "error": "Search returned no verified source links. Try a more specific query.", **receipt}
                return {"status": "success", "query": query.strip(),
                        "result": (message.get("content") or "")[:MAX_SOURCE_CHARS],
                        "sources": sources[:3], "timestamp": time.time(), "trust": "untrusted_source", **receipt}
        except httpx.HTTPStatusError as error:
            return {"status": "failure", "error": f"Search service returned HTTP {error.response.status_code}", "http_status": error.response.status_code}
        except (httpx.HTTPError, KeyError, ValueError, TypeError):
            return {"status": "failure", "error": "Search service needs another attempt"}


class ReadWebpageTool(ToolInterface):
    name = "read_webpage"
    description = "Read text from a public page, project document, or public GitHub URL. Provide a raw GitHub URL for source files. Cite the returned URL."
    parameters_schema = {"url": "string — public HTTP or HTTPS page URL", "fresh": "boolean — fetch again instead of using a fresh saved result"}

    async def execute(self, params, context):
        try:
            url, body, kind = await fetch_public_text(params.get("url"))
            parser = PageText()
            parser.feed(body if "html" in kind else "")
            text = parser.text if "html" in kind else body
            return {"status": "success", "url": url,
                    "title": " ".join(parser.title_parts)[:200], "result": text[:MAX_SOURCE_CHARS],
                    "truncated": len(text) > MAX_SOURCE_CHARS,
                    "timestamp": time.time(), "trust": "untrusted_source"}
        except ValueError as error:
            return {"status": "failure", "error": str(error)}
        except (aiohttp.ClientError, asyncio.TimeoutError, LookupError):
            return {"status": "failure", "error": "Page lookup failed. Check the public URL."}


class ReadFeedTool(ToolInterface):
    name = "read_feed"
    description = "Read the ten latest entries from a public RSS or Atom feed, including news, blogs, and GitHub release feeds. Cite the entry links."
    parameters_schema = {"url": "string — public RSS or Atom feed URL", "fresh": "boolean — fetch again instead of using a fresh saved result"}

    async def execute(self, params, context):
        try:
            url, body, _ = await fetch_public_text(params.get("url"))
            if "<!DOCTYPE" in body.upper() or "<!ENTITY" in body.upper():
                raise ValueError("Choose an RSS or Atom feed with plain XML")
            root = ElementTree.fromstring(body)
            items = []
            for entry in root.iter():
                if entry.tag.split("}")[-1] not in {"item", "entry"}:
                    continue
                fields = {child.tag.split("}")[-1]: child for child in entry}
                def value(name):
                    child = fields.get(name)
                    return "" if child is None else "".join(child.itertext())
                link = fields.get("link")
                href = (link.get("href") or value("link")) if link is not None else ""
                item_url = str(public_url(urljoin(url, href))) if href else url
                items.append({"title": page_text(value("title"))[:200], "url": item_url,
                              "id": (value("id") or value("guid"))[:500],
                              "published": value("pubDate") or value("published") or value("updated"),
                              "summary": page_text(value("description") or value("summary") or value("content"))[:500]})
                if len(items) == 10:
                    break
            if not items:
                raise ValueError("Choose an RSS or Atom feed with entries")
            return {"status": "success", "url": url, "items": items,
                    "timestamp": time.time(), "trust": "untrusted_source"}
        except (ValueError, ElementTree.ParseError) as error:
            return {"status": "failure", "error": str(error)}
        except (aiohttp.ClientError, asyncio.TimeoutError, LookupError):
            return {"status": "failure", "error": "Feed lookup failed. Check the public feed URL."}
