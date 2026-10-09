"""Read public news and social sources for the current chat request."""

import asyncio
import json
import re
import time
from urllib.parse import urlencode

import aiohttp

from .base import ToolInterface
from .web_tools import ReadFeedTool, WebSearchTool, fetch_public_text, page_text, public_url


NEWS_SOURCES = {
    "bbc_world": {"name": "BBC News", "topic": "world", "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
    "bbc_technology": {"name": "BBC Technology", "topic": "tech", "url": "https://feeds.bbci.co.uk/news/technology/rss.xml"},
    "hacker_news": {"name": "Hacker News", "topic": "developer", "url": "https://news.ycombinator.com/rss"},
    "coindesk": {"name": "CoinDesk", "topic": "crypto", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
}
SOCIAL_DOMAINS = {
    "reddit": ("reddit.com",),
    "farcaster": ("farcaster.xyz", "warpcast.com"),
    "x": ("x.com", "twitter.com"),
    "bluesky": ("bsky.app",),
}


def listed_params(params, fields):
    if not isinstance(params, dict) or set(params) - set(fields):
        raise ValueError("Use the listed source tool parameters")


class ListPublicSourcesTool(ToolInterface):
    name = "list_public_sources"
    description = "Show the agent's public news and social sources, their access type, and the tools used to read them."
    parameters_schema = {}

    async def execute(self, params, context):
        try:
            listed_params(params, ())
            return {"status": "success", "news": NEWS_SOURCES,
                    "social_search": {name: {"domains": domains, "access": "public_search_index"} for name, domains in SOCIAL_DOMAINS.items()},
                    "bluesky_feed": {"tool": "read_bluesky_feed", "access": "public_api"},
                    "message": "Public feeds provide news. Social search reads indexed public posts. Bluesky author feeds use its public API."}
        except ValueError as error:
            return {"status": "failure", "error": str(error)}


class ReadNewsTool(ToolInterface):
    name = "read_news"
    description = "Read current public headlines and source links from BBC News, BBC Technology, Hacker News, or CoinDesk. Credit the publisher when sharing its stories."
    parameters_schema = {"source": "string - bbc_world, bbc_technology, hacker_news, or coindesk; default bbc_world",
                         "fresh": "boolean - Fetch again instead of using a fresh saved result"}

    async def execute(self, params, context):
        try:
            listed_params(params, self.parameters_schema)
            name = params.get("source", "bbc_world")
            if not isinstance(name, str) or name not in NEWS_SOURCES:
                raise ValueError("Choose a listed news source")
            source = NEWS_SOURCES[name]
            result = await ReadFeedTool().execute({"url": source["url"]}, context)
            return {**result, "source": name, "publisher": source["name"], "access": "public_feed"}
        except ValueError as error:
            return {"status": "failure", "error": str(error)}


class SearchSocialTool(ToolInterface):
    name = "search_social"
    description = "Find indexed public Reddit, Farcaster, X, or Bluesky posts about a focused topic. This uses public web search. Describe the result as indexed public posts and cite the returned links."
    parameters_schema = {"platform": "string - reddit, farcaster, x, or bluesky",
                         "query": "string - Public topic to search, up to 350 characters",
                         "fresh": "boolean - Fetch again instead of using a fresh saved result"}

    async def execute(self, params, context):
        try:
            listed_params(params, self.parameters_schema)
            platform, query = params.get("platform"), params.get("query")
            if not isinstance(platform, str) or platform not in SOCIAL_DOMAINS:
                raise ValueError("Choose reddit, farcaster, x, or bluesky")
            if not isinstance(query, str) or not query.strip() or len(query) > 350:
                raise ValueError("Provide a public topic of 1 to 350 characters")
            domains = SOCIAL_DOMAINS[platform]
            result = await WebSearchTool().search(query.strip(), context, domains=domains)
            if result.get("status") != "success":
                return {**result, "platform": platform, "access": "public_search_index"}
            sources = []
            for source in result.get("sources", []):
                try:
                    url = public_url(source.get("url"))
                    if any(url.host == domain or url.host.endswith("." + domain) for domain in domains):
                        sources.append({"url": str(url), "title": page_text(source.get("title", ""))[:200],
                                        "content": page_text(source.get("content", ""))[:1500]})
                except (ValueError, TypeError):
                    continue
            if not sources:
                return {"status": "failure", "error": "Try a more specific topic to find indexed public posts.",
                        "platform": platform, "access": "public_search_index"}
            return {"status": "success", "platform": platform, "query": query.strip(),
                    "access": "public_search_index", "sources": sources[:3],
                    "timestamp": result.get("timestamp", time.time()), "trust": "untrusted_source"}
        except ValueError as error:
            return {"status": "failure", "error": str(error)}


class ReadBlueskyFeedTool(ToolInterface):
    name = "read_bluesky_feed"
    description = "Read ten public posts from a Bluesky author's handle, with text, dates, and post links. Use a handle such as bsky.app."
    parameters_schema = {"actor": "string - Public Bluesky handle, without @",
                         "fresh": "boolean - Fetch again instead of using a fresh saved result"}

    async def execute(self, params, context):
        try:
            listed_params(params, self.parameters_schema)
            actor = params.get("actor")
            if not isinstance(actor, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9.-]{1,252}\.[a-zA-Z]{2,63}", actor):
                raise ValueError("Provide a public Bluesky handle such as bsky.app")
            endpoint = "https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed?" + urlencode({"actor": actor.lower(), "limit": 10, "filter": "posts_no_replies"})
            _, body, _ = await fetch_public_text(endpoint)
            data = json.loads(body)
            items = []
            for entry in data.get("feed", [])[:10]:
                post = entry.get("post", {})
                record = post.get("record", {})
                uri = post.get("uri", "")
                parts = uri.split("/")
                if len(parts) != 5 or parts[0] != "at:" or parts[3] != "app.bsky.feed.post":
                    continue
                url = str(public_url("https://bsky.app/profile/" + parts[2] + "/post/" + parts[4]))
                text = str(record.get("text", ""))[:1200]
                items.append({"title": text[:140], "summary": text, "url": url,
                              "published": str(record.get("createdAt", ""))[:100],
                              "author": str(post.get("author", {}).get("handle", actor))[:253]})
            return {"status": "success", "actor": actor.lower(), "items": items,
                    "access": "public_api", "timestamp": time.time(), "trust": "untrusted_source"}
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            return {"status": "failure", "error": "Check the public Bluesky handle and try again."}
        except (aiohttp.ClientError, asyncio.TimeoutError, LookupError):
            return {"status": "failure", "error": "Bluesky needs another lookup attempt."}
