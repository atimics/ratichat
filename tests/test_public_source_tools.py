"""Public sources, domain limits, bounded content, and tool availability."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from chatbot.core.orchestration.capability_policy import CapabilityPolicy, ExecutionScope, READ_ONLY_SOURCE_TOOLS
from chatbot.core.orchestration.main_orchestrator import MainOrchestrator, OrchestratorConfig
from chatbot.tools import public_source_tools as source


def test_public_source_tools_are_registered_and_scoped(tmp_path):
    names = {"read_news", "search_social", "read_bluesky_feed", "list_public_sources"}
    main = MainOrchestrator(OrchestratorConfig(db_path=str(tmp_path / "main.db")))
    assert names <= set(main.tool_registry.get_tool_names())
    policy = CapabilityPolicy(approved_discord_channel_ids=["20"])
    scope = ExecutionScope("20", "discord", frozenset({"40"}), "40", "guest")
    assert names <= policy.filter_tool_names(READ_ONLY_SOURCE_TOOLS, scope)
    for name in names:
        assert policy.denial_reason(name, {}, scope) is None
        assert policy.denial_reason(name, {}, None)
        assert policy.denial_reason(name, {}, ExecutionScope("private", "discord", frozenset(), "secret", "guest"))


@pytest.mark.asyncio
async def test_news_source_supplies_its_fixed_public_feed(monkeypatch):
    read = AsyncMock(return_value={"status": "success", "items": [{"title": "News", "url": "https://example.com/news"}]})
    monkeypatch.setattr(source.ReadFeedTool, "execute", read)
    result = await source.ReadNewsTool().execute({"source": "bbc_technology"}, None)
    assert result["publisher"] == "BBC Technology"
    assert read.await_args.args[0] == {"url": source.NEWS_SOURCES["bbc_technology"]["url"]}
    for params in ({"source": "private"}, {"source": []}, {"url": "http://localhost/secret"}):
        assert (await source.ReadNewsTool().execute(params, None))["status"] == "failure"
    assert read.await_count == 1


@pytest.mark.asyncio
async def test_social_search_keeps_only_public_posts_from_the_requested_platform(monkeypatch):
    search = AsyncMock(return_value={"status": "success", "timestamp": 10, "sources": [
        {"url": "https://www.reddit.com/r/python/comments/one", "title": "Python", "content": "x" * 3000},
        {"url": "https://reddit.com.attacker.example/thread", "title": "Other"},
        {"url": "http://127.0.0.1/secret"},
    ]})
    monkeypatch.setattr(source.WebSearchTool, "execute", search)
    result = await source.SearchSocialTool().execute({"platform": "reddit", "query": "Python releases"}, None)
    assert result["status"] == "success" and result["access"] == "public_search_index"
    assert len(result["sources"]) == 1 and len(result["sources"][0]["content"]) == 1500
    assert search.await_args.args[0]["query"].startswith("(site:reddit.com)")
    for params in ({"platform": "private", "query": "one"}, {"platform": "reddit", "query": "x" * 351},
                   {"platform": "reddit", "query": "one", "sender_id": "owner"}):
        assert (await source.SearchSocialTool().execute(params, None))["status"] == "failure"
    assert search.await_count == 1


@pytest.mark.asyncio
async def test_social_search_failure_is_available_to_the_agent(monkeypatch):
    monkeypatch.setattr(source.WebSearchTool, "execute", AsyncMock(return_value={"status": "failure", "error": "Search service returned HTTP 429"}))
    result = await source.SearchSocialTool().execute({"platform": "x", "query": "AI tools"}, None)
    assert result["status"] == "failure" and "429" in result["error"]


@pytest.mark.asyncio
async def test_bluesky_uses_public_author_feed_and_builds_post_links(monkeypatch):
    data = {"feed": [{"post": {"uri": "at://did:plc:author/app.bsky.feed.post/123", "record": {"text": "Hello", "createdAt": "2026-10-08T17:00:00Z"}, "author": {"handle": "bsky.app"}}}]}
    fetch = AsyncMock(return_value=("https://public.api.bsky.app", json.dumps(data), "application/json"))
    monkeypatch.setattr(source, "fetch_public_text", fetch)
    result = await source.ReadBlueskyFeedTool().execute({"actor": "bsky.app"}, None)
    assert result["status"] == "success" and result["access"] == "public_api"
    assert result["items"][0]["url"] == "https://bsky.app/profile/did:plc:author/post/123"
    assert "actor=bsky.app" in fetch.await_args.args[0]
    for actor in ("https://private.local", "@bsky.app", "a&token=secret", None):
        assert (await source.ReadBlueskyFeedTool().execute({"actor": actor}, None))["status"] == "failure"
    assert fetch.await_count == 1
