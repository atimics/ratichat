"""Check deployment readiness against chat and background task failures."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from chatbot.api_server.main import ChatbotAPIServer
from chatbot.api_server.readiness import bot_readiness
from chatbot.api_server.routers.system import get_orchestrator
from chatbot.config import settings


def running_task():
    return Mock(done=Mock(return_value=False))


@pytest.fixture
def ready_bot(monkeypatch):
    for name, value in {
        "ADMIN_API_TOKEN": "a" * 32,
        "MATRIX_HOMESERVER": "https://matrix.example.org",
        "MATRIX_ACCESS_TOKEN": "matrix-secret",
        "DISCORD_BOT_TOKEN": "discord-secret",
        "TELEGRAM_BOT_TOKEN": None,
        "NEYNAR_API_KEY": None,
        "MATRIX_BACKUP_INTERVAL_SECONDS": 86400,
    }.items():
        monkeypatch.setattr(settings, name, value)
    matrix = SimpleNamespace(get_status=AsyncMock(return_value={
        "connected": True, "sync_task_running": True,
        "user_id": "private-user", "homeserver": "private-server",
    }))
    discord = SimpleNamespace(
        get_status=AsyncMock(return_value={"connected": True}),
        _task=running_task(),
    )
    return SimpleNamespace(
        running=True,
        processing_hub=SimpleNamespace(running=True),
        ai_engine=SimpleNamespace(api_key="linked-secret"),
        action_context=SimpleNamespace(matrix_observer=matrix, discord_observer=discord),
        config=SimpleNamespace(capability_profile="matrix_steward"),
        source_watch_task=running_task(),
        live_monitor_task=running_task(),
        proactive_source_task=running_task(),
        steward_task=running_task(),
        openrouter_link=None,
    )


def client_for(bot):
    server = ChatbotAPIServer.__new__(ChatbotAPIServer)
    server.orchestrator = bot
    server.app = FastAPI()
    server._setup_middleware()
    server._setup_routers()
    server.app.dependency_overrides[get_orchestrator] = lambda: bot
    return TestClient(server.app)


def test_public_readiness_and_private_details(ready_bot):
    with client_for(ready_bot) as client:
        public = client.get("/ready")
        assert public.status_code == 200
        assert public.json() == {"status": "ready"}
        assert public.headers["cache-control"] == "no-store"
        assert client.get("/api/system/readiness").status_code == 401
        details = client.get("/api/system/readiness", headers={
            "Authorization": "Bearer " + settings.ADMIN_API_TOKEN,
        })
        assert details.status_code == 200
        assert all(details.json()["checks"].values())
        assert all(type(value) is bool for value in details.json()["checks"].values())
        for private_value in ("linked-secret", "matrix-secret", "private-user", "private-server"):
            assert private_value not in details.text


def test_dead_matrix_sync_marks_deployment_degraded(ready_bot):
    ready_bot.action_context.matrix_observer.get_status.return_value["sync_task_running"] = False
    with client_for(ready_bot) as client:
        assert client.get("/health").status_code == 200
        readiness = client.get("/ready")
        assert readiness.status_code == 503
        assert readiness.json() == {"status": "degraded"}
        details = client.get("/api/system/readiness", headers={"X-Admin-Token": settings.ADMIN_API_TOKEN})
        assert details.status_code == 503
        assert details.json()["checks"]["matrix"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("attribute", [
    "source_watch_task", "live_monitor_task", "proactive_source_task", "steward_task",
])
async def test_background_task_failure_marks_deployment_degraded(ready_bot, attribute):
    getattr(ready_bot, attribute).done.return_value = True
    assert (await bot_readiness(ready_bot))["status"] == "degraded"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["orchestrator", "processing", "ai", "discord", "discord_task"])
async def test_chat_setup_failure_marks_deployment_degraded(ready_bot, failure):
    if failure == "orchestrator":
        ready_bot.running = False
    elif failure == "processing":
        ready_bot.processing_hub.running = False
    elif failure == "ai":
        ready_bot.ai_engine.api_key = None
    elif failure == "discord":
        ready_bot.action_context.discord_observer = None
    else:
        ready_bot.action_context.discord_observer._task.done.return_value = True
    assert (await bot_readiness(ready_bot))["status"] == "degraded"


@pytest.mark.asyncio
async def test_observer_error_produces_boolean_check(ready_bot):
    ready_bot.action_context.matrix_observer.get_status.side_effect = RuntimeError("private-token")
    result = await bot_readiness(ready_bot)
    assert result["checks"]["matrix"] is False
    assert "private-token" not in str(result)


@pytest.mark.asyncio
async def test_matrix_only_setup_is_ready(ready_bot, monkeypatch):
    monkeypatch.setattr(settings, "DISCORD_BOT_TOKEN", None)
    monkeypatch.setattr(settings, "MATRIX_BACKUP_INTERVAL_SECONDS", 0)
    ready_bot.action_context.discord_observer = None
    ready_bot.steward_task = None
    result = await bot_readiness(ready_bot)
    assert result["status"] == "ready"
    assert "discord" not in result["checks"]
    assert "matrix_backups" not in result["checks"]


@pytest.mark.asyncio
async def test_chat_connection_is_required(ready_bot, monkeypatch):
    monkeypatch.setattr(settings, "MATRIX_HOMESERVER", None)
    monkeypatch.setattr(settings, "MATRIX_ACCESS_TOKEN", None)
    monkeypatch.setattr(settings, "DISCORD_BOT_TOKEN", None)
    ready_bot.action_context.matrix_observer = None
    ready_bot.action_context.discord_observer = None
    assert (await bot_readiness(ready_bot))["checks"]["chat"] is False
