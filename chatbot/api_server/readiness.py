"""Read the running bot's local state for deployment checks."""

import asyncio

from chatbot.config import settings


def task_running(task):
    return bool(task is not None and not task.done())


async def bot_readiness(orchestrator):
    checks = {
        "orchestrator": bool(orchestrator.running),
        "processing": bool(orchestrator.processing_hub.running),
        "ai": bool(orchestrator.ai_engine.api_key),
    }
    context = orchestrator.action_context
    configured = {
        "matrix": bool(settings.MATRIX_HOMESERVER or settings.MATRIX_ACCESS_TOKEN),
        "discord": bool(settings.DISCORD_BOT_TOKEN),
        "telegram": bool(settings.TELEGRAM_BOT_TOKEN),
        "farcaster": bool(settings.NEYNAR_API_KEY),
    }
    chat_connections = []
    for platform, required in configured.items():
        observer = getattr(context, platform + "_observer", None)
        if observer is None and not required:
            continue
        connected = False
        if observer is not None:
            try:
                status = await asyncio.wait_for(observer.get_status(), timeout=1)
                connected = bool(status.get("connected"))
                if platform == "matrix":
                    connected = connected and bool(status.get("sync_task_running"))
                elif platform == "discord":
                    connected = connected and task_running(observer._task)
                elif platform == "telegram":
                    connected = connected and task_running(observer._poll_task)
            except Exception:
                connected = False
        checks[platform] = connected
        chat_connections.append(connected)
    checks["chat"] = bool(chat_connections) and all(chat_connections)

    for check, attribute in (
        ("source_watches", "source_watch_task"),
        ("live_monitoring", "live_monitor_task"),
        ("proactive_sources", "proactive_source_task"),
    ):
        checks[check] = task_running(getattr(orchestrator, attribute, None))
    if (orchestrator.config.capability_profile == "matrix_steward"
            and settings.MATRIX_BACKUP_INTERVAL_SECONDS > 0):
        checks["matrix_backups"] = task_running(getattr(orchestrator, "steward_task", None))

    return {"status": "ready" if all(checks.values()) else "degraded", "checks": checks}
