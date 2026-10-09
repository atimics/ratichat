"""Task tools use the sender and destination from the current request."""

from .base import ToolInterface


class TaskTool(ToolInterface):
    async def execute(self, params, context):
        service = getattr(context, "task_service", None)
        scope = getattr(context, "execution_scope", None)
        if not service or not scope:
            return {"status": "blocked", "message": "Use a current request with saved task access."}
        if not isinstance(params, dict) or set(params) - set(self.parameters_schema):
            return {"status": "blocked", "message": "Use the listed task parameters."}
        return await service.execute_tool(self.name, params, scope)


class GetTaskStatusTool(TaskTool):
    name = "get_task_status"
    description = "Read the current saved task, model route, worker results, and shared task budget."
    parameters_schema = {}


class RunTaskWorkersTool(TaskTool):
    name = "run_task_workers"
    description = "Run up to three saved workers in parallel on the current evidence. Each worker has a goal and a researcher, developer, critic, or ratichat persona. An optional preferred_model comes from get_model_catalog. Jev checks the exact route and budget. Results return to the current task for synthesis."
    parameters_schema = {"jobs": "array - One to three objects with goal (string), persona (string), and optional preferred_model (exact catalog ID)."}


class LinkChatAccountTool(TaskTool):
    name = "link_chat_account"
    description = "Link your Discord and Matrix accounts when you ask. Include the exact target account ID in the start request. Present the link ID and code in a message from that target account, then confirm the exact link ID in a message from the original account. Each step uses the actual sender."
    parameters_schema = {"stage": "string - start, prove, or confirm",
                         "target_platform": "string - discord or matrix for start",
                         "target_account_id": "string - Exact target platform account ID for start",
                         "link_id": "string - Saved link ID for prove or confirm",
                         "code": "string - Saved proof code for prove"}


class GetModelCatalogTool(TaskTool):
    name = "get_model_catalog"
    description = "Read the current OpenRouter model catalog with exact IDs, supported settings, context limits, and prices. Use query to find a worker model for a topic."
    parameters_schema = {"query": "string - Optional model or provider name filter",
                         "limit": "integer - Optional number of models, one to thirty"}
