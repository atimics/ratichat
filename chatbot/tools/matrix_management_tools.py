"""Tools exposed by the Matrix steward capability profile."""

import time

import httpx

from .base import ToolInterface


class MatrixManagementTool(ToolInterface):
    async def execute(self, params, context):
        steward = getattr(context, "matrix_steward", None)
        if steward is None:
            return {"status": "failure", "message": "Matrix management needs configuration."}
        try:
            return await self.run(steward, params, context)
        except ValueError as error:
            return {"status": "failure", "message": str(error), "timestamp": time.time()}
        except httpx.HTTPError:
            return {"status": "failure", "message": "The Matrix request needs attention. Check service health and the action receipt.", "timestamp": time.time()}


class MatrixServerStatusTool(MatrixManagementTool):
    name = "matrix_server_status"
    description = "Check live Matrix API health and the bot connection. Report the tool result to the requesting room."
    parameters_schema = {"source_event_id": "string - Latest Matrix request event ID"}

    async def run(self, steward, params, context):
        return await steward.status(context.matrix_observer)


class ManageMatrixRoomTool(MatrixManagementTool):
    name = "manage_matrix_room"
    description = "Update a managed public room under the authority in community_control. Operations: name, topic, publish, unpublish. Use one action per requested change."
    parameters_schema = {
        "source_event_id": "string - Current source event ID from community_control",
        "room_id": "string - Configured managed public room ID",
        "operation": "string - name, topic, publish, or unpublish",
        "value": "string - New name or topic; used for those two operations",
    }

    async def run(self, steward, params, context):
        return await steward.room_action(params, context.matrix_observer)


class ManageMatrixServerTool(MatrixManagementTool):
    name = "manage_matrix_server"
    description = "Ask the Matrix server for uptime, memory, backup, or list_backups under the authority in community_control. Report the returned server message; a pending receipt means the result still needs checking."
    parameters_schema = {
        "source_event_id": "string - Current source event ID from community_control",
        "operation": "string - uptime, memory, backup, or list_backups",
    }

    async def run(self, steward, params, context):
        return await steward.server_action(params.get("operation"), params.get("source_event_id"))
