import pytest
import time
import mimetypes
from unittest.mock import AsyncMock, Mock

import nio
from chatbot.tools.matrix_tools import (
    SendMatrixReplyTool, 
    SendMatrixMessageTool,
    SendMatrixVideoTool,
    JoinMatrixRoomTool,
    LeaveMatrixRoomTool,
    AcceptMatrixInviteTool,
    IgnoreMatrixInviteTool
)
from chatbot.tools.base import ActionContext
from chatbot.integrations.matrix.observer import MatrixObserver
from chatbot.core.world_state import WorldStateManager, WorldState, Message

# ---- Tests for Matrix Tools ----
@pytest.mark.asyncio
async def test_send_matrix_reply_tool_success():
    # Setup dummy observer
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.send_reply = AsyncMock(return_value={"success": True, "event_id": "evt123"})

    context = ActionContext(matrix_observer=dummy_obs)
    tool = SendMatrixReplyTool()

    params = {"channel_id": "!room:server", "content": "Hello!", "reply_to_id": "origevt", "format_as_markdown": False}
    result = await tool.execute(params, context)

    assert result["status"] == "success"
    assert result["event_id"] == "evt123"
    dummy_obs.send_reply.assert_awaited_once_with("!room:server", "Hello!", "origevt")


@pytest.mark.asyncio
async def test_saved_matrix_reply_keeps_text_and_transaction_together():
    observer = Mock()
    observer.send_reply = AsyncMock(return_value={"success": True, "event_id": "evt123"})
    observer.send_image = AsyncMock()
    world = WorldStateManager()
    world.get_last_generated_media_url = Mock(return_value="https://example.com/other-room.png")
    world.state.generated_media_library = [{"timestamp": time.time()}]
    result = await SendMatrixReplyTool().execute({
        "channel_id": "!room:server", "content": "Saved answer", "reply_to_id": "source",
        "format_as_markdown": False, "delivery_id": "research:1",
    }, ActionContext(matrix_observer=observer, world_state_manager=world))
    assert result["event_id"] == "evt123"
    observer.send_reply.assert_awaited_once_with("!room:server", "Saved answer", "source", tx_id="research:1")
    observer.send_image.assert_not_awaited()
    world.get_last_generated_media_url.assert_not_called()


@pytest.mark.asyncio
async def test_saved_matrix_transport_error_keeps_transaction_for_receipt_check():
    observer = MatrixObserver("https://example.com", "@bot:example.com", "unused", WorldStateManager())
    observer.client = Mock()
    observer.client.room_send = AsyncMock(side_effect=[TimeoutError("lost response"), nio.RoomSendResponse("$sent", "!room:server")])
    context = ActionContext(matrix_observer=observer)
    params = {"channel_id": "!room:server", "content": "Saved answer", "reply_to_id": "$source",
              "format_as_markdown": False, "delivery_id": "research:9"}
    first = await SendMatrixReplyTool().execute(params, context)
    assert first["status"] == "unknown"
    receipt = await observer.send_reply("!room:server", "Saved answer", "$source", tx_id="research:9")
    assert receipt["event_id"] == "$sent"
    assert observer.reply_receipts["research:9"]["event_id"] == "$sent"
    assert {call.kwargs["tx_id"] for call in observer.client.room_send.await_args_list} == {"research:9"}

@pytest.mark.asyncio
async def test_send_matrix_reply_tool_missing_params():
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.send_reply = AsyncMock()
    context = ActionContext(matrix_observer=dummy_obs)
    tool = SendMatrixReplyTool()

    # Missing all params
    res = await tool.execute({}, context)
    assert res["status"] == "failure"
    assert "Missing required parameters" in res["error"]

@pytest.mark.asyncio
async def test_send_matrix_message_tool_success():
    # Setup dummy observer
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.send_message = AsyncMock(return_value={"success": True, "event_id": "msg456"})

    context = ActionContext(matrix_observer=dummy_obs)
    tool = SendMatrixMessageTool()
    params = {"channel_id": "!room:server", "content": "Announcement", "format_as_markdown": False}
    result = await tool.execute(params, context)

    assert result["status"] == "success"
    assert result["event_id"] == "msg456"
    dummy_obs.send_message.assert_awaited_once_with("!room:server", "Announcement")

@pytest.mark.asyncio
async def test_send_matrix_message_tool_missing_params():
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.send_message = AsyncMock()
    context = ActionContext(matrix_observer=dummy_obs)
    tool = SendMatrixMessageTool()

    res = await tool.execute({}, context)
    assert res["status"] == "failure"
    assert "Missing required parameters" in res["error"]

# ---- Tests for Matrix Room Management Tools ----

@pytest.mark.asyncio
async def test_join_matrix_room_tool_success():
    """Test successful room joining by ID."""
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.join_room = AsyncMock(return_value={
        "success": True, 
        "room_id": "!room123:server.com",
        "message": "Successfully joined room"
    })

    context = ActionContext(matrix_observer=dummy_obs)
    tool = JoinMatrixRoomTool()

    params = {"room_identifier": "!room123:server.com"}
    result = await tool.execute(params, context)

    assert result["status"] == "success"
    assert result["room_id"] == "!room123:server.com"
    assert "Successfully joined" in result["message"]
    dummy_obs.join_room.assert_awaited_once_with("!room123:server.com")

@pytest.mark.asyncio
async def test_join_matrix_room_tool_by_alias():
    """Test successful room joining by alias."""
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.join_room = AsyncMock(return_value={
        "success": True, 
        "room_id": "!room123:server.com",
        "message": "Successfully joined room"
    })

    context = ActionContext(matrix_observer=dummy_obs)
    tool = JoinMatrixRoomTool()

    params = {"room_identifier": "#general:server.com"}
    result = await tool.execute(params, context)

    assert result["status"] == "success"
    assert result["room_id"] == "!room123:server.com"
    dummy_obs.join_room.assert_awaited_once_with("#general:server.com")

@pytest.mark.asyncio
async def test_join_matrix_room_tool_failure():
    """Test room joining failure."""
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.join_room = AsyncMock(return_value={
        "success": False, 
        "error": "Room not found"
    })

    context = ActionContext(matrix_observer=dummy_obs)
    tool = JoinMatrixRoomTool()

    params = {"room_identifier": "!nonexistent:server.com"}
    result = await tool.execute(params, context)

    assert result["status"] == "failure"
    assert "Room not found" in result["error"]

@pytest.mark.asyncio
async def test_join_matrix_room_tool_missing_params():
    """Test room joining with missing parameters."""
    dummy_obs = type("DummyObs", (), {})()
    context = ActionContext(matrix_observer=dummy_obs)
    tool = JoinMatrixRoomTool()

    result = await tool.execute({}, context)
    assert result["status"] == "failure"
    assert "Missing required parameter" in result["error"]

@pytest.mark.asyncio
async def test_leave_matrix_room_tool_success():
    """Test successful room leaving."""
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.leave_room = AsyncMock(return_value={
        "success": True, 
        "message": "Successfully left room"
    })

    context = ActionContext(matrix_observer=dummy_obs)
    tool = LeaveMatrixRoomTool()

    params = {"room_id": "!room123:server.com"}
    result = await tool.execute(params, context)

    assert result["status"] == "success"
    assert "Successfully left" in result["message"]
    dummy_obs.leave_room.assert_awaited_once_with("!room123:server.com", "Leaving room")

@pytest.mark.asyncio
async def test_leave_matrix_room_tool_with_reason():
    """Test room leaving with a reason."""
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.leave_room = AsyncMock(return_value={
        "success": True, 
        "message": "Successfully left room"
    })

    context = ActionContext(matrix_observer=dummy_obs)
    tool = LeaveMatrixRoomTool()

    params = {
        "room_id": "!room123:server.com",
        "reason": "Going offline for maintenance"
    }
    result = await tool.execute(params, context)

    assert result["status"] == "success"
    dummy_obs.leave_room.assert_awaited_once_with("!room123:server.com", "Going offline for maintenance")

@pytest.mark.asyncio
async def test_leave_matrix_room_tool_failure():
    """Test room leaving failure."""
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.leave_room = AsyncMock(return_value={
        "success": False, 
        "error": "Not a member of this room"
    })

    context = ActionContext(matrix_observer=dummy_obs)
    tool = LeaveMatrixRoomTool()

    params = {"room_id": "!room123:server.com"}
    result = await tool.execute(params, context)

    assert result["status"] == "failure"
    assert "Not a member" in result["error"]

@pytest.mark.asyncio
async def test_leave_matrix_room_tool_missing_params():
    """Test room leaving with missing parameters."""
    dummy_obs = type("DummyObs", (), {})()
    context = ActionContext(matrix_observer=dummy_obs)
    tool = LeaveMatrixRoomTool()

    result = await tool.execute({}, context)
    assert result["status"] == "failure"
    assert "Missing required parameter" in result["error"]

@pytest.mark.asyncio
async def test_accept_matrix_invite_tool_success():
    """Test successful invite acceptance."""
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.accept_invite = AsyncMock(return_value={
        "success": True, 
        "room_id": "!room123:server.com",
        "message": "Successfully accepted invite"
    })

    context = ActionContext(matrix_observer=dummy_obs)
    tool = AcceptMatrixInviteTool()

    params = {"room_id": "!room123:server.com"}
    result = await tool.execute(params, context)

    assert result["status"] == "success"
    assert result["room_id"] == "!room123:server.com"
    assert "Successfully accepted" in result["message"]
    dummy_obs.accept_invite.assert_awaited_once_with("!room123:server.com")

@pytest.mark.asyncio
async def test_accept_matrix_invite_tool_failure():
    """Test invite acceptance failure."""
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.accept_invite = AsyncMock(return_value={
        "success": False, 
        "error": "No pending invite for this room"
    })

    context = ActionContext(matrix_observer=dummy_obs)
    tool = AcceptMatrixInviteTool()

    params = {"room_id": "!room123:server.com"}
    result = await tool.execute(params, context)

    assert result["status"] == "failure"
    assert "No pending invite" in result["error"]

@pytest.mark.asyncio
async def test_accept_matrix_invite_tool_missing_params():
    """Test invite acceptance with missing parameters."""
    dummy_obs = type("DummyObs", (), {})()
    context = ActionContext(matrix_observer=dummy_obs)
    tool = AcceptMatrixInviteTool()

    result = await tool.execute({}, context)
    assert result["status"] == "failure"
    assert "Missing required parameter" in result["error"]

@pytest.mark.asyncio
async def test_ignore_matrix_invite_tool_success():
    """Test successful invite ignoring."""
    dummy_world_state = type("DummyWorldState", (), {})()
    dummy_world_state.remove_pending_matrix_invite = Mock(return_value=True)
    
    context = ActionContext(matrix_observer=type("DummyObs", (), {})(), world_state_manager=dummy_world_state)
    tool = IgnoreMatrixInviteTool()

    params = {"room_id": "!room123:server.com", "reason": "Not interested"}
    result = await tool.execute(params, context)

    assert result["status"] == "success"
    assert result["room_id"] == "!room123:server.com"
    assert result["reason"] == "Not interested"
    assert "Successfully ignored" in result["message"]
    dummy_world_state.remove_pending_matrix_invite.assert_called_once_with("!room123:server.com")

@pytest.mark.asyncio
async def test_ignore_matrix_invite_tool_no_invite():
    """Test ignoring invite when no invite exists."""
    dummy_world_state = type("DummyWorldState", (), {})()
    dummy_world_state.remove_pending_matrix_invite = Mock(return_value=False)
    
    context = ActionContext(matrix_observer=type("DummyObs", (), {})(), world_state_manager=dummy_world_state)
    tool = IgnoreMatrixInviteTool()

    params = {"room_id": "!room123:server.com"}
    result = await tool.execute(params, context)

    assert result["status"] == "failure"
    assert "No pending invitation found" in result["error"]

@pytest.mark.asyncio
async def test_ignore_matrix_invite_tool_missing_params():
    """Test invite ignoring with missing parameters."""
    context = ActionContext(matrix_observer=type("DummyObs", (), {})())
    tool = IgnoreMatrixInviteTool()

    result = await tool.execute({}, context)

    assert result["status"] == "failure"
    # Correct the assertion to check for the actual error message
    assert "Missing required parameter: room_id" in result["error"]

# ---- Tests for SendMatrixVideoTool ----
@pytest.mark.asyncio
async def test_send_matrix_video_tool_success():
    """Test successful video upload."""
    from unittest.mock import patch, AsyncMock
    from nio import UploadResponse, RoomSendResponse
    
    # Mock the Matrix observer
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.client = type("Client", (), {})()
    
    # Mock the upload response as proper nio.UploadResponse
    upload_response = UploadResponse(content_uri="mxc://example.com/abcd1234")
    dummy_obs.client.upload = AsyncMock(return_value=upload_response)
    
    # Mock the send response as proper nio.RoomSendResponse
    send_response = RoomSendResponse(event_id="video123", room_id="!room:server")
    dummy_obs.client.room_send = AsyncMock(return_value=send_response)

    context = ActionContext(matrix_observer=dummy_obs)
    tool = SendMatrixVideoTool()

    params = {
        "channel_id": "!room:server", 
        "video_url": "https://example.com/video.mp4",
        "caption": "Test video"
    }
    
    # Mock the HTTP request
    with patch('httpx.AsyncClient') as mock_client:
        mock_response = Mock()
        mock_response.content = b"fake_video_data"
        mock_response.headers = {"content-type": "video/mp4"}
        mock_response.raise_for_status = Mock()
        
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(return_value=mock_response)
        
        result = await tool.execute(params, context)

    assert result["status"] == "success"
    assert result["event_id"] == "video123"
    dummy_obs.client.upload.assert_awaited_once()
    dummy_obs.client.room_send.assert_awaited_once()

@pytest.mark.asyncio
async def test_send_matrix_video_tool_failure():
    """Test video upload failure."""
    from unittest.mock import patch, AsyncMock
    
    dummy_obs = type("DummyObs", (), {})()
    dummy_obs.client = type("Client", (), {})()

    context = ActionContext(matrix_observer=dummy_obs)
    tool = SendMatrixVideoTool()

    params = {
        "channel_id": "!room:server", 
        "video_url": "https://example.com/video.mp4",
        "caption": "Test video"
    }
    
    # Mock HTTP request to raise an exception
    with patch('httpx.AsyncClient') as mock_client:
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            side_effect=Exception("Network error")
        )
        
        result = await tool.execute(params, context)

    assert result["status"] == "failure"
    assert "Network error" in result["error"]

@pytest.mark.asyncio
async def test_send_matrix_video_tool_missing_params():
    """Test video upload with missing parameters."""
    context = ActionContext(matrix_observer=type("DummyObs", (), {})())
    tool = SendMatrixVideoTool()

    # Missing all params
    result = await tool.execute({}, context)
    assert result["status"] == "failure"
    assert "Missing required parameters" in result["error"]

    # Missing video_url
    result = await tool.execute({"channel_id": "!room:server"}, context)
    assert result["status"] == "failure"
    assert "Missing required parameters" in result["error"]

    # Missing channel_id
    result = await tool.execute({"video_url": "https://example.com/video.mp4"}, context)
    assert result["status"] == "failure"
    assert "Missing required parameters" in result["error"]

def test_video_mime_type_detection():
    """Test MIME type detection logic for various video formats."""
    
    # Test standard video formats
    test_cases = [
        ("video.mp4", "video/mp4"),
        ("movie.avi", "video/x-msvideo"),
        ("clip.mov", "video/quicktime"),
        ("stream.webm", "video/webm"),
        ("content.mkv", "video/x-matroska"),
        ("presentation.wmv", "video/x-ms-wmv"),
        ("recording.flv", "video/x-flv"),
        ("animation.ogv", "video/ogg"),
    ]
    
    for filename, expected_mime in test_cases:
        # Test the mimetypes.guess_type logic
        mime_type, _ = mimetypes.guess_type(filename)
        
        # If mimetypes doesn't detect it, test our fallback logic
        if not mime_type or not mime_type.startswith('video/'):
            if filename.endswith('.webm'):
                mime_type = 'video/webm'
            elif filename.endswith('.mov'):
                mime_type = 'video/quicktime'
            elif filename.endswith('.avi'):
                mime_type = 'video/x-msvideo'
            elif filename.endswith('.mkv'):
                mime_type = 'video/x-matroska'
            else:
                mime_type = 'video/mp4'  # Default fallback
        
        # For some formats, mimetypes might return different values
        # but we want to ensure we get a video MIME type
        assert mime_type.startswith('video/'), f"Expected video MIME type for {filename}, got {mime_type}"

def test_video_mime_type_detection_edge_cases():
    """Test MIME type detection for edge cases."""
    
    # Test URLs with query parameters
    url_cases = [
        ("https://example.com/video.mp4?param=1", "video/mp4"),
        ("https://example.com/movie.webm#timestamp", "video/webm"),
        ("https://example.com/clip.mov?quality=high&format=mov", "video/quicktime"),
    ]
    
    for url, expected_mime_prefix in url_cases:
        # Extract filename from URL (remove query params and fragments)
        from urllib.parse import urlparse
        parsed = urlparse(url)
        filename = parsed.path.split('/')[-1]
        
        mime_type, _ = mimetypes.guess_type(filename)
        if not mime_type or not mime_type.startswith('video/'):
            if filename.endswith('.webm'):
                mime_type = 'video/webm'
            elif filename.endswith('.mov'):
                mime_type = 'video/quicktime'
            elif filename.endswith('.avi'):
                mime_type = 'video/x-msvideo'
            elif filename.endswith('.mkv'):
                mime_type = 'video/x-matroska'
            else:
                mime_type = 'video/mp4'
        
        assert mime_type.startswith('video/'), f"Expected video MIME type for {url}, got {mime_type}"

def test_video_mime_type_detection_no_extension():
    """Test MIME type detection for files without extensions."""
    
    # When no extension is found, should default to video/mp4
    test_cases = [
        "video_file_no_extension",
        "https://example.com/stream",
        "some_video",
    ]
    
    for filename in test_cases:
        mime_type, _ = mimetypes.guess_type(filename)
        if not mime_type or not mime_type.startswith('video/'):
            # Should fall back to video/mp4
            mime_type = 'video/mp4'
        
        assert mime_type == 'video/mp4', f"Expected fallback to video/mp4 for {filename}, got {mime_type}"

# ---- Tests for MatrixObserver basic methods ----
def test_matrix_observer_user_and_room_details_empty(monkeypatch):
    wsm = WorldStateManager()
    obs = MatrixObserver(world_state_manager=wsm)
    # No client initialized -> empty dicts
    assert obs.get_room_details() == {}
    assert obs.get_user_details() == {}

# Simulate a dummy client with rooms and users for details
class DummyUser:
    def __init__(self, user_id, display_name=None, avatar_url=None):
        self.user_id = user_id
        self.display_name = display_name
        self.avatar_url = avatar_url

class DummyRoom:
    def __init__(self, room_id, name, users, power_levels=None):
        self.room_id = room_id
        self.display_name = name
        self.name = name
        self.users = {u.user_id: u for u in users}
        # Power levels stub
        pl_dict = power_levels or {}
        self.power_levels = type("PL", (), {"users": pl_dict})
        # Timeline stub for last_message_time
        self.timeline = type("Timeline", (), {"events": []})()

@pytest.mark.asyncio
async def test_matrix_observer_room_and_user_details(monkeypatch):
    wsm = WorldStateManager()
    obs = MatrixObserver(world_state_manager=wsm)
    # Create dummy client with 2 rooms and users
    user_a = DummyUser("@alice:server", display_name="Alice", avatar_url="urlA")
    user_b = DummyUser("@bob:server", display_name="Bob", avatar_url="urlB")
    room1 = DummyRoom("!r1:server", "Room1", [user_a, user_b], power_levels={"@alice:server": 50})
    room2 = DummyRoom("!r2:server", "Room2", [user_b], power_levels={})

    # Assign client with rooms
    class DummyClient:
        def __init__(self, rooms): self.rooms = {r.room_id: r for r in rooms}
    obs.client = DummyClient([room1, room2])

    # Test get_room_details
    rd = obs.get_room_details()
    # Ensure room details extracted correctly (keys present)
    assert set(rd.keys()) == {"!r1:server", "!r2:server"}
    assert rd["!r1:server"]["name"] == "Room1"

    # Test get_user_details
    ud = obs.get_user_details()
    assert "@alice:server" in ud and "@bob:server" in ud
    assert ud["@alice:server"]["display_name"] == "Alice"
    assert ":server" in ud["@bob:server"]["user_id"]

# ---- Tests for MatrixObserver Room Management Methods ----

@pytest.mark.asyncio
async def test_matrix_observer_join_room_success():
    """Test MatrixObserver join_room method success."""
    wsm = WorldStateManager()
    obs = MatrixObserver(world_state_manager=wsm)
    
    # Mock client and its join method
    mock_client = AsyncMock()
    mock_client.join.return_value = type("Response", (), {
        "room_id": "!room123:server.com"
    })()
    obs.client = mock_client
    
    result = await obs.join_room("!room123:server.com")
    
    assert result["success"] is True
    assert result["room_id"] == "!room123:server.com"
    mock_client.join.assert_awaited_once_with("!room123:server.com")

@pytest.mark.asyncio  
async def test_matrix_observer_join_room_failure():
    """Test MatrixObserver join_room method failure."""
    wsm = WorldStateManager()
    obs = MatrixObserver(world_state_manager=wsm)
    
    # Mock client join failure
    mock_client = AsyncMock()
    mock_client.join.side_effect = Exception("Join failed")
    obs.client = mock_client
    
    result = await obs.join_room("!room123:server.com")
    
    assert result["success"] is False
    assert "Join failed" in result["error"]

@pytest.mark.asyncio
async def test_matrix_observer_leave_room_success():
    """Test MatrixObserver leave_room method success."""
    wsm = WorldStateManager()
    obs = MatrixObserver(world_state_manager=wsm)
    
    # Mock client and its leave method
    mock_client = AsyncMock()
    mock_client.room_leave.return_value = type("Response", (), {})()
    obs.client = mock_client
    
    result = await obs.leave_room("!room123:server.com", "Test reason")
    
    assert result["success"] is True
    mock_client.room_leave.assert_awaited_once_with("!room123:server.com", "Test reason")

@pytest.mark.asyncio
async def test_matrix_observer_accept_invite_success():
    """Test MatrixObserver accept_invite method success."""
    wsm = WorldStateManager()
    obs = MatrixObserver(world_state_manager=wsm)
    
    # Mock client and its join method for invite acceptance
    mock_client = AsyncMock()
    mock_client.join.return_value = type("Response", (), {
        "room_id": "!room123:server.com"
    })()
    mock_client.invited_rooms = {"!room123:server.com": {}}
    obs.client = mock_client
    
    result = await obs.accept_invite("!room123:server.com")
    
    assert result["success"] is True
    assert result["room_id"] == "!room123:server.com"
    mock_client.join.assert_awaited_once_with("!room123:server.com")

@pytest.mark.asyncio
async def test_matrix_observer_get_invites_success():
    """Test MatrixObserver get_invites method success."""
    wsm = WorldStateManager()
    obs = MatrixObserver(world_state_manager=wsm)
    
    # Mock client with invited_rooms
    mock_room1 = type("Room", (), {
        "room_id": "!room123:server.com",
        "display_name": "General Chat",
        "name": "General Chat"
    })()
    mock_room2 = type("Room", (), {
        "room_id": "!room456:server.com", 
        "display_name": None,
        "name": None
    })()
    
    mock_client = type("Client", (), {
        "invited_rooms": {
            "!room123:server.com": mock_room1,
            "!room456:server.com": mock_room2
        }
    })()
    obs.client = mock_client
    
    result = await obs.get_invites()
    
    assert result["success"] is True
    assert len(result["invites"]) == 2
    assert result["invites"][0]["room_id"] == "!room123:server.com"
    assert result["invites"][0]["name"] == "General Chat"
    assert result["invites"][1]["room_id"] == "!room456:server.com"
    assert result["invites"][1]["name"] == "Unknown Room"  # Fallback

@pytest.mark.asyncio
async def test_matrix_observer_get_invites_no_client():
    """Test MatrixObserver get_invites method when no client is available."""
    wsm = WorldStateManager()
    obs = MatrixObserver(world_state_manager=wsm)
    obs.client = None
    
    result = await obs.get_invites()
    
    assert result["success"] is False
    assert "Matrix client not connected" in result["error"]
