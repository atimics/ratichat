"""
Tests for AI decision engine functionality.
"""
import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock, Mock
from chatbot.core.ai_engine import AIDecisionEngine, DecisionResult, ActionPlan


class TestAIDecisionEngine:
    """Test the AI decision engine with various scenarios."""
    
    def test_initialization_with_config(self):
        """Test initialization with custom config."""
        engine = AIDecisionEngine(api_key="test_key", model="claude-3-haiku")
        
        assert engine.api_key == "test_key"
        assert engine.model == "claude-3-haiku"
        assert engine.base_url is not None
    
    def test_initialization_without_api_key(self):
        """Test initialization fails without API key."""
        with pytest.raises(TypeError):
            # Should fail because api_key is required parameter
            AIDecisionEngine()
    
    @pytest.mark.asyncio
    async def test_make_decision_successful_response(self):
        """Test successful decision making with mocked response."""
        engine = AIDecisionEngine(api_key="test_key")
        
        # Mock response data that matches current JSON structure
        mock_response_data = {
            "choices": [{
                "message": {
                    "content": json.dumps({
                        "observations": "Test observation",
                        "potential_actions": [],
                        "selected_actions": [{
                            "action_type": "wait",
                            "parameters": {},
                            "reasoning": "No action needed",
                            "priority": 1
                        }],
                        "reasoning": "Test reasoning"
                    })
                }
            }]
        }
        
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data
        mock_response.raise_for_status = MagicMock()
        
        # Mock httpx.AsyncClient properly
        with patch('httpx.AsyncClient') as mock_client_class:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            
            # Set up the async context manager
            mock_client_class.return_value = mock_client
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            
            result = await engine.make_decision({"test": "state"}, "test_cycle")
            
            assert result.cycle_id == "test_cycle"
            assert result.observations == "Test observation"
            assert len(result.selected_actions) == 1
            assert result.selected_actions[0].action_type == "wait"
    
    @pytest.mark.asyncio
    async def test_make_decision_invalid_json_response(self):
        """Test handling of invalid JSON response."""
        engine = AIDecisionEngine(api_key="test_key")
        
        mock_response_data = {
            "choices": [{
                "message": {
                    "content": "Invalid JSON response"
                }
            }]
        }
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data
        mock_response.raise_for_status.return_value = None
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            mock_client.return_value = mock_context
            
            result = await engine.make_decision({"test": "state"}, "test_cycle")
            
            # Should handle invalid JSON gracefully
            assert result.cycle_id == "test_cycle"
            assert "error" in result.reasoning.lower() or "failed" in result.reasoning.lower()
    
    @pytest.mark.asyncio
    async def test_make_decision_http_error(self):
        """Test handling of HTTP errors."""
        engine = AIDecisionEngine(api_key="test_key")
        
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_response.raise_for_status.return_value = None
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            mock_client.return_value = mock_context
            
            result = await engine.make_decision({"test": "state"}, "test_cycle")
            
            assert result.cycle_id == "test_cycle"
            assert len(result.selected_actions) == 0
            assert "API Error" in result.reasoning
    
    @pytest.mark.asyncio
    async def test_make_decision_network_exception(self):
        """Test handling of network exceptions."""
        engine = AIDecisionEngine(api_key="test_key")
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(side_effect=Exception("Network timeout"))
            mock_client.return_value = mock_context
            
            result = await engine.make_decision({"test": "state"}, "test_cycle")
            
            assert result.cycle_id == "test_cycle"
            assert len(result.selected_actions) == 0
            assert "error" in result.reasoning.lower()
    
    @pytest.mark.asyncio
    async def test_make_decision_no_choices_in_response(self):
        """Test handling of response with no choices."""
        engine = AIDecisionEngine(api_key="test_key")
        
        mock_response_data = {
            "choices": []
        }
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data
        mock_response.raise_for_status.return_value = None
        
        with patch('httpx.AsyncClient') as mock_client:
            mock_context = AsyncMock()
            mock_context.__aenter__.return_value.post = AsyncMock(return_value=mock_response)
            mock_client.return_value = mock_context
            
            result = await engine.make_decision({"test": "state"}, "test_cycle")
            
            assert result.cycle_id == "test_cycle"
            assert len(result.selected_actions) == 0
    
    def test_cleanup(self):
        """Test cleanup method (if it exists)."""
        engine = AIDecisionEngine(api_key="test_key")
        # Current implementation doesn't have cleanup method, so just verify it doesn't crash
        # If cleanup method is added later, this test should be updated
        assert engine.api_key == "test_key"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["node_based", "traditional"])
async def test_node_decisions_request_valid_json_from_provider(mode):
    import httpx
    requests = []
    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps({
            "selected_actions": [{"action_type": "send_discord_reply", "parameters": {"channel_id": "20", "reply_to_id": "40", "content": "[Docs](https://docs.python.org/)"}, "reasoning": "Cite source", "priority": 5}],
            "reasoning": "Reply", "observations": "Source read",
        })}}]})
    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    engine = AIDecisionEngine(api_key="test-key")
    with patch("chatbot.core.ai_engine.httpx.AsyncClient", return_value=client):
        result = await engine.make_decision({"processing_mode": mode, "current_request": {"id": "40", "content": "Read this page"}}, "test")
    assert result.selected_actions[0].parameters["reply_to_id"] == "40"
    if mode == "node_based":
        assert requests[0]["response_format"] == {"type": "json_object"}
        assert requests[0]["temperature"] == 0.2
        assert "Answer current_request" in requests[0]["messages"][0]["content"]
    else:
        assert "response_format" not in requests[0]


@pytest.mark.asyncio
async def test_answer_composition_returns_text_from_scoped_sources():
    import httpx
    requests = []
    def respond(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps({"content": "Answer [source](https://example.com)"})}}]})
    client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    engine = AIDecisionEngine(api_key="test-key")
    with patch("chatbot.core.ai_engine.httpx.AsyncClient", return_value=client):
        text = await engine.compose_reply({"current_request": {"content": "question"}, "answer_nodes": {"source": "public evidence"}, "private_state": "PRIVATE SECRET"})
    assert text == "Answer [source](https://example.com)"
    assert "public evidence" in requests[0]["messages"][1]["content"]
    assert "PRIVATE SECRET" not in requests[0]["messages"][1]["content"]
    assert requests[0]["response_format"] == {"type": "json_object"}
