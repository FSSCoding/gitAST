"""Tests for GitAST LLM client."""
from gitast.llm import LLMConfig, LLMClient, repair_json


class TestLLMConfig:
    def test_defaults(self):
        config = LLMConfig()
        assert config.endpoint == "http://llm.internal.bobai.com.au:11433/v1"
        assert config.fallback_endpoint == "http://lmstudio.internal.bobai.com.au:1234/v1"
        assert config.remote_endpoint == "http://inference.internal.bobai.com.au:1234/v1"
        assert config.model == "qwen/qwen3-coder-30b"
        assert config.temperature == 0.2
        assert config.max_tokens == 4000
        assert config.timeout == 120
        assert config.use_remote is False

    def test_custom_config(self):
        config = LLMConfig(endpoint="http://custom:8000/v1", model="test-model", use_remote=True)
        assert config.endpoint == "http://custom:8000/v1"
        assert config.model == "test-model"
        assert config.use_remote is True


class TestRepairJson:
    def test_valid_json(self):
        result = repair_json('{"key": "value"}')
        assert result == {"key": "value"}

    def test_markdown_fenced(self):
        result = repair_json('```json\n{"key": "value"}\n```')
        assert result == {"key": "value"}

    def test_markdown_fenced_no_lang(self):
        result = repair_json('```\n{"key": "value"}\n```')
        assert result == {"key": "value"}

    def test_trailing_comma(self):
        result = repair_json('{"items": ["a", "b",]}')
        assert result == {"items": ["a", "b"]}

    def test_trailing_comma_object(self):
        result = repair_json('{"a": 1, "b": 2,}')
        assert result == {"a": 1, "b": 2}

    def test_extra_text_before_json(self):
        result = repair_json('Here is the result:\n{"key": "value"}')
        assert result == {"key": "value"}

    def test_extra_text_after_json(self):
        result = repair_json('{"key": "value"}\nHope this helps!')
        assert result == {"key": "value"}

    def test_empty_string(self):
        assert repair_json("") is None

    def test_none(self):
        assert repair_json(None) is None

    def test_whitespace_only(self):
        assert repair_json("   \n  ") is None

    def test_no_json(self):
        assert repair_json("this is just text") is None

    def test_nested_object(self):
        result = repair_json('{"outer": {"inner": [1, 2, 3]}}')
        assert result == {"outer": {"inner": [1, 2, 3]}}

    def test_array_not_returned(self):
        result = repair_json('[1, 2, 3]')
        assert result is None

    def test_complex_schema(self):
        text = '''```json
{
  "headline": "Active project",
  "overview": "This is a test.",
  "key_findings": ["finding 1", "finding 2"],
  "risk_assessment": "Low risk."
}
```'''
        result = repair_json(text)
        assert result is not None
        assert result['headline'] == "Active project"
        assert len(result['key_findings']) == 2


class TestLLMClient:
    def test_endpoints_without_remote(self):
        config = LLMConfig(use_remote=False)
        client = LLMClient(config)
        endpoints = client._get_endpoints()
        assert len(endpoints) == 2
        assert config.remote_endpoint not in endpoints

    def test_endpoints_with_remote(self):
        config = LLMConfig(use_remote=True)
        client = LLMClient(config)
        endpoints = client._get_endpoints()
        assert len(endpoints) == 3
        assert config.remote_endpoint in endpoints

    def test_complete_returns_none_without_endpoint(self):
        """Without a reachable endpoint, complete should return None."""
        config = LLMConfig(endpoint="http://localhost:1/v1")
        client = LLMClient(config)
        result = client.complete("test prompt")
        assert result is None

    def test_health_check_unreachable(self):
        """Health check against unreachable endpoint returns None."""
        config = LLMConfig(
            endpoint="http://localhost:1/v1",
            fallback_endpoint="http://localhost:2/v1",
        )
        client = LLMClient(config)
        result = client.health_check()
        assert result is None
