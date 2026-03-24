"""LLM client for GitAST report prose generation."""
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class LLMConfig:
    """Configuration for LLM endpoint chain."""
    endpoint: str = "http://llm.internal.bobai.com.au:11433/v1"
    fallback_endpoint: str = "http://lmstudio.internal.bobai.com.au:1234/v1"
    remote_endpoint: str = "http://inference.internal.bobai.com.au:1234/v1"
    model: str = "qwen/qwen3-coder-30b"
    temperature: float = 0.2
    max_tokens: int = 4000
    timeout: int = 120
    use_remote: bool = False


def repair_json(text: str) -> Optional[Dict]:
    """Extract and repair JSON from LLM output.

    Handles common issues: markdown fences, trailing commas, extra text.
    Returns parsed dict or None on failure.
    """
    if not text or not text.strip():
        return None

    s = text.strip()

    # Strip Qwen-style thinking blocks
    s = re.sub(r'<think>.*?</think>', '', s, flags=re.DOTALL).strip()

    # Strip markdown code fences
    if s.startswith("```"):
        # Remove opening fence (with optional language tag)
        s = re.sub(r'^```\w*\s*\n?', '', s)
        # Remove closing fence
        s = re.sub(r'\n?```\s*$', '', s)
        s = s.strip()

    # Try direct parse first
    try:
        result = json.loads(s)
        if isinstance(result, dict):
            return result
    except json.JSONDecodeError:
        pass

    # Extract first {...} object
    brace_depth = 0
    start = None
    for i, ch in enumerate(s):
        if ch == '{':
            if brace_depth == 0:
                start = i
            brace_depth += 1
        elif ch == '}':
            brace_depth -= 1
            if brace_depth == 0 and start is not None:
                candidate = s[start:i + 1]
                # Fix trailing commas before } or ]
                candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
                try:
                    result = json.loads(candidate)
                    if isinstance(result, dict):
                        return result
                except json.JSONDecodeError:
                    pass
                start = None

    return None


class LLMClient:
    """OpenAI-compatible LLM client with endpoint fallback chain."""

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig()
        self._available_endpoint: Optional[str] = None

    def _get_endpoints(self) -> List[str]:
        """Return ordered list of endpoints to try."""
        endpoints = [self.config.endpoint, self.config.fallback_endpoint]
        if self.config.use_remote:
            endpoints.append(self.config.remote_endpoint)
        return endpoints

    def health_check(self) -> Optional[str]:
        """Find the first healthy endpoint. Returns endpoint URL or None."""
        try:
            from openai import OpenAI
        except ImportError:
            return None

        for endpoint in self._get_endpoints():
            try:
                client = OpenAI(base_url=endpoint, api_key="not-needed")
                client.with_options(timeout=5.0).models.list()
                self._available_endpoint = endpoint
                return endpoint
            except Exception:
                continue

        return None

    def complete(self, prompt: str, schema_hint: str = "") -> Optional[Dict]:
        """Send a prompt to the LLM and return parsed JSON dict.

        Args:
            prompt: The full prompt text
            schema_hint: Optional JSON schema description (included in system message)

        Returns:
            Parsed JSON dict from LLM response, or None on failure.
        """
        try:
            from openai import OpenAI
        except ImportError:
            return None

        if not self._available_endpoint:
            if not self.health_check():
                return None

        endpoint = self._available_endpoint
        backoff_delays = [1, 2, 4]
        temp = self.config.temperature

        for attempt in range(3):
            try:
                client = OpenAI(
                    base_url=endpoint,
                    api_key="not-needed",
                )

                system_msg = "You are a code analysis assistant. IMPORTANT: You MUST respond ONLY in English. Never use Chinese, Japanese, or any other non-English language. Respond ONLY with valid JSON matching the requested schema. No markdown, no explanation, no preamble, no thinking, just the raw JSON object."
                if schema_hint:
                    system_msg += f"\n\nExpected JSON schema:\n{schema_hint}"

                response = client.with_options(
                    timeout=float(self.config.timeout)
                ).chat.completions.create(
                    model=self.config.model,
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=temp,
                    max_tokens=self.config.max_tokens,
                )

                content = response.choices[0].message.content
                result = repair_json(content)
                if result is not None:
                    return result

                # JSON repair failed — retry with higher temp
                temp = min(temp + 0.1, 1.0)

            except Exception:
                pass

            if attempt < 2:
                time.sleep(backoff_delays[attempt])

        return None
