"""LiteLLM Client for standardized LLM interaction."""

import asyncio
import copy
import json
import logging
from typing import Any, Dict, List, Optional

import litellm
from litellm import acompletion
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# Drop requests to litellm proxy telemetry
litellm.telemetry = False

class LiteLLMClient:
    """Client for interacting with LLM providers using LiteLLM."""

    def __init__(self, provider: str, api_key: str, model: str, base_url: Optional[str] = None):
        self.provider = provider
        self.api_key = api_key
        self.model = model
        self.base_url = base_url
        
        if self.provider == "ollama" and not self.base_url:
            self.base_url = "http://localhost:11434"

    def _prepare_kwargs(self) -> Dict[str, Any]:
        """Prepare kwargs for litellm.acompletion."""
        # Format the model string appropriately for LiteLLM
        model_str = self.model
        if self.provider not in ("openai", "anthropic") and not model_str.startswith(f"{self.provider}/"):
            if self.provider == "ollama":
                model_str = f"ollama/{self.model}"
            elif self.provider == "groq":
                model_str = f"groq/{self.model}"
            elif self.provider == "mistral":
                model_str = f"mistral/{self.model}"

        kwargs = {
            "model": model_str,
            "api_key": self.api_key,
        }
        if self.base_url:
            kwargs["api_base"] = self.base_url
            
        return kwargs

    @retry(
        retry=retry_if_exception_type((litellm.RateLimitError, litellm.APIConnectionError, litellm.ServiceUnavailableError)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(5)
    )
    async def complete(self, messages: List[Dict[str, str]], temperature: float = 0.7, max_tokens: int = 2000, response_format: Optional[Dict] = None) -> str:
        """Call the LLM with backoff for rate limits."""
        kwargs = self._prepare_kwargs()
        if response_format:
            kwargs["response_format"] = response_format

        response = await acompletion(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs
        )
        return response.choices[0].message.content

    async def complete_batch(self, batch: List[List[Dict[str, str]]], concurrency: int = 5, delay_between: float = 0.5) -> List[str]:
        """Run multiple completions concurrently with rate limit protection."""
        sem = asyncio.Semaphore(concurrency)
        
        async def _bounded_complete(i: int, msgs: List[Dict[str, str]]) -> tuple[int, str]:
            async with sem:
                await asyncio.sleep(delay_between) # Delay to smooth out traffic
                try:
                    res = await self.complete(msgs)
                    return (i, res)
                except Exception as e:
                    logger.error(f"Batch item {i} failed: {e}")
                    return (i, "")

        tasks = [_bounded_complete(i, msgs) for i, msgs in enumerate(batch)]
        results = await asyncio.gather(*tasks)
        
        # Sort by index to maintain original order
        results.sort(key=lambda x: x[0])
        return [r[1] for r in results]

    async def complete_json(self, messages: List[Dict[str, str]], schema: Optional[Dict] = None) -> Dict:
        """Guarantees JSON response — retries if response isn't valid JSON."""
        # Use litellm JSON mode if supported
        response_format = {"type": "json_object"} if self.provider in ("openai", "groq", "mistral") else None
        
        # Modify system prompt to strongly ask for JSON
        msgs = copy.deepcopy(messages)
        if msgs and msgs[0]["role"] == "system":
            msgs[0]["content"] += "\n\nRespond ONLY with valid JSON."

        for attempt in range(3):
            try:
                content = await self.complete(msgs, response_format=response_format, temperature=0.1)
                
                # Strip markdown code blocks if present (often LLMs wrap JSON in ```json ... ```)
                content = content.strip()
                if content.startswith("```json"):
                    content = content[7:]
                if content.startswith("```"):
                    content = content[3:]
                if content.endswith("```"):
                    content = content[:-3]
                    
                return json.loads(content.strip())
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse JSON on attempt {attempt+1}. Content: {content[:100]}...")
                if attempt == 2:
                    raise ValueError(f"LLM failed to return valid JSON after 3 attempts.")
                msgs.append({"role": "assistant", "content": content})
                msgs.append({"role": "user", "content": "That wasn't valid JSON. Please reply with ONLY valid JSON."})
        return {}
        
    def test_connection(self) -> bool:
        """Makes a minimal API call to verify credentials work."""
        try:
            kwargs = self._prepare_kwargs()
            # Synchronous call just for quick testing
            response = litellm.completion(
                messages=[{"role": "user", "content": "Say 'ok'"}],
                max_tokens=5,
                **kwargs
            )
            return len(response.choices) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
