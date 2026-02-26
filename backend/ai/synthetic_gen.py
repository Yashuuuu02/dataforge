"""Generates synthetic training data asynchronously."""

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from ai.litellm_client import LiteLLMClient

logger = logging.getLogger(__name__)

@dataclass
class SyntheticGenConfig:
    temperature: float = 0.8
    diversity_prompt: str = ""
    domain_hint: str = ""
    quality_filter: bool = True
    batch_size: int = 10
    output_format: str = "same_as_input"

class SyntheticDataGenerator:
    def __init__(self, llm_client: LiteLLMClient):
        self.llm = llm_client

    async def generate(self, df: pd.DataFrame, count: int, config: SyntheticGenConfig, dataset_type: str = "instruction_pairs") -> pd.DataFrame:
        """Generate synthetic examples resembling the input."""
        if df.empty or not self.llm:
            return pd.DataFrame()

        # Extract schema and small sample
        sample_df = df.sample(min(5, len(df)))
        columns = list(sample_df.columns)
        examples_json = sample_df.to_dict(orient="records")

        system_prompt = f"""You are an advanced AI generating synthetic training data.
The user wants {config.batch_size} new, diverse examples matching the EXACT schema of the provided input.
Domain constraint: {config.domain_hint if config.domain_hint else 'Match the domain of the examples'}
Diversity instruction: {config.diversity_prompt if config.diversity_prompt else 'Vary the topics and formats but keep realism.'}
Output MUST be a JSON array of objects with these exact keys: {json.dumps(columns)}
Do NOT output anything other than the JSON array."""

        user_prompt = f"Learn from these {len(examples_json)} examples:\n{json.dumps(examples_json, indent=2)}\n\nGenerate {config.batch_size} net-new, highly diverse examples now."

        generated_rows = []
        batches_needed = (count // config.batch_size) + (1 if count % config.batch_size > 0 else 0)

        # Send concurrent batches
        logger.info(f"Generating {count} synthetic rows in {batches_needed} batches.")
        
        # We can construct multiple identical requests for the batch_completer
        batch_msgs = [
            [
               {"role": "system", "content": system_prompt},
               {"role": "user", "content": user_prompt}
            ]
        ] * batches_needed

        results = await self.llm.complete_batch(batch_msgs, concurrency=3, delay_between=1.0)

        for res in results:
            try:
                # Need to manually extract the JSON array since complete_batch doesn't parse JSON natively
                content = res.strip()
                if content.startswith("```json"): content = content[7:]
                if content.startswith("```"): content = content[3:]
                if content.endswith("```"): content = content[:-3]
                
                rows = json.loads(content.strip())
                if isinstance(rows, list):
                    for r in rows:
                        # minimal validation
                        if all(k in r for k in columns):
                            generated_rows.append(r)
            except Exception as e:
                logger.warning(f"Failed to parse synthetic batch: {e}")

        # Post-process
        new_df = pd.DataFrame(generated_rows)
        # trim if overshot
        if len(new_df) > count:
            new_df = new_df.head(count)
        
        if not new_df.empty:
            new_df["is_synthetic"] = True
            
        return new_df
