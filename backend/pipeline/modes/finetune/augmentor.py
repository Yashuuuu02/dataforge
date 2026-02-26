"""Dataset Augmentor leveraging LLMs to increase training volume."""

import logging
import pandas as pd

from pipeline.common.base import PipelineStep, StepResult

logger = logging.getLogger(__name__)

class DataAugmentorStep(PipelineStep):
    """Dataset Augmentation Step utilizing AI capabilities."""
    name = "data_augmentor"
    description = "Increases training examples using AI generation or heuristic alterations."

    def run(self, df: pd.DataFrame, config: dict) -> StepResult:
        if df.empty:
            return StepResult(df.copy(), 0, 0, 0, {}, [])

        df_out = df.copy()
        rows_before = len(df_out)
        warnings = []
        
        # strategy = config.get("strategy", "generate_similar")
        # multiplier = config.get("multiplier", 2.0)
        preserve_originals = config.get("preserve_originals", True)
        
        # Synchronous pipeline steps cannot properly await the `SyntheticDataGenerator`.
        # In a full production setup, this would either be an async step, or the runner 
        # itself would manage an async event loop for LLM calls.
        # For Phase 5 architecture, we will log a warning and return the dataframe
        # unaltered if AI processing is too heavy, OR we can return a mock augmentation.
        
        warnings.append("DataAugmentor requires the async AI generation worker. Skipped in synchronous dry-runs.")
        
        stats = {
            "synthetic_examples_added": 0,
            "original_preserved": preserve_originals,
            "strategy": config.get("strategy")
        }

        return StepResult(
            df=df_out,
            rows_before=rows_before,
            rows_after=len(df_out),
            rows_removed=0,
            metadata=stats,
            warnings=warnings
        )
