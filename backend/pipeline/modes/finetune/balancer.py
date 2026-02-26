"""Category balancer step for fine-tuning datasets."""

import logging
from typing import Dict
import pandas as pd
import numpy as np

from pipeline.common.base import PipelineStep, StepResult
# We would import SyntheticDataGenerator in a full implementation for the "augment" method
# from ai.synthetic_gen import SyntheticDataGenerator

logger = logging.getLogger(__name__)

class CategoryBalancerStep(PipelineStep):
    """Balances dataset categories via undersampling or oversampling."""
    name = "category_balancer"
    description = "Balances dataset categories to prevent model bias."

    def run(self, df: pd.DataFrame, config: dict) -> StepResult:
        if df.empty:
            return StepResult(df.copy(), 0, 0, 0, {}, [])

        df_out = df.copy()
        rows_before = len(df_out)
        warnings = []
        
        method = config.get("method", "undersample") # undersample | oversample | augment
        target_col = config.get("target_column", "auto")
        max_per_cat = config.get("max_per_category")
        min_per_cat = config.get("min_per_category")
        balance_ratio = config.get("balance_ratio", 1.0)
        
        # 1. Detect Category Column
        if target_col == "auto":
             # Look for low cardinality string columns
             target_col = self._auto_detect_target(df_out)
             if not target_col:
                 warnings.append("Could not auto-detect a category column. Balancer skipped.")
                 return StepResult(df_out, rows_before, rows_before, 0, {"status": "skipped_no_column"}, warnings)
             logger.info(f"Auto-detected category column: {target_col}")

        if target_col not in df_out.columns:
             warnings.append(f"Target column '{target_col}' not found. Balancer skipped.")
             return StepResult(df_out, rows_before, rows_before, 0, {"status": "skipped_missing_column"}, warnings)

        # 2. Compute Distribution Before
        dist_before = df_out[target_col].value_counts().to_dict()
        
        if not dist_before:
             return StepResult(df_out, rows_before, rows_before, 0, {"status": "skipped_empty_col"}, warnings)

        # 3. Apply Balancing Logic
        synthetic_added = 0
        min_class_count = min(dist_before.values())
        max_class_count = max(dist_before.values())
        
        if method == "undersample":
             # Target is the minority class, or max_per_category
             target_count = min_class_count
             if max_per_cat and max_per_cat < target_count:
                 target_count = max_per_cat
                 
             # Apply ratio: if ratio is 0.5, majority can be 2x minority
             # For a pure undersample, usually we just downsample to the target
             target_count = max(1, int(target_count / balance_ratio))
             
             dfs = []
             for cat, group in df_out.groupby(target_col):
                  if len(group) > target_count:
                       dfs.append(group.sample(n=target_count, random_state=42))
                  else:
                       dfs.append(group)
             df_out = pd.concat(dfs).reset_index(drop=True)

        elif method == "oversample":
             # Target is the majority class, or min_per_cat
             target_count = max_class_count
             if min_per_cat and target_count < min_per_cat:
                  target_count = min_per_cat
             if max_per_cat and target_count > max_per_cat:
                  target_count = max_per_cat
                  
             dfs = []
             for cat, group in df_out.groupby(target_col):
                  curr_len = len(group)
                  if curr_len < target_count:
                       # Oversample with replacement
                       needed = target_count - curr_len
                       sampled = group.sample(n=needed, replace=True, random_state=42)
                       dfs.append(pd.concat([group, sampled]))
                  else:
                       dfs.append(group.sample(n=target_count, random_state=42) if curr_len > target_count else group)
             df_out = pd.concat(dfs).reset_index(drop=True)
             
        elif method == "augment":
             # In a real pipeline, we'd invoke the SemanticDataGenerator here asynchronously per class.
             # For this synchronous pipeline step, we will log a warning indicating it requires the AI worker layer.
             warnings.append("'augment' method requires async AI generation. Falling back to 'oversample'.")
             # Fallback to oversample
             return self.run(df, {**config, "method": "oversample"})

        # 4. Final Stats
        df_out = df_out.sample(frac=1.0, random_state=42).reset_index(drop=True) # Shuffle
        dist_after = df_out[target_col].value_counts().to_dict()

        return StepResult(
            df=df_out,
            rows_before=rows_before,
            rows_after=len(df_out),
            rows_removed=rows_before - len(df_out),
            metadata={
                "category_column": target_col,
                "distribution_before": dist_before,
                "distribution_after": dist_after,
                "method": method,
                "synthetic_examples_added": synthetic_added
            },
            warnings=warnings
        )
        
    def _auto_detect_target(self, df: pd.DataFrame) -> str | None:
         # Find columns with low cardinality (<50) and string/categorical types
         best_col = None
         lowest = float('inf')
         
         for col in df.columns:
              if col in ["_norm_instruction", "_norm_input", "_norm_output"]: continue
              
              if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_categorical_dtype(df[col]):
                   uniq = df[col].nunique()
                   if 1 < uniq <= 50 and uniq < lowest:
                        lowest = uniq
                        best_col = col
         return best_col
