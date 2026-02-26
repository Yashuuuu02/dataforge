"""Evaluates instruction-following quality for FT datasets."""

import re
import logging
import pandas as pd

from pipeline.common.base import PipelineStep, StepResult

logger = logging.getLogger(__name__)

class ResponseQualityStep(PipelineStep):
    """Filters low-quality or refusal responses."""
    name = "response_quality"
    description = "Filters low-quality, incomplete, or refusal responses from chat/instruction datasets."

    # Default refusal phrases common in base safety layers
    DEFAULT_REFUSALS = [
        "I cannot", "I can not", "I'm unable", "I am unable", 
        "As an AI", "As a language model", "I don't have the ability",
        "I am an AI", "I'm sorry, but", "I apologize, but", "is not appropriate"
    ]

    def run(self, df: pd.DataFrame, config: dict) -> StepResult:
        df_out = df.copy()
        rows_before = len(df_out)
        warnings = []
        
        min_resp_len = config.get("min_response_length", 10)
        max_resp_len = config.get("max_response_length", 2000)
        min_inst_len = config.get("min_instruction_length", 3)
        check_completeness = config.get("check_response_completeness", True)
        filter_refusals = config.get("filter_refusals", True)
        refusals = config.get("refusal_phrases", self.DEFAULT_REFUSALS)
        action = config.get("action", "filter")

        # We assume FinetuneFormatterStep has run and populated _norm_instruction & _norm_output
        inst_col = "_norm_instruction" if "_norm_instruction" in df_out.columns else df_out.columns[0]
        out_col = "_norm_output" if "_norm_output" in df_out.columns else df_out.columns[-1]

        # Check column existence
        if inst_col not in df_out.columns or out_col not in df_out.columns:
             warnings.append(f"Could not find instruction/output columns ({inst_col}, {out_col}). Skipping Response Quality.")
             return StepResult(df_out, rows_before, rows_before, 0, {}, warnings)

        refusal_pattern = re.compile("|".join([re.escape(r) for r in refusals]), re.IGNORECASE)
        
        def evaluate_row(row):
            inst = str(row.get(inst_col, ""))
            out = str(row.get(out_col, ""))
            
            inst_words = len(inst.split())
            out_words = len(out.split())
            
            score = 10.0
            reasons = []
            
            # 1. Length penalties
            if inst_words < min_inst_len:
                 score -= 5.0
                 reasons.append("instruction_too_short")
            if out_words < min_resp_len:
                 score -= 5.0
                 reasons.append("response_too_short")
            if out_words > max_resp_len:
                 score -= 2.0
                 reasons.append("response_too_long")
                 
            # 2. Refusals
            if filter_refusals and refusal_pattern.search(out):
                 score -= 8.0 # Heavy penalty
                 reasons.append("refusal_detected")
                 
            # 3. Completeness (heuristic: ends with punctuation)
            if check_completeness and out:
                 out_clean = out.strip()
                 if not out_clean[-1] in ".!?\"'”’]}>":
                      # Might be cut off
                      score -= 3.0
                      reasons.append("incomplete_response")

            return pd.Series([max(0.0, score), ",".join(reasons)])

        res = df_out.apply(evaluate_row, axis=1)
        df_out["_response_quality_score"] = res[0]
        df_out["_response_quality_reasons"] = res[1]

        filtered_out = 0
        if action == "filter":
             filtered_df = df_out[df_out["_response_quality_score"] >= 6.0].copy()
             filtered_out = len(df_out) - len(filtered_df)
             df_out = filtered_df
             # Drop temp columns unless debugging
             df_out = df_out.drop(columns=["_response_quality_score", "_response_quality_reasons"], errors='ignore')

        stats = {
             "avg_quality_score": df["_response_quality_score"].mean() if "_response_quality_score" in df else 0.0,
             "total_filtered": filtered_out
        }

        return StepResult(
            df=df_out,
            rows_before=rows_before,
            rows_after=len(df_out),
            rows_removed=filtered_out,
            metadata=stats,
            warnings=warnings
        )
