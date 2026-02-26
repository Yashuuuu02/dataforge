"""Language detection and filtering pipeline step."""

import logging

import pandas as pd
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException

from pipeline.common.base import PipelineStep, StepResult, register_step

logger = logging.getLogger(__name__)

# Make langdetect deterministic
DetectorFactory.seed = 0


@register_step
class LanguageFilterStep(PipelineStep):
    name = "language_filter"
    description = "Detect language per row and filter/tag based on language"

    def validate_config(self, config: dict) -> None:
        action = config.get("action", "tag_only")
        if action not in ("filter_keep", "filter_remove", "tag_only"):
            raise ValueError(f"Invalid action: {action}. Use 'filter_keep', 'filter_remove', or 'tag_only'.")

    def run(self, df: pd.DataFrame, config: dict) -> StepResult:
        rows_before = len(df)
        warnings: list[str] = []
        action = config.get("action", "tag_only")
        languages = config.get("languages", ["en"])
        min_confidence = config.get("min_confidence", 0.8)
        text_column = config.get("text_column", "auto")
        tag_col = config.get("tag_column_name", "language")

        result_df = df.copy()

        # Auto-detect text column
        if text_column == "auto":
            text_cols = result_df.select_dtypes(include=["object"]).columns
            if len(text_cols) == 0:
                warnings.append("No text columns found for language detection.")
                return StepResult(df=result_df, rows_before=rows_before, rows_after=rows_before,
                                  rows_removed=0, metadata={"language_distribution": {}}, warnings=warnings)
            # Use the column with the longest average text
            avg_lengths = {col: result_df[col].astype(str).str.len().mean() for col in text_cols}
            text_column = max(avg_lengths, key=avg_lengths.get)
            logger.info("Auto-detected text column for language: %s", text_column)

        if text_column not in result_df.columns:
            warnings.append(f"Column '{text_column}' not found.")
            return StepResult(df=result_df, rows_before=rows_before, rows_after=rows_before,
                              rows_removed=0, metadata={"language_distribution": {}}, warnings=warnings)

        # Detect language for each row
        detected_langs: list[str] = []
        confidences: list[float] = []

        for _, row in result_df.iterrows():
            text = str(row[text_column]) if pd.notna(row[text_column]) else ""
            if len(text.strip()) < 20:
                detected_langs.append("unknown")
                confidences.append(0.0)
                continue
            try:
                lang = detect(text)
                detected_langs.append(lang)
                confidences.append(1.0)  # langdetect doesn't expose confidence easily
            except LangDetectException:
                detected_langs.append("unknown")
                confidences.append(0.0)

        result_df[tag_col] = detected_langs
        result_df[f"{tag_col}_confidence"] = confidences

        # Count distribution
        lang_dist = result_df[tag_col].value_counts().to_dict()

        # Apply filter
        rows_removed = 0
        if action == "filter_keep":
            mask = result_df[tag_col].isin(languages)
            result_df = result_df[mask].reset_index(drop=True)
            rows_removed = rows_before - len(result_df)
        elif action == "filter_remove":
            mask = ~result_df[tag_col].isin(languages)
            result_df = result_df[mask].reset_index(drop=True)
            rows_removed = rows_before - len(result_df)

        return StepResult(
            df=result_df,
            rows_before=rows_before,
            rows_after=len(result_df),
            rows_removed=rows_removed,
            metadata={
                "language_distribution": lang_dist,
                "rows_removed": rows_removed,
                "action": action,
                "text_column_used": text_column,
            },
            warnings=warnings,
        )
