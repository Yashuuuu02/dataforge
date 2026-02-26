"""PII scrubbing pipeline step — Presidio + regex fallback."""

import logging
import re
from typing import Optional

import pandas as pd

from pipeline.common.base import PipelineStep, StepResult, register_step

logger = logging.getLogger(__name__)

# ── Regex patterns for common PII ──
PII_PATTERNS = {
    "EMAIL": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
    "PHONE": r"(?:\+?1[-.\s]?)?\(?[2-9]\d{2}\)?[-.\s]?\d{3}[-.\s]?\d{4}",
    "SSN": r"\b\d{3}[-]?\d{2}[-]?\d{4}\b",
    "CREDIT_CARD": r"\b(?:\d{4}[-\s]?){3}\d{4}\b",
    "IP_ADDRESS": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
    "URL": r"https?://[^\s<>\"']+|www\.[^\s<>\"']+",
}


def _has_presidio() -> bool:
    try:
        from presidio_analyzer import AnalyzerEngine  # noqa: F401
        from presidio_anonymizer import AnonymizerEngine  # noqa: F401
        return True
    except ImportError:
        return False


@register_step
class PIIScrubberStep(PipelineStep):
    name = "pii_scrubbing"
    description = "Detect and redact/remove/flag personally identifiable information"

    def validate_config(self, config: dict) -> None:
        action = config.get("action", "redact")
        if action not in ("redact", "remove_row", "flag"):
            raise ValueError(f"Invalid action: {action}. Use 'redact', 'remove_row', or 'flag'.")

    def run(self, df: pd.DataFrame, config: dict) -> StepResult:
        rows_before = len(df)
        warnings: list[str] = []

        action = config.get("action", "redact")
        entities = config.get("entities", ["ALL"])
        redact_with = config.get("redact_with", "[REDACTED]")
        columns = config.get("columns", "all_text")

        # Determine text columns
        if columns == "all_text":
            text_cols = list(df.select_dtypes(include=["object"]).columns)
        else:
            text_cols = [c for c in columns if c in df.columns]

        if not text_cols:
            warnings.append("No text columns found for PII scanning.")
            return StepResult(df=df.copy(), rows_before=rows_before, rows_after=rows_before,
                              rows_removed=0, metadata={"rows_with_pii": 0}, warnings=warnings)

        result_df = df.copy()
        pii_counts: dict[str, int] = {}
        rows_with_pii = 0
        total_instances = 0

        # Try Presidio first, fall back to regex
        if _has_presidio() and entities != ["REGEX_ONLY"]:
            result_df, pii_counts, rows_with_pii, total_instances = self._presidio_scan(
                result_df, text_cols, action, entities, redact_with, warnings
            )
        else:
            if not _has_presidio():
                warnings.append("Presidio not available, using regex patterns only.")
            result_df, pii_counts, rows_with_pii, total_instances = self._regex_scan(
                result_df, text_cols, action, entities, redact_with
            )

        # Handle actions
        if action == "remove_row" and "pii_detected" in result_df.columns:
            result_df = result_df[~result_df["pii_detected"]].drop(columns=["pii_detected"], errors="ignore")
            result_df = result_df.reset_index(drop=True)
        elif action == "flag" and "pii_detected" not in result_df.columns:
            result_df["pii_detected"] = False
            result_df["pii_entities"] = ""

        rows_after = len(result_df)
        return StepResult(
            df=result_df,
            rows_before=rows_before,
            rows_after=rows_after,
            rows_removed=rows_before - rows_after,
            metadata={
                "rows_with_pii": rows_with_pii,
                "total_pii_instances": total_instances,
                "entities_found": pii_counts,
                "action_taken": action,
                "columns_scanned": text_cols,
            },
            warnings=warnings,
        )

    def _presidio_scan(self, df, text_cols, action, entities, redact_with, warnings):
        from presidio_analyzer import AnalyzerEngine
        from presidio_anonymizer import AnonymizerEngine
        from presidio_anonymizer.entities import OperatorConfig

        analyzer = AnalyzerEngine()
        anonymizer = AnonymizerEngine()

        pii_counts: dict[str, int] = {}
        rows_with_pii = 0
        total_instances = 0
        pii_flags = []
        pii_entity_lists = []

        entity_list = None if "ALL" in entities else entities

        for idx, row in df.iterrows():
            row_has_pii = False
            row_entities: list[str] = []

            for col in text_cols:
                text = str(row[col]) if pd.notna(row[col]) else ""
                if not text:
                    continue

                results = analyzer.analyze(text=text, language="en", entities=entity_list)

                if results:
                    row_has_pii = True
                    for r in results:
                        pii_counts[r.entity_type] = pii_counts.get(r.entity_type, 0) + 1
                        total_instances += 1
                        if r.entity_type not in row_entities:
                            row_entities.append(r.entity_type)

                    if action == "redact":
                        operators = {
                            "DEFAULT": OperatorConfig("replace", {"new_value": redact_with})
                        }
                        if redact_with == "<ENTITY_TYPE>":
                            operators = {}  # Use default which replaces with entity type
                        anonymized = anonymizer.anonymize(text=text, analyzer_results=results, operators=operators)
                        df.at[idx, col] = anonymized.text

            if row_has_pii:
                rows_with_pii += 1
            pii_flags.append(row_has_pii)
            pii_entity_lists.append(",".join(row_entities))

        if action in ("remove_row", "flag"):
            df["pii_detected"] = pii_flags
            df["pii_entities"] = pii_entity_lists

        return df, pii_counts, rows_with_pii, total_instances

    def _regex_scan(self, df, text_cols, action, entities, redact_with):
        pii_counts: dict[str, int] = {}
        rows_with_pii = 0
        total_instances = 0
        pii_flags = []
        pii_entity_lists = []

        patterns = PII_PATTERNS if "ALL" in entities else {
            k: v for k, v in PII_PATTERNS.items() if k in entities
        }

        for idx, row in df.iterrows():
            row_has_pii = False
            row_entities: list[str] = []

            for col in text_cols:
                text = str(row[col]) if pd.notna(row[col]) else ""
                if not text:
                    continue

                for entity_type, pattern in patterns.items():
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    if matches:
                        row_has_pii = True
                        count = len(matches)
                        pii_counts[entity_type] = pii_counts.get(entity_type, 0) + count
                        total_instances += count
                        if entity_type not in row_entities:
                            row_entities.append(entity_type)

                        if action == "redact":
                            replacement = f"<{entity_type}>" if redact_with == "<ENTITY_TYPE>" else redact_with
                            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
                            df.at[idx, col] = text

            if row_has_pii:
                rows_with_pii += 1
            pii_flags.append(row_has_pii)
            pii_entity_lists.append(",".join(row_entities))

        if action in ("remove_row", "flag"):
            df["pii_detected"] = pii_flags
            df["pii_entities"] = pii_entity_lists

        return df, pii_counts, rows_with_pii, total_instances
