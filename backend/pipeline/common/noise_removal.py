"""Noise removal pipeline step — encoding fixes, HTML stripping, normalization."""

import logging
import re
import unicodedata

import pandas as pd

from pipeline.common.base import PipelineStep, StepResult, register_step

logger = logging.getLogger(__name__)


@register_step
class NoiseRemovalStep(PipelineStep):
    name = "noise_removal"
    description = "Clean text: fix encoding, strip HTML, normalize whitespace and unicode"

    def run(self, df: pd.DataFrame, config: dict) -> StepResult:
        rows_before = len(df)
        warnings: list[str] = []

        columns = config.get("columns", "all_text")
        if columns == "all_text":
            text_cols = list(df.select_dtypes(include=["object"]).columns)
        else:
            text_cols = [c for c in columns if c in df.columns]

        if not text_cols:
            warnings.append("No text columns found for noise removal.")
            return StepResult(df=df.copy(), rows_before=rows_before, rows_after=rows_before,
                              rows_removed=0, metadata={}, warnings=warnings)

        result_df = df.copy()
        encoding_fixes = 0
        html_stripped = 0
        total_chars_cleaned = 0

        for col in text_cols:
            for idx in result_df.index:
                text = result_df.at[idx, col]
                if pd.isna(text) or not isinstance(text, str):
                    continue

                original_len = len(text)
                cleaned = text

                # 1. Fix encoding
                if config.get("fix_encoding", True):
                    try:
                        import ftfy
                        fixed = ftfy.fix_text(cleaned)
                        if fixed != cleaned:
                            encoding_fixes += 1
                            cleaned = fixed
                    except ImportError:
                        if not hasattr(self, "_ftfy_warned"):
                            warnings.append("ftfy not installed, skipping encoding fixes.")
                            self._ftfy_warned = True

                # 2. Strip HTML
                if config.get("strip_html", True):
                    if "<" in cleaned and ">" in cleaned:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(cleaned, "html.parser")
                        stripped = soup.get_text(separator=" ")
                        if stripped != cleaned:
                            html_stripped += 1
                            cleaned = stripped

                # 3. Normalize unicode
                if config.get("normalize_unicode", True):
                    cleaned = unicodedata.normalize("NFC", cleaned)
                    # Remove zero-width characters
                    cleaned = re.sub(r"[\u200b\u200c\u200d\ufeff\u00ad]", "", cleaned)

                # 4. Remove control characters (keep \n, \t)
                if config.get("remove_control_chars", True):
                    cleaned = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", cleaned)

                # 5. Normalize whitespace
                if config.get("normalize_whitespace", True):
                    cleaned = re.sub(r"[ \t]+", " ", cleaned)
                    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
                    cleaned = cleaned.strip()

                # 6. Strip URLs
                if config.get("strip_urls", False):
                    cleaned = re.sub(r"https?://[^\s<>\"']+", "", cleaned)
                    cleaned = re.sub(r"www\.[^\s<>\"']+", "", cleaned)

                # 7. Custom patterns
                for pattern in config.get("custom_patterns", []):
                    try:
                        cleaned = re.sub(pattern, "", cleaned)
                    except re.error as exc:
                        warnings.append(f"Invalid regex pattern '{pattern}': {exc}")

                total_chars_cleaned += abs(original_len - len(cleaned))
                result_df.at[idx, col] = cleaned

        # 8. Length filtering
        min_len = config.get("min_text_length", 0)
        max_len = config.get("max_text_length", 0)
        rows_removed_by_length = 0

        if min_len > 0 or max_len > 0:
            primary_col = text_cols[0]
            lengths = result_df[primary_col].astype(str).str.len()
            mask = pd.Series(True, index=result_df.index)

            if min_len > 0:
                mask = mask & (lengths >= min_len)
            if max_len > 0:
                mask = mask & (lengths <= max_len)

            rows_removed_by_length = (~mask).sum()
            result_df = result_df[mask].reset_index(drop=True)

        rows_after = len(result_df)
        avg_cleaned = total_chars_cleaned / rows_before if rows_before > 0 else 0

        return StepResult(
            df=result_df,
            rows_before=rows_before,
            rows_after=rows_after,
            rows_removed=rows_before - rows_after,
            metadata={
                "encoding_fixes": encoding_fixes,
                "html_stripped": html_stripped,
                "rows_removed_by_length": rows_removed_by_length,
                "chars_cleaned_per_row_avg": round(avg_cleaned, 2),
            },
            warnings=warnings,
        )
