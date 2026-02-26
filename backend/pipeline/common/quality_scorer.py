"""Quality scoring pipeline step — heuristic + optional AI scoring."""

import logging
import math
import re
import time
from collections import Counter
from typing import Optional

import pandas as pd

from pipeline.common.base import PipelineStep, StepResult, register_step

logger = logging.getLogger(__name__)


@register_step
class QualityScorerStep(PipelineStep):
    name = "quality_scorer"
    description = "Score each row 0-10 for quality using heuristics and optional AI"

    def validate_config(self, config: dict) -> None:
        method = config.get("method", "heuristic")
        if method not in ("heuristic", "ai", "both"):
            raise ValueError(f"Invalid method: {method}.")
        action = config.get("action", "score_only")
        if action not in ("score_only", "filter", "flag"):
            raise ValueError(f"Invalid action: {action}.")

    def run(self, df: pd.DataFrame, config: dict) -> StepResult:
        rows_before = len(df)
        warnings: list[str] = []
        method = config.get("method", "heuristic")
        action = config.get("action", "score_only")
        threshold = config.get("threshold", 0.0)
        score_col = config.get("score_column_name", "quality_score")
        reason_col = config.get("reason_column_name", "quality_reason")

        # Determine text columns
        text_cols = config.get("text_columns", "auto")
        if text_cols == "auto":
            text_cols = list(df.select_dtypes(include=["object"]).columns)
        else:
            text_cols = [c for c in text_cols if c in df.columns]

        if not text_cols:
            warnings.append("No text columns found for quality scoring.")
            result_df = df.copy()
            result_df[score_col] = 5.0
            result_df[reason_col] = "No text columns"
            return StepResult(df=result_df, rows_before=rows_before, rows_after=rows_before,
                              rows_removed=0, metadata={}, warnings=warnings)

        result_df = df.copy()
        scores: list[float] = []
        reasons: list[str] = []

        # ── Heuristic scoring ──
        if method in ("heuristic", "both"):
            for _, row in result_df.iterrows():
                text = " ".join(str(row[c]) for c in text_cols if pd.notna(row[c]))
                score, reason = self._heuristic_score(text)
                scores.append(score)
                reasons.append(reason)

        # ── AI scoring ──
        if method in ("ai", "both"):
            ai_scores, ai_reasons, ai_warnings = self._ai_score_batch(
                result_df, text_cols, config
            )
            warnings.extend(ai_warnings)

            if method == "ai":
                scores = ai_scores
                reasons = ai_reasons
            elif method == "both" and ai_scores:
                # Average heuristic and AI
                scores = [(h + a) / 2 for h, a in zip(scores, ai_scores)]
                reasons = [f"H: {hr} | AI: {ar}" for hr, ar in zip(reasons, ai_reasons)]

        if not scores:
            scores = [5.0] * len(result_df)
            reasons = ["Scoring unavailable"] * len(result_df)

        result_df[score_col] = scores
        result_df[reason_col] = reasons

        # Apply action
        rows_filtered = 0
        if action == "filter" and threshold > 0:
            mask = result_df[score_col] >= threshold
            rows_filtered = (~mask).sum()
            result_df = result_df[mask].reset_index(drop=True)
        elif action == "flag" and threshold > 0:
            result_df["quality_flag"] = result_df[score_col] < threshold

        # Score distribution
        score_dist = {"0-2": 0, "2-4": 0, "4-6": 0, "6-8": 0, "8-10": 0}
        for s in scores:
            if s < 2:
                score_dist["0-2"] += 1
            elif s < 4:
                score_dist["2-4"] += 1
            elif s < 6:
                score_dist["4-6"] += 1
            elif s < 8:
                score_dist["6-8"] += 1
            else:
                score_dist["8-10"] += 1

        rows_after = len(result_df)
        mean_score = sum(scores) / len(scores) if scores else 0
        sorted_scores = sorted(scores)
        median_score = sorted_scores[len(sorted_scores) // 2] if sorted_scores else 0

        return StepResult(
            df=result_df,
            rows_before=rows_before,
            rows_after=rows_after,
            rows_removed=rows_before - rows_after,
            metadata={
                "score_distribution": score_dist,
                "mean_score": round(mean_score, 2),
                "median_score": round(median_score, 2),
                "rows_filtered": rows_filtered,
                "method_used": method,
            },
            warnings=warnings,
        )

    def _heuristic_score(self, text: str) -> tuple[float, str]:
        """Score text 0-10 based on heuristic quality signals."""
        if not text or not text.strip():
            return 0.0, "Empty text"

        reasons: list[str] = []
        sub_scores: list[float] = []

        # 1. Length score (optimal: 50-5000 chars)
        length = len(text)
        if length < 10:
            sub_scores.append(1.0)
            reasons.append("Very short")
        elif length < 50:
            sub_scores.append(4.0)
            reasons.append("Short")
        elif length <= 5000:
            sub_scores.append(10.0)
        elif length <= 20000:
            sub_scores.append(7.0)
            reasons.append("Long")
        else:
            sub_scores.append(4.0)
            reasons.append("Very long")

        # 2. Vocabulary diversity
        words = text.lower().split()
        if len(words) > 0:
            unique_ratio = len(set(words)) / len(words)
            vocab_score = min(10.0, unique_ratio * 12)
            sub_scores.append(vocab_score)
            if unique_ratio < 0.3:
                reasons.append("Low vocabulary diversity")
        else:
            sub_scores.append(1.0)

        # 3. Repetition penalty
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip().lower() for s in sentences if s.strip()]
        if len(sentences) > 1:
            sent_counts = Counter(sentences)
            max_repeat = max(sent_counts.values())
            if max_repeat > 2:
                rep_score = max(1.0, 10.0 - (max_repeat - 1) * 2)
                sub_scores.append(rep_score)
                reasons.append(f"Repeated sentences ({max_repeat}x)")
            else:
                sub_scores.append(10.0)
        else:
            sub_scores.append(7.0)

        # 4. Special character ratio
        alpha_count = sum(1 for c in text if c.isalpha())
        if len(text) > 0:
            alpha_ratio = alpha_count / len(text)
            if alpha_ratio > 0.6:
                sub_scores.append(10.0)
            elif alpha_ratio > 0.4:
                sub_scores.append(7.0)
            else:
                sub_scores.append(3.0)
                reasons.append("High special char ratio")
        else:
            sub_scores.append(1.0)

        # 5. Capitalization consistency
        upper_count = sum(1 for c in text if c.isupper())
        if alpha_count > 0:
            upper_ratio = upper_count / alpha_count
            if 0.02 <= upper_ratio <= 0.15:
                sub_scores.append(10.0)
            elif upper_ratio > 0.5:
                sub_scores.append(3.0)
                reasons.append("Excessive caps")
            else:
                sub_scores.append(7.0)
        else:
            sub_scores.append(5.0)

        # Weighted average
        weights = [1.5, 2.0, 2.0, 1.0, 0.5]
        weighted = sum(s * w for s, w in zip(sub_scores, weights))
        total_weight = sum(weights[:len(sub_scores)])
        final_score = round(min(10.0, max(0.0, weighted / total_weight)), 2)

        reason = "; ".join(reasons) if reasons else "Good quality"
        return final_score, reason

    def _ai_score_batch(self, df, text_cols, config) -> tuple[list[float], list[str], list[str]]:
        """Score rows using AI (LiteLLM) with batch delay and exponential backoff."""
        warnings: list[str] = []
        scores: list[float] = []
        reasons: list[str] = []

        batch_size = config.get("ai_batch_size", 20)
        batch_delay = config.get("ai_batch_delay", 0.5)
        max_retries = config.get("ai_max_retries", 3)

        try:
            import litellm
        except ImportError:
            warnings.append("litellm not installed — skipping AI scoring.")
            return [], [], warnings

        provider = config.get("ai_provider", "openai")
        model = config.get("ai_model", "gpt-3.5-turbo")

        # Collect texts
        texts: list[str] = []
        for _, row in df.iterrows():
            text = " ".join(str(row[c]) for c in text_cols if pd.notna(row[c]))
            texts.append(text[:2000])  # Truncate to avoid token limits

        # Process in batches
        for batch_start in range(0, len(texts), batch_size):
            batch = texts[batch_start:batch_start + batch_size]

            # Build batch prompt
            examples_text = "\n---\n".join(
                f"Example {i+1}: {t[:500]}" for i, t in enumerate(batch)
            )
            prompt = (
                "Evaluate each example for quality, relevance, and usefulness for AI training.\n"
                "Score each 0-10. Return a JSON array of objects: "
                '[{"score": float, "reason": str}]\n\n'
                f"{examples_text}"
            )

            # Retry with exponential backoff
            for attempt in range(max_retries):
                try:
                    response = litellm.completion(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=0.1,
                        response_format={"type": "json_object"},
                    )
                    content = response.choices[0].message.content
                    import json
                    results = json.loads(content)
                    if isinstance(results, dict) and "results" in results:
                        results = results["results"]
                    if isinstance(results, list):
                        for r in results:
                            scores.append(float(r.get("score", 5.0)))
                            reasons.append(r.get("reason", ""))
                    break

                except Exception as exc:
                    error_str = str(exc).lower()
                    if "rate" in error_str or "429" in error_str:
                        backoff = batch_delay * (2 ** attempt)
                        logger.warning("Rate limited, backing off %.1fs (attempt %d/%d)", backoff, attempt + 1, max_retries)
                        time.sleep(backoff)
                    else:
                        warnings.append(f"AI scoring failed for batch: {exc}")
                        # Fill with heuristic fallback
                        for t in batch:
                            s, r = self._heuristic_score(t)
                            scores.append(s)
                            reasons.append(f"(fallback) {r}")
                        break
            else:
                warnings.append("AI scoring failed after max retries — using heuristic fallback.")
                for t in batch:
                    s, r = self._heuristic_score(t)
                    scores.append(s)
                    reasons.append(f"(fallback) {r}")

            # Delay between batches
            if batch_start + batch_size < len(texts):
                time.sleep(batch_delay)

        # Pad if needed
        while len(scores) < len(texts):
            scores.append(5.0)
            reasons.append("Scoring incomplete")

        return scores[:len(texts)], reasons[:len(texts)], warnings
