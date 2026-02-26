"""Analyzes datasets using heuristics and optionally an LLM."""

import json
from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd

from ai.litellm_client import LiteLLMClient
from pipeline.common.language_filter import LanguageFilterStep
from pipeline.common.pii_scrubber import PIIScrubberStep


@dataclass
class Issue:
    severity: str  # "low", "medium", "high"
    type: str
    description: str
    affected_rows: int
    affected_columns: List[str] = field(default_factory=list)


@dataclass
class RecommendedStep:
    step: str
    reason: str
    config: dict
    priority: int  # 1 = must have, 2 = recommended, 3 = optional


@dataclass
class DatasetAnalysis:
    dataset_type: str
    primary_text_columns: List[str]
    detected_language: str
    estimated_quality: float
    row_count: int
    issues_detected: List[Issue]
    recommended_pipeline: List[RecommendedStep]
    recommended_mode: str
    confidence: float
    summary: str


class DatasetAnalyzer:
    def __init__(self, llm_client: Optional[LiteLLMClient] = None):
        self.llm = llm_client

    async def analyze(self, df: pd.DataFrame, filename: str) -> DatasetAnalysis:
        """Analyze a dataset (up to 500 rows sampled)."""
        row_count = len(df)
        sample = df.sample(min(500, row_count)) if row_count > 500 else df
        
        # 1. Detect columns and dataset type
        columns = list(sample.columns)
        str_columns = [col for col in columns if pd.api.types.is_object_dtype(sample[col]) or pd.api.types.is_string_dtype(sample[col])]

        dataset_type = self._guess_dataset_type(columns, str_columns, sample)
        primary_text_columns = self._find_primary_text_columns(dataset_type, columns, str_columns, sample)

        # 2. Heuristic Scans
        issues = []
        
        # Duplicates
        num_dupes = sample.duplicated().sum()
        dupe_ratio = num_dupes / len(sample)
        if dupe_ratio > 0.1:
            issues.append(Issue("high", "duplicates", f"Found high duplicate ratio (~{dupe_ratio*100:.1f}%)", int(row_count * dupe_ratio)))
        elif dupe_ratio > 0.01:
            issues.append(Issue("medium", "duplicates", f"Found some duplicate rows (~{dupe_ratio*100:.1f}%)", int(row_count * dupe_ratio)))

        # PII Detection (using the pipeline step logic)
        pii_step = PIIScrubberStep()
        pii_res = pii_step.run(sample, {"action": "flag", "entities": ["ALL"], "columns": primary_text_columns or "all_text"})
        pii_rows = pii_res.metadata.get("rows_with_pii", 0)
        if pii_rows > 0:
            pii_ratio = pii_rows / len(sample)
            sev = "high" if pii_ratio > 0.05 else "medium"
            issues.append(Issue(sev, "pii_detected", f"Detected sensitive info (PII) in ~{pii_ratio*100:.1f}% of sampled rows", int(row_count * pii_ratio), primary_text_columns))

        # Language Detection
        lang_step = LanguageFilterStep()
        lang_res = lang_step.run(sample, {"action": "tag_only"})
        detected_lang_dist = lang_res.metadata.get("language_distribution", {})
        top_lang = max(detected_lang_dist.items(), key=lambda x: x[1])[0] if detected_lang_dist else "unknown"

        # Missing values
        for col in primary_text_columns:
            null_count = sample[col].isna().sum()
            if null_count > 0:
                null_ratio = null_count / len(sample)
                sev = "medium" if null_ratio > 0.1 else "low"
                issues.append(Issue(sev, "missing_values", f"Column '{col}' is missing ~{null_ratio*100:.1f}% values", int(row_count * null_ratio), [col]))

        # Quality scoring (basic heuristic for length and variety)
        # We assume empty or extremely short rows hurt quality
        short_rows = 0
        for col in primary_text_columns:
            short_rows += sample[col].fillna("").apply(lambda x: len(str(x)) < 10).sum()
        short_ratio = min((short_rows / max(1, len(primary_text_columns))) / len(sample), 1.0)
        
        if short_ratio > 0.2:
            issues.append(Issue("medium", "low_quality", f"Found many very short text entries (<10 chars)", int(row_count * short_ratio), primary_text_columns))

        # Base estimated quality (0-10)
        estimated_quality = max(0.0, 10.0 - (dupe_ratio * 10) - (short_ratio * 10) - (len(issues) * 0.5))

        # 3. Build Recommendations
        recommended_pipeline = self._build_recommendations(dataset_type, issues)
        recommended_mode = self._guess_mode(dataset_type)

        # 4. Generate Summary
        summary = self._generate_heuristic_summary(dataset_type, row_count, top_lang, estimated_quality, issues)
        confidence = 0.7  # Base confidence for heuristics

        if self.llm:
            try:
                summary, improved_confidence = await self._generate_ai_summary(
                    filename, dataset_type, row_count, top_lang, estimated_quality, issues, sample.head(5)
                )
                confidence = max(confidence, improved_confidence)
            except Exception as e:
                 # Fallback to heuristics if AI fails
                 pass

        return DatasetAnalysis(
            dataset_type=dataset_type,
            primary_text_columns=primary_text_columns,
            detected_language=top_lang,
            estimated_quality=round(estimated_quality, 1),
            row_count=row_count,
            issues_detected=issues,
            recommended_pipeline=recommended_pipeline,
            recommended_mode=recommended_mode,
            confidence=round(confidence, 2),
            summary=summary
        )

    def _guess_dataset_type(self, cls: List[str], str_cls: List[str], df: pd.DataFrame) -> str:
        lc_cols = set(c.lower() for c in cls)
        
        if {"instruction", "input", "output"}.issubset(lc_cols) or {"prompt", "completion"}.issubset(lc_cols):
            return "instruction_pairs"
        if {"question", "answer"}.issubset(lc_cols) or {"q", "a"}.issubset(lc_cols):
            return "qa"
        if "messages" in lc_cols or "conversations" in lc_cols or {"role", "content"}.issubset(lc_cols):
            return "chat"
        if "code" in lc_cols or "repository" in lc_cols:
            return "code"
        
        # If mainly numbers + 1 target
        if len(str_cls) <= 1 and len(cls) > 3:
            return "tabular"
            
        return "documents"

    def _find_primary_text_columns(self, ds_type: str, cols: List[str], str_cols: List[str], df: pd.DataFrame) -> List[str]:
        if ds_type == "instruction_pairs":
            return [c for c in cols if c.lower() in ("instruction", "input", "output", "prompt", "completion")]
        if ds_type == "qa":
            return [c for c in cols if c.lower() in ("question", "answer", "q", "a")]
        if ds_type == "chat":
            return [c for c in cols if c.lower() in ("messages", "content")]
            
        # Default: longest string columns
        if not str_cols:
            return []
        
        avg_lens = {c: df[c].fillna("").apply(lambda x: len(str(x))).mean() for c in str_cols}
        return [c for c, _ in sorted(avg_lens.items(), key=lambda x: x[1], reverse=True)[:3]]

    def _guess_mode(self, ds_type: str) -> str:
        if ds_type in ("instruction_pairs", "chat"):
            return "finetune"
        if ds_type == "documents":
            return "rag"
        if ds_type == "tabular":
            return "ml"
        return "common"

    def _build_recommendations(self, ds_type: str, issues: List[Issue]) -> List[RecommendedStep]:
        recs = []
        issue_types = [i.type for i in issues]

        if "duplicates" in issue_types:
            recs.append(RecommendedStep("deduplication", "High number of duplicates detected.", {"method": "exact", "columns": "all", "keep": "first"}, 1))
        
        # Always recommend noise removal
        recs.append(RecommendedStep("noise_removal", "Standard cleanup for all datasets.", {"fix_encoding": True, "strip_html": True, "normalize_whitespace": True}, 2))

        if "pii_detected" in issue_types:
            recs.append(RecommendedStep("pii_scrubbing", "Contains sensitive information.", {"action": "redact", "entities": ["ALL"]}, 1))
            
        if "low_quality" in issue_types or ds_type in ("instruction_pairs", "qa"):
            recs.append(RecommendedStep("quality_scorer", "Filter out low-quality rows for better training.", {"method": "heuristic", "action": "filter", "threshold": 4.0}, 2))

        return recs

    def _generate_heuristic_summary(self, ds_type: str, count: int, lang: str, qual: float, issues: List[Issue]) -> str:
        s = f"This appears to be a ~{count:,}-row {ds_type} dataset (language: {lang}). "
        s += f"The heuristic quality score is {qual:.1f}/10. "
        if not issues:
            s += "The data looks remarkably clean with no major issues detected."
        else:
            s += f"I found {len(issues)} potential issues, including " + ", ".join([i.type.replace('_',' ') for i in issues[:2]]) + "."
        return s

    async def _generate_ai_summary(self, filename: str, ds_type: str, count: int, lang: str, qual: float, issues: List[Issue], sample_df: pd.DataFrame) -> tuple[str, float]:
        sample_json = sample_df.to_json(orient="records")
        issue_str = json.dumps([{"type": i.type, "severity": i.severity} for i in issues])
        
        prompt = f"""You are an expert Data Engineer. Analyze this dataset metadata and provide a 2-3 sentence summary for the user.
Filename: {filename}
Detected Type: {ds_type}
Language: {lang}
Row count: {count}
Heuristic Quality: {qual}/10
Issues detected: {issue_str}
Sample data (first 5 rows):
{sample_json}

Return a valid JSON object with exactly two keys:
- "summary": (A human-readable, friendly 2-3 sentence summary explaining what this dataset is about, its quality, and any major issues found. Write directly to the user, e.g., "This dataset contains customer support logs...")
- "confidence": (A float between 0.0 and 1.0 indicating how confident you are in this analysis based on the sample)
"""
        res = await self.llm.complete_json([{"role": "system", "content": prompt}], schema={"type": "object"})
        
        return res.get("summary", ""), float(res.get("confidence", 0.8))
