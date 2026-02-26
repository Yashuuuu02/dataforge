"""Generates insights and reports for completed jobs."""

import json
from dataclasses import dataclass
from typing import List, Optional

from ai.litellm_client import LiteLLMClient
from ai.dataset_analyzer import DatasetAnalysis

@dataclass
class InsightReport:
    summary: str
    quality_assessment: str
    recommendations: List[str]
    warnings: List[str]
    stats_narrative: str
    readiness_score: float
    readiness_label: str


class InsightReporter:
    def __init__(self, llm_client: Optional[LiteLLMClient] = None):
        self.llm = llm_client

    async def generate(self, pipeline_result: dict, dataset_analysis: dict, mode: str) -> InsightReport:
        """Generate a post-run report."""
        
        # Heuristics extraction
        before = pipeline_result.get("total_rows_before", 0)
        after = pipeline_result.get("total_rows_after", 0)
        removed = pipeline_result.get("total_rows_removed", 0)
        dur = pipeline_result.get("duration_seconds", 0)
        
        base_score = 8.5 # optimistic starting point for processed data
        if removed > before * 0.5:
             base_score -= 2.0 # removed too much?
        
        readiness_label = "Good"
        if base_score > 9: readiness_label = "Excellent"
        elif base_score < 7: readiness_label = "Needs Work"

        heuristic_narrative = f"You started with {before:,} rows and successfully kept {after:,} rows, removing {removed:,} rows in {dur:.1f}s."
        
        if not self.llm:
             return InsightReport(
                 summary=f"The {mode} pipeline has completed. Data has been cleaned.",
                 quality_assessment="The dataset quality looks improved based on heuristics.",
                 recommendations=["Train your model", "Review removed rows"],
                 warnings=pipeline_result.get("warnings", []),
                 stats_narrative=heuristic_narrative,
                 readiness_score=base_score,
                 readiness_label=readiness_label
             )

        # LLM Enhanced
        prompt = f"""You are analyzing a data cleaning pipeline result.
Original Analysis: {json.dumps(dataset_analysis)[:1000]}
Pipeline Stats: {json.dumps(pipeline_result)[:1000]}
Mode: {mode}

Generate a concise insightful report for the user. Return exactly this JSON schema:
{{
  "summary": "3-4 sentence overall summary of what was achieved",
  "quality_assessment": "assessment of the resulting output quality",
  "recommendations": ["Action C", "Action D"],
  "stats_narrative": "A conversational version of the stats"
}}"""
        try:
             res = await self.llm.complete_json([{"role": "user", "content": prompt}])
             return InsightReport(
                  summary=res.get("summary", "Done"),
                  quality_assessment=res.get("quality_assessment", "Looks good."),
                  recommendations=res.get("recommendations", []),
                  warnings=pipeline_result.get("warnings", []),
                  stats_narrative=res.get("stats_narrative", heuristic_narrative),
                  readiness_score=base_score,
                  readiness_label=readiness_label
             )
        except Exception:
             # Fallback
             return InsightReport(
                 summary=f"The {mode} pipeline has completed successfully.",
                 quality_assessment="Output quality has improved from the source.",
                 recommendations=["Download processed data", "Initiate training"],
                 warnings=pipeline_result.get("warnings", []),
                 stats_narrative=heuristic_narrative,
                 readiness_score=base_score,
                 readiness_label=readiness_label
             )
