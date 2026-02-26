"""Builds and refines pipeline workflows."""

import json
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

from ai.dataset_analyzer import DatasetAnalysis
from ai.litellm_client import LiteLLMClient

@dataclass
class WorkflowStep:
    id: str
    step: str
    label: str
    config: dict
    reason: str
    is_required: bool
    can_be_skipped: bool

@dataclass
class WorkflowPlan:
    steps: List[WorkflowStep]
    estimated_duration_seconds: int
    estimated_rows_after: int
    warnings: List[str]
    explanation: str

    def to_dict(self) -> dict:
        return asdict(self)


class WorkflowBuilder:
    def __init__(self, llm_client: Optional[LiteLLMClient] = None):
        self.llm = llm_client

        # Definition of available steps to feed to the LLM
        self.available_steps_prompt = """Available pipeline steps:
1. deduplication
   - method: "exact" (SHA256) | "semantic" (embeddings) | "both"
   - columns: "all" | list of columns
   - keep: "first" | "last"
2. noise_removal
   - fix_encoding: bool
   - strip_html: bool
   - normalize_whitespace: bool
   - strip_urls: bool
3. pii_scrubbing
   - action: "redact" | "remove_row" | "flag"
   - entities: ["ALL", "EMAIL", "PHONE", "PERSON", "CREDIT_CARD"]
4. language_filter
   - action: "tag_only" | "filter_keep" | "filter_remove"
   - languages: list of language codes (e.g. ["en"])
5. quality_scorer
   - method: "heuristic" | "ai" | "both"
   - action: "score_only" | "filter" | "flag"
   - threshold: float 0-10"""

    async def build_from_text(self, user_message: str, dataset_analysis: DatasetAnalysis, conversation_history: List[dict]) -> WorkflowPlan:
        """Use LLM to build a workflow plan from natural language and dataset context."""
        if not self.llm:
            return self._build_from_heuristics(dataset_analysis, user_message)

        history_str = json.dumps(conversation_history[-3:]) if conversation_history else "[]"
        analysis_dict = asdict(dataset_analysis)
        # Strip some heavy fields for prompt
        analysis_dict.pop("recommended_pipeline", None)

        system_prompt = f"""You are DataForge's pipeline architect. You help users prepare datasets for AI training.
{self.available_steps_prompt}

You must return a raw JSON object matching this schema:
{{
  "steps": [
    {{
      "id": "unique-str",
      "step": "step_name",
      "label": "Human readable name",
      "config": {{}},
      "reason": "Why include this?",
      "is_required": true/false,
      "can_be_skipped": true/false
    }}
  ],
  "estimated_duration_seconds": int,
  "estimated_rows_after": int,
  "warnings": ["Array of string warnings"],
  "explanation": "Brief paragraph explaining the pipeline to the user"
}}

Analyze the dataset and the user's intent. Create a lean, efficient pipeline."""

        user_prompt = f"""Dataset Analysis: {json.dumps(analysis_dict)}
Recent Chat: {history_str}
User intent: {user_message}

Generate the optimal pipeline following the schema. Do not over-engineer."""

        try:
            res = await self.llm.complete_json([
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ])
            return self._parse_json_to_plan(res, dataset_analysis.row_count)
        except Exception:
            # Fallback
            return self._build_from_heuristics(dataset_analysis, user_message)

    async def refine(self, current_plan: WorkflowPlan, user_message: str, conversation_history: List[dict]) -> WorkflowPlan:
        """Update an existing plan based on a user's instruction."""
        if not self.llm:
            return current_plan # LLM required for complex refinement

        system_prompt = f"""You are refining an existing data pipeline based on a user's request.
{self.available_steps_prompt}

Current Plan JSON:
{json.dumps(current_plan.to_dict())}

User Request: {user_message}

Modify the existing plan to fulfill the request. Add, remove, or modify steps and configs as needed.
Return the full updated JSON plan exactly matching the original schema."""

        try:
            res = await self.llm.complete_json([
                {"role": "system", "content": system_prompt}
            ])
            return self._parse_json_to_plan(res, current_plan.estimated_rows_after) # Pass current estimate as fallback base
        except Exception:
            return current_plan

    def build_from_mode(self, mode: str, config: dict) -> WorkflowPlan:
        """Converts guided mode config into a WorkflowPlan statically without AI."""
        # Optional helper for direct UI mapping if needed
        return WorkflowPlan([], 0, 0, [], "Placeholder for direct build")

    def _parse_json_to_plan(self, data: dict, original_rows: int) -> WorkflowPlan:
        steps = []
        for i, s in enumerate(data.get("steps", [])):
            steps.append(WorkflowStep(
                id=s.get("id", f"step-{i}"),
                step=s.get("step", "unknown"),
                label=s.get("label", s.get("step")),
                config=s.get("config", {}),
                reason=s.get("reason", ""),
                is_required=s.get("is_required", False),
                can_be_skipped=s.get("can_be_skipped", True)
            ))

        return WorkflowPlan(
            steps=steps,
            estimated_duration_seconds=data.get("estimated_duration_seconds", 30),
            estimated_rows_after=data.get("estimated_rows_after", original_rows),
            warnings=data.get("warnings", []),
            explanation=data.get("explanation", "I have generated this pipeline for you.")
        )

    def _build_from_heuristics(self, analysis: DatasetAnalysis, intent: str) -> WorkflowPlan:
        """Fallback rule-based builder if no LLM provided."""
        steps = []
        for i, rec in enumerate(analysis.recommended_pipeline):
            steps.append(WorkflowStep(
                id=f"{rec.step}-{i}",
                step=rec.step,
                label=rec.step.replace("_", " ").title(),
                config=rec.config,
                reason=rec.reason,
                is_required=True,
                can_be_skipped=True
            ))
            
        return WorkflowPlan(
            steps=steps,
            estimated_duration_seconds=max(5, len(steps) * 10),
            estimated_rows_after=analysis.row_count - (analysis.row_count // 10), # Random guess
            warnings=["AI unavailable, generated a baseline recommendation."],
            explanation="Based on a heuristic analysis, I recommend these standard cleanup steps."
        )
