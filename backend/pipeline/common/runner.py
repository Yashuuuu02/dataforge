"""Pipeline runner — orchestrates step execution with progress reporting."""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import pandas as pd

from pipeline.common.base import PipelineStep, StepResult, STEP_REGISTRY

logger = logging.getLogger(__name__)

# Import all steps to trigger registration
import pipeline.common.deduplication  # noqa: F401
import pipeline.common.pii_scrubber  # noqa: F401
import pipeline.common.language_filter  # noqa: F401
import pipeline.common.noise_removal  # noqa: F401
import pipeline.common.quality_scorer  # noqa: F401


@dataclass
class PipelineRunResult:
    """Result of a complete pipeline run."""

    df: pd.DataFrame
    steps_results: list[StepResult]
    total_rows_before: int
    total_rows_after: int
    total_rows_removed: int
    pipeline_stats: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


class PipelineRunner:
    """Execute a sequence of pipeline steps with progress callbacks and error resilience."""

    def run(
        self,
        df: pd.DataFrame,
        steps: list[dict],
        job_id: str = "",
        progress_callback: Optional[Callable[[int, str, str], None]] = None,
    ) -> PipelineRunResult:
        """Run all pipeline steps in order.

        Args:
            df: Input DataFrame.
            steps: List of {"step": "step_name", "config": {...}} dicts.
            job_id: Job ID for logging.
            progress_callback: Callable(progress%, step_name, message).

        Returns:
            PipelineRunResult with final DataFrame and per-step results.
        """
        start_time = time.time()
        total_rows_before = len(df)
        current_df = df.copy()
        step_results: list[StepResult] = []
        all_warnings: list[str] = []
        total_steps = len(steps)

        for i, step_config in enumerate(steps):
            step_name = step_config.get("step", "unknown")
            config = step_config.get("config", {})

            # Calculate progress
            base_progress = int((i / total_steps) * 100)
            step_progress = int(((i + 1) / total_steps) * 100)

            logger.info("[%s] Starting step %d/%d: %s", job_id, i + 1, total_steps, step_name)

            if progress_callback:
                progress_callback(base_progress, step_name, f"Starting {step_name}...")

            # Look up step class
            step_class = STEP_REGISTRY.get(step_name)
            if step_class is None:
                warning = f"Unknown step '{step_name}' — skipped"
                logger.warning(warning)
                all_warnings.append(warning)
                # Add a placeholder result
                step_results.append(StepResult(
                    df=current_df,
                    rows_before=len(current_df),
                    rows_after=len(current_df),
                    rows_removed=0,
                    metadata={"skipped": True, "reason": "unknown step"},
                    warnings=[warning],
                ))
                continue

            step_instance: PipelineStep = step_class()

            try:
                # Validate config
                step_instance.validate_config(config)

                # Run step
                result = step_instance.run(current_df, config)
                step_results.append(result)
                current_df = result.df
                all_warnings.extend(result.warnings)

                logger.info(
                    "[%s] Step %s complete: %s",
                    job_id, step_name, result.summary,
                )

                if progress_callback:
                    progress_callback(
                        step_progress, step_name,
                        f"{step_name}: {result.summary}"
                    )

            except Exception as exc:
                error_msg = f"Step '{step_name}' failed: {exc}"
                logger.exception(error_msg)
                all_warnings.append(error_msg)

                # Add failed result but continue pipeline
                step_results.append(StepResult(
                    df=current_df,
                    rows_before=len(current_df),
                    rows_after=len(current_df),
                    rows_removed=0,
                    metadata={"skipped": True, "reason": str(exc)},
                    warnings=[error_msg],
                ))

                if progress_callback:
                    progress_callback(step_progress, step_name, f"{step_name}: SKIPPED ({exc})")

        duration = time.time() - start_time
        total_rows_after = len(current_df)

        return PipelineRunResult(
            df=current_df,
            steps_results=step_results,
            total_rows_before=total_rows_before,
            total_rows_after=total_rows_after,
            total_rows_removed=total_rows_before - total_rows_after,
            pipeline_stats={
                "steps_executed": len(step_results),
                "steps_skipped": sum(1 for r in step_results if r.metadata.get("skipped")),
            },
            warnings=all_warnings,
            duration_seconds=round(duration, 2),
        )
