"""Tests for the pipeline runner."""

import pandas as pd
import pytest
from pipeline.common.runner import PipelineRunner


@pytest.fixture
def runner():
    return PipelineRunner()


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "text": [
            "Hello world",
            "Hello world",  # duplicate
            "<p>HTML noise</p>",
            "Contact john@example.com",
            "Short",
        ],
        "label": ["a", "a", "b", "c", "d"],
    })


def test_full_pipeline_runs_all_steps_in_order(runner, sample_df):
    steps = [
        {"step": "deduplication", "config": {"method": "exact"}},
        {"step": "noise_removal", "config": {"strip_html": True}},
        {"step": "quality_scorer", "config": {"method": "heuristic", "action": "score_only"}},
    ]
    result = runner.run(sample_df, steps, job_id="test-1")
    assert result.total_rows_before == 5
    assert result.total_rows_after <= 5
    assert len(result.steps_results) == 3
    assert result.duration_seconds >= 0


def test_failed_step_is_skipped_not_crashed(runner, sample_df):
    steps = [
        {"step": "unknown_step_that_doesnt_exist", "config": {}},
        {"step": "deduplication", "config": {"method": "exact"}},
    ]
    result = runner.run(sample_df, steps, job_id="test-2")
    # Should not crash — unknown step skipped, dedup still runs
    assert len(result.steps_results) == 2
    assert result.steps_results[0].metadata.get("skipped") is True
    assert len(result.warnings) > 0


def test_progress_callback_called_after_each_step(runner, sample_df):
    progress_calls = []

    def callback(progress, step_name, message):
        progress_calls.append((progress, step_name, message))

    steps = [
        {"step": "deduplication", "config": {"method": "exact"}},
        {"step": "noise_removal", "config": {}},
    ]
    runner.run(sample_df, steps, job_id="test-3", progress_callback=callback)

    # Should have been called at least once per step (start + end)
    assert len(progress_calls) >= 2


def test_empty_steps_list(runner, sample_df):
    result = runner.run(sample_df, [], job_id="test-4")
    assert result.total_rows_before == 5
    assert result.total_rows_after == 5
    assert len(result.steps_results) == 0


def test_single_step(runner, sample_df):
    result = runner.run(sample_df, [{"step": "deduplication", "config": {"method": "exact"}}], job_id="test-5")
    assert len(result.steps_results) == 1
    assert result.steps_results[0].metadata["exact_duplicates_removed"] >= 0
