"""Tests for the quality scorer pipeline step."""

import pandas as pd
import pytest
from pipeline.common.quality_scorer import QualityScorerStep


@pytest.fixture
def step():
    return QualityScorerStep()


def test_heuristic_scores_between_0_and_10(step):
    df = pd.DataFrame({"text": [
        "This is a normal sentence.",
        "Short",
        "x" * 50000,
        "",
    ]})
    result = step.run(df, {"method": "heuristic", "action": "score_only"})
    assert "quality_score" in result.df.columns
    for score in result.df["quality_score"]:
        assert 0.0 <= score <= 10.0


def test_repetitive_text_gets_low_score(step):
    repeated = "This is repeated. " * 50
    normal = "The quick brown fox jumps over the lazy dog. Each sentence is unique and interesting."
    df = pd.DataFrame({"text": [repeated, normal]})
    result = step.run(df, {"method": "heuristic"})
    assert result.df.iloc[0]["quality_score"] < result.df.iloc[1]["quality_score"]


def test_high_quality_text_gets_high_score(step):
    df = pd.DataFrame({"text": [
        "Machine learning models require high-quality training data. "
        "This includes diverse examples with proper grammar, clear intent, "
        "and minimal noise. Data preparation is a crucial step in the ML pipeline."
    ]})
    result = step.run(df, {"method": "heuristic"})
    assert result.df.iloc[0]["quality_score"] >= 5.0


def test_filter_action_removes_below_threshold(step):
    df = pd.DataFrame({"text": [
        "Good quality text with diverse vocabulary and proper structure.",
        "",
        "x",
    ]})
    result = step.run(df, {"method": "heuristic", "action": "filter", "threshold": 3.0})
    assert result.rows_after < result.rows_before
    assert result.metadata["rows_filtered"] > 0


def test_flag_action_adds_quality_flag(step):
    df = pd.DataFrame({"text": ["good text", ""]})
    result = step.run(df, {"method": "heuristic", "action": "flag", "threshold": 3.0})
    assert "quality_flag" in result.df.columns


def test_score_distribution_metadata(step):
    df = pd.DataFrame({"text": ["a", "bb", "This is a nice sentence."] * 10})
    result = step.run(df, {"method": "heuristic"})
    dist = result.metadata["score_distribution"]
    assert "0-2" in dist
    assert "8-10" in dist
    assert result.metadata["mean_score"] >= 0
    assert result.metadata["median_score"] >= 0


def test_empty_dataframe(step):
    df = pd.DataFrame({"text": []})
    result = step.run(df, {"method": "heuristic"})
    assert result.rows_before == 0
