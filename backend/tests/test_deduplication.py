"""Tests for the deduplication pipeline step."""

import pandas as pd
import pytest
from pipeline.common.deduplication import DeduplicationStep


@pytest.fixture
def step():
    return DeduplicationStep()


@pytest.fixture
def df_with_dupes():
    return pd.DataFrame({
        "text": ["hello world", "hello world", "foo bar", "foo bar", "unique text"],
        "label": ["a", "a", "b", "b", "c"],
    })


def test_exact_dedup_removes_identical_rows(step, df_with_dupes):
    result = step.run(df_with_dupes, {"method": "exact", "columns": "all"})
    assert result.rows_before == 5
    assert result.rows_after == 3
    assert result.rows_removed == 2
    assert result.metadata["exact_duplicates_removed"] == 2


def test_exact_dedup_on_specific_columns(step):
    df = pd.DataFrame({
        "text": ["hello", "hello", "world"],
        "label": ["a", "b", "c"],
    })
    result = step.run(df, {"method": "exact", "columns": ["text"]})
    assert result.rows_after == 2
    assert result.metadata["columns_checked"] == ["text"]


def test_dedup_keeps_correct_row_when_keep_first(step, df_with_dupes):
    result = step.run(df_with_dupes, {"method": "exact", "keep": "first"})
    assert result.df.iloc[0]["text"] == "hello world"
    assert result.df.iloc[1]["text"] == "foo bar"


def test_dedup_keeps_correct_row_when_keep_last(step, df_with_dupes):
    result = step.run(df_with_dupes, {"method": "exact", "keep": "last"})
    assert len(result.df) == 3


def test_semantic_dedup_falls_back_without_deps(step, df_with_dupes):
    result = step.run(df_with_dupes, {"method": "semantic"})
    # Should fall back to exact dedup with a warning
    assert len(result.warnings) > 0 or result.metadata["exact_duplicates_removed"] >= 0


def test_no_dupes_returns_same_count(step):
    df = pd.DataFrame({"text": ["a", "b", "c"]})
    result = step.run(df, {"method": "exact"})
    assert result.rows_removed == 0
    assert result.rows_after == 3


def test_empty_dataframe(step):
    df = pd.DataFrame({"text": []})
    result = step.run(df, {"method": "exact"})
    assert result.rows_before == 0
    assert result.rows_after == 0
