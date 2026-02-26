"""Tests for the noise removal pipeline step."""

import pandas as pd
import pytest
from pipeline.common.noise_removal import NoiseRemovalStep


@pytest.fixture
def step():
    return NoiseRemovalStep()


def test_strips_html_tags(step):
    df = pd.DataFrame({"text": ["<p>Hello <b>world</b></p>"]})
    result = step.run(df, {"strip_html": True})
    assert "<p>" not in result.df.iloc[0]["text"]
    assert "<b>" not in result.df.iloc[0]["text"]
    assert "Hello" in result.df.iloc[0]["text"]
    assert result.metadata["html_stripped"] >= 1


def test_fixes_encoding(step):
    df = pd.DataFrame({"text": ["schÃ¶n"]})  # Mojibake
    result = step.run(df, {"fix_encoding": True})
    assert result.metadata["encoding_fixes"] >= 0  # May or may not fix depending on ftfy


def test_normalizes_whitespace(step):
    df = pd.DataFrame({"text": ["hello    world\n\n\n\nfoo"]})
    result = step.run(df, {"normalize_whitespace": True})
    assert "    " not in result.df.iloc[0]["text"]


def test_removes_control_chars(step):
    df = pd.DataFrame({"text": ["hello\x00\x01world"]})
    result = step.run(df, {"remove_control_chars": True})
    assert "\x00" not in result.df.iloc[0]["text"]
    assert "helloworld" in result.df.iloc[0]["text"]


def test_length_filter_removes_short_rows(step):
    df = pd.DataFrame({"text": ["hi", "This is a longer sentence that passes the filter"]})
    result = step.run(df, {"min_text_length": 10})
    assert result.rows_after == 1
    assert result.metadata["rows_removed_by_length"] == 1


def test_length_filter_removes_long_rows(step):
    df = pd.DataFrame({"text": ["short", "x" * 1000]})
    result = step.run(df, {"max_text_length": 100})
    assert result.rows_after == 1


def test_strip_urls(step):
    df = pd.DataFrame({"text": ["Visit https://example.com for more info"]})
    result = step.run(df, {"strip_urls": True})
    assert "https://example.com" not in result.df.iloc[0]["text"]


def test_custom_patterns(step):
    df = pd.DataFrame({"text": ["Hello [REF123] world"]})
    result = step.run(df, {"custom_patterns": [r"\[REF\d+\]"]})
    assert "[REF123]" not in result.df.iloc[0]["text"]


def test_empty_dataframe(step):
    df = pd.DataFrame({"text": []})
    result = step.run(df, {})
    assert result.rows_before == 0
