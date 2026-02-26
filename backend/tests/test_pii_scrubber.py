"""Tests for the PII scrubber pipeline step."""

import pandas as pd
import pytest
from pipeline.common.pii_scrubber import PIIScrubberStep


@pytest.fixture
def step():
    return PIIScrubberStep()


def test_redacts_email_addresses(step):
    df = pd.DataFrame({"text": ["Contact me at john@example.com please"]})
    result = step.run(df, {"action": "redact", "entities": ["EMAIL"], "redact_with": "[REDACTED]"})
    assert "[REDACTED]" in result.df.iloc[0]["text"] or "john@example.com" not in result.df.iloc[0]["text"]
    assert result.metadata["total_pii_instances"] >= 1


def test_redacts_phone_numbers(step):
    df = pd.DataFrame({"text": ["Call me at (555) 123-4567"]})
    result = step.run(df, {"action": "redact", "entities": ["PHONE"], "redact_with": "[REDACTED]"})
    assert result.metadata["total_pii_instances"] >= 1


def test_remove_row_action(step):
    df = pd.DataFrame({
        "text": ["Clean text here", "Email: test@test.com", "Another clean one"],
    })
    result = step.run(df, {"action": "remove_row", "entities": ["EMAIL"]})
    assert result.rows_after <= result.rows_before
    # Should have removed the row with the email
    assert result.metadata["rows_with_pii"] >= 1


def test_flag_action_adds_columns(step):
    df = pd.DataFrame({"text": ["test@test.com", "clean text"]})
    result = step.run(df, {"action": "flag", "entities": ["EMAIL"]})
    assert "pii_detected" in result.df.columns
    assert "pii_entities" in result.df.columns


def test_no_false_positives_on_clean_text(step):
    df = pd.DataFrame({"text": ["The quick brown fox jumps over the lazy dog"]})
    result = step.run(df, {"action": "flag", "entities": ["EMAIL", "PHONE", "SSN"]})
    assert result.metadata["rows_with_pii"] == 0


def test_handles_empty_text(step):
    df = pd.DataFrame({"text": ["", None, "normal"]})
    result = step.run(df, {"action": "redact"})
    assert result.rows_before == 3


def test_multiple_entity_types(step):
    df = pd.DataFrame({"text": ["john@example.com and (555) 123-4567"]})
    result = step.run(df, {"action": "redact", "entities": ["ALL"]})
    assert result.metadata["total_pii_instances"] >= 2
