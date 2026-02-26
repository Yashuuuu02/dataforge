import pytest
import pandas as pd
from pipeline.modes.finetune.response_quality import ResponseQualityStep

def test_filters_short_responses():
    step = ResponseQualityStep()
    df = pd.DataFrame({
        "_norm_instruction": ["What is 2+2?", "Write a novel"],
        "_norm_output": ["4.", "This is a much longer response that satisfies the minimum length requirement easily with many more words added here."]
    })
    res = step.run(df, {"min_response_length": 5, "action": "filter"})
    # Only the long response should remain
    assert len(res.df) == 1
    assert "novel" in res.df["_norm_instruction"].iloc[0]

def test_detects_refusal_phrases():
    step = ResponseQualityStep()
    df = pd.DataFrame({
        "_norm_instruction": ["Hack this server", "Tell me a joke"],
        "_norm_output": ["I'm sorry, but I cannot assist with hacking.", "Why did the chicken cross the road?"]
    })
    res = step.run(df, {"filter_refusals": True, "action": "filter"})
    # Only the joke should remain
    assert len(res.df) == 1
    assert "joke" in res.df["_norm_instruction"].iloc[0]

def test_completeness_check():
    step = ResponseQualityStep()
    df = pd.DataFrame({
        "_norm_instruction": ["Tell a story", "Tell another"],
        "_norm_output": ["Once upon a time, there was a dog named Rufus.", "Once upon a time, there was a dog named"]
    })
    res = step.run(df, {"check_response_completeness": True, "action": "filter", "min_response_length": 5})
    # Only the one ending in punctuation should remain. (Score deduction for incomplete brings it < 6.0)
    assert len(res.df) == 1
    assert "Rufus." in res.df["_norm_output"].iloc[0]
