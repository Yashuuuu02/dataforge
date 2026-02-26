import pytest
import pandas as pd
from pipeline.modes.finetune.formatter import FinetuneFormatterStep

def test_auto_detects_alpaca_format():
    step = FinetuneFormatterStep()
    df = pd.DataFrame({
        "instruction": ["Translate this"],
        "input": ["Hello"],
        "output": ["Hola"]
    })
    _, fmt, warn = step._normalize_input(df, "auto", "auto", "auto", "auto")
    assert fmt == "alpaca"
    
def test_auto_detects_sharegpt_format():
    step = FinetuneFormatterStep()
    df = pd.DataFrame({
        "conversations": [{"from": "human", "value": "Hi"}, {"from": "gpt", "value": "Hello"}]
    })
    _, fmt, warn = step._normalize_input(df, "auto", "auto", "auto", "auto")
    assert fmt == "sharegpt"

def test_converts_to_openai_format_correctly():
    step = FinetuneFormatterStep()
    res = step._format_row("Tell me a joke", "", "Why did the chicken cross the road?", "openai", "You are funny.")
    assert "messages" in res
    assert len(res["messages"]) == 3
    assert res["messages"][0]["role"] == "system"

def test_converts_to_llama3_format_correctly():
    step = FinetuneFormatterStep()
    res = step._format_row("Test inst", "Test inp", "Test out", "llama3", "Sys prompt")
    assert "<|begin_of_text|>" in res
    assert "<|start_header_id|>system<|end_header_id|>" in res
    assert "<|start_header_id|>user<|end_header_id|>" in res
    assert "<|start_header_id|>assistant<|end_header_id|>" in res
    assert "<|eot_id|>" in res

def test_filters_examples_exceeding_max_tokens():
    step = FinetuneFormatterStep()
    df = pd.DataFrame({
        "instruction": ["Say hi" * 5000], # Artificially large
        "input": [""],
        "output": ["Hi" * 5000]
    })
    res = step.run(df, {"max_tokens_per_example": 100})
    assert len(res.df) == 0
    assert res.rows_removed == 1

def test_token_count_column_added():
    step = FinetuneFormatterStep()
    df = pd.DataFrame({
        "instruction": ["Say hi"],
        "input": [""],
        "output": ["Hi"]
    })
    res = step.run(df, {})
    assert "token_count" in res.df.columns
    assert res.df["token_count"].iloc[0] > 0
