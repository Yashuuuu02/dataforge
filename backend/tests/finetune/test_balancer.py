import pytest
import pandas as pd
from pipeline.modes.finetune.balancer import CategoryBalancerStep

def test_undersample_balances_classes():
    step = CategoryBalancerStep()
    df = pd.DataFrame({
        "category": ["A"] * 100 + ["B"] * 10,
        "text": ["some text"] * 110
    })
    res = step.run(df, {"method": "undersample", "target_column": "category"})
    counts = res.df["category"].value_counts()
    assert counts["A"] == 10
    assert counts["B"] == 10

def test_oversample_increases_minority_class():
    step = CategoryBalancerStep()
    df = pd.DataFrame({
        "category": ["A"] * 100 + ["B"] * 10,
        "text": ["some text"] * 110
    })
    res = step.run(df, {"method": "oversample", "target_column": "category"})
    counts = res.df["category"].value_counts()
    assert counts["A"] == 100
    assert counts["B"] == 100 # Resampled up to majority class

def test_balance_ratio_respected():
    step = CategoryBalancerStep()
    df = pd.DataFrame({
        "category": ["A"] * 100 + ["B"] * 10,
        "text": ["some text"] * 110
    })
    # target_count = 10 / 0.5 = 20
    res = step.run(df, {"method": "undersample", "target_column": "category", "balance_ratio": 0.5})
    counts = res.df["category"].value_counts()
    assert counts["A"] == 20
    assert counts["B"] == 10
