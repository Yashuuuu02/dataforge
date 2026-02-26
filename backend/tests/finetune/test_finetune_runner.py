import pytest
import pandas as pd
from pipeline.modes.finetune.runner import FinetunePipelineRunner, FinetuneConfig

def test_full_finetune_pipeline_produces_train_val_split():
    runner = FinetunePipelineRunner()
    df = pd.DataFrame({
        "prompt": ["Say hi", "Say bye", "Say yes", "Say no", "Say maybe"] * 10,
        "completion": ["Hi", "Bye", "Yes", "No", "Maybe"] * 10
    })
    
    config = FinetuneConfig(
        run_deduplication=False,
        run_noise_removal=False,
        run_pii_scrubbing=False,
        run_quality_scoring=False,
        output_format="openai",
        train_split=0.8,
        val_split=0.2
    )
    
    def mock_cb(prog, msg): pass
    
    res = runner.run(df, config, "test_job_123", mock_cb)
    
    assert res.output_format == "openai"
    assert len(res.train_df) == 40
    assert len(res.val_df) == 10
    assert res.total_examples == 50
    assert res.output_files["train"] == "/tmp/test_job_123_train.jsonl"
    assert res.output_files["val"] == "/tmp/test_job_123_val.jsonl"

def test_stratified_split_preserves_distribution():
    runner = FinetunePipelineRunner()
    # 40 A's, 10 B's
    df = pd.DataFrame({
        "instruction": ["Do A"] * 40 + ["Do B"] * 10,
        "output": ["A output"] * 40 + ["B output"] * 10,
        "category_col": ["A"] * 40 + ["B"] * 10
    })
    
    config = FinetuneConfig(
        run_deduplication=False,
        run_noise_removal=False,
        run_pii_scrubbing=False,
        run_quality_scoring=False,
        run_balancer=True,
        balancer_config={"target_column": "category_col", "method": "undersample"}, # balances to 10 A, 10 B
        output_format="alpaca",
        train_split=0.8,
        val_split=0.2
    )
    
    def mock_cb(prog, msg): pass
    res = runner.run(df, config, "test_job_strat", mock_cb)
    
    # After undersample we should have 20 total. 80% train = 16, 20% val = 4.
    assert res.total_examples == 20
    assert len(res.train_df) == 16
    assert len(res.val_df) == 4
    
    # In the train split we should ideally see 8 A's and 8 B's due to stratification over the balanced set
    train_dist = res.train_df["_norm_instruction"].value_counts()
    assert train_dist.get("Do A", 0) == 8
    assert train_dist.get("Do B", 0) == 8
