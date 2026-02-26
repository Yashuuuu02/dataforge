"""Fine-tuning Pipeline Orchestrator."""

import logging
from dataclasses import dataclass, field
import pandas as pd
from typing import Callable, Any
from sklearn.model_selection import train_test_split

from pipeline.common.runner import PipelineRunner
from pipeline.modes.finetune.formatter import FinetuneFormatterStep
from pipeline.modes.finetune.balancer import CategoryBalancerStep
from pipeline.modes.finetune.response_quality import ResponseQualityStep
from pipeline.modes.finetune.augmentor import DataAugmentorStep
from pipeline.modes.finetune.exporter import FinetuneExporter

logger = logging.getLogger(__name__)

@dataclass
class FinetuneConfig:
    # Common steps
    run_deduplication: bool = True
    deduplication_config: dict = field(default_factory=dict)
    run_pii_scrubbing: bool = True
    pii_config: dict = field(default_factory=dict)
    run_noise_removal: bool = True
    noise_config: dict = field(default_factory=dict)
    run_quality_scoring: bool = True
    quality_config: dict = field(default_factory=dict)

    # Finetune specific
    output_format: str = "openai"
    system_prompt: str = ""
    max_tokens_per_example: int = 4096
    run_response_quality: bool = True
    response_quality_config: dict = field(default_factory=dict)
    run_balancer: bool = False
    balancer_config: dict = field(default_factory=dict)
    run_augmentation: bool = False
    augmentation_config: dict = field(default_factory=dict)

    train_split: float = 0.9
    val_split: float = 0.1
    shuffle: bool = True
    seed: int = 42

@dataclass
class FinetunePipelineResult:
    train_df: pd.DataFrame
    val_df: pd.DataFrame
    output_format: str
    total_examples: int
    train_examples: int
    val_examples: int
    avg_tokens: float
    estimated_training_time: str
    output_files: dict
    pipeline_stats: dict


class FinetunePipelineRunner:
    """Executes the specialized fine-tuning preparation pipeline."""

    def run(self, df: pd.DataFrame, config: FinetuneConfig, job_id: str, progress_callback: Callable[[int, str], Any]) -> FinetunePipelineResult:
        logger.info(f"Starting FinetunePipelineRunner for job {job_id}")
        
        # 1. Map config to common workflow plan steps
        common_steps = []
        if config.run_deduplication:
            common_steps.append({"step": "deduplication", "config": config.deduplication_config})
        if config.run_noise_removal:
            common_steps.append({"step": "noise_removal", "config": config.noise_config})
        if config.run_pii_scrubbing:
            common_steps.append({"step": "pii_scrubber", "config": config.pii_config})
        if config.run_quality_scoring:
            common_steps.append({"step": "quality_scorer", "config": config.quality_config})

        # Base Common Runner (Reusing Phase 3 engine)
        if common_steps:
             progress_callback(10, "Running common preprocessing...")
             base_runner = PipelineRunner(common_steps)
             base_result = base_runner.run(df, progress_callback, start_progress=10, end_progress=40)
             df_curr = base_result.df
             stats = base_result.step_results
        else:
             df_curr = df.copy()
             stats = []

        total_fine_steps = sum([1, config.run_response_quality, config.run_balancer, config.run_augmentation])
        progress_per_step = 40 / max(1, total_fine_steps)
        curr_prog = 40

        # 2. Finetune Formatter (Always runs to convert schema)
        curr_prog += progress_per_step
        progress_callback(int(curr_prog), f"Formatting to {config.output_format} schema...")
        fmt_step = FinetuneFormatterStep()
        fmt_res = fmt_step.run(df_curr, {
            "output_format": config.output_format,
            "system_prompt": config.system_prompt,
            "max_tokens_per_example": config.max_tokens_per_example
        })
        df_curr = fmt_res.df
        stats.append({"step": fmt_step.name, "metadata": fmt_res.metadata, "warnings": fmt_res.warnings})

        # 3. Response Quality
        if config.run_response_quality and not df_curr.empty:
            curr_prog += progress_per_step
            progress_callback(int(curr_prog), "Filtering low quality responses...")
            rq_step = ResponseQualityStep()
            rq_res = rq_step.run(df_curr, config.response_quality_config)
            df_curr = rq_res.df
            stats.append({"step": rq_step.name, "metadata": rq_res.metadata, "warnings": rq_res.warnings})

        # 4. Balancer
        if config.run_balancer and not df_curr.empty:
            curr_prog += progress_per_step
            progress_callback(int(curr_prog), "Balancing categories...")
            bal_step = CategoryBalancerStep()
            bal_res = bal_step.run(df_curr, config.balancer_config)
            df_curr = bal_res.df
            stats.append({"step": bal_step.name, "metadata": bal_res.metadata, "warnings": bal_res.warnings})
            
        # 5. Augmentor
        if config.run_augmentation and not df_curr.empty:
             curr_prog += progress_per_step
             progress_callback(int(curr_prog), "Augmenting dataset...")
             aug_step = DataAugmentorStep()
             aug_res = aug_step.run(df_curr, config.augmentation_config)
             df_curr = aug_res.df
             stats.append({"step": aug_step.name, "metadata": aug_res.metadata, "warnings": aug_res.warnings})

        # 6. Train / Val Split
        progress_callback(85, "Splitting dataset into train/val...")
        
        if len(df_curr) < 2:
             # Too small to split
             train_df = df_curr
             val_df = pd.DataFrame(columns=df_curr.columns)
        else:
             stratify = None
             if config.run_balancer and "category_column" in locals().get('bal_res', StepResult(df,0,0,0,{},[])).metadata:
                  cat_col = bal_res.metadata.get("category_column")
                  if cat_col in df_curr.columns and df_curr[cat_col].nunique() > 1:
                       stratify = df_curr[cat_col]
                       
             try:
                 train_df, val_df = train_test_split(
                     df_curr, 
                     test_size=config.val_split, 
                     train_size=config.train_split,
                     random_state=config.seed if config.shuffle else None,
                     shuffle=config.shuffle,
                     stratify=stratify
                 )
             except ValueError:
                 # Stratification fails if a class has only 1 member, fallback to random
                 train_df, val_df = train_test_split(
                     df_curr, 
                     test_size=config.val_split, 
                     train_size=config.train_split,
                     random_state=config.seed if config.shuffle else None,
                     shuffle=config.shuffle
                 )

        # Calculate final output stats
        total = len(train_df) + len(val_df)
        avg_tok = fmt_res.metadata.get("avg_token_count", 0.0)
        
        # Super rough estimate for A100: ~5k tokens/sec throughput
        # (Total Examples * Avg Tokens * 3 Epochs) / 5000 / 3600
        hrs = (total * avg_tok * 3) / 5000 / 3600
        time_str = f"~{hrs:.1f} hours" if hrs >= 0.1 else "< 6 mins"

        # 7. Export files (handled next by Exporter class called by Celery task directly or here)
        progress_callback(90, "Exporting target formats...")
        exporter = FinetuneExporter()
        
        # We will save to local tmp first, then the task uploads to MinIO.
        # But for pipeline structure, Exporter handles parsing to exact json/jsonl structures
        train_path = f"/tmp/{job_id}_train.jsonl"
        val_path = f"/tmp/{job_id}_val.jsonl"
        cfg_path = f"/tmp/{job_id}_training_config.json"
        
        exporter.export(train_df, config.output_format, train_path)
        if not val_df.empty:
             exporter.export(val_df, config.output_format, val_path)
             
        exporter.generate_config(total, avg_tok, config.output_format, cfg_path)

        return FinetunePipelineResult(
            train_df=train_df,
            val_df=val_df,
            output_format=config.output_format,
            total_examples=total,
            train_examples=len(train_df),
            val_examples=len(val_df),
            avg_tokens=avg_tok,
            estimated_training_time=time_str,
            output_files={"train": train_path, "val": val_path if not val_df.empty else None, "config": cfg_path},
            pipeline_stats={"step_results": stats}
        )
