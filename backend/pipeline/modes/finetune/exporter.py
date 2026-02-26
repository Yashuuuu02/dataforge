"""Exports DataFrames to native LLM target formats."""

import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class FinetuneExporter:
    """Writes train/val splits to exact target formats like JSONL for Llama 3."""
    
    def export(self, df: pd.DataFrame, output_format: str, output_path: str) -> str:
        if df.empty:
            with open(output_path, "w") as f: f.write("")
            return output_path
            
        # The `formatted_text` column holds the precise dictionaries or strings
        format_col = "formatted_text" if "formatted_text" in df.columns else df.columns[0]
        
        is_json_obj = output_format in ("alpaca", "sharegpt")
        
        if is_json_obj:
            # Dump array of JSON objects
            records = df[format_col].tolist()
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False)
        else:
            # Dump JSONL
            with open(output_path, "w", encoding="utf-8") as f:
                for val in df[format_col]:
                    if isinstance(val, dict):
                         f.write(json.dumps(val, ensure_ascii=False) + "\n")
                    else:
                         # For Llama tags that are plain text, we still generally wrap it in a JSON structure 
                         # for tools like Unsloth, OR write raw text files. standard format is usually `{"text": "<llama tokens>"}`
                         f.write(json.dumps({"text": str(val)}, ensure_ascii=False) + "\n")
                         
        return output_path

    def generate_config(self, num_examples: int, avg_tokens: float, dataset_format: str, output_path: str) -> str:
        """Generates the recommended training script configs (e.g., Unsloth/Axolotl)."""
        
        model_rec = "meta-llama/Meta-Llama-3-8B-Instruct"
        if dataset_format == "mistral": model_rec = "mistralai/Mistral-7B-Instruct-v0.2"
        elif dataset_format == "gemma": model_rec = "google/gemma-7b-it"
        
        epochs = 3 if num_examples > 5000 else (5 if num_examples > 1000 else 10)
        
        config = {
          "model_recommendation": model_rec,
          "dataset_format": dataset_format,
          "num_examples": num_examples,
          "avg_tokens": round(avg_tokens, 1),
          "recommended_epochs": epochs,
          "recommended_batch_size": 4,
          "recommended_learning_rate": 2e-4,
          "frameworks": {
            "unsloth": f"from unsloth import FastLanguageModel\\nmodel = FastLanguageModel.from_pretrained(model_name='{model_rec}'...)",
            "axolotl": f"base_model: {model_rec}\\ndatasets:\\n  - path: train.jsonl\\n    type: {dataset_format}\\nepochs: {epochs}\\nmicro_batch_size: 4\\nlearning_rate: 0.0002"
          }
        }
        
        with open(output_path, "w", encoding="utf-8") as f:
             json.dump(config, f, indent=2)
             
        return output_path
