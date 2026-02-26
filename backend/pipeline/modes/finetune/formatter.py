"""Format normalizer and converter for fine-tuning datasets."""

import ast
import json
import logging
import tiktoken
import pandas as pd
from typing import Any, Dict

from pipeline.common.base import PipelineStep, StepResult

logger = logging.getLogger(__name__)

class FinetuneFormatterStep(PipelineStep):
    """Normalizes input instructions into specific LLM chat template formats."""
    name = "finetune_formatter"
    description = "Normalizes formats to target LLM prompts (e.g. Llama 3) and filters by token limits."

    def run(self, df: pd.DataFrame, config: dict) -> StepResult:
        df_out = df.copy()
        rows_before = len(df_out)
        warnings = []
        
        in_format = config.get("input_format", "auto")
        out_format = config.get("output_format", "openai")
        sys_prompt = config.get("system_prompt", "")
        max_tokens = config.get("max_tokens_per_example", 4096)
        tokenizer_name = config.get("tokenizer", "cl100k_base")
        
        inst_col = config.get("instruction_column", "auto")
        in_col =   config.get("input_column", "auto")
        out_col =  config.get("output_column", "auto")

        # 1. Detect and Normalize Input into standard columns: 
        # _norm_instruction, _norm_input, _norm_output
        df_out, detected_format, warn = self._normalize_input(df_out, in_format, inst_col, in_col, out_col)
        if warn: warnings.append(warn)

        # 2. Format to Target
        df_out["formatted_text"] = df_out.apply(
            lambda row: self._format_row(
                row.get("_norm_instruction", ""), 
                row.get("_norm_input", ""), 
                row.get("_norm_output", ""), 
                out_format, 
                sys_prompt
            ), axis=1
        )
        
        # 3. Tokenize and Filter
        try:
             enc = tiktoken.get_encoding(tokenizer_name)
        except Exception:
             warnings.append(f"Tokenizer {tokenizer_name} not found, falling back to cl100k_base.")
             enc = tiktoken.get_encoding("cl100k_base")
             
        df_out["token_count"] = df_out["formatted_text"].apply(lambda x: len(enc.encode(x if isinstance(x, str) else json.dumps(x))))
        
        filtered_df = df_out[df_out["token_count"] <= max_tokens].copy()
        filtered_out_count = len(df_out) - len(filtered_df)
        
        # Calculate stats
        tc = filtered_df["token_count"]
        stats = {
            "input_format_detected": detected_format,
            "output_format": out_format,
            "examples_formatted": len(filtered_df),
            "examples_filtered_token_limit": filtered_out_count,
            "avg_token_count": float(tc.mean()) if not tc.empty else 0.0,
            "max_token_count": int(tc.max()) if not tc.empty else 0,
            "min_token_count": int(tc.min()) if not tc.empty else 0,
             "token_distribution": {
                "0-512": int((tc <= 512).sum()),
                "512-1024": int(((tc > 512) & (tc <= 1024)).sum()),
                "1024-2048": int(((tc > 1024) & (tc <= 2048)).sum()),
                "2048-4096": int(((tc > 2048) & (tc <= 4096)).sum()),
                "4096+": int((tc > 4096).sum())
            }
        }

        # Cleanup internal columns, keeping only target formatted output and tokens
        keep_cols = ["formatted_text", "token_count"]
        # Retain other original data if needed, but for export we usually just dump `formatted_text`
        for col in df_out.columns:
            if col not in ["_norm_instruction", "_norm_input", "_norm_output", "formatted_text", "token_count"]:
                keep_cols.append(col)
                
        filtered_df = filtered_df[keep_cols]

        return StepResult(
            df=filtered_df,
            rows_before=rows_before,
            rows_after=len(filtered_df),
            rows_removed=filtered_out_count,
            metadata=stats,
            warnings=warnings
        )
        
    def _normalize_input(self, df: pd.DataFrame, in_format: str, inst_col: str, in_col: str, out_col: str) -> tuple[pd.DataFrame, str, str]:
        cols = [c.lower() for c in df.columns]
        actual_format = in_format
        warning = ""

        # Priority resolution
        if in_format == "auto":
            if "messages" in cols or "conversations" in cols:
                actual_format = "sharegpt"
            elif "instruction" in cols and "output" in cols:
                actual_format = "alpaca"
            elif ("question" in cols and "answer" in cols) or ("q" in cols and "a" in cols):
                actual_format = "qa_pairs"
            elif "prompt" in cols and "completion" in cols:
                actual_format = "raw_pairs"
            else:
                actual_format = "raw_pairs" # Fallback
                warning = "Could not confidently auto-detect input format. Falling back to 'raw_pairs' taking first two text columns."
            
            if not warning:
                 warning = f"Auto-detected input format: {actual_format} based on column structures."
                 logger.info(warning)

        # Mapping logic
        if actual_format == "sharegpt":
            msg_col = "messages" if "messages" in df.columns else "conversations" if "conversations" in df.columns else df.columns[0]
            def extract_sharegpt(val):
                try:
                     # Parse stringified lists
                     if isinstance(val, str): val = ast.literal_eval(val)
                     if not isinstance(val, list) or len(val) < 2: return "", "", ""
                     # Simple mapping: first user gives instruction, first assistant gives output
                     instruction = next((m.get("content", m.get("value", "")) for m in val if m.get("role") == "user" or m.get("from") == "human"), "")
                     output = next((m.get("content", m.get("value", "")) for m in val if m.get("role") == "assistant" or m.get("from") == "gpt"), "")
                     return instruction, "", output
                except Exception:
                     return "", "", ""
            extracted = df[msg_col].apply(extract_sharegpt)
            df["_norm_instruction"] = extracted.apply(lambda x: x[0])
            df["_norm_input"] = extracted.apply(lambda x: x[1])
            df["_norm_output"] = extracted.apply(lambda x: x[2])

        elif actual_format == "alpaca":
            col_map = {c.lower(): c for c in df.columns}
            df["_norm_instruction"] = df[col_map.get("instruction", df.columns[0])] if "instruction" in col_map else ""
            df["_norm_input"] = df[col_map.get("input", "")] if "input" in col_map else ""
            df["_norm_output"] = df[col_map.get("output", df.columns[-1])] if "output" in col_map else ""
            
        elif actual_format == "qa_pairs":
            col_map = {c.lower(): c for c in df.columns}
            q_col = col_map.get("question", col_map.get("q", df.columns[0]))
            a_col = col_map.get("answer", col_map.get("a", df.columns[-1]))
            df["_norm_instruction"] = df[q_col]
            df["_norm_input"] = ""
            df["_norm_output"] = df[a_col]
            
        elif actual_format == "raw_pairs":
            col_map = {c.lower(): c for c in df.columns}
            p_col = col_map.get("prompt", df.columns[0])
            c_col = col_map.get("completion", df.columns[1] if len(df.columns) > 1 else df.columns[0])
            df["_norm_instruction"] = df[p_col]
            df["_norm_input"] = ""
            df["_norm_output"] = df[c_col]
            
        else: # explicit columns mapped
             df["_norm_instruction"] = df[inst_col] if inst_col in df.columns else ""
             df["_norm_input"] = df[in_col] if in_col in df.columns else ""
             df["_norm_output"] = df[out_col] if out_col in df.columns else ""

        # Fill NaNs
        df["_norm_instruction"] = df["_norm_instruction"].fillna("").astype(str)
        df["_norm_input"] = df["_norm_input"].fillna("").astype(str)
        df["_norm_output"] = df["_norm_output"].fillna("").astype(str)
        
        return df, actual_format, warning

    def _format_row(self, inst: str, inp: str, out: str, target_format: str, sys_prompt: str) -> Any:
        full_inst = f"{inst}\n{inp}".strip() if inp else inst

        if target_format == "llama3":
            sys_block = f"<|start_header_id|>system<|end_header_id|>\n{sys_prompt}<|eot_id|>" if sys_prompt else ""
            return f"<|begin_of_text|>{sys_block}<|start_header_id|>user<|end_header_id|>\n{full_inst}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n{out}<|eot_id|>"
            
        elif target_format == "llama2":
            sys_block = f"<<SYS>>{sys_prompt}<</SYS>> " if sys_prompt else ""
            return f"<s>[INST] {sys_block}{full_inst} [/INST] {out} </s>"
            
        elif target_format == "mistral":
             # Mistral doesn't natively use a system prompt in its standard chat template as strongly, 
             # but often it's prepended to the instruction
             inst_with_sys = f"{sys_prompt}\n\n{full_inst}".strip() if sys_prompt else full_inst
             return f"<s>[INST] {inst_with_sys} [/INST] {out}</s>"
             
        elif target_format == "gemma":
             sys_block = f"<start_of_turn>user\n{sys_prompt}\n\n" if sys_prompt else "<start_of_turn>user\n"
             return f"{sys_block}{full_inst}<end_of_turn>\n<start_of_turn>model\n{out}<end_of_turn>"
             
        elif target_format == "alpaca":
             # Return a dict, exporter will handle jsonl dumping
             return {"instruction": inst, "input": inp, "output": out}
             
        elif target_format == "sharegpt":
             return {"conversations": [{"from": "human", "value": full_inst}, {"from": "gpt", "value": out}]}
             
        else: # "openai" default
             msgs = []
             if sys_prompt: msgs.append({"role": "system", "content": sys_prompt})
             msgs.append({"role": "user", "content": full_inst})
             msgs.append({"role": "assistant", "content": out})
             return {"messages": msgs}
