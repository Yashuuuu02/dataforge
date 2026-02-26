"""Deduplication pipeline step — exact (SHA256) and optional semantic dedup."""

import hashlib
import logging
from typing import Optional

import pandas as pd

from pipeline.common.base import PipelineStep, StepResult, register_step

logger = logging.getLogger(__name__)


def _has_semantic_deps() -> bool:
    """Check if sentence-transformers and faiss are installed."""
    try:
        import sentence_transformers  # noqa: F401
        import faiss  # noqa: F401
        return True
    except ImportError:
        return False


@register_step
class DeduplicationStep(PipelineStep):
    name = "deduplication"
    description = "Remove duplicate rows using exact hash matching or semantic similarity"

    def validate_config(self, config: dict) -> None:
        method = config.get("method", "exact")
        if method not in ("exact", "semantic", "both"):
            raise ValueError(f"Invalid method: {method}. Use 'exact', 'semantic', or 'both'.")
        if method in ("semantic", "both") and not _has_semantic_deps():
            logger.warning("sentence-transformers/faiss not installed — semantic dedup will fall back to exact.")

    def run(self, df: pd.DataFrame, config: dict) -> StepResult:
        rows_before = len(df)
        warnings: list[str] = []
        exact_removed = 0
        semantic_removed = 0
        method = config.get("method", "exact")

        columns = config.get("columns", "all")
        if columns == "all":
            cols = list(df.columns)
        else:
            cols = [c for c in columns if c in df.columns]
            if not cols:
                cols = list(df.columns)
                warnings.append("Specified columns not found, using all columns")

        keep = config.get("keep", "first")
        result_df = df.copy()

        # ── Exact deduplication ──
        if method in ("exact", "both"):
            # Hash rows for dedup
            hashes = result_df[cols].astype(str).apply(
                lambda row: hashlib.sha256("|".join(row).encode()).hexdigest(), axis=1
            )
            mask = ~hashes.duplicated(keep=keep)
            exact_removed = (~mask).sum()
            result_df = result_df[mask].reset_index(drop=True)
            logger.info("Exact dedup: removed %d rows", exact_removed)

        # ── Semantic deduplication ──
        if method in ("semantic", "both"):
            if not _has_semantic_deps():
                warnings.append(
                    "sentence-transformers/faiss not installed. "
                    "Falling back to exact dedup only. "
                    "Install with: pip install -r requirements-optional.txt"
                )
                if method == "semantic" and exact_removed == 0:
                    # Run exact as fallback
                    hashes = result_df[cols].astype(str).apply(
                        lambda row: hashlib.sha256("|".join(row).encode()).hexdigest(), axis=1
                    )
                    mask = ~hashes.duplicated(keep=keep)
                    exact_removed = (~mask).sum()
                    result_df = result_df[mask].reset_index(drop=True)
            else:
                result_df, semantic_removed = self._semantic_dedup(
                    result_df, cols, config, warnings
                )

        rows_after = len(result_df)
        return StepResult(
            df=result_df,
            rows_before=rows_before,
            rows_after=rows_after,
            rows_removed=rows_before - rows_after,
            metadata={
                "exact_duplicates_removed": exact_removed,
                "semantic_duplicates_removed": semantic_removed,
                "method_used": method if _has_semantic_deps() or method == "exact" else "exact (fallback)",
                "columns_checked": cols,
            },
            warnings=warnings,
        )

    def _semantic_dedup(
        self, df: pd.DataFrame, cols: list[str], config: dict, warnings: list[str]
    ) -> tuple[pd.DataFrame, int]:
        """Run semantic deduplication using sentence-transformers + faiss."""
        import numpy as np
        from sentence_transformers import SentenceTransformer
        import faiss

        threshold = config.get("semantic_threshold", 0.95)
        model_name = config.get("semantic_model", "all-MiniLM-L6-v2")

        # Combine text columns
        texts = df[cols].astype(str).apply(lambda row: " ".join(row), axis=1).tolist()

        logger.info("Embedding %d texts for semantic dedup with %s...", len(texts), model_name)
        model = SentenceTransformer(model_name)
        embeddings = model.encode(texts, show_progress_bar=False, batch_size=256)
        embeddings = np.array(embeddings, dtype="float32")

        # Normalize for cosine similarity
        faiss.normalize_L2(embeddings)

        # Build FAISS index
        dim = embeddings.shape[1]
        index = faiss.IndexFlatIP(dim)
        index.add(embeddings)

        # Find near-duplicates
        k = min(10, len(embeddings))
        scores, indices = index.search(embeddings, k)

        to_remove: set[int] = set()
        keep = config.get("keep", "first")

        for i in range(len(embeddings)):
            if i in to_remove:
                continue
            for j_pos in range(1, k):
                j = indices[i][j_pos]
                sim = scores[i][j_pos]
                if j != i and sim >= threshold and j not in to_remove:
                    if keep == "first":
                        to_remove.add(j)
                    else:
                        to_remove.add(i)
                        break

        semantic_removed = len(to_remove)
        result_df = df.drop(index=list(to_remove)).reset_index(drop=True)
        logger.info("Semantic dedup: removed %d rows (threshold=%.2f)", semantic_removed, threshold)

        return result_df, semantic_removed
