"""Pipeline step base classes and result types."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd


@dataclass
class StepResult:
    """Result of a single pipeline step execution."""

    df: pd.DataFrame
    rows_before: int
    rows_after: int
    rows_removed: int
    metadata: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        return f"{self.rows_removed} rows removed ({self.rows_before} → {self.rows_after})"


class PipelineStep(ABC):
    """Abstract base class for all pipeline processing steps.

    Every step:
    - Takes a DataFrame + config dict
    - Returns a StepResult with a NEW DataFrame (never modifies in place)
    - Is stateless and independently testable
    """

    name: str = "unnamed_step"
    description: str = ""

    @abstractmethod
    def run(self, df: pd.DataFrame, config: dict) -> StepResult:
        """Execute the pipeline step.

        Args:
            df: Input DataFrame (do not modify in-place).
            config: Step-specific configuration dictionary.

        Returns:
            StepResult with processed DataFrame and metadata.
        """
        pass

    def validate_config(self, config: dict) -> None:
        """Validate step configuration. Raise ValueError if invalid."""
        pass


# Step registry for lookup by name
STEP_REGISTRY: dict[str, type[PipelineStep]] = {}


def register_step(cls: type[PipelineStep]) -> type[PipelineStep]:
    """Decorator to register a pipeline step class."""
    STEP_REGISTRY[cls.name] = cls
    return cls
