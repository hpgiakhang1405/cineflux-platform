"""Provide the lifecycle shared by all CineFlux Spark jobs.

Reads: job-specific input through subclass hooks.
Writes: job-specific output, validations, lineage logs, and a JSON-safe summary.
Runs: around every DP1 and DP2 execution.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Iterator

from pyspark.sql import DataFrame, SparkSession

LOGGER = logging.getLogger(__name__)


class SparkJob(ABC):
    """Template Method for read, validate, transform, write, and lineage."""

    def __init__(self, spark: SparkSession, run_id: str) -> None:
        """Create one job execution with a stable pipeline run identifier."""
        self.spark = spark
        self.run_id = run_id
        self.started_at = datetime.now(timezone.utc)
        self.metrics: dict[str, Any] = {}

    @contextmanager
    def spark_action(self, group_id: str, description: str) -> Iterator[None]:
        """Label Spark actions so their jobs are identifiable in the Spark UI."""
        context = self.spark.sparkContext
        property_names = (
            "spark.jobGroup.id",
            "spark.job.description",
            "spark.job.interruptOnCancel",
        )
        previous = {
            name: context.getLocalProperty(name) for name in property_names
        }
        context.setJobGroup(
            f"{self.run_id}:{group_id}",
            description,
            interruptOnCancel=True,
        )
        context.setJobDescription(description)
        try:
            yield
        finally:
            for name, value in previous.items():
                context.setLocalProperty(name, value)

    def run(self) -> dict[str, Any]:
        """Execute the standard Spark job lifecycle and return its summary."""
        started = perf_counter()
        LOGGER.info("spark_job_started job=%s run_id=%s", self.job_name, self.run_id)
        inputs = self.read()
        self.validate_input(inputs)
        outputs = self.transform(inputs)
        self.write(outputs)
        self.validate_output(outputs)
        self.publish_lineage()
        return self._summary(started)

    def validate(self) -> dict[str, Any]:
        """Run source and persisted-output checks without writing data."""
        started = perf_counter()
        LOGGER.info("spark_validation_started job=%s run_id=%s", self.job_name, self.run_id)
        inputs = self.read()
        self.validate_input(inputs)
        self.validate_output({})
        self.publish_lineage()
        return self._summary(started)

    def _summary(self, started: float) -> dict[str, Any]:
        """Build the common structured execution summary."""
        duration = perf_counter() - started
        finished_at = datetime.now(timezone.utc)
        summary = {
            "job_name": self.job_name,
            "pipeline_run_id": self.run_id,
            "spark_application_id": self.spark.sparkContext.applicationId,
            "started_at": self.started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": round(duration, 4),
            "configuration": self.configuration,
            "metrics": self.metrics,
        }
        LOGGER.info(
            "spark_job_completed job=%s run_id=%s duration_seconds=%.4f",
            self.job_name,
            self.run_id,
            duration,
        )
        return summary

    @property
    @abstractmethod
    def job_name(self) -> str:
        """Return the configured Spark application name."""

    @property
    @abstractmethod
    def configuration(self) -> dict[str, Any]:
        """Return the non-secret configuration captured in the run summary."""

    @abstractmethod
    def read(self) -> dict[str, DataFrame]:
        """Read the job inputs."""

    @abstractmethod
    def validate_input(self, inputs: dict[str, DataFrame]) -> None:
        """Validate source contracts before transformation."""

    @abstractmethod
    def transform(self, inputs: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """Apply job-specific transformations."""

    @abstractmethod
    def write(self, outputs: dict[str, DataFrame]) -> None:
        """Persist transformed outputs."""

    @abstractmethod
    def validate_output(self, outputs: dict[str, DataFrame]) -> None:
        """Validate the persisted target state."""

    @abstractmethod
    def publish_lineage(self) -> None:
        """Publish or log source-to-target lineage."""
