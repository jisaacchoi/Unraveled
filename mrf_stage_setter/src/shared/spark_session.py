"""Spark utilities removed (no Spark dependency)."""
from __future__ import annotations


class SparkUnavailableError(RuntimeError):
    """Raised when Spark functionality is requested in a non-Spark build."""


def setup_spark_environment(*_args, **_kwargs) -> None:
    """Spark support has been removed from this project."""
    raise SparkUnavailableError(
        "Spark support has been removed. This project no longer depends on Spark."
    )


def create_spark_session_simple(*_args, **_kwargs) -> None:
    """Spark support has been removed from this project."""
    raise SparkUnavailableError(
        "Spark support has been removed. This project no longer depends on Spark."
    )


def create_spark_session(*_args, **_kwargs) -> None:
    """Spark support has been removed from this project."""
    raise SparkUnavailableError(
        "Spark support has been removed. This project no longer depends on Spark."
    )
