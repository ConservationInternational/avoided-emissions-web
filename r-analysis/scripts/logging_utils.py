"""Shared logging helpers for avoided-emissions Python scripts."""

from __future__ import annotations

import logging
import os

THIRD_PARTY_LOGGERS = ("boto3", "botocore", "s3transfer", "urllib3")


def parse_log_level(raw_level: str | None, *, default: int = logging.WARNING) -> int:
    """Convert a log-level string to a logging level, with safe fallback."""
    if raw_level is None:
        return default
    return getattr(logging, str(raw_level).upper(), default)


def configure_third_party_logging(
    *,
    env_var: str = "THIRD_PARTY_LOG_LEVEL",
    default_level: str = "WARNING",
    logger_names: tuple[str, ...] = THIRD_PARTY_LOGGERS,
) -> int:
    """Reduce noisy third-party logging while keeping warnings/errors visible."""
    third_party_level = parse_log_level(
        os.getenv(env_var, default_level),
        default=logging.WARNING,
    )
    for logger_name in logger_names:
        logging.getLogger(logger_name).setLevel(third_party_level)
    return third_party_level
