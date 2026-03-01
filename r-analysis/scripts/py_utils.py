"""Shared Python utilities for avoided-emissions analysis scripts.

Provides Rollbar error/message reporting using the official ``rollbar``
(pyrollbar) SDK, mirroring the R ``utils.R`` helpers so that Python
pipeline steps report errors through the same channel.

Environment variables
---------------------
ROLLBAR_SCRIPT_TOKEN   Access token for Rollbar.
                       If unset, all reporting calls are silently skipped.
ROLLBAR_ENVIRONMENT    Rollbar environment tag (default: value of
                       ``ENVIRONMENT``, or ``"development"``).
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

import rollbar

log = logging.getLogger("py_utils")

_rollbar_enabled: bool = False


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def rollbar_init(
    token: str | None = None,
    environment: str | None = None,
) -> None:
    """Initialise Rollbar reporting.  Call once at script startup.

    Parameters
    ----------
    token : str, optional
        Rollbar access token.  Falls back to ``ROLLBAR_SCRIPT_TOKEN`` env var.
    environment : str, optional
        Rollbar environment name.  Falls back to ``ROLLBAR_ENVIRONMENT``
        then ``ENVIRONMENT`` then ``"development"``.
    """
    global _rollbar_enabled  # noqa: PLW0603

    access_token = token or os.environ.get("ROLLBAR_SCRIPT_TOKEN", "")
    env = (
        environment
        or os.environ.get("ROLLBAR_ENVIRONMENT")
        or os.environ.get("ENVIRONMENT", "development")
    )

    if not access_token:
        log.info("ROLLBAR_SCRIPT_TOKEN not set — error tracking disabled")
        _rollbar_enabled = False
        return

    rollbar.init(
        access_token,
        environment=env,
        framework="script",
        code_version=os.environ.get("CODE_VERSION"),
    )
    _rollbar_enabled = True
    log.info("Rollbar initialized (environment=%s)", env)


# ---------------------------------------------------------------------------
# Error reporting
# ---------------------------------------------------------------------------


def rollbar_report_error(
    error_msg: str,
    *,
    exc_info: BaseException | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    """Report an error to Rollbar.

    Parameters
    ----------
    error_msg : str
        Human-readable error description.
    exc_info : BaseException, optional
        If supplied, the exception (with traceback) is sent to Rollbar.
        Otherwise ``sys.exc_info()`` is tried, falling back to a plain
        message report.
    extra : dict, optional
        Arbitrary key/value metadata attached to the error item.
    """
    if not _rollbar_enabled:
        return

    try:
        if exc_info is not None:
            rollbar.report_exc_info(
                (type(exc_info), exc_info, exc_info.__traceback__),
                extra_data=extra,
            )
        elif sys.exc_info()[0] is not None:
            # Called inside an except block — use current exception
            rollbar.report_exc_info(extra_data=extra)
        else:
            rollbar.report_message(error_msg, level="error", extra_data=extra)
    except Exception as exc:
        log.warning("Rollbar: could not send error report: %s", exc)


# ---------------------------------------------------------------------------
# Info / warning message reporting
# ---------------------------------------------------------------------------


def rollbar_report_message(
    msg: str,
    level: str = "info",
    extra: dict[str, Any] | None = None,
) -> None:
    """Report an informational message to Rollbar.

    Parameters
    ----------
    msg : str
        Message body.
    level : str
        Rollbar level: ``"debug"``, ``"info"``, ``"warning"``,
        ``"error"``, ``"critical"``.
    extra : dict, optional
        Arbitrary metadata.
    """
    if not _rollbar_enabled:
        return

    try:
        rollbar.report_message(msg, level=level, extra_data=extra)
    except Exception as exc:
        log.warning("Rollbar: could not send message: %s", exc)


# ---------------------------------------------------------------------------
# Context manager for wrapping a step (like R's with_rollbar)
# ---------------------------------------------------------------------------


class with_rollbar:
    """Context manager that reports exceptions to Rollbar before re-raising.

    Usage::

        with with_rollbar("01_extract_covariates"):
            ...  # pipeline code
    """

    def __init__(self, step_name: str = "Python analysis") -> None:
        self.step_name = step_name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is not None:
            rollbar_report_error(
                str(exc_val),
                exc_info=exc_val,
                extra={"step": self.step_name},
            )
        return False  # re-raise
