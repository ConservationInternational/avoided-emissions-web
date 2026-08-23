"""Layouts package — page layouts and shared UI components.

Re-exports the public API used by app.py and callbacks.py.
"""

from layouts.admin import admin_layout
from layouts.auth import (
    forgot_password_layout,
    login_layout,
    register_layout,
    reset_password_layout,
)
from layouts.common import (
    ALL_COVARIATES,
    DEFAULT_COVARIATES,
    DEFAULT_EXACT_MATCH,
    DUAL_PURPOSE_VARS,
    EXACT_MATCH_OPTIONS,
    RESULTS_TOTAL_COLUMNS,
    RESULTS_YEARLY_COLUMNS,
    TASK_LIST_COLUMNS,
    _make_ag_grid,
    footer,
    navbar,
)
from layouts.dashboard import dashboard_layout
from layouts.settings import not_found_layout, settings_layout
from layouts.submit import submit_layout
from layouts.task_detail import task_detail_layout

__all__ = [
    # constants / column defs
    "ALL_COVARIATES",
    "DEFAULT_COVARIATES",
    "DEFAULT_EXACT_MATCH",
    "DUAL_PURPOSE_VARS",
    "EXACT_MATCH_OPTIONS",
    "RESULTS_TOTAL_COLUMNS",
    "RESULTS_YEARLY_COLUMNS",
    "TASK_LIST_COLUMNS",
    # shared UI helpers
    "_make_ag_grid",
    # page layouts
    "admin_layout",
    "dashboard_layout",
    "footer",
    "forgot_password_layout",
    "login_layout",
    "navbar",
    "not_found_layout",
    "register_layout",
    "reset_password_layout",
    "settings_layout",
    "submit_layout",
    "task_detail_layout",
]
