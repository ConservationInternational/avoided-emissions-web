"""Task submission page layout."""

import random

import dash_bootstrap_components as dbc
from dash import dcc, html

from layouts.common import (
    DEFAULT_EXACT_MATCH,
    EXACT_MATCH_OPTIONS,
    MATCHING_JOB_QUEUE_OPTIONS,
    navbar,
)
from services import ANALYSIS_DEFAULTS, DEFAULT_MATCHING_JOB_QUEUE


def submit_layout(user, recompute_config=None):
    """Task submission form with file upload and covariate selection.

    Parameters
    ----------
    user : User
        The currently logged-in user.
    recompute_config : dict or None
        When recomputing a previous task, a dict of settings returned by
        :func:`services.get_recompute_config`.  All form fields are
        pre-filled with the previous task's values (except for a fresh
        random seed).  When *None* the form uses normal defaults.
    """
    rc = recompute_config or {}
    default_random_seed = rc.get(
        "random_seed", random.SystemRandom().randint(1, 2147483647)
    )

    return dbc.Container(
        [
            navbar(user, active_page="/submit"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.H2("Submit Analysis Task", className="mb-1"),
                            html.P(
                                "Use the guided tabs to select uploaded sites, configure matching, and submit.",
                                className="text-muted mb-0",
                            ),
                        ],
                        width=True,
                    )
                ],
                className="mb-3",
            ),
            html.Div(
                [
                    html.Div(id="submit-errors"),
                    html.Div(id="submit-result", className="mt-2"),
                ],
                className="submit-feedback-sticky mb-3",
            ),
            dbc.Form(
                [
                    dbc.Tabs(
                        [
                            dbc.Tab(
                                label="1. Task & Sites",
                                tab_id="tab-submit-sites",
                                children=[
                                    html.Div(
                                        [
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        [
                                                            dbc.Label("Task Name"),
                                                            dbc.Input(
                                                                id="task-name",
                                                                type="text",
                                                                placeholder="My analysis",
                                                                value=rc.get(
                                                                    "task_name", ""
                                                                ),
                                                            ),
                                                        ],
                                                        xs=12,
                                                        md=6,
                                                    ),
                                                    dbc.Col(
                                                        [
                                                            dbc.Label(
                                                                "Description (optional)"
                                                            ),
                                                            dbc.Input(
                                                                id="task-description",
                                                                type="text",
                                                                placeholder="Brief description",
                                                                value=rc.get(
                                                                    "description", ""
                                                                ),
                                                            ),
                                                        ],
                                                        xs=12,
                                                        md=6,
                                                    ),
                                                ],
                                                className="g-3 mb-3",
                                            ),
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        [
                                                            dbc.Label(
                                                                "Analysis Resolution"
                                                            ),
                                                            dbc.Select(
                                                                id="resolution-m",
                                                                options=[
                                                                    {
                                                                        "label": "1 km (default)",
                                                                        "value": "1000",
                                                                    },
                                                                    {
                                                                        "label": "250 m",
                                                                        "value": "250",
                                                                    },
                                                                ],
                                                                value=str(
                                                                    rc.get(
                                                                        "resolution_m",
                                                                        ANALYSIS_DEFAULTS[
                                                                            "resolution_m"
                                                                        ],
                                                                    )
                                                                ),
                                                            ),
                                                            html.Small(
                                                                "Pixel resolution for covariate data "
                                                                "and matching. Only covariates "
                                                                "available at this resolution are "
                                                                "shown below.",
                                                                className="text-muted",
                                                            ),
                                                        ],
                                                        xs=12,
                                                        sm=4,
                                                    ),
                                                ],
                                                className="mb-3",
                                            ),
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Use Uploaded Sites"
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            dbc.Row(
                                                                [
                                                                    dbc.Col(
                                                                        dbc.Select(
                                                                            id="site-set-selector",
                                                                            options=[],
                                                                            placeholder="Select a saved site set...",
                                                                        ),
                                                                        xs=12,
                                                                        md=12,
                                                                    ),
                                                                ],
                                                                className="g-2 mb-2",
                                                            ),
                                                            html.Small(
                                                                "Upload, rename, archive, and delete site sets from the Admin page.",
                                                                className="d-block text-muted mb-2",
                                                            )
                                                            if user.is_admin
                                                            else None,
                                                            dbc.Checkbox(
                                                                id="show-archived-site-sets",
                                                                label="Show archived site sets",
                                                                value=False,
                                                                className="mb-2",
                                                            ),
                                                            dcc.Loading(
                                                                html.Div(
                                                                    id="site-set-metadata"
                                                                ),
                                                                type="circle",
                                                                delay_show=250,
                                                                color="#0d6efd",
                                                            ),
                                                        ]
                                                    ),
                                                ],
                                                className="ae-section-card mb-3",
                                            ),
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader("Site Preview"),
                                                    dbc.CardBody(
                                                        dcc.Loading(
                                                            [
                                                                html.Div(
                                                                    id="site-preview-map",
                                                                    className="mb-3",
                                                                ),
                                                                html.Div(
                                                                    id="site-preview"
                                                                ),
                                                            ],
                                                            type="circle",
                                                            delay_show=250,
                                                            color="#0d6efd",
                                                        )
                                                    ),
                                                ],
                                                className="ae-section-card mb-3",
                                            ),
                                            dbc.Card(
                                                [
                                                    dbc.CardHeader(
                                                        "Need to add more sites?"
                                                    ),
                                                    dbc.CardBody(
                                                        [
                                                            html.P(
                                                                "Site uploads are now managed asynchronously from the Admin page.",
                                                                className="mb-2",
                                                            ),
                                                            dbc.Button(
                                                                "Open Admin Site Uploads",
                                                                href="/admin",
                                                                color="secondary",
                                                                outline=True,
                                                                disabled=not user.is_admin,
                                                            ),
                                                        ]
                                                    ),
                                                    dbc.CardFooter(
                                                        html.Small(
                                                            "After selecting a site set, continue to the Matching Setup tab.",
                                                            className="text-muted",
                                                        )
                                                    ),
                                                ],
                                                className="ae-section-card",
                                            ),
                                        ],
                                        className="pt-3",
                                    )
                                ],
                            ),
                            dbc.Tab(
                                label="2. Matching Setup",
                                tab_id="tab-submit-matching",
                                children=[
                                    html.Div(
                                        [
                                            dbc.Row(
                                                [
                                                    dbc.Col(
                                                        [
                                                            dbc.Card(
                                                                [
                                                                    dbc.CardHeader(
                                                                        "Matching Covariates"
                                                                    ),
                                                                    dbc.CardBody(
                                                                        [
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Select(
                                                                                                id="preset-selector",
                                                                                                placeholder="Load a saved preset…",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        md=5,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Button(
                                                                                                "Load",
                                                                                                id="load-preset-btn",
                                                                                                color="primary",
                                                                                                size="sm",
                                                                                                className="me-1",
                                                                                            ),
                                                                                            dbc.Button(
                                                                                                "Delete",
                                                                                                id="delete-preset-btn",
                                                                                                color="danger",
                                                                                                outline=True,
                                                                                                size="sm",
                                                                                            ),
                                                                                        ],
                                                                                        xs="auto",
                                                                                        md=3,
                                                                                        className="d-flex align-items-center",
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.InputGroup(
                                                                                                [
                                                                                                    dbc.Input(
                                                                                                        id="preset-name-input",
                                                                                                        type="text",
                                                                                                        placeholder="Preset name",
                                                                                                        size="sm",
                                                                                                    ),
                                                                                                    dbc.Button(
                                                                                                        "Save",
                                                                                                        id="save-preset-btn",
                                                                                                        color="success",
                                                                                                        size="sm",
                                                                                                    ),
                                                                                                ],
                                                                                                size="sm",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        md=4,
                                                                                    ),
                                                                                ],
                                                                                className="mb-2",
                                                                            ),
                                                                            html.Div(
                                                                                id="preset-feedback",
                                                                                className="mb-2 small",
                                                                            ),
                                                                            html.Hr(
                                                                                className="my-2"
                                                                            ),
                                                                            html.H6(
                                                                                "Matching Covariates",
                                                                                className="mb-2 fw-semibold",
                                                                            ),
                                                                            dbc.Card(
                                                                                dbc.CardBody(
                                                                                    dbc.Checklist(
                                                                                        id="covariate-selection",
                                                                                        options=[],
                                                                                        value=[],
                                                                                        inline=False,
                                                                                        className="ms-2",
                                                                                    ),
                                                                                    className="ae-scroll-panel",
                                                                                ),
                                                                                className="mb-3",
                                                                            ),
                                                                            html.Hr(
                                                                                className="my-2"
                                                                            ),
                                                                            html.H6(
                                                                                "Exact Match Variables",
                                                                                className="mb-1 fw-semibold",
                                                                            ),
                                                                            html.Small(
                                                                                "At least one must be selected. Controls are drawn only from areas sharing these attributes with treatment sites.",
                                                                                className="text-muted d-block mb-2",
                                                                            ),
                                                                            dbc.Checklist(
                                                                                id="exact-match-selection",
                                                                                options=EXACT_MATCH_OPTIONS,
                                                                                value=rc.get(
                                                                                    "exact_match_vars",
                                                                                    DEFAULT_EXACT_MATCH,
                                                                                ),
                                                                                inline=False,
                                                                                className="ms-2",
                                                                            ),
                                                                        ]
                                                                    ),
                                                                ],
                                                                className="ae-section-card",
                                                            ),
                                                        ],
                                                        xs=12,
                                                        lg=6,
                                                    ),
                                                    dbc.Col(
                                                        [
                                                            dbc.Card(
                                                                [
                                                                    dbc.CardHeader(
                                                                        "Other Matching Settings"
                                                                    ),
                                                                    dbc.CardBody(
                                                                        [
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Select(
                                                                                                id="settings-preset-selector",
                                                                                                placeholder="Load a saved preset…",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        md=5,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Button(
                                                                                                "Load",
                                                                                                id="load-settings-preset-btn",
                                                                                                color="primary",
                                                                                                size="sm",
                                                                                                className="me-1",
                                                                                            ),
                                                                                            dbc.Button(
                                                                                                "Delete",
                                                                                                id="delete-settings-preset-btn",
                                                                                                color="danger",
                                                                                                outline=True,
                                                                                                size="sm",
                                                                                            ),
                                                                                        ],
                                                                                        xs="auto",
                                                                                        md=3,
                                                                                        className="d-flex align-items-center",
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.InputGroup(
                                                                                                [
                                                                                                    dbc.Input(
                                                                                                        id="settings-preset-name-input",
                                                                                                        type="text",
                                                                                                        placeholder="Preset name",
                                                                                                        size="sm",
                                                                                                    ),
                                                                                                    dbc.Button(
                                                                                                        "Save",
                                                                                                        id="save-settings-preset-btn",
                                                                                                        color="success",
                                                                                                        size="sm",
                                                                                                    ),
                                                                                                ],
                                                                                                size="sm",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        md=4,
                                                                                    ),
                                                                                ],
                                                                                className="mb-2",
                                                                            ),
                                                                            html.Div(
                                                                                id="settings-preset-feedback",
                                                                                className="mb-2 small",
                                                                            ),
                                                                            html.Hr(
                                                                                className="my-2"
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Max treatment pixels"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="max-treatment-pixels",
                                                                                                type="number",
                                                                                                min=1,
                                                                                                max=100000,
                                                                                                step=1,
                                                                                                value=rc.get(
                                                                                                    "max_treatment_pixels",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "max_treatment_pixels"
                                                                                                    ],
                                                                                                ),
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Maximum treatment pixels sampled per group/site.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Control multiplier"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="control-multiplier",
                                                                                                type="number",
                                                                                                min=1,
                                                                                                max=500,
                                                                                                step=1,
                                                                                                value=rc.get(
                                                                                                    "control_multiplier",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "control_multiplier"
                                                                                                    ],
                                                                                                ),
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Maximum controls sampled per treatment pixel.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mb-3",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Minimum site area (ha)"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="min-site-area-ha",
                                                                                                type="number",
                                                                                                min=0,
                                                                                                max=100000,
                                                                                                step=1,
                                                                                                value=rc.get(
                                                                                                    "min_site_area_ha",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "min_site_area_ha"
                                                                                                    ],
                                                                                                ),
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Sites smaller than this are filtered before extraction.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Min GLM treatment pixels"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="min-glm-treatment-pixels",
                                                                                                type="number",
                                                                                                min=1,
                                                                                                max=10000,
                                                                                                step=1,
                                                                                                value=rc.get(
                                                                                                    "min_glm_treatment_pixels",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "min_glm_treatment_pixels"
                                                                                                    ],
                                                                                                ),
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Below this, matching uses Mahalanobis distance instead of GLM.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Caliper width"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="caliper-width",
                                                                                                type="number",
                                                                                                min=0,
                                                                                                max=5.0,
                                                                                                step=0.05,
                                                                                                value=rc.get(
                                                                                                    "caliper_width",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "caliper_width"
                                                                                                    ],
                                                                                                ),
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Maximum distance (in SD) for a valid match. "
                                                                                                "Tighter values improve balance but reduce matched pairs. "
                                                                                                "Set to 0 to disable the caliper.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Max controls per treatment pixel"
                                                                                            ),
                                                                                            dcc.Dropdown(
                                                                                                id="max-controls-per-treatment",
                                                                                                options=[
                                                                                                    {
                                                                                                        "label": "1 (pair matching)",
                                                                                                        "value": 1,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "3",
                                                                                                        "value": 3,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "5",
                                                                                                        "value": 5,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "No limit (full matching)",
                                                                                                        "value": 0,
                                                                                                    },
                                                                                                ],
                                                                                                value=rc.get(
                                                                                                    "max_controls_per_treatment",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "max_controls_per_treatment"
                                                                                                    ],
                                                                                                ),
                                                                                                clearable=False,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "More controls per treatment reduces variance "
                                                                                                "but may increase bias. Controls are weighted "
                                                                                                "inversely by group size.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mt-1",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Min control distance (km)"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="min-control-distance-km",
                                                                                                type="number",
                                                                                                min=0,
                                                                                                max=500,
                                                                                                step=1,
                                                                                                value=rc.get(
                                                                                                    "min_control_distance_km",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "min_control_distance_km"
                                                                                                    ],
                                                                                                ),
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Controls closer than this to treatment polygons "
                                                                                                "are excluded. Set to 0 to disable.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Matching method"
                                                                                            ),
                                                                                            dcc.Dropdown(
                                                                                                id="matching-method",
                                                                                                options=[
                                                                                                    {
                                                                                                        "label": "Optimal (optmatch)",
                                                                                                        "value": "optimal",
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "Nearest neighbour (MatchIt, faster)",
                                                                                                        "value": "nearest",
                                                                                                    },
                                                                                                ],
                                                                                                value=rc.get(
                                                                                                    "matching_method",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "matching_method"
                                                                                                    ],
                                                                                                ),
                                                                                                clearable=False,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Optimal uses optmatch for globally optimal matches "
                                                                                                "(slower). Nearest uses MatchIt greedy nearest-neighbour "
                                                                                                "(faster, comparable balance).",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mt-1",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Checkbox(
                                                                                                id="separation-fallback-mahalanobis",
                                                                                                label="Use Mahalanobis distance when GLM separation detected",
                                                                                                value=rc.get(
                                                                                                    "separation_fallback_mahalanobis",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "separation_fallback_mahalanobis"
                                                                                                    ],
                                                                                                ),
                                                                                            ),
                                                                                            html.Small(
                                                                                                "When covariates perfectly separate treatment/control groups, "
                                                                                                "the GLM cannot converge. Enable this to fall back to "
                                                                                                "Mahalanobis distance matching for those groups instead of "
                                                                                                "failing the site.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mt-1",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Checkbox(
                                                                                                id="group-by-exact-matches",
                                                                                                label="Group sites by exact-match regions (cross-site grouping)",
                                                                                                value=rc.get(
                                                                                                    "group_by_exact_matches",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "group_by_exact_matches"
                                                                                                    ],
                                                                                                ),
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Build a separate propensity score model for each unique combination "
                                                                                                "of exact-match values across all sites. Sites spanning multiple "
                                                                                                "exact-match regions are automatically split into sub-polygons. "
                                                                                                "Useful when sites share exact-match regions and have few pixels — "
                                                                                                "pooling improves model quality and reduces computation time.",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mt-1",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Number of replicates"
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="n-replicates",
                                                                                                type="number",
                                                                                                min=1,
                                                                                                max=1000,
                                                                                                step=1,
                                                                                                value=rc.get(
                                                                                                    "n_replicates",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "n_replicates"
                                                                                                    ],
                                                                                                ),
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Run matching multiple times with different random "
                                                                                                "samples to construct confidence intervals around "
                                                                                                "deforestation and emissions estimates. "
                                                                                                "Set to 1 for a single run (no CIs).",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            html.Div(
                                                                                                [
                                                                                                    dbc.Label(
                                                                                                        "Random seed (optional)",
                                                                                                        className="me-2 mb-0",
                                                                                                    ),
                                                                                                    dbc.Button(
                                                                                                        "\u21bb",
                                                                                                        id="refresh-random-seed",
                                                                                                        size="sm",
                                                                                                        color="link",
                                                                                                        title="Generate new random seed",
                                                                                                        className="p-0 ms-1",
                                                                                                        style={
                                                                                                            "fontSize": "1.1rem",
                                                                                                            "lineHeight": "1",
                                                                                                        },
                                                                                                    ),
                                                                                                ],
                                                                                                className="d-flex align-items-center",
                                                                                            ),
                                                                                            dbc.Input(
                                                                                                id="random-seed",
                                                                                                type="number",
                                                                                                min=1,
                                                                                                max=2147483647,
                                                                                                step=1,
                                                                                                value=default_random_seed,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Prefilled with a random value. ",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mt-1",
                                                                            ),
                                                                            dbc.Row(
                                                                                [
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Matching memory (GB)"
                                                                                            ),
                                                                                            dcc.Dropdown(
                                                                                                id="match-memory-gb",
                                                                                                options=[
                                                                                                    {
                                                                                                        "label": "30 GB (default)",
                                                                                                        "value": 30,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "60 GB",
                                                                                                        "value": 60,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "120 GB",
                                                                                                        "value": 120,
                                                                                                    },
                                                                                                    {
                                                                                                        "label": "240 GB",
                                                                                                        "value": 240,
                                                                                                    },
                                                                                                ],
                                                                                                value=rc.get(
                                                                                                    "match_memory_gb",
                                                                                                    ANALYSIS_DEFAULTS[
                                                                                                        "match_memory_gb"
                                                                                                    ],
                                                                                                ),
                                                                                                clearable=False,
                                                                                            ),
                                                                                            html.Small(
                                                                                                "Increase if matching jobs fail with exit code -9 (out of memory). "
                                                                                                "Costs increase if more memory is requested",
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                    dbc.Col(
                                                                                        [
                                                                                            dbc.Label(
                                                                                                "Batch job queue"
                                                                                            ),
                                                                                            dcc.Dropdown(
                                                                                                id="matching-job-queue",
                                                                                                options=MATCHING_JOB_QUEUE_OPTIONS,
                                                                                                value=rc.get(
                                                                                                    "matching_job_queue",
                                                                                                    DEFAULT_MATCHING_JOB_QUEUE,
                                                                                                ),
                                                                                                clearable=False,
                                                                                            ),
                                                                                            html.Small(
                                                                                                [
                                                                                                    "Use ",
                                                                                                    html.Code(
                                                                                                        "ae-ondemand-gp3"
                                                                                                    ),
                                                                                                    " only when needed — it incurs much higher costs.",
                                                                                                ],
                                                                                                className="text-muted",
                                                                                            ),
                                                                                        ],
                                                                                        xs=12,
                                                                                        sm=6,
                                                                                    ),
                                                                                ],
                                                                                className="g-3 mt-1",
                                                                            ),
                                                                        ]
                                                                    ),
                                                                ],
                                                                className="ae-section-card",
                                                            ),
                                                        ],
                                                        xs=12,
                                                        lg=6,
                                                    ),
                                                ],
                                                className="g-3",
                                            ),
                                        ],
                                        className="pt-3",
                                    )
                                ],
                            ),
                            dbc.Tab(
                                label="3. Review & Submit",
                                tab_id="tab-submit-review",
                                children=[
                                    html.Div(
                                        [
                                            html.Div(
                                                id="review-summary",
                                                className="mb-3",
                                            ),
                                            dbc.Card(
                                                [
                                                    dbc.CardBody(
                                                        [
                                                            dcc.Loading(
                                                                dbc.Button(
                                                                    "Submit Task",
                                                                    id="submit-task-button",
                                                                    color="primary",
                                                                    size="lg",
                                                                    className="w-100",
                                                                ),
                                                                type="circle",
                                                            ),
                                                            html.Div(
                                                                id="submit-progress-message",
                                                                className="mt-2",
                                                            ),
                                                        ]
                                                    )
                                                ],
                                                className="ae-section-card",
                                            ),
                                        ],
                                        className="pt-3",
                                    )
                                ],
                            ),
                        ],
                        id="submit-tabs",
                        active_tab="tab-submit-sites",
                        className="ae-content-tabs",
                    ),
                ]
            ),
            # Hidden stores
            dcc.Store(id="parsed-sites-store"),
            dcc.Store(id="presets-store"),
            dcc.Store(id="matching-settings-presets-store"),
            dcc.Store(id="site-set-refresh-store"),
            dcc.Store(id="site-upload-columns-store"),
            dcc.Store(id="submit-lock-store", data=False),
            dcc.Store(id="recompute-config-store", data=rc or None),
        ]
    )
