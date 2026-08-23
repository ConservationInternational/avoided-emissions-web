"""Dash application layout definitions.

Defines the page layouts for login, dashboard, task submission, task detail,
admin panel, and navigation components. Uses AG Grid for sortable/filterable
tables following the same patterns as the trends.earth-api-ui.
"""

import dash_ag_grid as dag
import dash_bootstrap_components as dbc
from dash import html

from config import Config

# Default covariates for the matching formula
DEFAULT_COVARIATES = [
    "precip",
    "temp",
    "elev",
    "slope",
    "dist_cities",
    "friction_surface",
    "pop_2015",
    "pop_growth",
    "total_biomass_2025",
]

# All available covariates (matching + additional options)
ALL_COVARIATES = DEFAULT_COVARIATES + [
    "sdg_baseline",
    "sdg_status_2019",
    "sdg_status_2023",
    "pop_2000",
    "pop_2005",
    "pop_2010",
    "pop_2020",
    "cropland_2003",
    "cropland_2007",
    "cropland_2011",
    "cropland_2015",
    "cropland_2019",
    "ecoregion",
    "pa",
]

# Exact match variables — at least one must be selected for each task.
# Names must match the output_name values in webapp/rasterize_vectors.py.
EXACT_MATCH_OPTIONS = [
    {"label": "Admin 0 (Country)", "value": "admin0"},
    {"label": "Admin 1 (Province / State)", "value": "admin1"},
    {"label": "Admin 2 (District)", "value": "admin2"},
    {"label": "Ecoregion", "value": "ecoregion"},
    {"label": "Protected Area (WDPA)", "value": "pa"},
]

DEFAULT_EXACT_MATCH = ["admin1", "ecoregion", "pa"]

# Variables that can be used as *either* exact match *or* covariates
# (but never both simultaneously).  When used as a covariate they are
# included in the propensity score formula; when used as an exact match
# they define stratification groups.
DUAL_PURPOSE_VARS = ["ecoregion", "pa"]

MATCHING_JOB_QUEUE_OPTIONS = [
    {
        "label": "ae-spot-gp3 (default)",
        "value": "ae-spot-gp3",
    },
    {
        "label": "ae-ondemand-gp3",
        "value": "ae-ondemand-gp3",
    },
]

# -- Column definitions (AG Grid) -------------------------------------------

TRUNCATED_CELL = {
    "whiteSpace": "nowrap",
    "overflow": "hidden",
    "textOverflow": "ellipsis",
}

TASK_LIST_COLUMNS = [
    {
        "headerName": "Name",
        "field": "name",
        "flex": 2,
        "minWidth": 200,
        "pinned": "left",
        "cellStyle": {**TRUNCATED_CELL, "cursor": "pointer"},
        "tooltipField": "name",
        "filter": "agTextColumnFilter",
        "sortable": True,
        "cellRenderer": "TaskLink",
    },
    {
        "headerName": "Status",
        "field": "status",
        "flex": 1,
        "minWidth": 110,
        "cellStyle": {"fontSize": "12px"},
        "filter": "agTextColumnFilter",
        "sortable": True,
        "filterParams": {
            "buttons": ["clear", "apply"],
            "closeOnApply": True,
        },
        "cellRenderer": "StatusBadge",
    },
    {
        "headerName": "Sites",
        "field": "n_sites",
        "flex": 0.6,
        "minWidth": 80,
        "filter": "agNumberColumnFilter",
        "sortable": True,
    },
    {
        "headerName": "Covariates",
        "field": "covariates_short",
        "flex": 1.2,
        "minWidth": 140,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "filter": "agTextColumnFilter",
        "sortable": True,
        "cellRenderer": "TruncatedList",
        "tooltipField": "covariates_full",
    },
    {
        "headerName": "Exact Matches",
        "field": "exact_matches_short",
        "flex": 1,
        "minWidth": 120,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "filter": "agTextColumnFilter",
        "sortable": True,
        "cellRenderer": "TruncatedList",
        "tooltipField": "exact_matches_full",
    },
    {
        "headerName": "Max Tx Px",
        "field": "max_treatment_pixels",
        "flex": 0.6,
        "minWidth": 80,
        "filter": "agNumberColumnFilter",
        "sortable": True,
        "headerTooltip": "Max treatment pixels",
    },
    {
        "headerName": "Ctrl Mult",
        "field": "control_multiplier",
        "flex": 0.5,
        "minWidth": 70,
        "filter": "agNumberColumnFilter",
        "sortable": True,
        "headerTooltip": "Control multiplier",
    },
    {
        "headerName": "Caliper",
        "field": "caliper_width",
        "flex": 0.5,
        "minWidth": 70,
        "filter": "agNumberColumnFilter",
        "sortable": True,
        "headerTooltip": "Caliper width",
    },
    {
        "headerName": "Max Ctrl/Tx",
        "field": "max_controls_per_treatment",
        "flex": 0.6,
        "minWidth": 80,
        "filter": "agNumberColumnFilter",
        "sortable": True,
        "headerTooltip": "Max controls per treatment",
    },
    {
        "headerName": "Method",
        "field": "matching_method",
        "flex": 0.7,
        "minWidth": 90,
        "filter": "agTextColumnFilter",
        "sortable": True,
        "headerTooltip": "Matching method (optimal or nearest)",
        "filterParams": {
            "buttons": ["clear", "apply"],
            "closeOnApply": True,
        },
    },
    {
        "headerName": "Created",
        "field": "created_at",
        "flex": 1.5,
        "minWidth": 160,
        "sort": "desc",
        "sortIndex": 0,
        "sortable": True,
        "filter": "agTextColumnFilter",
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Submitted",
        "field": "submitted_at",
        "flex": 1.5,
        "minWidth": 160,
        "sortable": True,
        "filter": "agTextColumnFilter",
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Completed",
        "field": "completed_at",
        "flex": 1.5,
        "minWidth": 160,
        "sortable": True,
        "filter": "agTextColumnFilter",
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Actions",
        "field": "actions",
        "flex": 0.8,
        "minWidth": 100,
        "cellRenderer": "TaskActions",
        "sortable": False,
        "filter": False,
        "pinned": "right",
    },
]

COVARIATE_COLUMNS = [
    {
        "headerName": "Covariate",
        "field": "covariate_name",
        "checkboxSelection": True,
        "headerCheckboxSelection": True,
        "headerCheckboxSelectionFilteredOnly": True,
        "flex": 2,
        "minWidth": 200,
        "pinned": "left",
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "description",
    },
    {
        "headerName": "Category",
        "field": "category",
        "flex": 1.2,
        "minWidth": 120,
        "filter": "agTextColumnFilter",
    },
    {
        "headerName": "Resolution",
        "field": "resolution",
        "flex": 0.7,
        "minWidth": 80,
        "filter": "agTextColumnFilter",
    },
    {
        "headerName": "Status",
        "field": "status",
        "flex": 1,
        "minWidth": 110,
        "cellRenderer": "StatusBadge",
        "filter": "agTextColumnFilter",
    },
    {
        "headerName": "GEE Task ID",
        "field": "gee_task_id",
        "flex": 1.5,
        "minWidth": 150,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "11px"},
        "tooltipField": "gee_task_id",
    },
    {
        "headerName": "GCS Tiles",
        "field": "gcs_tiles",
        "flex": 0.7,
        "minWidth": 85,
        "cellRenderer": "TileCount",
    },
    {
        "headerName": "On S3",
        "field": "on_s3",
        "flex": 0.5,
        "minWidth": 65,
        "cellRenderer": "S3Status",
    },
    {
        "headerName": "Size (MB)",
        "field": "size_mb",
        "flex": 0.8,
        "minWidth": 90,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {
            "function": "params.value ? d3.format(',.1f')(params.value) : ''"
        },
        "type": "numericColumn",
    },
    {
        "headerName": "Merged URL",
        "field": "merged_url",
        "flex": 2.5,
        "minWidth": 250,
        "cellRenderer": "CogLink",
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "11px"},
        "tooltipField": "merged_url",
    },
    {
        "headerName": "Error",
        "field": "error_message",
        "flex": 2,
        "minWidth": 200,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "11px", "color": "#721C24"},
        "tooltipField": "error_message",
    },
    {
        "headerName": "Actions",
        "field": "actions",
        "flex": 1.5,
        "minWidth": 170,
        "cellRenderer": "CovariateActions",
        "sortable": False,
        "filter": False,
        "pinned": "right",
    },
]

RESULTS_TOTAL_COLUMNS = [
    {
        "headerName": "Site ID",
        "field": "site_id",
        "flex": 1,
        "minWidth": 120,
        "pinned": "left",
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "site_id",
    },
    {
        "headerName": "Name",
        "field": "site_name",
        "flex": 1.5,
        "minWidth": 150,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "site_name",
    },
    {
        "headerName": "Emissions Avoided (MgCO₂e)",
        "field": "emissions_avoided_mgco2e",
        "flex": 1.5,
        "minWidth": 180,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
        "sort": "desc",
        "sortIndex": 0,
    },
    {
        "headerName": "Forest Loss Avoided (ha)",
        "field": "forest_loss_avoided_ha",
        "flex": 1.5,
        "minWidth": 170,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Area (ha)",
        "field": "area_ha",
        "flex": 1,
        "minWidth": 110,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.0f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Period",
        "field": "period",
        "flex": 1,
        "minWidth": 110,
    },
    {
        "headerName": "Sampled %",
        "field": "sampled_percent",
        "flex": 0.8,
        "minWidth": 100,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format('.1f')(params.value)"},
        "type": "numericColumn",
    },
]

RESULTS_YEARLY_COLUMNS = [
    {
        "headerName": "Site ID",
        "field": "site_id",
        "flex": 1,
        "minWidth": 120,
        "pinned": "left",
        "cellStyle": {**TRUNCATED_CELL},
    },
    {
        "headerName": "Year",
        "field": "year",
        "flex": 0.6,
        "minWidth": 80,
        "filter": "agNumberColumnFilter",
        "sort": "asc",
        "sortIndex": 0,
    },
    {
        "headerName": "Site Defor. (ha)",
        "field": "treatment_defor_ha",
        "flex": 1.2,
        "minWidth": 140,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Control Defor. (ha)",
        "field": "control_defor_ha",
        "flex": 1.2,
        "minWidth": 150,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Emissions Avoided (MgCO₂e)",
        "field": "emissions_avoided_mgco2e",
        "flex": 1.5,
        "minWidth": 180,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Forest Loss Avoided (ha)",
        "field": "forest_loss_avoided_ha",
        "flex": 1.5,
        "minWidth": 170,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',.1f')(params.value)"},
        "type": "numericColumn",
    },
    {
        "headerName": "Matched Pixels",
        "field": "n_matched_pixels",
        "flex": 1,
        "minWidth": 120,
        "filter": "agNumberColumnFilter",
        "valueFormatter": {"function": "d3.format(',')(params.value)"},
        "type": "numericColumn",
    },
]

USER_MANAGEMENT_COLUMNS = [
    {
        "headerName": "Name",
        "field": "name",
        "flex": 1.5,
        "minWidth": 150,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "name",
    },
    {
        "headerName": "Email",
        "field": "email",
        "flex": 2,
        "minWidth": 200,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "email",
    },
    {
        "headerName": "Role",
        "field": "role",
        "flex": 0.8,
        "minWidth": 90,
        "filter": "agTextColumnFilter",
    },
    {
        "headerName": "Approved",
        "field": "is_approved",
        "flex": 0.7,
        "minWidth": 90,
        "cellRenderer": "ApprovalBadge",
        "filter": "agTextColumnFilter",
    },
    {
        "headerName": "Created",
        "field": "created_at",
        "flex": 1.5,
        "minWidth": 160,
        "sort": "desc",
        "sortIndex": 0,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Last Login",
        "field": "last_login",
        "flex": 1.5,
        "minWidth": 160,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Active",
        "field": "is_active",
        "flex": 0.6,
        "minWidth": 80,
    },
]

USER_SITE_SET_COLUMNS = [
    {
        "headerName": "Name",
        "field": "name",
        "flex": 1.6,
        "minWidth": 180,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "name",
    },
    {
        "headerName": "Source File",
        "field": "filename",
        "flex": 1.8,
        "minWidth": 200,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "filename",
    },
    {
        "headerName": "Sites",
        "field": "n_sites",
        "flex": 0.7,
        "minWidth": 90,
        "type": "numericColumn",
        "filter": "agNumberColumnFilter",
    },
    {
        "headerName": "Skipped",
        "field": "ingest_stats",
        "flex": 0.7,
        "minWidth": 90,
        "type": "numericColumn",
        "filter": "agNumberColumnFilter",
        "valueGetter": {
            "function": "params.data.ingest_stats ? (params.data.ingest_stats.skipped_total || null) : null"
        },
        "valueFormatter": {"function": "params.value != null ? params.value : ''"},
        "tooltipValueGetter": {
            "function": "params.data.ingest_stats && params.data.ingest_stats.skipped_total ? 'Missing start date: ' + (params.data.ingest_stats.skipped_missing_required || 0) + ', Bad start date: ' + (params.data.ingest_stats.skipped_bad_start_date || 0) + ', Bad geometry: ' + (params.data.ingest_stats.skipped_bad_geometry || 0) : null"
        },
    },
    {
        "headerName": "Format",
        "field": "file_format",
        "flex": 0.7,
        "minWidth": 90,
    },
    {
        "headerName": "Uploaded",
        "field": "uploaded_at",
        "flex": 1.3,
        "minWidth": 170,
        "sort": "desc",
        "sortIndex": 0,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Archived",
        "field": "is_archived",
        "flex": 0.8,
        "minWidth": 100,
        "valueFormatter": {"function": "params.value ? 'Yes' : 'No'"},
    },
    {
        "headerName": "Actions",
        "field": "id",
        "flex": 1.2,
        "minWidth": 230,
        "sortable": False,
        "filter": False,
        "pinned": "right",
        "cellRenderer": "SiteSetActions",
    },
]

USER_SITE_UPLOAD_COLUMNS = [
    {
        "headerName": "Source File",
        "field": "filename",
        "flex": 1.7,
        "minWidth": 200,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "filename",
    },
    {
        "headerName": "Status",
        "field": "status",
        "flex": 0.9,
        "minWidth": 110,
        "cellRenderer": "StatusBadge",
    },
    {
        "headerName": "Detected Features",
        "field": "n_features",
        "flex": 0.9,
        "minWidth": 130,
        "type": "numericColumn",
        "filter": "agNumberColumnFilter",
    },
    {
        "headerName": "Imported Sites",
        "field": "n_sites_imported",
        "flex": 0.9,
        "minWidth": 130,
        "type": "numericColumn",
        "filter": "agNumberColumnFilter",
    },
    {
        "headerName": "Skipped",
        "field": "ingest_stats",
        "flex": 0.7,
        "minWidth": 90,
        "type": "numericColumn",
        "filter": "agNumberColumnFilter",
        "valueGetter": {
            "function": "params.data.ingest_stats ? (params.data.ingest_stats.skipped_total || null) : null"
        },
        "valueFormatter": {"function": "params.value != null ? params.value : ''"},
        "tooltipValueGetter": {
            "function": "params.data.ingest_stats && params.data.ingest_stats.skipped_total ? 'Missing start date: ' + (params.data.ingest_stats.skipped_missing_required || 0) + ', Bad start date: ' + (params.data.ingest_stats.skipped_bad_start_date || 0) + ', Bad geometry: ' + (params.data.ingest_stats.skipped_bad_geometry || 0) : null"
        },
    },
    {
        "headerName": "Site Set",
        "field": "site_set_name",
        "flex": 1.4,
        "minWidth": 180,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "site_set_name",
    },
    {
        "headerName": "Queued",
        "field": "created_at",
        "flex": 1.2,
        "minWidth": 165,
        "sort": "desc",
        "sortIndex": 0,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Started",
        "field": "started_at",
        "flex": 1.2,
        "minWidth": 165,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Completed",
        "field": "completed_at",
        "flex": 1.2,
        "minWidth": 165,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Error",
        "field": "error_message",
        "flex": 1.8,
        "minWidth": 220,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "error_message",
    },
    {
        "headerName": "Actions",
        "field": "id",
        "flex": 0.95,
        "minWidth": 120,
        "sortable": False,
        "filter": False,
        "pinned": "right",
        "cellRenderer": "SiteUploadActions",
    },
]

USER_SITE_UPLOAD_ROW_STYLES = [
    {
        "condition": "params.data.status === 'pending'",
        "style": {"backgroundColor": "#E2E3E5", "color": "#495057"},
    },
    {
        "condition": "params.data.status === 'running'",
        "style": {"backgroundColor": "#CCE5FF", "color": "#084298"},
    },
    {
        "condition": "params.data.status === 'completed'",
        "style": {"backgroundColor": "#D1E7DD", "color": "#0F5132"},
    },
    {
        "condition": "params.data.status === 'failed'",
        "style": {"backgroundColor": "#F8D7DA", "color": "#721C24"},
    },
    {
        "condition": "params.data.status === 'cancelled'",
        "style": {"backgroundColor": "#FFF3CD", "color": "#664D03"},
    },
]

COMBINED_SITE_UPLOAD_COLUMNS = [
    {
        "headerName": "Name",
        "field": "site_set_name",
        "flex": 1.5,
        "minWidth": 160,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "site_set_name",
    },
    {
        "headerName": "Source File",
        "field": "filename",
        "flex": 1.8,
        "minWidth": 200,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "filename",
    },
    {
        "headerName": "Status",
        "field": "status",
        "flex": 0.9,
        "minWidth": 110,
        "cellRenderer": "StatusBadge",
    },
    {
        "headerName": "Sites Imported",
        "field": "n_sites_imported",
        "flex": 0.9,
        "minWidth": 130,
        "type": "numericColumn",
        "filter": "agNumberColumnFilter",
    },
    {
        "headerName": "Skipped",
        "field": "ingest_stats",
        "flex": 0.7,
        "minWidth": 90,
        "type": "numericColumn",
        "filter": "agNumberColumnFilter",
        "valueGetter": {
            "function": "params.data.ingest_stats ? (params.data.ingest_stats.skipped_total || null) : null"
        },
        "valueFormatter": {"function": "params.value != null ? params.value : ''"},
        "tooltipValueGetter": {
            "function": "params.data.ingest_stats && params.data.ingest_stats.skipped_total ? 'Missing start date: ' + (params.data.ingest_stats.skipped_missing_required || 0) + ', Bad start date: ' + (params.data.ingest_stats.skipped_bad_start_date || 0) + ', Bad geometry: ' + (params.data.ingest_stats.skipped_bad_geometry || 0) : null"
        },
    },
    {
        "headerName": "Archived",
        "field": "is_archived",
        "flex": 0.7,
        "minWidth": 90,
        "valueFormatter": {"function": "params.value ? 'Yes' : ''"},
    },
    {
        "headerName": "Queued",
        "field": "created_at",
        "flex": 1.2,
        "minWidth": 165,
        "sort": "desc",
        "sortIndex": 0,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Completed",
        "field": "completed_at",
        "flex": 1.2,
        "minWidth": 165,
        "cellStyle": {**TRUNCATED_CELL, "fontSize": "12px"},
        "cellRenderer": "LocalDateTime",
    },
    {
        "headerName": "Error",
        "field": "error_message",
        "flex": 1.8,
        "minWidth": 220,
        "cellStyle": {**TRUNCATED_CELL},
        "tooltipField": "error_message",
    },
    {
        "headerName": "Actions",
        "field": "id",
        "flex": 1.5,
        "minWidth": 240,
        "sortable": False,
        "filter": False,
        "pinned": "right",
        "cellRenderer": "SiteSetAndUploadActions",
    },
]


# -- AG Grid defaults (mirroring api-ui patterns) ---------------------------

DEFAULT_GRID_OPTIONS = {
    "enableCellTextSelection": True,
    "ensureDomOrder": True,
    "animateRows": False,
    "suppressMenuHide": True,
    "suppressHorizontalScroll": False,
    "alwaysShowHorizontalScroll": True,
    "rowHeight": 32,
    "headerHeight": 32,
}

DEFAULT_COL_DEF = {
    "resizable": True,
    "sortable": True,
    "filter": True,
    "minWidth": 50,
    "suppressSizeToFit": True,
    "wrapText": True,
    "autoHeight": False,
}

TASK_STATUS_ROW_STYLES = [
    {
        "condition": "params.data.status === 'failed'",
        "style": {"backgroundColor": "#F8D7DA", "color": "#721C24"},
    },
    {
        "condition": "params.data.status === 'succeeded'",
        "style": {"backgroundColor": "#D1E7DD", "color": "#0F5132"},
    },
    {
        "condition": "params.data.status === 'running'",
        "style": {"backgroundColor": "#CCE5FF", "color": "#084298"},
    },
    {
        "condition": "params.data.status === 'submitted'",
        "style": {"backgroundColor": "#FFF3CD", "color": "#664D03"},
    },
    {
        "condition": "params.data.status === 'submitting'",
        "style": {"backgroundColor": "#E2E3E5", "color": "#495057"},
    },
    {
        "condition": "params.data.status === 'pending'",
        "style": {"backgroundColor": "#E2E3E5", "color": "#495057"},
    },
]

COVARIATE_STATUS_ROW_STYLES = [
    # Greyed-out: nothing anywhere
    {
        "condition": "(!params.data.gcs_tiles || params.data.gcs_tiles === 0) && !params.data.on_s3 && !params.data.status",
        "style": {"backgroundColor": "#F5F5F5", "color": "#AAAAAA"},
    },
    # Export phase
    {
        "condition": "params.data.status === 'pending_export'",
        "style": {"backgroundColor": "#E2E3E5", "color": "#495057"},
    },
    {
        "condition": "params.data.status === 'exporting'",
        "style": {"backgroundColor": "#CCE5FF", "color": "#084298"},
    },
    {
        "condition": "params.data.status === 'exported'",
        "style": {"backgroundColor": "#FFF3CD", "color": "#664D03"},
    },
    # Merge phase
    {
        "condition": "params.data.status === 'pending_merge'",
        "style": {"backgroundColor": "#E2E3E5", "color": "#495057"},
    },
    {
        "condition": "params.data.status === 'merging'",
        "style": {"backgroundColor": "#CCE5FF", "color": "#084298"},
    },
    # Merged / on S3
    {
        "condition": "params.data.on_s3 && !params.data.status",
        "style": {"backgroundColor": "#D1E7DD", "color": "#0F5132"},
    },
    {
        "condition": "params.data.status === 'merged' && params.data.on_s3",
        "style": {"backgroundColor": "#D1E7DD", "color": "#0F5132"},
    },
    # Failed / cancelled
    {
        "condition": "params.data.status === 'failed'",
        "style": {"backgroundColor": "#F8D7DA", "color": "#721C24"},
    },
    {
        "condition": "params.data.status === 'cancelled'",
        "style": {"backgroundColor": "#F8D7DA", "color": "#721C24"},
    },
]


def _make_ag_grid(
    table_id,
    column_defs,
    *,
    row_model="clientSide",
    height="600px",
    style_conditions=None,
    grid_options_extra=None,
    row_data=None,
):
    """Create an AG Grid component using api-ui conventions.

    Args:
        table_id: HTML id for the grid component.
        column_defs: list of AG-Grid column definitions.
        row_model: 'clientSide' or 'infinite'.
        height: CSS height string.
        style_conditions: optional row-style conditions list.
        grid_options_extra: dict merged into DEFAULT_GRID_OPTIONS.
        row_data: initial row data (clientSide mode only).
    """
    grid_opts = {**DEFAULT_GRID_OPTIONS}
    if grid_options_extra:
        grid_opts.update(grid_options_extra)

    kwargs = {
        "id": table_id,
        "columnDefs": column_defs,
        "defaultColDef": DEFAULT_COL_DEF,
        "rowModelType": row_model,
        "dashGridOptions": grid_opts,
        "style": {"height": height, "width": "100%"},
        "className": "ag-theme-alpine",
    }

    if style_conditions:
        kwargs["getRowStyle"] = {"styleConditions": style_conditions}

    if row_data is not None and row_model == "clientSide":
        kwargs["rowData"] = row_data

    return dag.AgGrid(**kwargs)


# -- Navigation bar ----------------------------------------------------------


def navbar(user=None, active_page=None):
    """Top navigation bar.

    Parameters
    ----------
    active_page : str | None
        One of ``"/submit"``, ``"/"``, ``"/admin"``, ``"/settings"``.
        The matching nav link is rendered bold to indicate the current page.
    """
    nav_items = []
    if user:
        nav_items = [
            dbc.NavItem(
                dbc.NavLink(
                    "Submit Task",
                    href="/submit",
                    active=(active_page == "/submit"),
                    className="fw-bold" if active_page == "/submit" else "",
                )
            ),
            dbc.NavItem(
                dbc.NavLink(
                    "View Tasks",
                    href="/",
                    active=(active_page == "/"),
                    className="fw-bold" if active_page == "/" else "",
                )
            ),
        ]
        nav_items.append(
            dbc.NavItem(
                dbc.NavLink(
                    "Admin",
                    href="/admin",
                    active=(active_page == "/admin"),
                    className="fw-bold" if active_page == "/admin" else "",
                )
            )
        )
        nav_items.append(
            dbc.NavItem(
                dbc.NavLink(
                    "Profile",
                    href="/settings",
                    active=(active_page == "/settings"),
                    className="fw-bold" if active_page == "/settings" else "",
                )
            )
        )

    right_items = []
    if user:
        right_items = [
            dbc.NavItem(dbc.NavLink(user.name, disabled=True, className="text-light")),
            dbc.NavItem(dbc.NavLink("Logout", href="/logout")),
        ]
    else:
        right_items = [
            dbc.NavItem(dbc.NavLink("Login", href="/login")),
            dbc.NavItem(dbc.NavLink("Register", href="/register")),
        ]

    return dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand(
                    [
                        html.Img(
                            src="/assets/CI_Logo.png",
                            height="36",
                            className="me-3",
                            alt="Conservation International",
                        ),
                        html.Span(
                            "Avoided Emissions",
                            style={
                                "borderLeft": "1px solid rgba(255,255,255,0.3)",
                                "paddingLeft": "0.75rem",
                            },
                        ),
                    ],
                    href="/",
                    className="fw-bold d-flex align-items-center",
                ),
                dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                dbc.Collapse(
                    [
                        dbc.Nav(nav_items, className="me-auto", navbar=True),
                        dbc.Nav(right_items, navbar=True),
                    ],
                    id="navbar-collapse",
                    is_open=False,
                    navbar=True,
                ),
            ],
            fluid="lg",
        ),
        color="dark",
        dark=True,
        className="mb-4 ae-navbar shadow-sm",
    )


def footer():
    """Footer bar with legal links — shown on authenticated pages."""
    # Build git version display with link to GitHub commit
    git_sha = Config.GIT_REVISION
    version_display = []
    if git_sha:
        short_sha = git_sha[:7]
        github_url = (
            f"https://github.com/ConservationInternational/avoided-emissions-web"
            f"/commit/{git_sha}"
        )
        version_display = [
            html.Span("·", className="ae-footer-separator"),
            html.A(
                f"Version: {short_sha}",
                href=github_url,
                target="_blank",
                rel="noopener noreferrer",
                className="ae-footer-link",
                style={"fontFamily": "monospace"},
            ),
        ]

    return html.Footer(
        dbc.Container(
            [
                dbc.Row(
                    dbc.Col(
                        html.Div(
                            [
                                html.Span(
                                    "Powered by",
                                    className="ae-footer-powered-text",
                                ),
                                html.Img(
                                    src="/assets/trends_earth_bl_print.png",
                                    alt="Trends.Earth",
                                    className="ae-footer-powered-logo",
                                ),
                            ],
                            className="ae-footer-powered",
                        ),
                        className="text-center",
                    ),
                ),
                dbc.Row(
                    dbc.Col(
                        [
                            html.A(
                                "Privacy Policy",
                                href="https://www.conservation.org/privacy-policy",
                                target="_blank",
                                rel="noopener noreferrer",
                                className="ae-footer-link",
                            ),
                            html.Span("·", className="ae-footer-separator"),
                            html.A(
                                "Terms of Use",
                                href="https://www.conservation.org/terms-of-use",
                                target="_blank",
                                rel="noopener noreferrer",
                                className="ae-footer-link",
                            ),
                            *version_display,
                        ],
                        className="text-center",
                    ),
                ),
            ],
            fluid="lg",
        ),
        className="ae-footer",
    )


# -- Page layouts ------------------------------------------------------------
