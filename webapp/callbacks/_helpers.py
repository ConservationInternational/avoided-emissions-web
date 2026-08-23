"""Shared helper utilities used across callback modules."""

import json
import traceback
import uuid as _uuid

import dash_bootstrap_components as dbc
from auth import get_current_user
from dash import dcc, html
from models import Covariate, get_db
from services import get_task_detail, validate_share_token


def _is_valid_uuid(value):
    """Return True if *value* is a valid UUID string."""
    try:
        _uuid.UUID(str(value))
        return True
    except (ValueError, AttributeError):
        return False


def _check_task_access(task_id, user):
    """Return True if *user* may access the task identified by *task_id*.

    Admins can view any task; regular users can only view their own.
    Returns False (and logs a warning) for invalid UUIDs or ownership
    violations.
    """
    if not _is_valid_uuid(task_id):
        return False
    detail = get_task_detail(task_id)
    if not detail:
        return False
    if user.is_admin:
        return True
    return str(detail["task"].submitted_by) == str(user.id)


def _authorize_task_access(task_id, share_token=None):
    """Check whether the current request may access *task_id*.

    Supports two authentication modes:
    1. **Authenticated user**: checks user login and ownership/admin.
    2. **Share token**: validates the token and confirms it belongs to
       the requested *task_id*.

    Returns the task_id (str) if access is granted, or ``None``.
    """
    # Mode 1: share token (lightweight check — access was already
    # recorded when the page was loaded via display_page)
    if share_token:
        token_task_id = validate_share_token(share_token, record_access=False)
        if token_task_id and str(token_task_id) == str(task_id):
            return str(task_id)
        return None

    # Mode 2: authenticated user
    user = get_current_user()
    if not user:
        return None
    if not _check_task_access(task_id, user):
        return None
    return str(task_id)


def _render_share_links_list(links, task_id):
    """Build the UI list of existing share links for the share modal."""
    if not links:
        return html.P("No share links yet.", className="text-muted")

    from flask import request as flask_request

    base_url = flask_request.host_url.rstrip("/")

    items = []
    for lnk in links:
        expires = (lnk["expires_at"] or "")[:10]
        badge_color = "success" if lnk["is_valid"] else "secondary"
        badge_text = "Active" if lnk["is_valid"] else "Expired / Revoked"
        share_url = f"{base_url}/shared/{lnk['token']}"
        input_id = f"share-link-{lnk['id']}"

        url_display = (
            dbc.InputGroup(
                [
                    dbc.Input(
                        value=share_url,
                        id=input_id,
                        readonly=True,
                        size="sm",
                    ),
                    dcc.Clipboard(
                        target_id=input_id,
                        className="btn btn-outline-secondary btn-sm",
                        style={"display": "inline-block"},
                    ),
                ],
                size="sm",
                className="flex-grow-1 me-2",
            )
            if lnk["is_valid"]
            else html.Code(
                f"...{lnk['token'][-12:]}",
                className="me-2 text-muted",
            )
        )

        row = html.Div(
            [
                html.Div(
                    [
                        dbc.Badge(badge_text, color=badge_color, className="me-2"),
                        url_display,
                        html.Small(
                            f"Expires {expires} ┬╖ {lnk['access_count']} view(s)",
                            className="text-muted text-nowrap",
                        ),
                    ],
                    className="d-flex align-items-center flex-grow-1",
                ),
                *(
                    [
                        dbc.Button(
                            "Revoke",
                            id={
                                "type": "revoke-share-link",
                                "index": lnk["id"],
                            },
                            color="outline-danger",
                            size="sm",
                            className="ms-2 text-nowrap",
                        )
                    ]
                    if lnk["is_valid"]
                    else []
                ),
            ],
            className="d-flex justify-content-between align-items-center mb-2",
        )
        items.append(row)
    return html.Div(items)


def _fmt_dt(dt):
    """Format a datetime as an ISO 8601 UTC string for client-side
    conversion to the browser's local timezone, or '-' if *None*."""
    if dt is None:
        return "-"
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _record_covariate_action_failure(
    covariate_name, action, user_id, resolution_m=1000
):
    """Create a ``failed`` Covariate record so the table shows the error.

    Called when a reexport/remerge action raises before the GEE task is
    submitted.  Without this, the old DB records are already deleted and
    the table row would show blank status.
    """

    error_msg = (
        f"Action '{action}' failed before the task was submitted to GEE. "
        f"{traceback.format_exc(limit=3)}"
    )

    db = get_db()
    try:
        rec = Covariate(
            covariate_name=covariate_name,
            resolution_m=resolution_m,
            status="failed",
            error_message=error_msg,
            started_by=user_id,
        )
        db.add(rec)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def _openlayers_map_component(
    map_id,
    geojson_text,
    height="260px",
    enable_cog_layers=False,
    cog_filter_covariates=None,
    task_id=None,
    resolution_m=None,
    tile_url=None,
    centroids_url=None,
    bounds=None,
):
    """Build an ``ol-sites-map`` div for the OpenLayers site map.

    When *tile_url* is provided the JS switches to an ``ol.layer.VectorTile``
    rendering layer that fetches MVT tiles directly from the server without any
    Dash callback round-trip on pan/zoom.  *centroids_url* points to the async
    endpoint that populates the hidden companion ``ol.layer.Vector`` used for
    ``_featureBySiteId`` lookups and zoom-to-feature.  *bounds* (a
    ``{west, south, east, north}`` dict or the ``UserSiteSet.bounds`` JSON
    column value) is used for the initial view fit instead of computing it from
    the loaded feature extent.

    *geojson_text* is still accepted; on the results map it carries a small
    centroid+emissions FeatureCollection that the JS uses to build
    ``_emissionsBySiteId`` for colour-coding circles on task-scoped tiles.
    """
    import json as _json

    attrs = {
        "data-geojson": geojson_text or "",
        "data-height": height,
    }
    if enable_cog_layers:
        attrs["data-enable-cog-layers"] = "true"
    if cog_filter_covariates:
        attrs["data-cog-filter"] = ",".join(cog_filter_covariates)
    if task_id:
        attrs["data-task-id"] = str(task_id)
    if resolution_m is not None:
        attrs["data-resolution"] = str(resolution_m)
    if tile_url:
        attrs["data-tile-url"] = tile_url
    if centroids_url:
        attrs["data-centroids-url"] = centroids_url
    if bounds:
        attrs["data-bounds"] = (
            bounds if isinstance(bounds, str) else _json.dumps(bounds)
        )
    return html.Div(
        id=map_id,
        className="ol-sites-map",
        **attrs,
    )


def _normalize_metadata_list(value):
    """Ensure a metadata field is a list of dicts.

    The R analysis script serialises some lists (e.g. ``subsampled_sites``)
    as named R lists which ``jsonlite::write_json(auto_unbox=TRUE)`` emits as
    JSON **objects** (``{"1": {...}, "2": {...}}``).  When Python reads them
    they become dicts whose values are the actual records.  This helper
    converts such dicts to a flat list so callers can always iterate over
    dicts.

    A single-element list may also be unboxed to a flat dict — i.e. the
    record itself rather than a dict-of-dicts.  We detect this by checking
    whether the dict values are themselves dicts (nested) or scalars (flat
    record that should be wrapped).
    """
    if isinstance(value, dict):
        # If every value is a dict, it's a dict-of-dicts (named list).
        # Otherwise it's a single flat record that was auto-unboxed.
        if value and all(isinstance(v, dict) for v in value.values()):
            return list(value.values())
        return [value]
    if isinstance(value, list):
        return value
    return []


def _attach_totals_to_geojson(sites_geojson, totals):
    if not sites_geojson:
        return None

    fc = (
        json.loads(sites_geojson)
        if isinstance(sites_geojson, str)
        else dict(sites_geojson)
    )
    totals_by_site = {t.site_id: t for t in totals or []}
    for feature in fc.get("features", []):
        props = feature.setdefault("properties", {})
        site_id = str(props.get("site_id", ""))
        total = totals_by_site.get(site_id)
        if total:
            props["emissions_avoided_mgco2e"] = (
                total.extrapolated_emissions_avoided_mgco2e or 0
            )
            props["forest_loss_avoided_ha"] = (
                total.extrapolated_forest_loss_avoided_ha or 0
            )
            props["total_area_ha"] = total.area_ha or 0
    return json.dumps(fc)
