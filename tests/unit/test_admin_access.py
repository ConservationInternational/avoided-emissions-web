"""Access-control tests for the site setup area."""

from types import SimpleNamespace

import dash_bootstrap_components as dbc
import pytest

from layouts.admin import admin_layout
from layouts.common import navbar


def _find_components(component, component_type):
    """Return all descendants of a Dash component matching a type."""
    matches = []
    if isinstance(component, component_type):
        matches.append(component)

    children = getattr(component, "children", None)
    if isinstance(children, (list, tuple)):
        for child in children:
            matches.extend(_find_components(child, component_type))
    elif children is not None:
        matches.extend(_find_components(children, component_type))

    return matches


@pytest.mark.unit
def test_regular_user_sees_site_setup_but_not_privileged_tabs():
    """Regular users can reach site setup without covariate or user controls."""
    user = SimpleNamespace(name="Regular User", is_admin=False)

    nav_links = _find_components(navbar(user), dbc.NavLink)
    admin_link = next(link for link in nav_links if link.href == "/admin")
    assert admin_link.children == "Admin"

    tabs = {tab.tab_id: tab for tab in _find_components(admin_layout(user), dbc.Tab)}
    assert tabs["tab-site-uploads"].tab_style is None
    assert tabs["tab-covariates"].tab_style == {"display": "none"}
    assert tabs["tab-users"].tab_style == {"display": "none"}


@pytest.mark.unit
def test_admin_sees_all_setup_tabs():
    """Administrators retain access to all existing setup tabs."""
    user = SimpleNamespace(name="Administrator", is_admin=True)

    tabs = {tab.tab_id: tab for tab in _find_components(admin_layout(user), dbc.Tab)}
    assert tabs["tab-site-uploads"].tab_style is None
    assert tabs["tab-covariates"].tab_style == {}
    assert tabs["tab-users"].tab_style == {}
