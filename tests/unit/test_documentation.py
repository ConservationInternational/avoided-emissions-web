"""Documentation coverage, deep links, public routing, and contextual help."""

import re
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import unquote, urlsplit

import dash_bootstrap_components as dbc
import pytest
from dash import Dash, dcc, html
from documentation import (
    DOCS_DIR,
    PAGES,
    SETTING_SECTIONS,
    VARIABLES,
    heading_id,
    load_document,
)
from ingest_sdg_cog import SDG_LAYERS
from layouts.common import DEFAULT_COVARIATES, EXACT_MATCH_OPTIONS, navbar
from layouts.documentation import documentation_layout, variable_cards
from layouts.submit import submit_layout
from rasterize_vectors import VECTOR_LAYERS
from services import ANALYSIS_DEFAULTS

from gee_export import gee_config

pytestmark = pytest.mark.unit


def walk(component):
    yield component
    children = getattr(component, "children", None)
    if isinstance(children, (list, tuple)):
        for child in children:
            yield from walk(child)
    elif children is not None:
        yield from walk(children)


def anchors(page):
    return {
        component.id
        for component in walk(documentation_layout(page=page))
        if isinstance(getattr(component, "id", None), str)
    }


def test_metadata_covers_every_supported_layer_with_scientific_fields():
    expected = (
        set(gee_config.COVARIATES)
        | set(SDG_LAYERS)
        | {layer["output_name"] for layer in VECTOR_LAYERS}
        | {"defor_pre_intervention"}
    )
    assert set(VARIABLES) == expected
    for metadata in VARIABLES.values():
        for value in vars(metadata).values():
            assert isinstance(value, str) and value.strip()
        assert metadata.url.startswith("https://")
    assert set(DEFAULT_COVARIATES) <= VARIABLES.keys()
    assert {option["value"] for option in EXACT_MATCH_OPTIONS} <= VARIABLES.keys()


@pytest.mark.parametrize(
    "query, expected",
    [
        ("SDG 2019", {"sdg_status_2019"}),
        ("worldpop 2005", {"pop_2005"}),
        ("protected", {"pa"}),
        ("not-a-real-layer", set()),
        ("", set(VARIABLES)),
    ],
)
def test_metadata_search_matches_names_categories_and_sources(query, expected):
    result = variable_cards(query)
    found = {
        component.id
        for component in walk(html.Div(result))
        if isinstance(component, html.H2)
    }
    assert found == expected
    assert result[0].children == f"{len(expected)} variables shown"
    assert result[0].role == "status"


def test_all_published_links_and_anchors_resolve():
    page_anchors = {page: anchors(page) for page in PAGES}
    for page in PAGES:
        content = load_document(page)
        assert "{{" not in content
        headings = [
            heading_id(title.replace("`", "").replace("\\", ""))
            for title in re.findall(r"^#{1,3} (.+)$", content, re.MULTILINE)
        ]
        assert len(headings) == len(set(headings))
        for href in re.findall(r"\]\(([^)]+)\)", content):
            url = urlsplit(href)
            if url.scheme:
                assert url.scheme == "https"
                continue
            target_page = url.path.removeprefix("/docs/") if url.path else page
            assert target_page in PAGES, (page, href)
            if url.fragment:
                assert unquote(url.fragment) in page_anchors[target_page], (page, href)
    assert set(SETTING_SECTIONS.values()) <= page_anchors["setup"]
    assert set(VARIABLES) <= page_anchors["variables"]


def test_setup_uses_live_defaults_and_every_form_option_has_help():
    user = SimpleNamespace(name="Test user", is_admin=False)
    layout = submit_layout(user, {"random_seed": 123, "max_treatment_pixels": 777})
    components = list(walk(layout))
    fields = {
        component.id: component
        for component in components
        if isinstance(getattr(component, "id", None), str)
    }
    assert fields["random-seed"].value == 123
    assert fields["max-treatment-pixels"].value == 777
    links = [
        component
        for component in components
        if isinstance(component, html.A) and component.href.startswith("/docs/")
    ]
    assert {f"/docs/setup#{section}" for section in SETTING_SECTIONS.values()} <= {
        link.href for link in links
    }
    for link in links:
        assert link.target == "_blank"
        assert link.rel == "noopener noreferrer"
        assert "opens in a new tab" in link.to_plotly_json()["props"]["aria-label"]
    content = load_document("setup")
    assert f"**Default: {ANALYSIS_DEFAULTS['max_treatment_pixels']}." in content
    assert "not confidence intervals" in str(layout)


@pytest.mark.parametrize("user", [None, SimpleNamespace(name="Test user")])
def test_documentation_nav_and_layout_are_accessible_without_task_data(user):
    links = [
        component
        for component in walk(navbar(user, "/docs"))
        if isinstance(component, dbc.NavLink)
    ]
    link = next(
        component for component in links if getattr(component, "href", "") == "/docs"
    )
    assert link.active
    layout = documentation_layout(user)
    assert any(isinstance(component, html.Main) for component in walk(layout))
    assert any(isinstance(component, html.Nav) for component in walk(layout))
    assert not any(isinstance(component, dcc.Store) for component in walk(layout))


def test_search_and_dynamic_selector_help_callbacks(mocker):
    from callbacks._registration import register_callbacks

    app = Dash(__name__, suppress_callback_exceptions=True)
    app.layout = html.Div()
    register_callbacks(app)
    key = next(
        name for name in app.callback_map if "covariate-metadata-links.children" in name
    )
    callback = app.callback_map[key]["callback"].__wrapped__
    covariates, exact = callback(
        [{"label": "Newly ready SDG", "value": "sdg_status_2023"}],
        [{"value": "admin0", "disabled": True}],
    )
    assert covariates[0].children.href == "/docs/variables#sdg_status_2023"
    assert exact[0].children.href == "/docs/variables#admin0"
    key = "docs-variable-list.children"
    search = app.callback_map[key]["callback"].__wrapped__
    assert search("not-a-real-layer")[0].children == "0 variables shown"


def test_public_routing_does_not_bypass_task_auth_or_expose_files(monkeypatch):
    from config import Config

    monkeypatch.setattr(Config, "DEBUG", True)
    import app as application

    monkeypatch.setattr(
        application.flask_login, "current_user", SimpleNamespace(is_authenticated=False)
    )
    with application.server.test_request_context("/docs"):
        for path in ("/docs", "/docs/", *(f"/docs/{page}" for page in PAGES)):
            layout = application.display_page(path, "")
            assert any(isinstance(component, html.Main) for component in walk(layout))
        for path in (
            "/docs/../README.md",
            "/docs/documentation-section-plan",
            "/docs/no-such-page",
        ):
            layout = application.display_page(path, "")
            assert not any(
                isinstance(component, html.Main) for component in walk(layout)
            )
        redirect = application.display_page("/submit", "")
        assert isinstance(redirect, dcc.Location)
        assert redirect.href.startswith("/login?")


def test_image_includes_markdown_and_uses_same_document_root():
    root = Path(__file__).resolve().parents[2]
    assert DOCS_DIR == root / "docs"
    dockerfile = (root / "webapp" / "Dockerfile").read_text()
    assert "COPY docs/ /docs/" in dockerfile
    assert "!docs/*.md" in (root / ".dockerignore").read_text()
