"""Accessible, public documentation pages and reusable contextual links."""

import re

import dash_bootstrap_components as dbc
from dash import dcc, html
from documentation import PAGES, SETTING_SECTIONS, VARIABLES, heading_id, load_document

from layouts.common import navbar


def help_link(page: str, section: str = "", label: str = "Learn more"):
    return html.A(
        label,
        href=f"/docs/{page}" + (f"#{section}" if section else ""),
        target="_blank",
        rel="noopener noreferrer",
        className="ae-help-link",
        **{"aria-label": f"{label}: {section or PAGES[page][0]} (opens in a new tab)"},
    )


def setting_help(control_id: str):
    return help_link("setup", SETTING_SECTIONS[control_id])


def setting_label(text: str, control_id: str):
    return html.Div(
        [dbc.Label(text, html_for=control_id), setting_help(control_id)],
        className="d-flex flex-wrap align-items-baseline gap-1",
    )


def variable_links(names):
    return [
        html.Li(
            help_link("variables", name, f"{VARIABLES[name].title} ({name})")
            if name in VARIABLES
            else html.Span(f"{name}: metadata is not documented.")
        )
        for name in names
    ]


def render_markdown(content: str):
    """Give Markdown headings explicit, stable IDs for deep links."""
    blocks = []
    start = 0
    for match in re.finditer(r"^(#{1,3}) (.+)$", content, flags=re.MULTILINE):
        blocks.append(dcc.Markdown(content[start : match.start()], mathjax=True))
        title = match[2].replace("`", "").replace("\\", "")
        heading = {1: html.H1, 2: html.H2, 3: html.H3}[len(match[1])]
        blocks.append(heading(title, id=heading_id(title), tabIndex=-1))
        start = match.end()
    blocks.append(dcc.Markdown(content[start:], mathjax=True))
    return blocks


def variable_cards(query: str = ""):
    terms = (query or "").casefold().split()
    cards = []
    for name, meta in VARIABLES.items():
        searchable = f"{name} {meta.title} {meta.category} {meta.source}".casefold()
        if not all(term in searchable for term in terms):
            continue
        fields = (
            ("Category", meta.category),
            ("Role", meta.role),
            ("Source and citation", html.A(meta.source, href=meta.url)),
            ("Version", meta.version),
            ("Period", meta.period),
            ("Native resolution", meta.native_resolution),
            (
                "Analysis grid",
                (
                    "1 km (30 arc-seconds) or 250 m (7.5 arc-seconds), EPSG:4326. "
                    "Finer output pixels do not imply finer source information."
                ),
            ),
            ("Units and codes", meta.units),
            ("Processing", meta.processing),
            ("Missing values", meta.nodata),
            ("Limitations and timing", meta.limitations),
        )
        cards.append(
            html.Section(
                [
                    html.H2(f"{meta.title} ({name})", id=name, tabIndex=-1),
                    html.Dl(
                        [
                            html.Div([html.Dt(label), html.Dd(value)])
                            for label, value in fields
                        ]
                    ),
                ],
                className="ae-variable-card",
            )
        )
    return [
        html.P(
            f"{len(cards)} variables shown", role="status", **{"aria-live": "polite"}
        ),
        *cards,
    ]


def documentation_layout(user=None, page: str = "overview"):
    content = load_document(page)
    before_catalog, _, after_catalog = content.partition("<!-- variable-catalog -->")
    toc = [
        html.Li(html.A(title, href=f"#{heading_id(title)}"))
        for title in re.findall(r"^## (.+)$", content, flags=re.MULTILINE)
    ]
    return dbc.Container(
        [
            navbar(user, active_page="/docs"),
            html.A(
                "Skip to documentation",
                href="#documentation-content",
                className="ae-skip-link",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        html.Nav(
                            [
                                html.H2("Documentation", className="h5"),
                                dbc.Nav(
                                    [
                                        html.A(
                                            title,
                                            href=f"/docs/{slug}",
                                            className="nav-link active"
                                            if slug == page
                                            else "nav-link",
                                            **{
                                                "aria-current": "page"
                                                if slug == page
                                                else "false"
                                            },
                                        )
                                        for slug, (title, _) in PAGES.items()
                                    ],
                                    vertical=True,
                                    pills=True,
                                ),
                                html.H3("On this page", className="h6 mt-3"),
                                html.Ul(toc),
                            ],
                            className="ae-docs-sidebar",
                            **{"aria-label": "Documentation"},
                        ),
                        xs=12,
                        lg=3,
                    ),
                    dbc.Col(
                        html.Main(
                            [
                                *render_markdown(before_catalog),
                                *(
                                    [
                                        dbc.Label(
                                            "Find a variable by name, category, or source",
                                            html_for="docs-variable-search",
                                        ),
                                        dbc.Input(
                                            id="docs-variable-search",
                                            type="search",
                                            placeholder="For example: precipitation, SDG, or protected",
                                            value="",
                                            className="mb-3",
                                        ),
                                        html.Div(
                                            variable_cards(), id="docs-variable-list"
                                        ),
                                    ]
                                    if page == "variables"
                                    else []
                                ),
                                *(
                                    render_markdown(after_catalog)
                                    if after_catalog
                                    else []
                                ),
                            ],
                            id="documentation-content",
                            className="ae-docs-content",
                            tabIndex=-1,
                        ),
                        xs=12,
                        lg=9,
                    ),
                ],
                className="g-4",
            ),
        ],
        fluid="lg",
        className="pb-4",
    )
