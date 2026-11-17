import io
import json
from types import SimpleNamespace

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
import pytest
from botocore.response import StreamingBody
from callbacks import _registration as registration
from callbacks._detail_builders import _add_range_error_bars, _build_plots
from callbacks._helpers import _render_share_links_list
from callbacks._match_quality import _calculate_match_quality_summary
from callbacks._plot_colors import OTHER_SITES_COLOR, TOP_SITE_COLORS
from dash import Dash, dcc, html
from flask import Flask
from layouts.task_detail import task_detail_layout
from plot_selection import parse_plot_selection, plot_selection_query
from services import analysis_task


@pytest.mark.unit
@pytest.mark.parametrize("large_task", [False, True])
@pytest.mark.parametrize("n_sites", [1, 5, 7, 12])
def test_yearly_bars_rank_each_metric_and_combine_remaining_sites(large_task, n_sites):
    rows = []
    for index in range(1, n_sites + 1):
        for year, multiplier in ((2020, 1), (2021, 2)):
            if index == 7 and year == 2020:
                continue
            rows.append(
                {
                    "site_id": f"site-{index:03d}",
                    "year": year,
                    "emissions_avoided_mgco2e": (-10 if index == 1 else index * 10)
                    * multiplier,
                    "forest_loss_avoided_ha": (8 - index) * 10 * multiplier,
                    "treatment_defor_ha": 0,
                    "control_defor_ha": 0,
                }
            )
    agg_yearly = []
    for year in (2020, 2021):
        aggregate = {"year": year, "treatment_defor_ha": 0, "control_defor_ha": 0}
        for metric in ("emissions_avoided_mgco2e", "forest_loss_avoided_ha"):
            aggregate[metric] = sum(row[metric] for row in rows if row["year"] == year)
            aggregate[f"{metric}_ci_lower"] = aggregate[metric] - 3
            aggregate[f"{metric}_ci_upper"] = aggregate[metric] + 4
        aggregate["treatment_defor_ha_ci_lower"] = 0
        agg_yearly.append(aggregate)
    totals = [
        SimpleNamespace(
            site_id=f"site-{index:03d}",
            display_site_id="duplicate" if index in (5, 6) else f"ID-{index}",
            site_name=None,
            area_ha=1000 / index,
            extrapolated_emissions_avoided_mgco2e=0,
            extrapolated_emissions_avoided_mgco2e_ci_lower=None,
            extrapolated_emissions_avoided_mgco2e_ci_upper=None,
            extrapolated_forest_loss_avoided_ha=0,
        )
        for index in range(1, n_sites + 1)
    ]
    results = []
    if not large_task:
        for row in rows:
            values = {
                column.name: row.get(column.name.removeprefix("extrapolated_"))
                for column in analysis_task.TaskResult.__table__.columns
            }
            results.append(SimpleNamespace(**values))
    content = _build_plots(
        results=results if not large_task else None,
        totals=totals,
        agg_yearly=agg_yearly,
        is_large=large_task,
        bar_site_yearly=(
            [
                row
                for row in rows
                if n_sites != 12 or int(row["site_id"][-3:]) not in (6, 7)
            ]
            if large_task
            else None
        ),
    )
    container = next(
        child
        for child in content.children
        if getattr(child, "id", None) == "task-results-plots"
    )
    figures = [
        graph.figure
        for graph in container.children[0].children
        if graph.figure.layout.title.text
        in ("Avoided Emissions by Year", "Forest Loss Avoided by Year")
    ]
    for metric, figure in zip(
        ("emissions_avoided_mgco2e", "forest_loss_avoided_ha"), figures
    ):
        order = (
            {
                1: [1],
                5: [5, 4, 3, 2, 1],
                7: [6, 5, 7, 4, 3],
                12: [12, 11, 10, 9, 8],
            }[n_sites]
            if metric == "emissions_avoided_mgco2e"
            else list(range(1, min(n_sites, 5) + 1))
        )
        top_ids = [f"site-{index:03d}" for index in order]
        labels = {
            total.site_id: (
                f"duplicate [{total.site_id}]"
                if total.display_site_id == "duplicate" and n_sites > 5
                else total.display_site_id
            )
            for total in totals
        }
        bars = [trace for trace in figure.data if trace.type == "bar"]
        assert [trace.name for trace in bars[:5]] == [
            labels[site_id] for site_id in top_ids
        ]
        assert [trace.marker.color for trace in bars[:5]] == list(
            TOP_SITE_COLORS[: min(n_sites, 5)]
        )
        assert len(bars) == min(n_sites, 5) + (n_sites > 5)
        if n_sites > 5:
            assert bars[-1].name == "All other sites"
            assert bars[-1].marker.color == OTHER_SITES_COLOR
            assert list(bars[-1].y) == [
                sum(
                    row[metric]
                    for row in rows
                    if row["site_id"] not in top_ids and row["year"] == year
                )
                for year in (2020, 2021)
            ]
        assert len({trace.name for trace in bars}) == len(bars)
        for trace, site_id in zip(bars, top_ids):
            assert trace.meta == {"site_id": site_id}
            assert list(trace.y) == [
                sum(
                    row[metric]
                    for row in rows
                    if row["site_id"] == site_id and row["year"] == year
                )
                for year in (2020, 2021)
            ]
        assert [sum(trace.y[i] for trace in bars) for i in range(2)] == pytest.approx(
            [row[metric] for row in agg_yearly]
        )
        assert figure.layout.barmode == "relative"
        error = next(trace for trace in figure.data if trace.error_y.visible)
        assert list(error.y) == [row[metric] for row in agg_yearly]
        assert list(error.error_y.array) == [4, 4]
        assert list(error.error_y.arrayminus) == [3, 3]


@pytest.mark.unit
@pytest.mark.parametrize("selection", ["all", "group", "single", "positive_only"])
def test_ranked_emissions_charts_limit_sort_and_link_sites(selection):
    totals = [
        SimpleNamespace(
            site_id=f"canonical-{index:+04d}",
            display_site_id=f"ID {index}",
            site_name=None,
            extrapolated_emissions_avoided_mgco2e=float(index),
        )
        for index in range(-25, 26)
    ]
    if selection == "group":
        totals = [
            total
            for total in totals
            if abs(total.extrapolated_emissions_avoided_mgco2e) < 3
        ]
    elif selection == "single":
        totals = totals[-1:]
    elif selection == "positive_only":
        totals = totals[-2:]
    content = _build_plots(
        results=None,
        totals=totals,
        is_large=True,
        agg_yearly=[
            {
                "year": 2020,
                "treatment_defor_ha": 0,
                "control_defor_ha": 0,
                "emissions_avoided_mgco2e": 0,
                "forest_loss_avoided_ha": 0,
            }
        ],
    )
    container = next(
        child
        for child in content.children
        if getattr(child, "id", None) == "task-results-plots"
    )
    graphs = container.children[0].children
    ranked = [
        graph
        for graph in graphs
        if graph.figure.layout.title.text.startswith("Top 20 Sites")
    ]
    if selection == "single":
        assert not ranked
        return
    assert graphs[1:3] == ranked
    expected_values = {
        "all": [list(range(25, 5, -1)), list(range(-25, -5))],
        "group": [[2, 1], [-2, -1]],
        "positive_only": [[25, 24], []],
    }[selection]
    for graph, values, color in zip(ranked, expected_values, ("#0072B2", "#D55E00")):
        figure = graph.figure
        trace = figure.data[0]
        assert list(trace.y) == values
        assert trace.marker.color == color
        assert list(trace.customdata) == [f"canonical-{value:+04d}" for value in values]
        assert list(figure.layout.xaxis.ticktext) == [f"ID {value}" for value in values]
        assert list(figure.layout.xaxis.tickvals) == list(trace.customdata)
        assert figure.layout.yaxis.title.text == "Emissions Avoided (MgCO₂e)"
        assert graph.className == "site-selectable-plot"
        if not values:
            assert (
                figure.layout.annotations[0].text
                == "No sites with negative avoided emissions."
            )


@pytest.mark.unit
def test_range_error_bars_show_the_full_asymmetric_replicate_range():
    fig = go.Figure()
    _add_range_error_bars(
        fig,
        pd.Series([2020, 2021]),
        pd.Series([11.0, 17.5]),
        pd.Series([8.0, 14.0]),
        pd.Series([13.0, 20.0]),
        color="#0072B2",
        name="Project Site (range across matching replicates)",
    )

    trace = fig.data[0]
    assert trace.error_y.visible is True
    assert list(trace.error_y.array) == pytest.approx([2.0, 2.5])
    assert list(trace.error_y.arrayminus) == pytest.approx([3.0, 3.5])
    assert list(trace.y) == pytest.approx([11.0, 17.5])


@pytest.mark.unit
def test_range_error_bars_skip_years_without_both_bounds():
    fig = go.Figure()
    _add_range_error_bars(
        fig,
        pd.Series([2020, 2021]),
        pd.Series([10.0, 13.0]),
        pd.Series([8.0, None]),
        pd.Series([12.0, 15.0]),
        color="#0072B2",
        name="Project Site",
    )

    assert list(fig.data[0].x) == [2020]


@pytest.mark.unit
def test_avoided_emissions_aggregate_plots_show_replicate_ranges():
    total = SimpleNamespace(
        site_id="site-1",
        display_site_id="site-1",
        site_name="Site 1",
        extrapolated_emissions_avoided_mgco2e=10.0,
        extrapolated_emissions_avoided_mgco2e_ci_lower=8.0,
        extrapolated_emissions_avoided_mgco2e_ci_upper=12.0,
        extrapolated_forest_loss_avoided_ha=0.0,
    )
    aggregate_year = {
        "year": 2020,
        "treatment_defor_ha": 0.0,
        "control_defor_ha": 0.0,
        "treatment_defor_ha_ci_lower": 0.0,
        "treatment_defor_ha_ci_upper": 0.0,
        "cum_treatment_defor_ha": 0.0,
        "cum_control_defor_ha": 0.0,
        "emissions_avoided_mgco2e": 10.0,
        "emissions_avoided_mgco2e_ci_lower": 8.0,
        "emissions_avoided_mgco2e_ci_upper": 12.0,
        "forest_loss_avoided_ha": 0.0,
        "forest_loss_avoided_ha_ci_lower": 0.0,
        "forest_loss_avoided_ha_ci_upper": 0.0,
    }

    content = _build_plots(
        results=[],
        totals=[total],
        agg_yearly=[aggregate_year],
    )
    graphs = content.children[0].children[0].children
    yearly = next(
        graph.figure
        for graph in graphs
        if graph.figure.layout.title.text.startswith("Avoided Emissions by Year")
    )
    per_site = next(
        graph.figure
        for graph in graphs
        if graph.figure.layout.title.text == "Total Avoided Emissions by Site"
    )

    assert any(
        trace.error_y is not None and trace.error_y.visible for trace in yearly.data
    )
    assert any(
        trace.error_y is not None and trace.error_y.visible for trace in per_site.data
    )


@pytest.mark.unit
def test_site_selector_sort_modes_use_display_ids_and_matching_labels():
    totals = [
        SimpleNamespace(site_id=sid, display_site_id=display_id, site_name=name)
        for sid, display_id, name in [
            ("canonical-a", "C", "alpha"),
            ("canonical-b", "A", "Zulu"),
            ("canonical-c", "B", None),
            ("duplicate-a", "D", "Delta"),
            ("duplicate-b", "D", "Delta"),
            ("canonical-d", "E", "E"),
        ]
    ]
    content = _build_plots(
        results=None,
        totals=totals,
        is_large=True,
        task=SimpleNamespace(
            extra_metadata={
                "subsampled_sites": [{"site_id": "canonical-a", "sampled_percent": 25}]
            }
        ),
        agg_yearly=[
            {
                "year": 2020,
                "treatment_defor_ha": 1.0,
                "control_defor_ha": 2.0,
                "cum_treatment_defor_ha": 1.0,
                "cum_control_defor_ha": 2.0,
                "emissions_avoided_mgco2e": 10.0,
                "forest_loss_avoided_ha": 1.0,
            }
        ],
    )
    store = next(
        item
        for item in content.children
        if getattr(item, "id", None) == "site-defor-sort-options"
    )
    assert store.data["by_site"] == [
        {"label": "A (Zulu)", "value": "canonical-b"},
        {"label": "B", "value": "canonical-c"},
        {"label": "C (alpha) (subsampled 25%)", "value": "canonical-a"},
        {"label": "D (Delta) [duplicat]", "value": "duplicate-a"},
        {"label": "D (Delta) [duplicat]", "value": "duplicate-b"},
        {"label": "E", "value": "canonical-d"},
    ]
    assert store.data["by_name"] == [
        {"label": "alpha (C) (subsampled 25%)", "value": "canonical-a"},
        {"label": "B", "value": "canonical-c"},
        {"label": "Delta (D) [duplicat]", "value": "duplicate-a"},
        {"label": "Delta (D) [duplicat]", "value": "duplicate-b"},
        {"label": "E", "value": "canonical-d"},
        {"label": "Zulu (A)", "value": "canonical-b"},
    ]
    controls = next(
        item
        for item in content.children
        if any(
            getattr(child, "id", None) == "site-defor-sort-order"
            for child in (getattr(item, "children", None) or [])
        )
    )
    radio = next(
        child
        for child in controls.children
        if getattr(child, "id", None) == "site-defor-sort-order"
    )
    selector = next(
        item
        for item in content.children
        if getattr(item, "id", None) == "site-defor-selector"
    )
    assert selector.options == store.data[radio.value]


@pytest.mark.unit
@pytest.mark.parametrize("years", [[2020], [2019, 2020, 2021]])
def test_aggregate_and_site_plots_use_unformatted_integer_year_ticks(years, monkeypatch):
    rows = [
        {
            "site_id": "site-1",
            "year": year,
            "is_pre_intervention": False,
            "treatment_defor_ha": 1.0,
            "control_defor_ha": 2.0,
            "cum_treatment_defor_ha": 1.0,
            "cum_control_defor_ha": 2.0,
            "treatment_emissions_mgco2e": 10.0,
            "control_emissions_mgco2e": 20.0,
            "emissions_avoided_mgco2e": 10.0,
            "forest_loss_avoided_ha": 1.0,
        }
        for year in years
    ]
    aggregate = _build_plots(results=None, totals=[], is_large=True, agg_yearly=rows)
    callbacks = {}

    def callback(*args, **kwargs):
        def capture(function):
            callbacks[function.__name__] = function
            return function

        return capture

    registration.register_callbacks(
        SimpleNamespace(
            callback=callback, clientside_callback=lambda *args, **kwargs: None
        )
    )
    monkeypatch.setattr(registration, "_authorize_task_access", lambda *_: True)
    monkeypatch.setattr(
        registration,
        "get_filtered_portfolio_total",
        lambda *_args, **_kwargs: {
            "emissions_avoided_mgco2e": 10.0,
            "forest_loss_avoided_ha": 1.0,
        },
    )
    site = callbacks["update_filtered_aggregate_plots"](
        {"site": "site-1"},
        {},
        "task",
        None,
        {
            "results": rows,
            "summary_totals": {
                "site-1": {
                    "site_id": "site-1",
                    "area_ha": 100.0,
                    "sampled_fraction": 0.5,
                    "site_name": "Site 1",
                }
            },
        },
    )
    for content in (aggregate, site):
        if content is aggregate:
            results_container = next(
                child
                for child in content.children
                if getattr(child, "id", None) == "task-results-plots"
            )
            children = results_container.children[0].children
        else:
            children = content
        graphs = [item for item in children if isinstance(item, dcc.Graph)]
        assert len(graphs) == 4
        for graph in graphs:
            axis = graph.figure.layout.xaxis
            assert axis.tickformat == "d"
            assert axis.dtick == 1
            assert list(graph.figure.data[0].x) == years


@pytest.mark.unit
@pytest.mark.parametrize("large_task", [False, True])
@pytest.mark.parametrize("with_ranges", [False, True])
def test_site_comparison_plots_use_bands_and_avoided_loss_uses_bars(
    monkeypatch, large_task, with_ranges
):
    rows = []
    for year, multiplier in [(2020, 1), (2021, 2)]:
        row = {
            "site_id": "site-1",
            "year": year,
            "is_pre_intervention": year == 2020,
            "treatment_defor_ha": 2 * multiplier,
            "control_defor_ha": 4 * multiplier,
            "cumulative_treatment_defor_ha": 3 * multiplier,
            "cumulative_control_defor_ha": 6 * multiplier,
            "treatment_emissions_mgco2e": 10 * multiplier,
            "control_emissions_mgco2e": 20 * multiplier,
            "forest_loss_avoided_ha": 2 * multiplier,
        }
        if with_ranges:
            for metric in (
                "treatment_defor_ha",
                "control_defor_ha",
                "cumulative_treatment_defor_ha",
                "cumulative_control_defor_ha",
                "treatment_emissions_mgco2e",
                "control_emissions_mgco2e",
                "forest_loss_avoided_ha",
            ):
                row[f"{metric}_ci_lower"] = row[metric] - 1
                row[f"{metric}_ci_upper"] = row[metric] + 2
        rows.append(row)

    callbacks = {}

    def callback(*args, **kwargs):
        def capture(function):
            callbacks[function.__name__] = function
            return function

        return capture

    registration.register_callbacks(
        SimpleNamespace(
            callback=callback, clientside_callback=lambda *args, **kwargs: None
        )
    )
    monkeypatch.setattr(registration, "get_task_site_results", lambda *_: rows)
    monkeypatch.setattr(registration, "_authorize_task_access", lambda *_: True)
    monkeypatch.setattr(
        registration,
        "get_filtered_portfolio_total",
        lambda *_args, **_kwargs: {
            "emissions_avoided_mgco2e": 10.0,
            "emissions_avoided_mgco2e_ci_lower": 8.0,
            "emissions_avoided_mgco2e_ci_upper": 12.0,
            "forest_loss_avoided_ha": 2.0,
            "forest_loss_avoided_ha_ci_lower": 1.0,
            "forest_loss_avoided_ha_ci_upper": 3.0,
        },
    )
    store = {
        "large_task": large_task,
        "results": rows,
        "summary_totals": {
            "site-1": {
                "site_id": "site-1",
                "area_ha": 100.0,
                "sampled_fraction": 0.5,
                "site_name": "Site 1",
            }
        },
        "n_replicates": 2,
    }
    content = callbacks["update_filtered_aggregate_plots"](
        {"site": "site-1"}, {}, "task-1", None, store
    )
    graphs = [item.figure for item in content if isinstance(item, dcc.Graph)]
    aggregate_rows = []
    for row in rows:
        aggregate_row = {
            **row,
            "cum_treatment_defor_ha": row["cumulative_treatment_defor_ha"],
            "cum_control_defor_ha": row["cumulative_control_defor_ha"],
            "emissions_avoided_mgco2e": 10,
        }
        if with_ranges:
            for kind in ("treatment", "control"):
                for bound in ("lower", "upper"):
                    aggregate_row[f"cum_{kind}_defor_ha_ci_{bound}"] = row[
                        f"cumulative_{kind}_defor_ha_ci_{bound}"
                    ]
            aggregate_row["emissions_avoided_mgco2e_ci_lower"] = 8
            aggregate_row["emissions_avoided_mgco2e_ci_upper"] = 12
        aggregate_rows.append(aggregate_row)
    aggregate = _build_plots(
        results=None, totals=[], is_large=True, agg_yearly=aggregate_rows
    )
    results_container = next(
        child
        for child in aggregate.children
        if getattr(child, "id", None) == "task-results-plots"
    )
    aggregate_graphs = [item.figure for item in results_container.children[0].children]

    for fig in aggregate_graphs[:2]:
        assert [trace.line.color for trace in fig.data[:2]] == [
            "#0072B2",
            "#D55E00",
        ]
        if with_ranges:
            bands = [trace for trace in fig.data if trace.fill == "tonexty"]
            assert [trace.fillcolor for trace in bands] == [
                "rgba(0,114,178,0.15)",
                "rgba(213,94,0,0.15)",
            ]

    for fig, metrics in zip(
        graphs[:3],
        [
            ("treatment_defor_ha", "control_defor_ha"),
            ("cumulative_treatment_defor_ha", "cumulative_control_defor_ha"),
            ("treatment_emissions_mgco2e", "control_emissions_mgco2e"),
        ],
    ):
        assert [trace.type for trace in fig.data[:2]] == ["scatter", "scatter"]
        assert [trace.line.color for trace in fig.data[:2]] == [
            "#0072B2",
            "#D55E00",
        ]
        assert all(trace.mode == "lines+markers" for trace in fig.data[:2])
        assert not any(trace.error_y.visible for trace in fig.data)
        bands = [trace for trace in fig.data if trace.fill == "tonexty"]
        assert len(bands) == (2 if with_ranges else 0)
        if with_ranges:
            assert [trace.fillcolor for trace in bands] == [
                "rgba(0,114,178,0.15)",
                "rgba(213,94,0,0.15)",
            ]
            for metric, upper, lower in zip(metrics, fig.data[2::2], bands):
                assert list(upper.y) == [row[f"{metric}_ci_upper"] for row in rows]
                assert list(lower.y) == [row[f"{metric}_ci_lower"] for row in rows]
        assert len(fig.layout.shapes) == 1

    avoided = graphs[3]
    assert avoided.data[0].type == "bar"
    assert any(trace.error_y.visible for trace in avoided.data[1:]) == with_ranges
    for site_fig, aggregate_fig in zip(
        (graphs[0], graphs[1], avoided),
        (aggregate_graphs[0], aggregate_graphs[1], aggregate_graphs[3]),
    ):
        assert site_fig.data[0].type == aggregate_fig.data[0].type


@pytest.mark.unit
def test_selection_controls_and_map_precede_the_single_results_plot():
    content = _build_plots(
        results=None,
        totals=[
            SimpleNamespace(site_id="site-1", display_site_id="1", site_name="Site One")
        ],
        is_large=True,
        agg_yearly=[
            {
                "year": 2020,
                "treatment_defor_ha": 1.0,
                "control_defor_ha": 2.0,
                "cum_treatment_defor_ha": 1.0,
                "cum_control_defor_ha": 2.0,
                "emissions_avoided_mgco2e": 10.0,
                "forest_loss_avoided_ha": 1.0,
            }
        ],
        site_map=dcc.Graph(id="task-sites-map"),
        sites_geojson={
            "features": [
                {
                    "properties": {
                        "site_id": "site-1",
                        "source_attributes": {"region": "North", "count": 0},
                    }
                }
            ]
        },
    )

    children = content.children
    component_ids = [getattr(child, "id", None) for child in children]
    detail_heading = next(
        index
        for index, child in enumerate(children)
        if getattr(child, "children", None) == "Results selection"
    )
    assert detail_heading < component_ids.index("task-sites-map")
    assert component_ids.index("site-filter-attributes") < component_ids.index(
        "task-sites-map"
    )
    attributes = children[component_ids.index("site-filter-attributes")]
    assert attributes.data == {"site-1": {"region": "North", "count": 0}}
    filtered_ids = children[component_ids.index("site-filtered-ids")]
    assert filtered_ids.data is None
    filter_panel = next(
        child
        for child in children
        if isinstance(child, html.Div)
        and any(isinstance(item, dbc.Row) for item in child.children)
    )
    filter_row = filter_panel.children[0]
    assert [column.children[1].id for column in filter_row.children] == [
        "site-filter-field",
        "site-filter-value",
    ]
    assert all(column.md == 6 for column in filter_row.children)
    assert component_ids.index("site-defor-selector") < component_ids.index("task-sites-map")
    assert component_ids.index("clear-plot-selection-btn") < component_ids.index(
        "task-sites-map"
    )
    assert component_ids.index("task-sites-map") < component_ids.index("task-results-plots")
    results_container = children[component_ids.index("task-results-plots")]
    match_quality = results_container.children[1]
    assert match_quality.id == "match-quality-section"
    assert isinstance(match_quality, dbc.Accordion)
    assert match_quality.start_collapsed is True
    assert match_quality.active_item is None
    assert match_quality.children[0].title == "Match Quality"
    assert match_quality.children[0].item_id == "match-quality"
    assert "site-map-selection" in component_ids


@pytest.mark.unit
def test_zero_deforestation_still_has_map_and_selection_controls():
    content = _build_plots(
        results=None,
        totals=[SimpleNamespace(site_id="site-1", display_site_id="1", site_name="Site One")],
        is_large=True,
        agg_yearly=[{
            "year": 2020, "treatment_defor_ha": 0, "control_defor_ha": 0,
            "cum_treatment_defor_ha": 0, "cum_control_defor_ha": 0,
            "emissions_avoided_mgco2e": 0, "forest_loss_avoided_ha": 0,
        }],
        site_map=dcc.Graph(id="task-sites-map"),
        plot_selection={"site": "site-1"},
    )
    components = {getattr(child, "id", None): child for child in content.children}
    assert components["site-defor-selector"].value == "site-1"
    assert "task-sites-map" in components
    assert "task-results-plots" in components


@pytest.mark.unit
def test_source_field_filter_values_and_site_selection():
    callbacks = {}

    def callback(*args, **kwargs):
        def capture(function):
            callbacks[function.__name__] = function
            return function

        return capture

    registration.register_callbacks(
        SimpleNamespace(
            callback=callback, clientside_callback=lambda *args, **kwargs: None
        )
    )
    attrs = {
        "a": {"region": "North", "flag": False},
        "b": {"region": "South", "flag": True},
        "c": {"region": "North"},
    }
    values, disabled = callbacks["update_site_filter_values"]("region", attrs)
    assert values == [
        {"label": "North", "value": '"North"'},
        {"label": "South", "value": '"South"'},
    ]
    assert not disabled
    assert callbacks["filter_site_ids"]({}, attrs) is None
    assert callbacks["filter_site_ids"]({"field": "region", "value": '"North"'}, attrs) == ["a", "c"]
    assert callbacks["filter_site_ids"]({"field": "flag", "value": "false"}, attrs) == ["a"]
    assert callbacks["filter_site_ids"]({"field": "flag", "value": "null"}, attrs) == ["c"]
    assert callbacks["filter_site_ids"]({"field": "unknown", "value": "null"}, attrs) == []


@pytest.mark.unit
def test_group_filter_rebuilds_aggregate_from_selected_results(mocker):
    callbacks = {}

    def callback(*args, **kwargs):
        def capture(function):
            callbacks[function.__name__] = function
            return function

        return capture

    registration.register_callbacks(
        SimpleNamespace(
            callback=callback, clientside_callback=lambda *args, **kwargs: None
        )
    )
    totals = [
        SimpleNamespace(
            site_id=site,
            display_site_id=site,
            site_name=site,
            area_ha=100 if site == "a" else 200,
            sampled_fraction=0.25 if site == "a" else 0.5,
        )
        for site in ("a", "b")
    ]
    task = SimpleNamespace(
        extra_metadata={"n_replicates": 2}, results_s3_uri="s3://bucket/output"
    )
    mocker.patch.object(registration, "_authorize_task_access", return_value=True)
    mocker.patch.object(
        registration,
        "get_task_detail",
        return_value={
            "task": task,
            "totals": totals,
            "results": None,
            "sites": [],
            "agg_yearly": [],
            "is_large": True,
        },
    )
    aggregate = mocker.patch.object(
        registration,
        "get_filtered_portfolio_yearly",
        return_value=[{
            "year": 2020,
            "treatment_defor_ha": 1,
            "control_defor_ha": 3,
            "cum_treatment_defor_ha": 1,
            "cum_control_defor_ha": 3,
            "emissions_avoided_mgco2e": 2,
            "forest_loss_avoided_ha": 2,
        }],
    )
    portfolio_total = mocker.patch.object(
        registration,
        "get_filtered_portfolio_total",
        return_value={
            "emissions_avoided_mgco2e": 12,
            "emissions_avoided_mgco2e_ci_lower": 10,
            "emissions_avoided_mgco2e_ci_upper": 14,
            "forest_loss_avoided_ha": 3,
            "forest_loss_avoided_ha_ci_lower": 2,
            "forest_loss_avoided_ha_ci_upper": 4,
        },
    )
    top_sites = mocker.patch.object(
        registration,
        "get_task_top_site_yearly",
        return_value=[
            {
                "site_id": "a",
                "year": 2020,
                "emissions_avoided_mgco2e": 2,
                "forest_loss_avoided_ha": 2,
            }
        ],
    )
    content = callbacks["update_filtered_aggregate_plots"](
        {"field": "region", "value": '"North"'},
        {"a": {"region": "North"}, "b": {"region": "South"}},
        "task",
        None,
        {},
    )
    aggregate.assert_called_once_with(
        "task", {"a"}, 2, results_s3_uri="s3://bucket/output"
    )
    portfolio_total.assert_called_once_with(
        "task", {"a"}, 2, results_s3_uri="s3://bucket/output"
    )
    top_sites.assert_called_once_with("task", ["a"])
    assert "region = North" in content[0].children
    assert isinstance(content[1], dbc.Card)
    summary_columns = content[1].children[1].children.children
    assert summary_columns[1].children[1].children == "100.0 ha"
    assert summary_columns[2].children[1].children == "25.0 ha"
    assert summary_columns[2].children[2].children == "25.0% of total area"
    graphs = content[2:]
    assert len(graphs) == 4
    assert list(graphs[0].figure.data[0].y) == [1]
    assert "1 sites" in graphs[0].figure.layout.title.text
    assert graphs[2].figure.data[0].name == "a"
    message = callbacks["update_filtered_aggregate_plots"](
        {"field": "region", "value": '"North"'},
        {"a": {"region": "South"}}, "task", None, {}
    )
    assert "No results" in message.children
    missing = callbacks["update_filtered_aggregate_plots"](
        {"field": "unknown", "value": "null"}, {"a": {"region": "North"}},
        "task", None, {}
    )
    assert missing.children == "The selected source field is not available."


@pytest.mark.unit
def test_filtered_portfolio_aggregates_each_replicate_before_quantiles(mocker):
    rows = []
    for site, values in (("a", [1, 9]), ("b", [9, 1]), ("c", [100, 100])):
        for replicate, value in enumerate(values, 1):
            for year in (2020, 2021):
                row = {"site_id": site, "year": year, "replicate": replicate}
                row.update({name: value for name in analysis_task._YEAR_METRICS})
                rows.append(row)
    content = pd.DataFrame(rows).to_csv(index=False).encode()
    s3 = mocker.MagicMock()
    s3.get_object.return_value = {"Body": StreamingBody(io.BytesIO(content), len(content))}
    mocker.patch.object(analysis_task, "get_s3_client", return_value=s3)
    output = analysis_task.get_filtered_portfolio_yearly(
        "task", {"a", "b"}, 2, results_s3_uri="s3://bucket/path"
    )
    assert output[0]["treatment_defor_ha"] == 10
    assert output[0]["treatment_defor_ha_ci_lower"] == 10
    assert output[0]["treatment_defor_ha_ci_upper"] == 10
    assert output[1]["cum_treatment_defor_ha_ci_lower"] == 20
    assert output[1]["cum_treatment_defor_ha_ci_upper"] == 20
    s3.get_object.assert_called_once_with(
        Bucket="bucket", Key="path/results_by_site_replicate_year.csv"
    )
    s3.get_object.return_value = {"Body": io.BytesIO(content)}
    single_site = analysis_task.get_filtered_portfolio_yearly(
        "task", {"a"}, 2, results_s3_uri="s3://bucket/path"
    )
    assert single_site[0]["treatment_defor_ha_ci_lower"] == pytest.approx(1.2)
    assert single_site[0]["treatment_defor_ha_ci_upper"] == pytest.approx(8.8)
    assert single_site[1]["cum_treatment_defor_ha_ci_lower"] == pytest.approx(2.4)
    assert single_site[1]["cum_treatment_defor_ha_ci_upper"] == pytest.approx(17.6)
    s3.get_object.return_value = {"Body": io.BytesIO(content)}
    with pytest.raises(analysis_task.ResultImportError, match="missing"):
        analysis_task.get_filtered_portfolio_yearly(
            "task", {"a", "absent"}, 2, results_s3_uri="s3://bucket/path"
        )
    incomplete = pd.DataFrame(rows).drop(index=0).to_csv(index=False).encode()
    s3.get_object.return_value = {"Body": io.BytesIO(incomplete)}
    with pytest.raises(analysis_task.ResultImportError, match="incomplete"):
        analysis_task.get_filtered_portfolio_yearly(
            "task", {"a"}, 2, results_s3_uri="s3://bucket/path"
        )
    one_rep = pd.DataFrame(rows).query("replicate == 1").to_csv(index=False).encode()
    s3.get_object.return_value = {"Body": io.BytesIO(one_rep)}
    output = analysis_task.get_filtered_portfolio_yearly(
        "task", {"a", "b"}, 1, results_s3_uri="s3://bucket/path"
    )
    assert output[0]["treatment_defor_ha"] == 10
    assert "treatment_defor_ha_ci_lower" not in output[0]


@pytest.mark.unit
def test_filtered_portfolio_handles_sites_spanning_stream_chunks(mocker):
    rows = []
    for site in ("a", "b"):
        for replicate in (1, 2):
            for year in (2020, 2021):
                row = {"site_id": site, "year": year, "replicate": replicate}
                row.update({name: 1 for name in analysis_task._YEAR_METRICS})
                rows.append(row)
    content = pd.DataFrame(rows).to_csv(index=False).encode()
    s3 = mocker.MagicMock()
    s3.get_object.return_value = {"Body": io.BytesIO(content)}
    mocker.patch.object(analysis_task, "get_s3_client", return_value=s3)
    read_csv = pd.read_csv

    def small_chunks(*args, **kwargs):
        kwargs["chunksize"] = 3
        return read_csv(*args, **kwargs)

    mocker.patch.object(analysis_task.pd, "read_csv", side_effect=small_chunks)
    output = analysis_task.get_filtered_portfolio_yearly(
        "task", {"a", "b"}, 2, results_s3_uri="s3://bucket/path"
    )
    assert [row["cum_treatment_defor_ha"] for row in output] == [2, 4]


@pytest.mark.unit
def test_filtered_portfolio_totals_sum_sites_within_each_replicate(mocker):
    rows = []
    for site, emissions, forest_loss in (
        ("a", [1, 9], [2, 10]),
        ("b", [9, 1], [8, 0]),
        ("c", [100, 100], [100, 100]),
    ):
        for replicate, (emissions_value, forest_value) in enumerate(
            zip(emissions, forest_loss), 1
        ):
            rows.append(
                {
                    "site_id": site,
                    "replicate": replicate,
                    "extrapolated_emissions_avoided_mgco2e": emissions_value,
                    "extrapolated_forest_loss_avoided_ha": forest_value,
                }
            )
    content = pd.DataFrame(rows).to_csv(index=False).encode()
    s3 = mocker.MagicMock()
    s3.get_object.return_value = {
        "Body": StreamingBody(io.BytesIO(content), len(content))
    }
    mocker.patch.object(analysis_task, "get_s3_client", return_value=s3)

    summary = analysis_task.get_filtered_portfolio_total(
        "task", {"a", "b"}, 2, results_s3_uri="s3://bucket/path"
    )

    assert summary == {
        "emissions_avoided_mgco2e": 10,
        "emissions_avoided_mgco2e_ci_lower": 10,
        "emissions_avoided_mgco2e_ci_upper": 10,
        "forest_loss_avoided_ha": 10,
        "forest_loss_avoided_ha_ci_lower": 10,
        "forest_loss_avoided_ha_ci_upper": 10,
    }
    s3.get_object.assert_called_once_with(
        Bucket="bucket", Key="path/results_by_site_replicate_total.csv"
    )


@pytest.mark.unit
@pytest.mark.parametrize("include_pscore", [False, True])
def test_filtered_match_quality_pixels_loads_selected_sites_and_scores(
    mocker, include_pscore
):
    rows = [
        {
            "cell": 1,
            "site_id": "site-a",
            "treatment": True,
            "match_group": "g1",
            "match_weight": 1.0,
            "pscore": 0.2,
            "elevation": 10,
        },
        {
            "cell": 2,
            "site_id": "site-b",
            "treatment": False,
            "match_group": "g2",
            "match_weight": 0.5,
            "pscore": 0.8,
            "elevation": 20,
        },
    ]
    pixels_csv = (
        pd.DataFrame(rows)
        .drop(columns=[] if include_pscore else ["pscore"])
        .to_csv(index=False)
        .encode()
    )
    scores_csv = (
        pd.DataFrame(rows)[
            ["cell", "site_id", "treatment", "match_group", "match_weight", "pscore"]
        ]
        .to_csv(index=False)
        .encode()
    )
    bodies = [
        {"Body": io.BytesIO(pixels_csv)},
        {"Body": io.BytesIO(scores_csv)},
    ]
    s3 = mocker.MagicMock()
    s3.get_object.side_effect = bodies
    mocker.patch.object(analysis_task, "get_s3_client", return_value=s3)

    pixels = analysis_task.get_filtered_match_quality_pixels(
        "task", {"site-a"}, results_s3_uri="s3://bucket/path"
    )

    assert pixels["site_id"].tolist() == ["site-a"]
    assert pixels["pscore"].tolist() == [0.2]
    assert s3.get_object.call_count == (1 if include_pscore else 2)


@pytest.mark.unit
def test_source_attributes_preserve_types_and_missing_values():
    from services.site_upload import _source_attributes_json

    assert json.loads(
        _source_attributes_json(
            {"group": "East", "flag": False, "count": 0, "missing": float("nan")}
        )
    ) == {"group": "East", "flag": False, "count": 0, "missing": None}


@pytest.mark.unit
def test_site_selection_preserves_group_filter(monkeypatch):
    callbacks = {}

    def callback(*args, **kwargs):
        def capture(function):
            callbacks[function.__name__] = function
            return function

        return capture

    registration.register_callbacks(
        SimpleNamespace(
            callback=callback, clientside_callback=lambda *args, **kwargs: None
        )
    )

    monkeypatch.setattr(
        registration, "callback_context", SimpleNamespace(triggered_id="site-map-selection")
    )
    group = {"field": "region", "value": '"North"'}
    result = callbacks["update_plot_selection"](
        "region", '"North"', None, "site-1", 0, 0, group
    )
    assert result[:4] == (
        {"field": "region", "value": '"North"', "site": "site-1"},
        registration.no_update,
        registration.no_update,
        "site-1",
    )
    with pytest.raises(registration.PreventUpdate):
        callbacks["update_plot_selection"](
            "region", '"North"', None, "", 0, 0, group
        )

    monkeypatch.setattr(
        registration, "callback_context", SimpleNamespace(triggered_id="site-defor-selector")
    )
    result = callbacks["update_plot_selection"](
        "region", '"North"', "site-2", "", 0, 0, group
    )
    assert result[0] == {"field": "region", "value": '"North"', "site": "site-2"}
    assert result[1:4] == (
        registration.no_update,
        registration.no_update,
        registration.no_update,
    )

    monkeypatch.setattr(
        registration, "callback_context", SimpleNamespace(triggered_id="site-filter-value")
    )
    result = callbacks["update_plot_selection"](
        "region", '"North"', "site-1", "", 0, 0,
        {"field": "region", "value": '"South"', "site": "site-1"},
    )
    assert result[0] == {"field": "region", "value": '"North"'}
    assert result[3] is None
    monkeypatch.setattr(
        registration, "callback_context", SimpleNamespace(triggered_id="site-filter-field")
    )
    with pytest.raises(registration.PreventUpdate):
        callbacks["update_plot_selection"](
            "region", '"North"', None, "", 0, 0, group
        )
    monkeypatch.setattr(
        registration, "callback_context", SimpleNamespace(triggered_id="site-map-clear")
    )
    assert callbacks["update_plot_selection"](
        "region", '"North"', None, "", 1, 0, group
    ) == (
        group,
        registration.no_update,
        registration.no_update,
        None,
        "",
    )
    monkeypatch.setattr(
        registration,
        "callback_context",
        SimpleNamespace(triggered_id="clear-plot-selection-btn"),
    )
    assert callbacks["update_plot_selection"](
        "region", '"North"', "site-1", "site-1", 0, 1, group
    ) == ({}, None, None, None, "")


@pytest.mark.unit
def test_task_detail_has_no_separate_map_tab():
    layout = task_detail_layout(None, "task-1")
    tabs = next(child for child in layout.children if isinstance(child, dbc.Tabs))

    assert "tab-plots" in {tab.tab_id for tab in tabs.children}
    assert "tab-map" not in {tab.tab_id for tab in tabs.children}
    assert "tab-match-quality" not in {tab.tab_id for tab in tabs.children}


@pytest.mark.unit
def test_selected_match_quality_summary_uses_pooled_weighted_pixel_data():
    pixels = pd.DataFrame(
        {
            "site_id": ["a", "a", "b", "b"],
            "treatment": [True, False, True, False],
            "match_weight": [1.0, 0.5, 1.0, 1.5],
            "pscore": [0.2, 0.3, 0.8, 0.7],
            "forest": [1, 0, 1, 0],
            "elevation": [10.0, 12.0, 20.0, 25.0],
        }
    )

    summary = _calculate_match_quality_summary(
        pixels,
        ["forest", "elevation"],
        ["forest"],
    )

    balance = {row["covariate"]: row for row in summary["balance_rows"]}
    assert balance["forest=0"]["smd"] == pytest.approx(-1.0)
    assert balance["forest=1"]["smd"] == pytest.approx(1.0)
    assert balance["elevation"]["mean_treatment"] == pytest.approx(15.0)
    assert summary["summary_stats"] == {
        "n_treatment": 2,
        "n_control": 2,
        "n_sites": 2,
    }
    assert len(summary["histograms"]["elevation"]["bin_edges"]) == 41
    assert len(summary["qq_quantiles"]["treatment_values"]) == 500


@pytest.mark.unit
def test_match_quality_callback_uses_current_group_and_site_selection(mocker):
    app = Dash(__name__, suppress_callback_exceptions=True)
    app.layout = html.Div()
    registration.register_callbacks(app)
    callback_key = "match-quality-plots-container.children"
    registered = app.callback_map[callback_key]
    update_match_quality = registered["callback"].__wrapped__
    assert registered["inputs"][0] == {
        "id": "match-quality-section",
        "property": "active_item",
    }
    totals = [
        SimpleNamespace(
            site_id=site_id,
            area_ha=100,
            n_sample_pixels=1,
            n_eligible_pixels=1,
            sampled_fraction=1,
        )
        for site_id in ("a", "b")
    ]
    task = SimpleNamespace(status="succeeded", results_s3_uri="s3://bucket/output")
    mocker.patch.object(registration, "_authorize_task_access", return_value=True)
    mocker.patch.object(
        registration,
        "get_task_detail",
        return_value={"task": task, "totals": totals},
    )
    mocker.patch.object(
        registration,
        "download_results_csv",
        return_value=json.dumps({"covariate_cols": [], "categorical_cols": []}),
    )
    mocker.patch.object(
        registration,
        "get_filtered_match_quality_pixels",
        return_value=pd.DataFrame(),
    )
    build = mocker.patch.object(
        registration,
        "_build_selected_match_quality",
        return_value=html.Div("plots"),
    )

    with pytest.raises(registration.PreventUpdate):
        update_match_quality(None, {}, {}, "task", None)
    build.assert_not_called()

    result = update_match_quality(
        "match-quality",
        {"field": "region", "value": '"North"', "site": "a"},
        {"a": {"region": "North"}, "b": {"region": "South"}},
        "task",
        None,
    )

    assert result.children == "plots"
    assert build.call_args.args[2] == {"a"}
    assert [total.site_id for total in build.call_args.args[3]] == ["a"]
    assert build.call_args.args[4] == "a"

    update_match_quality(
        "match-quality",
        {"field": "region", "value": '"North"'},
        {"a": {"region": "North"}, "b": {"region": "South"}},
        "task",
        None,
    )
    assert build.call_args.args[2] == {"a"}
    assert [total.site_id for total in build.call_args.args[3]] == ["a"]
    assert build.call_args.args[4] is None

    response = app.server.test_client().post(
        "/_dash-update-component",
        json={
            "output": callback_key,
            "outputs": {
                "id": "match-quality-plots-container",
                "property": "children",
            },
            "inputs": [
                {
                    "id": "match-quality-section",
                    "property": "active_item",
                    "value": "match-quality",
                },
                {"id": "task-plot-selection", "property": "data", "value": {}},
                {"id": "site-filter-attributes", "property": "data", "value": {}},
            ],
            "state": [
                {"id": "task-id-store", "property": "data", "value": "task"},
                {"id": "share-token-store", "property": "data", "value": None},
            ],
            "changedPropIds": ["match-quality-section.active_item"],
        },
    )
    assert response.status_code == 200
    assert response.json["response"]["match-quality-plots-container"]["children"][
        "props"
    ]["children"] == "plots"
    assert build.call_args.args[2] == {"a", "b"}


@pytest.mark.unit
@pytest.mark.parametrize(
    "selection",
    [
        {},
        {"site": "canonical-site / 1"},
        {"field": "country / region", "value": '"Côte & Islands"'},
        {
            "site": "canonical-site / 1",
            "field": "country / region",
            "value": '"Côte & Islands"',
        },
        {"field": "active", "value": "false"},
        {"field": "class", "value": "null"},
    ],
)
def test_plot_selection_query_round_trip_and_default_tab(selection):
    query = plot_selection_query(selection, "tab-plots")
    assert parse_plot_selection(query) == selection
    layout = task_detail_layout(None, "task", shared_token="share", search=query)
    tabs = next(child for child in layout.children if isinstance(child, dbc.Tabs))
    stored = next(child for child in layout.children if getattr(child, "id", None) == "task-plot-selection")
    assert tabs.active_tab == "tab-plots"
    assert stored.data == selection
    assert plot_selection_query(selection, "tab-overview") == ""


@pytest.mark.unit
@pytest.mark.parametrize(
    "query", ["?site=a&field=x", "?field=x", "?field=x&value=bad", "?site=a&site=b"]
)
def test_invalid_plot_selection_urls_are_reported(query):
    assert parse_plot_selection(query).get("error")


@pytest.mark.unit
def test_new_and_existing_share_links_include_the_current_plot_selection(monkeypatch):
    callbacks = {}

    def callback(*args, **kwargs):
        def capture(func):
            callbacks[func.__name__] = func
            return func
        return capture

    registration.register_callbacks(SimpleNamespace(
        callback=callback, clientside_callback=lambda *args, **kwargs: None
    ))
    monkeypatch.setattr(registration, "get_current_user", lambda: SimpleNamespace(id="user", is_admin=False))
    monkeypatch.setattr(registration, "_check_task_access", lambda *_: True)
    monkeypatch.setattr(registration, "create_share_link", lambda *a, **k: {
        "token": "new-token", "expires_at": "2030-01-01"
    })
    monkeypatch.setattr(registration, "list_share_links", lambda *a, **k: [])
    selection = {"field": "region", "value": '"North & West"'}
    query = plot_selection_query(selection, "tab-plots")
    link = {"id": "1", "token": "existing-token", "expires_at": "2030-01-01",
            "is_valid": True, "access_count": 0}
    with Flask(__name__).test_request_context(base_url="https://example.test/"):
        created, _ = callbacks["generate_share"](1, 30, "task", selection, "tab-plots")
        input_group = created.children[0].children[3]
        assert input_group.children[0].value == "https://example.test/shared/new-token" + query
        existing = _render_share_links_list([link], "task", selection, "tab-plots")
        existing_input = existing.children[0].children[0].children[1].children[0]
        assert existing_input.value == "https://example.test/shared/existing-token" + query
