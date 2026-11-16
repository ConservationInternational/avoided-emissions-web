from types import SimpleNamespace

import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objects as go
import pytest
from callbacks import _registration as registration
from callbacks._detail_builders import _add_range_error_bars, _build_plots
from dash import dcc
from layouts.task_detail import task_detail_layout


@pytest.mark.unit
def test_range_error_bars_show_the_full_asymmetric_replicate_range():
    fig = go.Figure()
    _add_range_error_bars(
        fig,
        pd.Series([2020, 2021]),
        pd.Series([11.0, 17.5]),
        pd.Series([8.0, 14.0]),
        pd.Series([13.0, 20.0]),
        color="#2ca02c",
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
        color="#2ca02c",
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
    graphs = [item for item in content.children if isinstance(item, dcc.Graph)]
    yearly = next(
        graph.figure for graph in graphs
        if graph.figure.layout.title.text.startswith("Avoided Emissions by Year")
    )
    per_site = next(
        graph.figure for graph in graphs
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
                "subsampled_sites": [
                    {"site_id": "canonical-a", "sampled_percent": 25}
                ]
            }
        ),
        agg_yearly=[{
            "year": 2020,
            "treatment_defor_ha": 1.0,
            "control_defor_ha": 2.0,
            "cum_treatment_defor_ha": 1.0,
            "cum_control_defor_ha": 2.0,
            "emissions_avoided_mgco2e": 10.0,
            "forest_loss_avoided_ha": 1.0,
        }],
    )
    store = next(
        item for item in content.children
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
        item for item in content.children
        if any(
            getattr(child, "id", None) == "site-defor-sort-order"
            for child in (getattr(item, "children", None) or [])
        )
    )
    radio = next(
        child for child in controls.children
        if getattr(child, "id", None) == "site-defor-sort-order"
    )
    selector = next(
        item for item in content.children
        if getattr(item, "id", None) == "site-defor-selector"
    )
    assert selector.options == store.data[radio.value]


@pytest.mark.unit
@pytest.mark.parametrize("years", [[2020], [2019, 2020, 2021]])
def test_aggregate_and_site_plots_use_unformatted_integer_year_ticks(years):
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
    aggregate = _build_plots(
        results=None, totals=[], is_large=True, agg_yearly=rows
    )
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
    site = callbacks["update_site_deforestation_plot"](
        "site-1", {"results": rows}, None
    )
    for content in (aggregate, site):
        graphs = [item for item in content.children if isinstance(item, dcc.Graph)]
        assert len(graphs) == 4
        for graph in graphs:
            axis = graph.figure.layout.xaxis
            assert axis.tickformat == "d"
            assert axis.dtick == 1
            assert list(graph.figure.data[0].x) == years


@pytest.mark.unit
def test_site_map_is_placed_before_the_site_selector():
    content = _build_plots(
        results=None,
        totals=[
            SimpleNamespace(
                site_id="site-1", display_site_id="1", site_name="Site One"
            )
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
    )

    children = content.children
    component_ids = [getattr(child, "id", None) for child in children]
    detail_heading = next(
        index
        for index, child in enumerate(children)
        if getattr(child, "children", None) == "Site-Level Deforestation Detail"
    )
    assert detail_heading < component_ids.index("task-sites-map")
    assert component_ids.index("task-sites-map") < component_ids.index(
        "site-defor-selector"
    )
    assert "site-map-selection" in component_ids


@pytest.mark.unit
def test_map_site_selection_updates_the_site_selector():
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

    assert callbacks["select_site_from_map"]("site-1") == "site-1"
    with pytest.raises(registration.PreventUpdate):
        callbacks["select_site_from_map"]("")


@pytest.mark.unit
def test_task_detail_has_no_separate_map_tab():
    layout = task_detail_layout(None, "task-1")
    tabs = next(child for child in layout.children if isinstance(child, dbc.Tabs))

    assert "tab-plots" in {tab.tab_id for tab in tabs.children}
    assert "tab-map" not in {tab.tab_id for tab in tabs.children}
