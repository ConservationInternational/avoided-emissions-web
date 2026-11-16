from types import SimpleNamespace

import pandas as pd
import plotly.graph_objects as go
import pytest
from callbacks._detail_builders import _add_range_error_bars, _build_plots
from dash import dcc


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
