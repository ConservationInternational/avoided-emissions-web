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
from dash import dcc
from flask import Flask
from layouts.task_detail import task_detail_layout
from plot_selection import parse_plot_selection, plot_selection_query
from services import analysis_task


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
    graphs = content.children[0].children
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
    site = callbacks["update_filtered_aggregate_plots"](
        {"site": "site-1"}, {}, "task", None, {"results": rows}
    )
    for content in (aggregate, site):
        children = (
            content.children[-1].children if content is aggregate else content.children
        )
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
    store = {"large_task": large_task, "results": rows}
    content = callbacks["update_filtered_aggregate_plots"](
        {"site": "site-1"}, {}, "task-1", None, store
    )
    graphs = [item.figure for item in content.children if isinstance(item, dcc.Graph)]
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
    aggregate_graphs = [item.figure for item in aggregate.children[-1].children]

    for fig in aggregate_graphs[:3]:
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
    assert component_ids.index("site-defor-selector") < component_ids.index("task-sites-map")
    assert component_ids.index("task-sites-map") < component_ids.index("task-results-plots")
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
        SimpleNamespace(site_id=site, display_site_id=site, site_name=site)
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
    content = callbacks["update_filtered_aggregate_plots"](
        {"field": "region", "value": '"North"'},
        {"a": {"region": "North"}, "b": {"region": "South"}}, "task", None, {}
    )
    aggregate.assert_called_once_with(
        "task", {"a"}, 2, results_s3_uri="s3://bucket/output"
    )
    assert "region = North" in content[0].children
    graphs = content[1:]
    assert len(graphs) == 4
    assert list(graphs[0].figure.data[0].y) == [1]
    assert "1 sites" in graphs[0].figure.layout.title.text
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
def test_source_attributes_preserve_types_and_missing_values():
    from services.site_upload import _source_attributes_json

    assert json.loads(
        _source_attributes_json(
            {"group": "East", "flag": False, "count": 0, "missing": float("nan")}
        )
    ) == {"group": "East", "flag": False, "count": 0, "missing": None}


@pytest.mark.unit
def test_map_site_selection_clears_group_and_selects_site(monkeypatch):
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
    result = callbacks["update_plot_selection"]("region", '"North"', None, "site-1", 0, group)
    assert result[:4] == ({"site": "site-1"}, None, None, "site-1")
    with pytest.raises(registration.PreventUpdate):
        callbacks["update_plot_selection"]("region", '"North"', None, "", 0, group)

    monkeypatch.setattr(
        registration, "callback_context", SimpleNamespace(triggered_id="site-filter-value")
    )
    result = callbacks["update_plot_selection"]("region", '"North"', "site-1", "", 0, {"site": "site-1"})
    assert result[0] == {"field": "region", "value": '"North"'}
    assert result[3] is None
    monkeypatch.setattr(
        registration, "callback_context", SimpleNamespace(triggered_id="site-filter-field")
    )
    with pytest.raises(registration.PreventUpdate):
        callbacks["update_plot_selection"]("region", '"North"', None, "", 0, group)
    monkeypatch.setattr(
        registration, "callback_context", SimpleNamespace(triggered_id="site-map-clear")
    )
    assert callbacks["update_plot_selection"]("region", '"North"', None, "", 1, group) == (
        {}, None, None, None, ""
    )


@pytest.mark.unit
def test_task_detail_has_no_separate_map_tab():
    layout = task_detail_layout(None, "task-1")
    tabs = next(child for child in layout.children if isinstance(child, dbc.Tabs))

    assert "tab-plots" in {tab.tab_id for tab in tabs.children}
    assert "tab-map" not in {tab.tab_id for tab in tabs.children}


@pytest.mark.unit
@pytest.mark.parametrize(
    "selection",
    [
        {},
        {"site": "canonical-site / 1"},
        {"field": "country / region", "value": '"Côte & Islands"'},
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
    "query", ["?site=a&field=x&value=1", "?field=x", "?field=x&value=bad", "?site=a&site=b"]
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
