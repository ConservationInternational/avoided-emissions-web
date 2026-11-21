"""URL representation of the results-plot selection."""

import json
from urllib.parse import parse_qs, urlencode

TASK_TABS = {
    "overview": "tab-overview",
    "results": "tab-results",
    "plots": "tab-plots",
    "raw-results": "tab-raw-results",
}


def parse_task_tab(search):
    """Honor explicit tabs while preserving legacy selection-only links."""
    params = parse_qs((search or "").lstrip("?"))
    tab = params.get("tab", [""])[0]
    return TASK_TABS.get(
        tab, "tab-plots" if parse_plot_selection(search) else "tab-overview"
    )


def parse_plot_selection(search):
    params = parse_qs((search or "").lstrip("?"), keep_blank_values=True)
    if any(
        len(params.get(key, [])) > 1
        for key in ("site", "field", "value", "field2", "value2", "year")
    ):
        return {"error": "The plot URL contains duplicate selection parameters."}
    site = params.get("site", [""])[0]
    selection = {"site": site} if site else {}
    for field_key, value_key in (("field", "value"), ("field2", "value2")):
        field = params.get(field_key, [""])[0]
        has_value = value_key in params
        if not field and not has_value:
            continue
        if not field or not has_value:
            return {"error": "The plot URL needs both a group field and value."}
        try:
            value = json.loads(params[value_key][0])
        except (ValueError, TypeError):
            return {"error": "The group value in the plot URL is invalid."}
        selection[field_key] = field
        selection[value_key] = json.dumps(value, sort_keys=True, ensure_ascii=False)
    if selection.get("field2") and (
        not selection.get("field") or selection["field2"] == selection["field"]
    ):
        return {"error": "The secondary filter needs a different primary field."}
    if "year" in params:
        try:
            year = int(params["year"][0])
        except ValueError:
            return {"error": "The ranking year in the plot URL is invalid."}
        if not 1 <= year <= 9999:
            return {"error": "The ranking year in the plot URL is invalid."}
        selection["year"] = year
    return selection


def plot_selection_query(selection, active_tab):
    if active_tab != "tab-plots":
        return ""
    selection = selection or {}
    params = {"tab": "plots"}
    if selection.get("site"):
        params["site"] = selection["site"]
    for field_key, value_key in (("field", "value"), ("field2", "value2")):
        if selection.get(field_key) and selection.get(value_key) is not None:
            params[field_key] = selection[field_key]
            params[value_key] = selection[value_key]
    if selection.get("year") is not None:
        params["year"] = selection["year"]
    return "?" + urlencode(params)
