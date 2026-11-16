"""URL representation of the results-plot selection."""

import json
from urllib.parse import parse_qs, urlencode


def parse_plot_selection(search):
    params = parse_qs((search or "").lstrip("?"), keep_blank_values=True)
    if any(len(params.get(key, [])) > 1 for key in ("site", "field", "value")):
        return {"error": "The plot URL contains duplicate selection parameters."}
    site = params.get("site", [""])[0]
    field = params.get("field", [""])[0]
    has_value = "value" in params
    if site and (field or has_value):
        return {"error": "Choose either a site or a group in the plot URL."}
    if site:
        return {"site": site}
    if field and has_value:
        try:
            value = json.loads(params["value"][0])
        except (ValueError, TypeError):
            return {"error": "The group value in the plot URL is invalid."}
        return {
            "field": field,
            "value": json.dumps(value, sort_keys=True, ensure_ascii=False),
        }
    if field or has_value:
        return {"error": "The plot URL needs both a group field and value."}
    return {}


def plot_selection_query(selection, active_tab):
    if active_tab != "tab-plots":
        return ""
    selection = selection or {}
    params = {"tab": "plots"}
    if selection.get("site"):
        params["site"] = selection["site"]
    elif selection.get("field") and selection.get("value") is not None:
        params["field"] = selection["field"]
        params["value"] = selection["value"]
    return "?" + urlencode(params)
