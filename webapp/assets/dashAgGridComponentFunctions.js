/**
 * Custom AG Grid cell renderer components for the avoided emissions webapp.
 *
 * These are registered as Dash AG Grid component functions and referenced
 * by name in column definitions (e.g. cellRenderer: "TaskLink").
 */

var dagcomponentfuncs = (window.dashAgGridComponentFunctions =
    window.dashAgGridComponentFunctions || {});

/**
 * TaskLink – renders the task name as a clickable link to /task/{id}.
 *
 * Expects row data to contain an `id` field with the task UUID.
 */
dagcomponentfuncs.TaskLink = function (props) {
    var value = props.value || "";
    var taskId = props.data && props.data.id;
    if (!taskId) {
        return value;
    }
    return React.createElement(
        "a",
        {
            href: "/task/" + taskId,
            style: {
                color: "#2c3e50",
                fontWeight: 500,
                textDecoration: "none",
            },
            onMouseOver: function (e) {
                e.target.style.textDecoration = "underline";
            },
            onMouseOut: function (e) {
                e.target.style.textDecoration = "none";
            },
        },
        value
    );
};

/**
 * TileLinks – renders a list of GCS tile URLs as clickable download links.
 *
 * Expects the cell value to be a JSON-encoded array of URL strings.
 * Each link shows a short filename and opens in a new tab.
 */
dagcomponentfuncs.TileLinks = function (props) {
    var raw = props.value;
    if (!raw) {
        return "";
    }
    var urls;
    try {
        urls = typeof raw === "string" ? JSON.parse(raw) : raw;
    } catch (e) {
        return raw;
    }
    if (!Array.isArray(urls) || urls.length === 0) {
        return "";
    }
    var children = [];
    for (var i = 0; i < urls.length; i++) {
        var url = urls[i];
        var parts = url.split("/");
        var label = parts[parts.length - 1] || url;
        if (i > 0) {
            children.push(React.createElement("br", { key: "br" + i }));
        }
        children.push(
            React.createElement(
                "a",
                {
                    key: "link" + i,
                    href: url,
                    target: "_blank",
                    rel: "noopener noreferrer",
                    style: {
                        color: "#0d6efd",
                        fontSize: "11px",
                        textDecoration: "none",
                        wordBreak: "break-all",
                    },
                    onMouseOver: function (e) {
                        e.target.style.textDecoration = "underline";
                    },
                    onMouseOut: function (e) {
                        e.target.style.textDecoration = "none";
                    },
                },
                label
            )
        );
    }
    return React.createElement("div", { style: { lineHeight: "1.4" } }, children);
};/**
 * StatusBadge – renders the status string as a colored Bootstrap-style badge.
 */
dagcomponentfuncs.StatusBadge = function (props) {
    var status = (props.value || "").toLowerCase();
    if (!status) return "";
    var colorMap = {
        pending: { bg: "#6c757d", text: "#ffffff" },
        pending_export: { bg: "#6c757d", text: "#ffffff" },
        pending_merge: { bg: "#6c757d", text: "#ffffff" },
        submitted: { bg: "#ffc107", text: "#664d03" },
        running: { bg: "#0d6efd", text: "#ffffff" },
        exporting: { bg: "#0d6efd", text: "#ffffff" },
        exported: { bg: "#ffc107", text: "#664d03" },
        merging: { bg: "#0d6efd", text: "#ffffff" },
        merged: { bg: "#198754", text: "#ffffff" },
        succeeded: { bg: "#198754", text: "#ffffff" },
        completed: { bg: "#198754", text: "#ffffff" },
        failed: { bg: "#dc3545", text: "#ffffff" },
        cancelled: { bg: "#6c757d", text: "#ffffff" },
    };
    var colors = colorMap[status] || { bg: "#adb5bd", text: "#212529" };

    return React.createElement(
        "span",
        {
            style: {
                display: "inline-block",
                padding: "2px 8px",
                borderRadius: "4px",
                fontSize: "11px",
                fontWeight: 600,
                textTransform: "uppercase",
                letterSpacing: "0.3px",
                backgroundColor: colors.bg,
                color: colors.text,
                lineHeight: "1.5",
            },
        },
        status.replace(/_/g, " ")
    );
};

/**
 * ApprovalBadge – renders a boolean approval status as a colored badge.
 *
 * True → green "Approved" badge; False → orange "Pending" badge.
 */
dagcomponentfuncs.ApprovalBadge = function (props) {
    var approved = props.value;
    var label = approved ? "Approved" : "Pending";
    var bg = approved ? "#198754" : "#fd7e14";
    var text = "#ffffff";

    return React.createElement(
        "span",
        {
            style: {
                display: "inline-block",
                padding: "2px 8px",
                borderRadius: "4px",
                fontSize: "11px",
                fontWeight: 600,
                textTransform: "uppercase",
                letterSpacing: "0.3px",
                backgroundColor: bg,
                color: text,
                lineHeight: "1.5",
            },
        },
        label
    );
};

/**
 * CogLink – renders a merged COG URL as a clickable download link.
 *
 * Shows the filename portion of the URL with a download icon.
 */
dagcomponentfuncs.CogLink = function (props) {
    var url = props.value;
    if (!url) {
        return "";
    }
    var parts = url.split("/");
    var label = parts[parts.length - 1] || url;
    return React.createElement(
        "a",
        {
            href: url,
            target: "_blank",
            rel: "noopener noreferrer",
            style: {
                color: "#0d6efd",
                fontSize: "11px",
                textDecoration: "none",
                wordBreak: "break-all",
            },
            onMouseOver: function (e) {
                e.target.style.textDecoration = "underline";
            },
            onMouseOut: function (e) {
                e.target.style.textDecoration = "none";
            },
        },
        "\u2B07 " + label
    );
};

/**
 * TileCount \u2013 renders a GCS tile count as a small green badge.
 * Shows a green pill with the count if > 0, otherwise a gray dash.
 */
dagcomponentfuncs.TileCount = function (props) {
    var count = props.value || 0;
    if (count > 0) {
        return React.createElement(
            "span",
            {
                style: {
                    display: "inline-block",
                    padding: "1px 8px",
                    borderRadius: "10px",
                    backgroundColor: "#198754",
                    color: "#ffffff",
                    fontSize: "11px",
                    fontWeight: 600,
                    minWidth: "24px",
                    textAlign: "center",
                },
            },
            count
        );
    }
    return React.createElement(
        "span",
        { style: { color: "#adb5bd" } },
        "\u2014"
    );
};

/**
 * CovariateActions \u2013 renders per-row action buttons for the covariate grid.
 *
 * Shows two small buttons:
 *   - "Re-export" : force re-export from GEE (deletes GCS tiles + S3 COG)
 *   - "Re-merge"  : force re-merge GCS tiles to S3 (deletes S3 COG)
 *
 * Buttons are disabled when the covariate is already in a transitional state.
 * Clicking a button triggers setData which the Dash cellClicked callback reads.
 */
dagcomponentfuncs.CovariateActions = function (props) {
    var data = props.data || {};
    var status = (data.status || "").toLowerCase();
    var hasTiles = (data.gcs_tiles || 0) > 0;

    // Disable buttons during transitional states
    var busy = [
        "pending_export", "exporting", "pending_merge", "merging"
    ].indexOf(status) >= 0;

    var reexportBtn = React.createElement(
        "button",
        {
            style: {
                padding: "1px 6px",
                fontSize: "10px",
                fontWeight: 600,
                border: "1px solid #dc3545",
                borderRadius: "3px",
                backgroundColor: busy ? "#e9ecef" : "#fff",
                color: busy ? "#6c757d" : "#dc3545",
                cursor: busy ? "not-allowed" : "pointer",
                marginRight: "4px",
            },
            disabled: busy,
            onClick: function (e) {
                e.stopPropagation();
                if (!busy) {
                    props.setData(Object.assign({}, data, {
                        _action: "reexport",
                        _actionTs: Date.now(),
                    }));
                }
            },
        },
        "\u21BB Export"
    );

    var remergeDisabled = busy || !hasTiles;
    var remergeBtn = React.createElement(
        "button",
        {
            style: {
                padding: "1px 6px",
                fontSize: "10px",
                fontWeight: 600,
                border: "1px solid #0d6efd",
                borderRadius: "3px",
                backgroundColor: remergeDisabled ? "#e9ecef" : "#fff",
                color: remergeDisabled ? "#6c757d" : "#0d6efd",
                cursor: remergeDisabled ? "not-allowed" : "pointer",
            },
            disabled: remergeDisabled,
            onClick: function (e) {
                e.stopPropagation();
                if (!remergeDisabled) {
                    props.setData(Object.assign({}, data, {
                        _action: "remerge",
                        _actionTs: Date.now(),
                    }));
                }
            },
        },
        "\u21BB Merge"
    );

    return React.createElement(
        "div",
        { style: { display: "flex", alignItems: "center", gap: "2px" } },
        reexportBtn,
        remergeBtn
    );
};

/**
 * S3Status \u2013 renders a boolean S3 presence as a check mark or dash.
 */
dagcomponentfuncs.S3Status = function (props) {
    if (props.value) {
        return React.createElement(
            "span",
            {
                style: {
                    color: "#198754",
                    fontWeight: "bold",
                    fontSize: "14px",
                },
            },
            "\u2713"
        );
    }
    return React.createElement(
        "span",
        { style: { color: "#adb5bd" } },
        "\u2014"
    );
};
