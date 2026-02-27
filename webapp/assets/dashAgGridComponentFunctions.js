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
    var colorMap = {
        pending: { bg: "#6c757d", text: "#ffffff" },
        submitted: { bg: "#ffc107", text: "#664d03" },
        running: { bg: "#0d6efd", text: "#ffffff" },
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
        status
    );
};
