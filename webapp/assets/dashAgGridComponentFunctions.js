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

    var label = status.replace(/_/g, " ");
    var colors = colorMap[status] || { bg: "#adb5bd", text: "#212529" };
    var errorMessage = props.data && props.data.error_message;

    return React.createElement(
        "span",
        {
            title: status === "failed" && errorMessage ? errorMessage : undefined,
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
                cursor: status === "failed" && errorMessage ? "help" : undefined,
            },
        },
        label
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

    // Re-export is disabled only while GEE is actively running (pending_export,
    // exporting) or while a merge task has the worker locked (merging).
    // "pending_merge" is intentionally allowed: clicking Re-export will cancel
    // the queued merge and start a fresh GEE export.
    var reexportBusy = [
        "pending_export", "exporting", "merging"
    ].indexOf(status) >= 0;

    // Re-merge is disabled during all transitional states (including
    // pending_merge, since the covariate is already queued for merge).
    var remergeBusy = [
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
                backgroundColor: reexportBusy ? "#e9ecef" : "#fff",
                color: reexportBusy ? "#6c757d" : "#dc3545",
                cursor: reexportBusy ? "not-allowed" : "pointer",
                marginRight: "4px",
            },
            disabled: reexportBusy,
            onClick: function (e) {
                e.stopPropagation();
                if (!reexportBusy) {
                    props.setData(Object.assign({}, data, {
                        _action: "reexport",
                        _actionTs: Date.now(),
                    }));
                }
            },
        },
        "\u21BB Export"
    );

    var remergeDisabled = remergeBusy || !hasTiles;
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
 * S3Status – renders a boolean S3 presence as a check mark or dash.
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

/**
 * LocalDateTime – converts a UTC ISO 8601 string to the browser's local
 * timezone and renders it as plain text (YYYY-MM-DD HH:MM).
 */
dagcomponentfuncs.LocalDateTime = function (props) {
    var v = props.value;
    if (!v || v === "-") return v || "-";
    var d = new Date(v);
    if (isNaN(d.getTime())) return v;
    var year = d.getFullYear();
    var month = String(d.getMonth() + 1).padStart(2, "0");
    var day = String(d.getDate()).padStart(2, "0");
    var hours = String(d.getHours()).padStart(2, "0");
    var minutes = String(d.getMinutes()).padStart(2, "0");
    return year + "-" + month + "-" + day + " " + hours + ":" + minutes;
};

/**
 * TaskActions \u2013 renders per-row action buttons for the task list grid.
 *
 * Shows a small "Recompute" button that triggers resubmission of the task
 * with a new random seed. Clicking the button fires setData which the Dash
 * cellRendererData callback reads.
 */
dagcomponentfuncs.TaskActions = function (props) {
    var data = props.data || {};
    var status = (data.status || "").toLowerCase();
    var canCancel = ["pending", "submitted", "running"].indexOf(status) >= 0;

    return React.createElement(
        "div",
        { style: { display: "flex", alignItems: "center", gap: "4px" } },
        React.createElement(
            "button",
            {
                style: {
                    padding: "1px 6px",
                    fontSize: "10px",
                    fontWeight: 600,
                    border: "1px solid #ffc107",
                    borderRadius: "3px",
                    backgroundColor: "#fff",
                    color: "#664d03",
                    cursor: "pointer",
                },
                title: "Resubmit with a new random seed",
                onClick: function (e) {
                    e.stopPropagation();
                    props.setData({
                        action: "recompute",
                        task_id: data.id,
                    });
                },
            },
            "\u21BB Recompute"
        ),
        React.createElement(
            "button",
            {
                style: {
                    padding: "1px 6px",
                    fontSize: "10px",
                    fontWeight: 600,
                    border: "1px solid #dc3545",
                    borderRadius: "3px",
                    backgroundColor: canCancel ? "#fff" : "#e9ecef",
                    color: canCancel ? "#842029" : "#6c757d",
                    cursor: canCancel ? "pointer" : "not-allowed",
                },
                disabled: !canCancel,
                title: canCancel
                    ? "Cancel this task"
                    : "Only pending/running tasks can be cancelled",
                onClick: function (e) {
                    e.stopPropagation();
                    if (canCancel) {
                        props.setData({
                            action: "cancel",
                            task_id: data.id,
                        });
                    }
                },
            },
            "\u2715 Cancel"
        )
    );
};

/**
 * SiteUploadActions – renders a per-row cancel button for background imports.
 */
dagcomponentfuncs.SiteUploadActions = function (props) {
    var data = props.data || {};
    var status = (data.status || "").toLowerCase();
    var useStateResult = React.useState(false);
    var pending = useStateResult[0];
    var setPending = useStateResult[1];
    var statusAllowsCancel = ["pending", "running"].indexOf(status) >= 0;
    var canCancel = statusAllowsCancel && !pending;

    return React.createElement(
        "button",
        {
            style: {
                padding: "1px 6px",
                fontSize: "10px",
                fontWeight: 600,
                border: "1px solid #dc3545",
                borderRadius: "3px",
                backgroundColor: canCancel ? "#fff" : "#e9ecef",
                color: canCancel ? "#842029" : "#6c757d",
                cursor: canCancel ? "pointer" : "not-allowed",
            },
            disabled: !canCancel,
            title: canCancel
                ? "Cancel this import"
                : "Only pending/running imports can be cancelled",
            onClick: function (e) {
                e.stopPropagation();
                if (!canCancel) return;
                if (!window.confirm("Cancel this background site import?")) return;
                setPending(true);
                props.setData({
                    action: "cancel_import",
                    upload_id: data.id,
                    _actionTs: Date.now(),
                });
            },
        },
        pending ? "Cancelling..." : "\u2715 Cancel"
    );
};

/**
 * SiteSetAndUploadActions – combined renderer for the merged site uploads table.
 * Shows Cancel for pending/running imports; Rename/Archive/Delete for completed
 * imports that have an associated site set.
 */
dagcomponentfuncs.SiteSetAndUploadActions = function (props) {
    var data = props.data || {};
    var status = (data.status || "").toLowerCase();
    var isArchived = !!data.is_archived;
    var hasSiteSet = !!data.site_set_id;
    var useStateResult = React.useState(false);
    var pending = useStateResult[0];
    var setPending = useStateResult[1];

    var canCancel = ["pending", "running"].indexOf(status) >= 0 && !pending;
    var canManage = hasSiteSet && !pending;

    var btnStyle = {
        padding: "1px 6px",
        fontSize: "10px",
        fontWeight: 600,
        borderRadius: "3px",
        backgroundColor: pending ? "#e9ecef" : "#fff",
        cursor: pending ? "not-allowed" : "pointer",
    };

    var buttons = [];

    if (["pending", "running"].indexOf(status) >= 0) {
        buttons.push(
            React.createElement(
                "button",
                {
                    key: "cancel",
                    style: Object.assign({}, btnStyle, {
                        border: "1px solid #dc3545",
                        color: canCancel ? "#842029" : "#6c757d",
                    }),
                    disabled: !canCancel,
                    title: canCancel ? "Cancel this import" : "Only pending/running imports can be cancelled",
                    onClick: function (e) {
                        e.stopPropagation();
                        if (!canCancel) return;
                        if (!window.confirm("Cancel this background site import?")) return;
                        setPending(true);
                        props.setData({
                            action: "cancel_import",
                            upload_id: data.id,
                            _actionTs: Date.now(),
                        });
                    },
                },
                pending ? "Cancelling..." : "\u2715 Cancel"
            )
        );
    }

    if (hasSiteSet) {
        buttons.push(
            React.createElement(
                "button",
                {
                    key: "rename",
                    style: Object.assign({}, btnStyle, {
                        border: "1px solid #0d6efd",
                        color: canManage ? "#0d6efd" : "#6c757d",
                    }),
                    disabled: !canManage,
                    title: "Rename this site set",
                    onClick: function (e) {
                        e.stopPropagation();
                        if (!canManage) return;
                        var currentName = data.site_set_name || "";
                        var nextName = window.prompt("Rename site set:", currentName);
                        if (nextName === null) return;
                        nextName = String(nextName).trim();
                        if (!nextName || nextName === currentName) return;
                        setPending(true);
                        props.setData({
                            action: "rename_site_set",
                            site_set_id: data.site_set_id,
                            new_name: nextName,
                            _actionTs: Date.now(),
                        });
                    },
                },
                pending ? "Working..." : "Rename"
            )
        );
        buttons.push(
            React.createElement(
                "button",
                {
                    key: "archive",
                    style: Object.assign({}, btnStyle, {
                        border: "1px solid #ffc107",
                        color: canManage ? "#664d03" : "#6c757d",
                    }),
                    disabled: !canManage,
                    title: isArchived ? "Restore this site set" : "Archive this site set",
                    onClick: function (e) {
                        e.stopPropagation();
                        if (!canManage) return;
                        var actionLabel = isArchived ? "restore" : "archive";
                        if (!window.confirm("Are you sure you want to " + actionLabel + " this site set?")) return;
                        setPending(true);
                        props.setData({
                            action: "toggle_archive_site_set",
                            site_set_id: data.site_set_id,
                            _actionTs: Date.now(),
                        });
                    },
                },
                pending ? "Working..." : (isArchived ? "Restore" : "Archive")
            )
        );
        buttons.push(
            React.createElement(
                "button",
                {
                    key: "delete",
                    style: Object.assign({}, btnStyle, {
                        border: "1px solid #dc3545",
                        color: canManage ? "#842029" : "#6c757d",
                    }),
                    disabled: !canManage,
                    title: "Delete this site set",
                    onClick: function (e) {
                        e.stopPropagation();
                        if (!canManage) return;
                        if (!window.confirm("Delete this site set? This cannot be undone.")) return;
                        setPending(true);
                        props.setData({
                            action: "delete_site_set",
                            site_set_id: data.site_set_id,
                            _actionTs: Date.now(),
                        });
                    },
                },
                pending ? "Working..." : "Delete"
            )
        );
    }

    return React.createElement(
        "div",
        { style: { display: "flex", alignItems: "center", gap: "4px" } },
        buttons
    );
};

/**
 * SiteSetActions – renders per-row rename/archive/delete buttons.
 */
dagcomponentfuncs.SiteSetActions = function (props) {
    var data = props.data || {};
    var isArchived = !!data.is_archived;
    var useStateResult = React.useState(false);
    var pending = useStateResult[0];
    var setPending = useStateResult[1];

    var btnStyle = {
        padding: "1px 6px",
        fontSize: "10px",
        fontWeight: 600,
        borderRadius: "3px",
        backgroundColor: pending ? "#e9ecef" : "#fff",
        cursor: pending ? "not-allowed" : "pointer",
    };

    return React.createElement(
        "div",
        { style: { display: "flex", alignItems: "center", gap: "4px" } },
        React.createElement(
            "button",
            {
                style: Object.assign({}, btnStyle, {
                    border: "1px solid #0d6efd",
                    color: pending ? "#6c757d" : "#0d6efd",
                }),
                disabled: pending,
                title: "Rename this site set",
                onClick: function (e) {
                    e.stopPropagation();
                    if (pending) return;
                    var currentName = data.name || "";
                    var nextName = window.prompt("Rename site set:", currentName);
                    if (nextName === null) return;
                    nextName = String(nextName).trim();
                    if (!nextName || nextName === currentName) return;
                    setPending(true);
                    props.setData({
                        action: "rename_site_set",
                        site_set_id: data.id,
                        new_name: nextName,
                        _actionTs: Date.now(),
                    });
                },
            },
            pending ? "Working..." : "Rename"
        ),
        React.createElement(
            "button",
            {
                style: Object.assign({}, btnStyle, {
                    border: "1px solid #ffc107",
                    color: pending ? "#6c757d" : "#664d03",
                }),
                disabled: pending,
                title: isArchived ? "Restore this site set" : "Archive this site set",
                onClick: function (e) {
                    e.stopPropagation();
                    if (pending) return;
                    var actionLabel = isArchived ? "restore" : "archive";
                    if (!window.confirm("Are you sure you want to " + actionLabel + " this site set?")) return;
                    setPending(true);
                    props.setData({
                        action: "toggle_archive_site_set",
                        site_set_id: data.id,
                        _actionTs: Date.now(),
                    });
                },
            },
            pending ? "Working..." : (isArchived ? "Restore" : "Archive")
        ),
        React.createElement(
            "button",
            {
                style: Object.assign({}, btnStyle, {
                    border: "1px solid #dc3545",
                    color: pending ? "#6c757d" : "#842029",
                }),
                disabled: pending,
                title: "Delete this site set",
                onClick: function (e) {
                    e.stopPropagation();
                    if (pending) return;
                    if (!window.confirm("Delete this site set? This cannot be undone.")) return;
                    setPending(true);
                    props.setData({
                        action: "delete_site_set",
                        site_set_id: data.id,
                        _actionTs: Date.now(),
                    });
                },
            },
            pending ? "Working..." : "Delete"
        )
    );
};

/**
 * TruncatedList – renders an abbreviated comma-separated list with a
 * tooltip showing the full list on mouseover.
 *
 * Expects the cell value to be a comma-separated string.
 * The tooltipField on the column should point to the full list field.
 * Shows the first few items plus a count of remaining items.
 */
dagcomponentfuncs.TruncatedList = function (props) {
    var raw = props.value;
    if (!raw) return React.createElement("span", { style: { color: "#adb5bd" } }, "\u2014");
    var items = raw.split(", ");
    var maxShow = 2;
    var display;
    if (items.length <= maxShow) {
        display = raw;
    } else {
        display = items.slice(0, maxShow).join(", ") + " +" + (items.length - maxShow);
    }
    return React.createElement(
        "span",
        {
            title: raw,
            style: {
                cursor: "default",
                whiteSpace: "nowrap",
                overflow: "hidden",
                textOverflow: "ellipsis",
            },
        },
        display
    );
};

/**
 * SeverityIcon – renders a coloured icon based on the severity value.
 *
 * "Critical" → red X-circle, anything else → amber warning triangle.
 */
dagcomponentfuncs.SeverityIcon = function (props) {
    var isCritical = props.value === "Critical";
    return React.createElement("i", {
        className: isCritical
            ? "bi bi-x-circle-fill"
            : "bi bi-exclamation-triangle-fill",
        style: {
            color: isCritical ? "#dc3545" : "#ffc107",
            fontSize: "16px",
        },
        title: props.value,
    });
};
