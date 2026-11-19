var dagfuncs = (window.dashAgGridFunctions = window.dashAgGridFunctions || {});

function parseGridDate(value) {
    if (value == null || value === "") return null;
    if (value instanceof Date) return isNaN(value) ? null : value;
    var text = String(value).trim();
    var dateOnly = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
    if (dateOnly) {
        return new Date(+dateOnly[1], +dateOnly[2] - 1, +dateOnly[3]);
    }
    if (!/^\d{4}-\d{2}-\d{2}T/.test(text)) return null;
    // Python isoformat() can emit microseconds, which some browsers reject.
    var parsed = new Date(text.replace(/(\.\d{3})\d+/, "$1"));
    return isNaN(parsed) ? null : parsed;
}

// Date filters compare calendar days in the browser's timezone, matching
// the LocalDateTime renderer; placeholders such as "-" count as blank.
dagfuncs.localDateFilterValue = function (params) {
    var raw = params.data ? params.data[params.colDef.field] : null;
    var parsed = parseGridDate(raw);
    return parsed
        ? new Date(parsed.getFullYear(), parsed.getMonth(), parsed.getDate())
        : null;
};

dagfuncs.isTruthyCell = function (_filterValues, cellValue) {
    if (typeof cellValue === "string") {
        return ["true", "yes", "1"].indexOf(cellValue.toLowerCase()) !== -1;
    }
    return Boolean(cellValue);
};

dagfuncs.isFalsyCell = function (filterValues, cellValue) {
    return !dagfuncs.isTruthyCell(filterValues, cellValue);
};

dagfuncs.cellEquals = function (expected) {
    return function (_filterValues, cellValue) {
        return String(cellValue == null ? "" : cellValue).toLowerCase() ===
            String(expected).toLowerCase();
    };
};
