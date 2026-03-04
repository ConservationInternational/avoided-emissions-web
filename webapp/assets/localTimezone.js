/**
 * Converts UTC datetimes to the user's local browser timezone.
 *
 * Two mechanisms:
 *
 * 1. AG Grid valueFormatter – used via inline function references in column
 *    definitions, calling `window.formatLocalDateTime(value)`.
 *
 * 2. MutationObserver – watches the DOM for elements with the CSS class
 *    `utc-datetime` and a `data-utc` attribute containing an ISO 8601
 *    timestamp.  On first appearance the element's text is replaced with
 *    the local-time equivalent.
 */

(function () {
    "use strict";

    /**
     * Format an ISO 8601 UTC string as a local datetime string.
     *
     * Returns the value unchanged (or "-") when the input is falsy or not
     * a valid date.
     *
     * @param {string} isoString  e.g. "2026-03-04T20:19:00Z"
     * @returns {string}          e.g. "2026-03-04 15:19" (in UTC-5)
     */
    function formatLocalDateTime(isoString) {
        if (!isoString || isoString === "-") {
            return isoString || "-";
        }
        var d = new Date(isoString);
        if (isNaN(d.getTime())) {
            return isoString;
        }
        var year = d.getFullYear();
        var month = String(d.getMonth() + 1).padStart(2, "0");
        var day = String(d.getDate()).padStart(2, "0");
        var hours = String(d.getHours()).padStart(2, "0");
        var minutes = String(d.getMinutes()).padStart(2, "0");
        return year + "-" + month + "-" + day + " " + hours + ":" + minutes;
    }

    // Expose globally so AG Grid valueFormatter expressions can call it.
    window.formatLocalDateTime = formatLocalDateTime;

    // --- MutationObserver for server-rendered <span class="utc-datetime"> ---

    function convertUtcSpans(root) {
        var spans = (root || document).querySelectorAll(
            "span.utc-datetime[data-utc]:not([data-converted])"
        );
        for (var i = 0; i < spans.length; i++) {
            var span = spans[i];
            span.textContent = formatLocalDateTime(span.getAttribute("data-utc"));
            span.setAttribute("data-converted", "1");
        }
    }

    // Run once on load, then observe for dynamic Dash updates.
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", function () {
            convertUtcSpans();
        });
    } else {
        convertUtcSpans();
    }

    var observer = new MutationObserver(function (mutations) {
        for (var i = 0; i < mutations.length; i++) {
            var added = mutations[i].addedNodes;
            for (var j = 0; j < added.length; j++) {
                if (added[j].nodeType === 1) {
                    convertUtcSpans(added[j]);
                }
            }
        }
    });

    observer.observe(document.documentElement, {
        childList: true,
        subtree: true,
    });
})();
