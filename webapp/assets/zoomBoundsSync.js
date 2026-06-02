/**
 * Sync map zoom/bounds changes to Dash store via window state.
 *
 * Stores the current zoom/bounds in a window variable that can be
 * read by a clientside callback to update the Dash zoom-bounds-store.
 */
(function () {
    // Initialize the window variable for zoom/bounds state
    if (!window._submitSitesMapState) {
        window._submitSitesMapState = {
            zoom: null,
            bounds: null,
            hasUpdate: false,
        };
    }

    function syncZoomBoundsToWindow(event) {
        if (!event || !event.detail) {
            return;
        }

        var detail = event.detail;
        window._submitSitesMapState.zoom = detail.zoom;
        window._submitSitesMapState.bounds = detail.bounds;
        window._submitSitesMapState.hasUpdate = true;

        console.log(
            "[zoom-bounds-sync] Map state updated: zoom=" +
                detail.zoom +
                ", bounds=" +
                JSON.stringify(detail.bounds)
        );
    }

    // Wait for the DOM to be ready
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", function () {
            document.addEventListener("map-zoom-bounds-changed", syncZoomBoundsToWindow);
        });
    } else {
        document.addEventListener("map-zoom-bounds-changed", syncZoomBoundsToWindow);
    }
})();
