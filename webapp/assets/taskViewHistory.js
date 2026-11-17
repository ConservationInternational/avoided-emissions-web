(function () {
    const stateKey = "aeTaskView";
    const tabs = {
        "tab-overview": "overview",
        "tab-results": "results",
        "tab-plots": "plots",
        "tab-raw-results": "raw-results",
    };
    let owner = null;
    let ownerPath = null;
    let ownerTask = null;
    let current = null;
    let restoring = null;
    let changingMap = false;
    let sequence = 0;

    function viewKey(selection, tab) {
        return JSON.stringify([
            tab, selection.site || "", selection.field || "", selection.value ?? null,
        ]);
    }

    function mapElement() {
        return document.getElementById("task-sites-map");
    }

    function snapshot() {
        const saved = window.history.state?.[stateKey];
        return saved?.owner === owner ? saved : null;
    }

    function replace(saved) {
        window.history.replaceState(
            { ...window.history.state, [stateKey]: saved }, "", window.location.href,
        );
    }

    function saveViewport() {
        const el = mapElement();
        const saved = snapshot();
        if (restoring || changingMap || !el?._olMap || !saved ||
            (el._selectedSiteId || "") !== (saved.selection.site || "")) {
            return;
        }
        const view = el._olMap.getView();
        const center = view.getCenter();
        const zoom = view.getZoom();
        if (center?.length === 2 && center.every(Number.isFinite) && Number.isFinite(zoom)) {
            replace({
                ...saved,
                viewport: { center: [...center], zoom, rotation: view.getRotation() },
            });
        }
    }

    function beforeMapChange(siteId) {
        if (!restoring && current && (siteId || "") !== (current.selection.site || "")) {
            saveViewport();
            changingMap = true;
        }
    }

    function watchMap(el) {
        if (el._historyViewportBound) {
            return;
        }
        el._historyViewportBound = true;
        el._olMap.on("moveend", saveViewport);
        el._cleanup.push(() => el._olMap.un("moveend", saveViewport));
    }

    function zoomToSite(siteId) {
        const el = mapElement();
        if (!el) {
            return window.dash_clientside.no_update;
        }
        const requested = siteId || "";
        const token = ++sequence;
        const path = window.location.pathname;
        beforeMapChange(requested);
        const zoom = function () {
            if (token !== sequence || path !== window.location.pathname || !el.isConnected) {
                return;
            }
            watchMap(el);
            const saved = snapshot();
            const viewport = saved && (saved.selection.site || "") === requested
                ? saved.viewport : null;
            el.dispatchEvent(new CustomEvent("zoom-to-site", {
                detail: { siteId: requested, viewport },
            }));
        };
        if (el._olMap) {
            zoom();
        } else {
            // Map-ready fires before the initial extent fit finishes.
            el.addEventListener("ol-map-ready", () => queueMicrotask(zoom), { once: true });
        }
        return window.dash_clientside.no_update;
    }

    function urlForView(selection, tab) {
        const url = new URL(window.location.href);
        url.searchParams.set("tab", tabs[tab]);
        ["site", "field", "value"].forEach(key => url.searchParams.delete(key));
        if (selection.site) {
            url.searchParams.set("site", selection.site);
        }
        if (selection.field && selection.value != null) {
            url.searchParams.set("field", selection.field);
            url.searchParams.set("value", selection.value);
        }
        return url;
    }

    function sync(selection, tab, taskId) {
        selection = selection || {};
        const nextOwner = window.location.pathname + ":" + taskId;
        if (!tabs[tab]) {
            return window.dash_clientside.no_update;
        }
        if (owner !== nextOwner) {
            owner = nextOwner;
            ownerPath = window.location.pathname;
            ownerTask = taskId;
            restoring = null;
            changingMap = false;
            current = null;
        }
        if (selection.error) {
            restoring = null;
            changingMap = false;
            current = { owner, selection, tab, viewport: null };
            return window.dash_clientside.no_update;
        }
        if (selection.field && selection.value == null) {
            changingMap = true;
            return window.dash_clientside.no_update;
        }
        const next = { owner, selection, tab, viewport: null };
        if (restoring) {
            const target = snapshot();
            if (target && viewKey(selection, tab) !== viewKey(target.selection, target.tab)) {
                return window.dash_clientside.no_update;
            }
            current = next;
            restoring = null;
            changingMap = false;
            zoomToSite(selection.site);
            return window.dash_clientside.no_update;
        }
        if (!current) {
            const saved = snapshot();
            current = next;
            replace(saved && viewKey(saved.selection, saved.tab) === viewKey(selection, tab)
                ? saved : next);
            window.history.replaceState(window.history.state, "", urlForView(selection, tab));
            zoomToSite(selection.site);
        } else if (viewKey(current.selection, current.tab) !== viewKey(selection, tab)) {
            saveViewport();
            const previous = snapshot();
            if ((current.selection.site || "") === (selection.site || "")) {
                next.viewport = previous?.viewport || null;
            }
            current = next;
            changingMap = false;
            window.history.pushState(
                { ...window.history.state, [stateKey]: next }, "", urlForView(selection, tab),
            );
        } else {
            changingMap = false;
            saveViewport();
        }
        return window.dash_clientside.no_update;
    }

    window.addEventListener("popstate", function () {
        if (!owner || ownerPath !== window.location.pathname ||
            !document.getElementById("detail-tabs")) {
            sequence++;
            current = null;
            restoring = null;
            changingMap = false;
            return;
        }
        if (!current) {
            return;
        }
        sequence++;
        restoring = true;
        changingMap = false;
        const el = mapElement();
        el?._lookupAbort?.abort();
        el?._olMap?.getView().cancelAnimations();
        window.dash_clientside.set_props("task-view-navigation", {
            data: {
                task_id: ownerTask,
                search: window.location.search,
                sequence,
            },
        });
    });

    window.dash_clientside = window.dash_clientside || {};
    window.dash_clientside.taskViewHistory = { sync, zoomToSite, beforeMapChange };
})();
