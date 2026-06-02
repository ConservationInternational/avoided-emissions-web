(function () {
    function getGridApi(gridId) {
        const candidates = [window.dash_ag_grid, window.dashAgGrid, window.dag];
        for (const candidate of candidates) {
            if (candidate && typeof candidate.getApi === "function") {
                try {
                    const api = candidate.getApi(gridId);
                    if (api) {
                        return api;
                    }
                } catch (_error) {
                    // Continue fallback checks.
                }
            }
        }
        return null;
    }

    function normalizeSiteId(value) {
        if (value === null || value === undefined) {
            return "";
        }
        return String(value).trim();
    }

    function extractSiteIdFromRowEl(rowEl) {
        if (!rowEl) {
            return "";
        }
        const siteCell = rowEl.querySelector('[col-id="site_id"]');
        if (!siteCell) {
            return "";
        }
        return normalizeSiteId(siteCell.textContent);
    }

    function focusTableRow(tableId, siteId) {
        const normalized = normalizeSiteId(siteId);
        if (!normalized) {
            return;
        }

        const api = getGridApi(tableId);
        if (api && typeof api.forEachNode === "function") {
            let targetNode = null;
            api.forEachNode(function (node) {
                if (normalizeSiteId(node?.data?.site_id) === normalized) {
                    targetNode = node;
                }
            });
            if (targetNode) {
                if (typeof api.ensureIndexVisible === "function") {
                    api.ensureIndexVisible(targetNode.rowIndex, "middle");
                }
                if (typeof api.deselectAll === "function") {
                    api.deselectAll();
                }
                if (typeof targetNode.setSelected === "function") {
                    targetNode.setSelected(true, true);
                }
                return;
            }
        }

        // Fallback: visible rows only.
        const rows = document.querySelectorAll("#" + tableId + " .ag-row");
        rows.forEach(function (rowEl) {
            const isMatch = extractSiteIdFromRowEl(rowEl) === normalized;
            rowEl.classList.toggle("ag-row-selected", isMatch);
            if (isMatch && typeof rowEl.scrollIntoView === "function") {
                rowEl.scrollIntoView({ block: "center", behavior: "smooth" });
            }
        });
    }

    function zoomToFeature(mapEl, map, siteId) {
        const normalized = normalizeSiteId(siteId);
        if (!normalized) {
            return;
        }
        const feature = mapEl?._featureBySiteId?.[normalized];
        if (!feature) {
            return;
        }

        const geom = feature.getGeometry();
        if (!geom) {
            return;
        }
        map.getView().fit(geom.getExtent(), {
            padding: [25, 25, 25, 25],
            duration: 250,
            maxZoom: 12,
        });
    }

    function setSelectedSite(mapEl, siteId) {
        const normalized = normalizeSiteId(siteId);
        mapEl._selectedSiteId = normalized || "";
        if (mapEl?._olSource && typeof mapEl._olSource.changed === "function") {
            mapEl._olSource.changed();
        }
    }

    function bindMapTableSync(mapEl, map, config) {
        if (!config || !config.mapId || !config.tableId || !config.boundFlag) {
            return;
        }
        if (mapEl.id !== config.mapId || mapEl[config.boundFlag]) {
            return;
        }
        mapEl[config.boundFlag] = true;

        map.on("singleclick", function (evt) {
            const feature = map.forEachFeatureAtPixel(evt.pixel, function (f) {
                return f;
            });
            if (!feature) {
                return;
            }

            const siteId = normalizeSiteId(feature.get("site_id"));
            if (!siteId) {
                return;
            }
            setSelectedSite(mapEl, siteId);
            focusTableRow(config.tableId, siteId);
            zoomToFeature(mapEl, map, siteId);
        });

        document.addEventListener("click", function (evt) {
            const rowEl = evt.target.closest("#" + config.tableId + " .ag-row");
            if (!rowEl) {
                return;
            }
            const siteId = extractSiteIdFromRowEl(rowEl);
            if (!siteId) {
                return;
            }
            setSelectedSite(mapEl, siteId);
            zoomToFeature(mapEl, map, siteId);
        });
    }

    function parseGeoJson(el) {
        const raw = el.getAttribute("data-geojson") || "";
        if (!raw) {
            return { type: "FeatureCollection", features: [] };
        }
        try {
            return JSON.parse(raw);
        } catch (_error) {
            return { type: "FeatureCollection", features: [] };
        }
    }

    function getRawGeoJson(el) {
        return el.getAttribute("data-geojson") || "";
    }

    function mapStyle(siteId, selectedSiteId) {
        const isSelected = siteId && siteId === selectedSiteId;
        return new ol.style.Style({
            fill: new ol.style.Fill({
                color: isSelected ? "rgba(245, 124, 0, 0.30)" : "rgba(38, 166, 91, 0.18)",
            }),
            stroke: new ol.style.Stroke({
                color: isSelected ? "#ef6c00" : "#2e7d32",
                width: isSelected ? 3 : 2,
            }),
            text: new ol.style.Text({
                text: siteId || "",
                font: "12px sans-serif",
                fill: new ol.style.Fill({ color: "#1f2937" }),
                stroke: new ol.style.Stroke({ color: "#ffffff", width: 3 }),
                overflow: true,
                padding: [2, 2, 2, 2],
                backgroundFill: new ol.style.Fill({ color: "rgba(255, 255, 255, 0.75)" }),
            }),
        });
    }

    function featureStyle(mapEl) {
        const styleCache = {};
        return function (feature) {
            const siteId = normalizeSiteId(feature.get("site_id"));
            const selectedSiteId = normalizeSiteId(mapEl._selectedSiteId);
            const key = siteId + "|" + selectedSiteId;
            if (!styleCache[key]) {
                styleCache[key] = mapStyle(siteId, selectedSiteId);
            }
            return styleCache[key];
        };
    }

    function fitToAllSites(mapEl, map) {
        const source = mapEl?._olSource;
        if (!source) {
            return;
        }
        const extent = source.getExtent();
        if (!extent || !isFinite(extent[0])) {
            map.getView().setCenter(ol.proj.fromLonLat([0, 0]));
            map.getView().setZoom(2);
            return;
        }
        map.getView().fit(extent, {
            padding: [20, 20, 20, 20],
            duration: 250,
            maxZoom: 12,
        });
    }

    function ensureZoomExtentControl(mapEl, map) {
        if (mapEl._zoomExtentControlAdded) {
            return;
        }

        const button = document.createElement("button");
        button.type = "button";
        button.className = "ol-zoom-extent-btn";
        button.title = "Zoom to all sites";
        button.setAttribute("aria-label", "Zoom to all sites");
        button.textContent = "\u25A1";

        button.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();
            fitToAllSites(mapEl, map);
        });

        const element = document.createElement("div");
        element.className = "ol-unselectable ol-control ol-zoom-extent";
        element.appendChild(button);

        const control = new ol.control.Control({ element: element });
        map.addControl(control);
        mapEl._zoomExtentControlAdded = true;
    }

    function ensureScaleBarControl(mapEl, map) {
        if (mapEl._scaleBarControlAdded) {
            return;
        }

        const control = new ol.control.ScaleLine({
            className: "ol-scale-line ae-scale-line",
            minWidth: 100,
        });
        map.addControl(control);
        mapEl._scaleBarControlAdded = true;
    }

    function ensureDragZoomControl(mapEl, map) {
        if (mapEl._dragZoomControlAdded) {
            return;
        }

        // Add a DragZoom interaction (active only when the button is toggled on).
        const dragZoom = new ol.interaction.DragZoom({
            condition: ol.events.condition.always,
        });
        dragZoom.setActive(false);
        map.addInteraction(dragZoom);

        const button = document.createElement("button");
        button.type = "button";
        button.className = "ol-drag-zoom-btn";
        button.title = "Drag to zoom to region";
        button.setAttribute("aria-label", "Drag to zoom to region");
        button.textContent = "\uD83D\uDD0D"; // 🔍

        var active = false;
        button.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();
            active = !active;
            dragZoom.setActive(active);
            button.classList.toggle("active", active);
        });

        // Deactivate after each zoom.
        dragZoom.on("boxend", function () {
            active = false;
            dragZoom.setActive(false);
            button.classList.remove("active");
        });

        const element = document.createElement("div");
        element.className = "ol-unselectable ol-control ol-drag-zoom";
        element.appendChild(button);

        const control = new ol.control.Control({ element: element });
        map.addControl(control);
        mapEl._dragZoomControlAdded = true;
    }

    // -- Matched-pixel layer --------------------------------------------------

    function pixelStyle(feature) {
        var isTreatment = feature.get("treatment") === true;
        return new ol.style.Style({
            image: new ol.style.Circle({
                radius: 4,
                fill: new ol.style.Fill({
                    color: isTreatment
                        ? "rgba(46, 125, 50, 0.7)"   // green for treatment
                        : "rgba(30, 136, 229, 0.7)",  // blue for control
                }),
                stroke: new ol.style.Stroke({
                    color: isTreatment ? "#1b5e20" : "#0d47a1",
                    width: 1,
                }),
            }),
        });
    }

    function ensurePixelLayerControl(mapEl, map) {
        var taskId = mapEl.getAttribute("data-task-id");
        if (!taskId || mapEl._pixelControlAdded) {
            return;
        }
        mapEl._pixelControlAdded = true;

        var pixelSource = new ol.source.Vector();
        var pixelLayer = new ol.layer.Vector({
            source: pixelSource,
            style: pixelStyle,
            visible: false,
            zIndex: 5,
        });
        map.addLayer(pixelLayer);
        mapEl._pixelLayer = pixelLayer;
        mapEl._pixelSource = pixelSource;

        var button = document.createElement("button");
        button.type = "button";
        button.className = "ol-pixel-toggle-btn";
        button.title = "Toggle matched pixels";
        button.setAttribute("aria-label", "Toggle matched pixels");
        button.innerHTML = "&#9679;&#9679;"; // ●●

        var loaded = false;
        var active = false;

        button.addEventListener("click", function (event) {
            event.preventDefault();
            event.stopPropagation();
            active = !active;
            button.classList.toggle("active", active);
            pixelLayer.setVisible(active);

            if (active && !loaded) {
                loaded = true;
                fetchPixels(mapEl, taskId, pixelSource);
            }
        });

        var element = document.createElement("div");
        element.className = "ol-unselectable ol-control ol-pixel-toggle";
        element.appendChild(button);

        map.addControl(new ol.control.Control({ element: element }));
    }

    function fetchPixels(mapEl, taskId, pixelSource) {
        var url = "/api/matched-pixels/" + encodeURIComponent(taskId);
        fetch(url, { credentials: "same-origin" })
            .then(function (resp) {
                if (!resp.ok) {
                    return null;
                }
                return resp.json();
            })
            .then(function (geojson) {
                if (!geojson || !geojson.features) {
                    return;
                }
                var features = new ol.format.GeoJSON().readFeatures(geojson, {
                    dataProjection: "EPSG:4326",
                    featureProjection: "EPSG:3857",
                });
                pixelSource.addFeatures(features);
                mapEl._allPixelFeatures = features;
                // Apply any pending site filter
                filterPixelsBySite(mapEl, mapEl._pixelFilterSiteId || "");
            })
            .catch(function () {
                // Silently ignore fetch errors.
            });
    }

    function filterPixelsBySite(mapEl, siteId) {
        var source = mapEl._pixelSource;
        if (!source) {
            return;
        }
        var allFeatures = mapEl._allPixelFeatures;
        if (!allFeatures) {
            return;
        }
        var normalized = normalizeSiteId(siteId);
        mapEl._pixelFilterSiteId = normalized;
        source.clear();
        if (!normalized) {
            source.addFeatures(allFeatures);
        } else {
            var filtered = allFeatures.filter(function (f) {
                return normalizeSiteId(f.get("site_id")) === normalized;
            });
            source.addFeatures(filtered);
        }
    }

    function ensureMap(el) {
        if (el._olMap) {
            return el._olMap;
        }

        const source = new ol.source.Vector();
        const vectorLayer = new ol.layer.Vector({ source: source, style: featureStyle(el) });
        const map = new ol.Map({
            target: el,
            layers: [
                new ol.layer.Tile({ source: new ol.source.OSM() }),
                vectorLayer,
            ],
            view: new ol.View({ center: ol.proj.fromLonLat([0, 0]), zoom: 2 }),
        });

        el._olMap = map;
        el._olSource = source;
        ensureZoomExtentControl(el, map);
        ensureScaleBarControl(el, map);
        ensureDragZoomControl(el, map);
        ensurePixelLayerControl(el, map);

        // Listen for zoom-to-site events from the Dash dropdown.
        el.addEventListener("zoom-to-site", function (evt) {
            var siteId = (evt.detail && evt.detail.siteId) || "";
            setSelectedSite(el, siteId);
            if (siteId) {
                zoomToFeature(el, map, siteId);
            } else {
                fitToAllSites(el, map);
            }
            filterPixelsBySite(el, siteId);
        });

        // Add zoom/bounds listener with debounce to emit zoom-aware sampling updates.
        // Only emits for the submit-sites-map to avoid duplicate requests.
        var zoomBoundsTimeout = null;
        var lastEmittedZoom = null;
        var lastEmittedBounds = null;

        function emitZoomBoundsChange() {
            var view = map.getView();
            var zoom = Math.round(view.getZoom());
            var extent = view.calculateExtent(map.getSize());
            var [minx, miny, maxx, maxy] = ol.proj.transformExtent(
                extent,
                "EPSG:3857",
                "EPSG:4326"
            );

            // Only emit if zoom or bounds have meaningfully changed
            var boundsChanged =
                lastEmittedBounds === null ||
                Math.abs(minx - lastEmittedBounds.minx) > 0.01 ||
                Math.abs(miny - lastEmittedBounds.miny) > 0.01 ||
                Math.abs(maxx - lastEmittedBounds.maxx) > 0.01 ||
                Math.abs(maxy - lastEmittedBounds.maxy) > 0.01;
            var zoomChanged = lastEmittedZoom !== zoom;

            if (boundsChanged || zoomChanged) {
                lastEmittedZoom = zoom;
                lastEmittedBounds = { minx: minx, miny: miny, maxx: maxx, maxy: maxy };

                // Find the Dash zoom-bounds-store and emit event
                var store = document.getElementById("zoom-bounds-store");
                if (store && store.dataset) {
                    // Emit a custom event that Dash can listen for
                    el.dispatchEvent(
                        new CustomEvent("map-zoom-bounds-changed", {
                            bubbles: true,
                            detail: {
                                zoom: zoom,
                                bounds: lastEmittedBounds,
                            },
                        })
                    );
                }
            }
        }

        map.getView().on("change:resolution", function () {
            // Clear existing timeout and set a new one (debounce)
            if (zoomBoundsTimeout !== null) {
                clearTimeout(zoomBoundsTimeout);
            }
            zoomBoundsTimeout = setTimeout(emitZoomBoundsChange, 500); // 500ms debounce
        });

        map.on("moveend", function () {
            // Also fire immediately on moveend in case user just stopped panning
            if (zoomBoundsTimeout !== null) {
                clearTimeout(zoomBoundsTimeout);
            }
            zoomBoundsTimeout = setTimeout(emitZoomBoundsChange, 300); // 300ms debounce
        });

        // Notify other scripts (e.g. COG layer control) that a map is ready.
        el.dispatchEvent(
            new CustomEvent("ol-map-ready", { bubbles: true, detail: { map: map } })
        );

        return map;
    }

    function renderElement(el) {
        if (!window.ol || !el || !el.classList.contains("ol-sites-map")) {
            return;
        }

        const height = el.getAttribute("data-height") || "260px";
        if (!el.style.height) {
            el.style.height = height;
        }
        el.style.width = "100%";

        const map = ensureMap(el);

        // Watch for user resize (CSS resize: vertical) and update the map.
        if (!el._resizeObserverAttached) {
            el._resizeObserverAttached = true;
            var ro = new ResizeObserver(function () {
                map.updateSize();
            });
            ro.observe(el);
        }
        const source = el._olSource;
        const rawGeojson = getRawGeoJson(el);
        const dataChanged = el._lastGeojsonRaw !== rawGeojson;
        el._lastGeojsonRaw = rawGeojson;
        source.clear();

        const fc = parseGeoJson(el);
        const features = new ol.format.GeoJSON().readFeatures(fc, {
            dataProjection: "EPSG:4326",
            featureProjection: "EPSG:3857",
        });
        source.addFeatures(features);

        const featureBySiteId = {};
        features.forEach(function (feature) {
            const siteId = normalizeSiteId(feature.get("site_id"));
            if (siteId) {
                featureBySiteId[siteId] = feature;
            }
        });
        el._featureBySiteId = featureBySiteId;

        const currentSelected = normalizeSiteId(el._selectedSiteId);
        if (currentSelected && !featureBySiteId[currentSelected]) {
            setSelectedSite(el, "");
        } else if (currentSelected) {
            setSelectedSite(el, currentSelected);
        }

        bindMapTableSync(el, map, {
            mapId: "submit-sites-map",
            tableId: "site-preview-table",
            boundFlag: "_submitSyncBound",
        });
        bindMapTableSync(el, map, {
            mapId: "task-sites-map",
            tableId: "results-totals-table",
            boundFlag: "_resultsSyncBound",
        });

        if (dataChanged && features.length > 0) {
            map.getView().fit(source.getExtent(), {
                padding: [20, 20, 20, 20],
                duration: 250,
                maxZoom: 12,
            });
        } else if (dataChanged) {
            map.getView().setCenter(ol.proj.fromLonLat([0, 0]));
            map.getView().setZoom(2);
        }

        setTimeout(function () {
            map.updateSize();
        }, 0);
    }

    function renderAll() {
        document.querySelectorAll(".ol-sites-map").forEach(renderElement);
    }

    const observer = new MutationObserver(function (mutations) {
        let shouldRender = false;
        for (const mutation of mutations) {
            if (mutation.type === "childList" && mutation.addedNodes.length > 0) {
                for (const node of mutation.addedNodes) {
                    if (!(node instanceof Element)) {
                        continue;
                    }
                    if (
                        node.classList.contains("ol-sites-map") ||
                        node.querySelector?.(".ol-sites-map")
                    ) {
                        shouldRender = true;
                        break;
                    }
                }
                if (shouldRender) {
                    break;
                }
            }
            if (
                mutation.type === "attributes" &&
                (mutation.attributeName === "data-geojson" || mutation.attributeName === "data-height")
            ) {
                shouldRender = true;
                break;
            }
        }
        if (shouldRender) {
            renderAll();
        }
    });

    function boot() {
        renderAll();
        observer.observe(document.body, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ["data-geojson", "data-height"],
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }
})();
