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
        if (mapEl?._olVtLayer && typeof mapEl._olVtLayer.changed === "function") {
            mapEl._olVtLayer.changed();
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

    function mapStylePoint(areaHa, siteId, selectedSiteId) {
        const isSelected = siteId && siteId === selectedSiteId;
        // Log-scale radius: 3 px for tiny sites, up to 12 px for large ones.
        // log10(1) → 3 px; log10(10000) → ~12 px.
        const radius =
            areaHa > 0
                ? Math.min(12, Math.max(3, 2.25 * Math.log10(areaHa) + 3))
                : 4;
        return new ol.style.Style({
            image: new ol.style.Circle({
                radius: radius,
                fill: new ol.style.Fill({
                    color: isSelected ? "rgba(245, 124, 0, 0.70)" : "rgba(38, 166, 91, 0.60)",
                }),
                stroke: new ol.style.Stroke({
                    color: isSelected ? "#ef6c00" : "#2e7d32",
                    width: isSelected ? 2 : 1.5,
                }),
            }),
        });
    }

    function featureStyle(mapEl) {
        const styleCache = {};
        return function (feature) {
            const geomType = feature.getGeometry()?.getType();
            const siteId = normalizeSiteId(feature.get("site_id"));
            const selectedSiteId = normalizeSiteId(mapEl._selectedSiteId);
            const key = geomType + "|" + siteId + "|" + selectedSiteId;
            if (!styleCache[key]) {
                if (geomType === "Point") {
                    const areaHa = feature.get("area_ha") || 0;
                    styleCache[key] = mapStylePoint(areaHa, siteId, selectedSiteId);
                } else {
                    styleCache[key] = mapStyle(siteId, selectedSiteId);
                }
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

    // Custom tileLoadFunction so that session cookies are always forwarded
    // (same-origin fetch) and 204 No Content responses are handled gracefully.
    function makeMvtTileLoadFunction() {
        return function (tile, url) {
            tile.setLoader(function (extent, resolution, projection) {
                fetch(url, { credentials: "same-origin" })
                    .then(function (response) {
                        if (!response.ok || response.status === 204) {
                            tile.setFeatures([]);
                            return null;
                        }
                        return response.arrayBuffer();
                    })
                    .then(function (data) {
                        if (!data) {
                            return;
                        }
                        const format = tile.getFormat();
                        const features = format.readFeatures(data, {
                            extent: extent,
                            featureProjection: projection,
                        });
                        tile.setFeatures(features);
                    })
                    .catch(function () {
                        tile.setFeatures([]);
                    });
            });
        };
    }

    function ensureMap(el) {
        if (el._olMap) {
            return el._olMap;
        }

        // Companion vector source: holds centroid Points fetched asynchronously
        // from data-centroids-url.  Used exclusively for _featureBySiteId lookups
        // and zoom-to-feature — never rendered visibly when tile-url is set.
        const source = new ol.source.Vector();
        const tileUrl = el.getAttribute("data-tile-url");

        const layers = [new ol.layer.Tile({ source: new ol.source.OSM() })];
        let vtLayer = null;

        if (tileUrl) {
            // MVT rendering layer — tiles served directly by Flask/PostGIS.
            vtLayer = new ol.layer.VectorTile({
                source: new ol.source.VectorTile({
                    format: new ol.format.MVT(),
                    url: tileUrl,
                    tileLoadFunction: makeMvtTileLoadFunction(),
                }),
                style: featureStyle(el),
            });
            el._olVtLayer = vtLayer;
            layers.push(vtLayer);
            // Companion source is hidden — used only for lookups.
            layers.push(new ol.layer.Vector({ source: source, visible: false }));
        } else {
            // Legacy GeoJSON path: companion source IS the visible layer.
            layers.push(new ol.layer.Vector({ source: source, style: featureStyle(el) }));
        }

        const map = new ol.Map({
            target: el,
            layers: layers,
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

        const tileUrl = el.getAttribute("data-tile-url");

        if (tileUrl) {
            // ── MVT tile path ─────────────────────────────────────────────────
            // The VectorTile layer fetches tiles directly; the companion hidden
            // source provides _featureBySiteId lookups (centroid Points loaded
            // asynchronously from data-centroids-url).

            // Detect tile URL change (e.g. user selected a different site set).
            // Re-initialise the VectorTile source and reset companion state.
            if (tileUrl !== el._lastTileUrl) {
                el._lastTileUrl = tileUrl;
                el._initialFitDone = false;
                el._lastCentroidsUrl = null;
                el._featureBySiteId = {};
                el._emissionsBySiteId = {};
                el._olSource.clear();
                if (el._olVtLayer) {
                    el._olVtLayer.setSource(
                        new ol.source.VectorTile({
                            format: new ol.format.MVT(),
                            url: tileUrl,
                            tileLoadFunction: makeMvtTileLoadFunction(),
                        })
                    );
                }
            }

            // Build _emissionsBySiteId from the companion data-geojson attribute
            // (centroid GeoJSON with emissions for the results map).
            const rawGeoJson = getRawGeoJson(el);
            if (rawGeoJson && rawGeoJson !== el._lastEmissionsGeojson) {
                el._lastEmissionsGeojson = rawGeoJson;
                try {
                    const fc = JSON.parse(rawGeoJson);
                    const emissionsBySiteId = {};
                    for (const feat of fc.features || []) {
                        const sid = normalizeSiteId(feat.properties?.site_id);
                        if (sid) {
                            emissionsBySiteId[sid] = feat.properties;
                        }
                    }
                    el._emissionsBySiteId = emissionsBySiteId;
                } catch (_e) {
                    el._emissionsBySiteId = {};
                }
            }

            // Load centroids asynchronously into the hidden companion source.
            const centroidsUrl = el.getAttribute("data-centroids-url");
            if (centroidsUrl && centroidsUrl !== el._lastCentroidsUrl) {
                el._lastCentroidsUrl = centroidsUrl;
                el._featureBySiteId = {};
                const source = el._olSource;
                source.clear();

                fetch(centroidsUrl, { credentials: "same-origin" })
                    .then(function (resp) {
                        if (!resp.ok) {
                            return null;
                        }
                        return resp.json();
                    })
                    .then(function (fc) {
                        if (!fc || !fc.features) {
                            return;
                        }
                        const features = new ol.format.GeoJSON().readFeatures(fc, {
                            dataProjection: "EPSG:4326",
                            featureProjection: "EPSG:3857",
                        });
                        source.clear();
                        source.addFeatures(features);
                        const featureBySiteId = {};
                        features.forEach(function (f) {
                            const sid = normalizeSiteId(f.get("site_id"));
                            if (sid) {
                                featureBySiteId[sid] = f;
                            }
                        });
                        el._featureBySiteId = featureBySiteId;

                        // Restore any pending selection.
                        const currentSelected = normalizeSiteId(el._selectedSiteId);
                        if (currentSelected && featureBySiteId[currentSelected]) {
                            setSelectedSite(el, currentSelected);
                        }

                        // Fit view to bounds once (on first load or site-set change).
                        if (!el._initialFitDone) {
                            el._initialFitDone = true;
                            const boundsAttr = el.getAttribute("data-bounds");
                            if (boundsAttr) {
                                try {
                                    const b = JSON.parse(boundsAttr);
                                    const extent4326 = [
                                        b.west ?? b.minx ?? -180,
                                        b.south ?? b.miny ?? -90,
                                        b.east ?? b.maxx ?? 180,
                                        b.north ?? b.maxy ?? 90,
                                    ];
                                    const extent3857 = ol.proj.transformExtent(
                                        extent4326,
                                        "EPSG:4326",
                                        "EPSG:3857"
                                    );
                                    map.getView().fit(extent3857, {
                                        padding: [20, 20, 20, 20],
                                        duration: 250,
                                        maxZoom: 12,
                                    });
                                } catch (_e) {
                                    fitToAllSites(el, map);
                                }
                            } else if (features.length > 0) {
                                fitToAllSites(el, map);
                            }
                        }
                    })
                    .catch(function () {
                        // Silently ignore; map still renders via MVT tiles.
                    });
            }
        } else {
            // ── Legacy GeoJSON path ───────────────────────────────────────────
            // Used when no tile URL is set (e.g. adopted tasks with no site_set_id).
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
                (mutation.attributeName === "data-geojson" ||
                    mutation.attributeName === "data-tile-url" ||
                    mutation.attributeName === "data-centroids-url" ||
                    mutation.attributeName === "data-height")
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
            attributeFilter: ["data-geojson", "data-tile-url", "data-centroids-url", "data-height"],
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }
})();
