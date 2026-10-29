(function () {
    const mountedMaps = new Set();
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
        const rowId = rowEl.getAttribute("row-id");
        if (rowId && !/^\d+$/.test(rowId)) {
            return normalizeSiteId(rowId);
        }
        const siteCell = rowEl.querySelector('[col-id="feature_id"], [col-id="site_id"]');
        if (!siteCell) {
            return "";
        }
        return normalizeSiteId(siteCell.textContent);
    }

    function focusTableRow(tableId, siteId, mapEl) {
        const normalized = normalizeSiteId(siteId);
        if (!normalized) {
            return;
        }

        const api = getGridApi(tableId);
        if (api) {
            // With Infinite Row Model the grid uses getRowId (= site_id) so
            // getRowNode() is an O(1) lookup into the loaded block cache.
            // If the row is not yet loaded (outside the current virtual window)
            // the node is undefined — we deselect and return silently rather
            // than scanning 150 k rows with forEachNode.
            const node =
                typeof api.getRowNode === "function" ? api.getRowNode(normalized) : null;
            if (node) {
                if (typeof api.ensureIndexVisible === "function") {
                    api.ensureIndexVisible(node.rowIndex, "middle");
                }
                if (typeof api.deselectAll === "function") {
                    api.deselectAll();
                }
                if (typeof node.setSelected === "function") {
                    node.setSelected(true, true);
                }
            } else if (tableId === "results-totals-table") {
                api.deselectAll();
                if (mapEl) {
                    showMapStatus(mapEl, "No result row is available for this original site. See the preparation report for exclusions.");
                }
            } else if (typeof api.setFilterModel === "function") {
                api.deselectAll();
                // Bring an unloaded feature into the server-backed first block.
                // Filtering by immutable identity also works for duplicate display IDs.
                const onLoaded = function () {
                    const selected = api.getRowNode(normalized);
                    if (selected) {
                        selected.setSelected(true, true);
                        api.ensureIndexVisible(selected.rowIndex, "middle");
                        api.removeEventListener("modelUpdated", onLoaded);
                        delete api._siteFocusListener;
                    }
                };
                if (api._siteFocusListener) {
                    api.removeEventListener("modelUpdated", api._siteFocusListener);
                }
                api._siteFocusListener = onLoaded;
                api.addEventListener("modelUpdated", onLoaded);
                api.setFilterModel({
                    feature_id: { filterType: "text", type: "equals", filter: normalized },
                });
            }
            return;
        }

        // Fallback: visible rows only (no AG Grid API available).
        const rows = document.querySelectorAll("#" + tableId + " .ag-row");
        rows.forEach(function (rowEl) {
            const isMatch = extractSiteIdFromRowEl(rowEl) === normalized;
            rowEl.classList.toggle("ag-row-selected", isMatch);
            if (isMatch && typeof rowEl.scrollIntoView === "function") {
                rowEl.scrollIntoView({ block: "center", behavior: "smooth" });
            }
        });
    }

    async function zoomToFeature(mapEl, map, siteId) {
        const normalized = normalizeSiteId(siteId);
        if (!normalized) {
            return;
        }
        const tileUrl = mapEl.getAttribute("data-tile-url") || "";
        const collection = tileUrl.match(/\/api\/(sites|task-sites)-tiles\/([^/]+)\//);
        if (collection) {
            mapEl._lookupAbort?.abort();
            const controller = new AbortController();
            mapEl._lookupAbort = controller;
            const generation = mapEl._requestGeneration;
            const route = collection[1] === "sites" ? "site-feature" : "task-site-feature";
            try {
                const response = await fetch(
                    "/api/" + route + "/" + collection[2] + "/" + encodeURIComponent(normalized),
                    { credentials: "same-origin", signal: controller.signal }
                );
                if (controller.signal.aborted || generation !== mapEl._requestGeneration || !mapEl.isConnected) {
                    return;
                }
                if (!response.ok || response.redirected) {
                    showMapStatus(mapEl, response.status === 401 || response.redirected
                        ? "Session expired. Sign in again to load the map." : "Could not load site details. Select the site to retry.");
                    return;
                }
                const detail = await response.json();
                if (controller.signal.aborted || generation !== mapEl._requestGeneration || !mapEl.isConnected) {
                    return;
                }
                fitBounds(map, detail.bounds);
            } catch (error) {
                if (error.name !== "AbortError" && !controller.signal.aborted &&
                    generation === mapEl._requestGeneration && mapEl.isConnected) {
                    showMapStatus(mapEl, "Could not load site details. Select the site to retry.");
                }
            }
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
            focusTableRow(config.tableId, siteId, mapEl);
            zoomToFeature(mapEl, map, siteId);
        });

        const rowClick = function (evt) {
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
        };
        document.addEventListener("click", rowClick);
        mapEl._cleanup.push(function () {
            document.removeEventListener("click", rowClick);
            const api = getGridApi(config.tableId);
            if (api?._siteFocusListener) {
                api.removeEventListener("modelUpdated", api._siteFocusListener);
                delete api._siteFocusListener;
            }
        });
    }

    function bindGridSelection(el, map, tableId, api) {
        if (!api || !el.isConnected || el._gridSelectionBound) {
            return;
        }
        el._gridSelectionBound = true;
        const onSelection = function () {
            const row = api.getSelectedRows()?.[0];
            const siteId = normalizeSiteId(row?.feature_id || row?.site_id);
            if (siteId) {
                setSelectedSite(el, siteId);
                zoomToFeature(el, map, siteId);
                filterPixelsBySite(el, siteId);
            }
        };
        api.addEventListener("selectionChanged", onSelection);
        el._cleanup.push(function () { api.removeEventListener("selectionChanged", onSelection); });
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

    function mapStyle(siteId, selectedSiteId, showLabel, displaySiteId) {
        const isSelected = siteId && siteId === selectedSiteId;
        const styleOptions = {
            fill: new ol.style.Fill({
                color: isSelected ? "rgba(245, 124, 0, 0.30)" : "rgba(38, 166, 91, 0.18)",
            }),
            stroke: new ol.style.Stroke({
                color: isSelected ? "#ef6c00" : "#2e7d32",
                width: isSelected ? 3 : 2,
            }),
        };

        if (showLabel) {
            styleOptions.text = new ol.style.Text({
                text: displaySiteId || siteId || "",
                font: "12px sans-serif",
                fill: new ol.style.Fill({ color: "#1f2937" }),
                stroke: new ol.style.Stroke({ color: "#ffffff", width: 3 }),
                overflow: true,
                padding: [2, 2, 2, 2],
                backgroundFill: new ol.style.Fill({ color: "rgba(255, 255, 255, 0.55)" }),
            });
        }

        return new ol.style.Style(styleOptions);
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
        const styleCache = new Map();
        const minPolygonLabelZoom = 9;
        return function (feature) {
            const geomType = feature.getGeometry()?.getType();
            const siteId = normalizeSiteId(feature.get("site_id"));
            const selectedSiteId = normalizeSiteId(mapEl._selectedSiteId);
            const zoom = mapEl._olMap?.getView()?.getZoom() || 0;
            const zoomBucket = Math.floor(zoom);
            const showPolygonLabel =
                geomType !== "Point" &&
                (zoomBucket >= minPolygonLabelZoom || siteId === selectedSiteId);
            const selected = Boolean(siteId && siteId === selectedSiteId);
            const displayId = normalizeSiteId(feature.get("display_site_id")) || siteId;
            const areaHa = feature.get("area_ha") || 0;
            const radius = Math.round(areaHa > 0 ? Math.min(12, Math.max(3, 2.25 * Math.log10(areaHa) + 3)) : 4);
            const key = [geomType, selected, radius, showPolygonLabel ? displayId : ""].join("|");
            if (!styleCache.has(key)) {
                if (styleCache.size >= 256) {
                    styleCache.clear();
                }
                if (geomType === "Point") {
                    styleCache.set(key, mapStylePoint(10 ** ((radius - 3) / 2.25), siteId, selectedSiteId));
                } else {
                    styleCache.set(key, mapStyle(siteId, selectedSiteId, showPolygonLabel, displayId));
                }
            }
            return styleCache.get(key);
        };
    }

    function fitBounds(map, bounds) {
        if (!bounds) {
            return false;
        }
        const extent = [bounds.west ?? bounds.minx, bounds.south ?? bounds.miny,
            bounds.east ?? bounds.maxx, bounds.north ?? bounds.maxy];
        if (!extent.every(Number.isFinite)) {
            return false;
        }
        map.getView().fit(ol.proj.transformExtent(extent, "EPSG:4326", "EPSG:3857"),
            { padding: [25, 25, 25, 25], duration: 250, maxZoom: 16 });
        return true;
    }

    function fitToAllSites(mapEl, map) {
        try {
            if (fitBounds(map, JSON.parse(mapEl.getAttribute("data-bounds") || "null"))) {
                return;
            }
        } catch (_error) {
            // Legacy maps can fit to their local source instead.
        }
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
        mapEl._pixelAbort?.abort();
        const controller = new AbortController();
        mapEl._pixelAbort = controller;
        const generation = mapEl._requestGeneration;
        fetch(url, { credentials: "same-origin", signal: controller.signal })
            .then(function (resp) {
                if (!resp.ok) {
                    return null;
                }
                return resp.json();
            })
            .then(function (geojson) {
                if (!geojson || !geojson.features || controller.signal.aborted ||
                    generation !== mapEl._requestGeneration || !mapEl.isConnected) {
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
    function showMapStatus(el, message) {
        if (!el._mapStatus) {
            const element = document.createElement("div");
            element.className = "ol-map-status bg-light p-2";
            element.style.cssText = "bottom:10px;left:10px;max-width:90%;z-index:10";
            element.setAttribute("role", "status");
            element.setAttribute("aria-live", "polite");
            const text = document.createElement("span");
            element.appendChild(text);
            const retry = document.createElement("button");
            retry.type = "button";
            retry.textContent = "Retry map";
            retry.className = "btn btn-sm btn-outline-secondary ms-2";
            retry.addEventListener("click", function () {
                showMapStatus(el, "");
                el._olVtLayer?.getSource()?.refresh();
            });
            element.appendChild(retry);
            el._olMap.addControl(new ol.control.Control({ element }));
            el._mapStatus = { element, text };
        }
        el._mapStatus.text.textContent = message;
        el._mapStatus.element.style.display = message ? "" : "none";
    }

    function makeMvtTileLoadFunction(el) {
        const generation = el._requestGeneration;
        return function (tile, url) {
            tile.setLoader(async function (extent, resolution, projection) {
                const controller = new AbortController();
                el._tileAborts.add(controller);
                try {
                    for (let attempt = 0; attempt < 3; attempt++) {
                        try {
                            const response = await fetch(url, {
                                credentials: "same-origin", signal: controller.signal,
                            });
                            if (generation !== el._requestGeneration || controller.signal.aborted) {
                                return;
                            }
                            if (response.status === 401 || response.redirected) {
                                showMapStatus(el, "Session expired. Sign in again to load the map.");
                                tile.setState(3); // OpenLayers TileState.ERROR, never an empty success.
                                return;
                            }
                            if (!response.ok) {
                                throw new Error("Tile request failed: " + response.status);
                            }
                            const data = await response.arrayBuffer();
                            if (generation !== el._requestGeneration || controller.signal.aborted) {
                                return;
                            }
                            tile.setFeatures(data.byteLength ? tile.getFormat().readFeatures(data, {
                                extent, featureProjection: projection,
                            }) : []);
                            return;
                        } catch (error) {
                            if (error.name === "AbortError" || generation !== el._requestGeneration) {
                                return;
                            }
                            if (attempt === 2) {
                                throw error;
                            }
                            await new Promise(resolve => setTimeout(resolve, 250 * (attempt + 1)));
                        }
                    }
                } catch (_error) {
                    if (!controller.signal.aborted && generation === el._requestGeneration) {
                        showMapStatus(el, "Some map tiles could not load. Check your connection and retry.");
                        tile.setState(3);
                    }
                } finally {
                    el._tileAborts.delete(controller);
                }
            });
        };
    }

    function ensureMap(el) {
        if (el._olMap) {
            return el._olMap;
        }

        el._cleanup = [];
        el._tileAborts = new Set();
        el._requestGeneration = 0;
        mountedMaps.add(el);
        // Local source is only used by legacy GeoJSON maps.
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
                    tileLoadFunction: makeMvtTileLoadFunction(el),
                }),
                style: featureStyle(el),
                declutter: true,
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
        const clearButton = document.createElement("button");
        clearButton.type = "button";
        clearButton.textContent = "Clear selection";
        clearButton.style.cssText = "width:auto;height:auto;padding:3px 8px";
        clearButton.setAttribute("aria-label", "Clear site selection and table filters");
        clearButton.addEventListener("click", function () {
            setSelectedSite(el, "");
            const tableId = el.id === "submit-sites-map" ? "site-preview-table" : "results-totals-table";
            const api = getGridApi(tableId);
            api?.setFilterModel(null);
            api?.deselectAll();
            fitToAllSites(el, map);
        });
        const clearElement = document.createElement("div");
        clearElement.className = "ol-control";
        clearElement.style.cssText = "top:8px;right:8px;left:auto";
        clearElement.appendChild(clearButton);
        map.addControl(new ol.control.Control({ element: clearElement }));

        // Listen for zoom-to-site events from the Dash dropdown.
        const zoomToSite = function (evt) {
            var siteId = (evt.detail && evt.detail.siteId) || "";
            setSelectedSite(el, siteId);
            if (siteId) {
                zoomToFeature(el, map, siteId);
            } else {
                fitToAllSites(el, map);
            }
            filterPixelsBySite(el, siteId);
        };
        el.addEventListener("zoom-to-site", zoomToSite);
        el._cleanup.push(function () { el.removeEventListener("zoom-to-site", zoomToSite); });

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
            el._cleanup.push(function () { ro.disconnect(); });
        }

        const tileUrl = el.getAttribute("data-tile-url");

        if (tileUrl) {
            // ── MVT tile path ─────────────────────────────────────────────────
            // Detect tile URL change (e.g. user selected a different site set).
            // Re-initialise the VectorTile source and reset companion state.
            if (tileUrl !== el._lastTileUrl) {
                el._lastTileUrl = tileUrl;
                el._requestGeneration++;
                el._lookupAbort?.abort();
                el._tileAborts.forEach(controller => controller.abort());
                if (el._mapStatus) {
                    showMapStatus(el, "");
                }
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
                            tileLoadFunction: makeMvtTileLoadFunction(el),
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

            if (!el._initialFitDone) {
                el._initialFitDone = true;
                fitToAllSites(el, map);
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
        const tableId = el.id === "submit-sites-map" ? "site-preview-table" : "results-totals-table";
        const api = getGridApi(tableId);
        if (api) {
            bindGridSelection(el, map, tableId, api);
        } else if (!el._gridSelectionPending && window.dash_ag_grid?.getApiAsync) {
            el._gridSelectionPending = true;
            window.dash_ag_grid.getApiAsync(tableId)
                .then(api => bindGridSelection(el, map, tableId, api))
                .catch(() => { el._gridSelectionPending = false; });
        }

        setTimeout(function () {
            map.updateSize();
        }, 0);
    }

    function renderAll() {
        for (const el of mountedMaps) {
            if (!el.isConnected) {
                el._requestGeneration++;
                el._lookupAbort?.abort();
                el._pixelAbort?.abort();
                el._tileAborts.forEach(controller => controller.abort());
                el._cleanup.forEach(cleanup => cleanup());
                el._olMap.setTarget(null);
                el._olMap.dispose();
                mountedMaps.delete(el);
            }
        }
        document.querySelectorAll(".ol-sites-map").forEach(renderElement);
    }

    const observer = new MutationObserver(function (mutations) {
        let shouldRender = false;
        for (const mutation of mutations) {
            if (mutation.type === "childList" && mutation.removedNodes.length > 0) {
                shouldRender = Array.from(mountedMaps).some(el => !el.isConnected);
            }
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
