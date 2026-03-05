/**
 * Covariate COG layer control for OpenLayers maps.
 *
 * Fetches available covariate layers from /api/cog-layers and renders
 * them as toggleable WebGLTile overlays on OL maps tagged with
 * ``data-enable-cog-layers="true"``.  A collapsible layer control
 * panel is added to the map.
 *
 * Each COG is loaded via ol.source.GeoTIFF using HTTP range requests
 * against pre-signed S3 URLs.  Styles (color ramps, opacity) come from
 * the server-side layer_config.py and are applied as WebGL expressions.
 */
(function () {
    "use strict";

    var LOG_PREFIX = "[COG layers] ";

    // ?????? Feature detection ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????

    function hasGeoTIFFSupport() {
        return (
            window.ol &&
            ol.source &&
            typeof ol.source.GeoTIFF === "function" &&
            ol.layer &&
            typeof ol.layer.WebGLTile === "function"
        );
    }

    // ?????? Helpers ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????

    function buildColorExpression(style) {
        var band = ["band", 1];
        var minVal = style.min_value != null ? style.min_value : 0;
        var maxVal = style.max_value != null ? style.max_value : 1;
        var range = maxVal - minVal || 1;

        var normalized = ["/", ["-", band, minVal], range];
        normalized = ["clamp", normalized, 0, 1];

        var stops = style.color_stops || [];

        if (style.type === "categorical" && stops.length > 0) {
            var matchExpr = ["match", band];
            for (var i = 0; i < stops.length; i++) {
                matchExpr.push(stops[i][0]);
                matchExpr.push([
                    "color",
                    stops[i][1],
                    stops[i][2],
                    stops[i][3],
                    stops[i][4] != null ? stops[i][4] : 1,
                ]);
            }
            matchExpr.push(["color", 0, 0, 0, 0]);
            return matchExpr;
        }

        if (stops.length < 2) {
            return ["color", 128, 128, 128, 0.5];
        }

        var interpExpr = ["interpolate", ["linear"], normalized];
        for (var j = 0; j < stops.length; j++) {
            interpExpr.push(stops[j][0]);
            interpExpr.push([
                "color",
                stops[j][1],
                stops[j][2],
                stops[j][3],
                stops[j][4] != null ? stops[j][4] : 1,
            ]);
        }
        return interpExpr;
    }

    function wrapNodata(colorExpr, nodataValue) {
        if (nodataValue == null) {
            return colorExpr;
        }
        return [
            "case",
            ["==", ["band", 1], nodataValue],
            ["color", 0, 0, 0, 0],
            colorExpr,
        ];
    }

    /**
     * Build a small legend element for a layer based on its style config.
     * Returns a DOM element (hidden by default).
     */
    function buildLegendElement(style) {
        var container = document.createElement("div");
        container.className = "ae-layer-legend";
        container.style.display = "none";

        var stops = style.color_stops || [];
        if (stops.length < 2) {
            return container;
        }

        if (style.type === "categorical") {
            // Categorical: small colored squares with labels
            for (var i = 0; i < stops.length; i++) {
                var item = document.createElement("div");
                item.className = "ae-legend-cat-item";

                var swatch = document.createElement("span");
                swatch.className = "ae-legend-swatch";
                swatch.style.backgroundColor =
                    "rgb(" + stops[i][1] + "," + stops[i][2] + "," + stops[i][3] + ")";
                item.appendChild(swatch);

                var lbl = document.createElement("span");
                lbl.className = "ae-legend-cat-label";
                lbl.textContent = String(stops[i][0]);
                item.appendChild(lbl);

                container.appendChild(item);
            }
        } else {
            // Continuous: gradient bar with min/max labels
            var gradientParts = [];
            for (var j = 0; j < stops.length; j++) {
                var pct = Math.round(stops[j][0] * 100);
                var rgb = "rgb(" + stops[j][1] + "," + stops[j][2] + "," + stops[j][3] + ")";
                gradientParts.push(rgb + " " + pct + "%");
            }

            var bar = document.createElement("div");
            bar.className = "ae-legend-gradient";
            bar.style.background = "linear-gradient(to right, " + gradientParts.join(", ") + ")";
            container.appendChild(bar);

            var labels = document.createElement("div");
            labels.className = "ae-legend-labels";
            var minVal = style.min_value != null ? style.min_value : 0;
            var maxVal = style.max_value != null ? style.max_value : 1;

            var minLbl = document.createElement("span");
            minLbl.textContent = formatLegendNum(minVal);
            labels.appendChild(minLbl);

            var maxLbl = document.createElement("span");
            maxLbl.textContent = formatLegendNum(maxVal);
            labels.appendChild(maxLbl);

            container.appendChild(labels);
        }

        return container;
    }

    function formatLegendNum(v) {
        if (Math.abs(v) >= 1000) {
            return v.toLocaleString(undefined, { maximumFractionDigits: 0 });
        }
        if (Math.abs(v) < 0.01 && v !== 0) {
            return v.toExponential(1);
        }
        return String(Math.round(v * 100) / 100);
    }

    // ?????? Vector layer helpers ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????

    // Colour palette for vector overlays (assigned per-layer).
    var VECTOR_COLORS = [
        [31, 119, 180],   // blue
        [255, 127, 14],   // orange
        [44, 160, 44],    // green
        [214, 39, 40],    // red
        [148, 103, 189],  // purple
    ];

    /**
     * Compute simplification tolerance from the current map view.
     * At low zoom we simplify aggressively; at high zoom we keep detail.
     */
    function simplifyTolerance(map) {
        var view = map.getView();
        var zoom = view.getZoom() || 3;
        if (zoom >= 12) return 0.0001;
        if (zoom >= 8) return 0.001;
        if (zoom >= 5) return 0.01;
        return 0.05;
    }

    /**
     * Return the current map extent as "west,south,east,north" in EPSG:4326.
     */
    function bboxString(map) {
        var extent = map.getView().calculateExtent(map.getSize());
        var transformed = ol.proj.transformExtent(extent, "EPSG:3857", "EPSG:4326");
        return transformed.map(function (v) { return v.toFixed(6); }).join(",");
    }

    /**
     * Build an OpenLayers style for a vector polygon layer.
     * Stroke colour comes from colorIdx; text labels use the "name" property.
     */
    function buildVectorStyle(colorIdx) {
        var c = VECTOR_COLORS[colorIdx % VECTOR_COLORS.length];
        var strokeColor = "rgba(" + c[0] + "," + c[1] + "," + c[2] + ",0.85)";
        var fillColor = "rgba(" + c[0] + "," + c[1] + "," + c[2] + ",0.08)";

        return function (feature, resolution) {
            var name = feature.get("name") || "";
            var fontSize = resolution < 500 ? "11px" : "10px";
            var showLabel = resolution < 5000;

            return new ol.style.Style({
                stroke: new ol.style.Stroke({ color: strokeColor, width: 1.5 }),
                fill: new ol.style.Fill({ color: fillColor }),
                text: showLabel
                    ? new ol.style.Text({
                        text: name,
                        font: fontSize + " sans-serif",
                        fill: new ol.style.Fill({ color: "#222" }),
                        stroke: new ol.style.Stroke({ color: "#fff", width: 3 }),
                        overflow: true,
                        placement: "point",
                    })
                    : undefined,
            });
        };
    }

    /**
     * Fetch GeoJSON from /api/vector-layer/<name> and replace the source
     * features on the given vector layer.
     */
    function loadVectorFeatures(map, layer, layerName) {
        var bbox = bboxString(map);
        var tol = simplifyTolerance(map);
        var url = "/api/vector-layer/" + layerName + "?bbox=" + bbox + "&simplify=" + tol;

        fetch(url, { credentials: "same-origin" })
            .then(function (resp) {
                if (!resp.ok) throw new Error("HTTP " + resp.status);
                return resp.json();
            })
            .then(function (geojson) {
                var fmt = new ol.format.GeoJSON();
                var features = fmt.readFeatures(geojson, {
                    featureProjection: "EPSG:3857",
                });
                layer.getSource().clear();
                layer.getSource().addFeatures(features);
                console.log(
                    LOG_PREFIX + "loaded " + features.length +
                    " vector features for " + layerName
                );
            })
            .catch(function (err) {
                console.warn(LOG_PREFIX + "vector fetch failed for " + layerName, err);
            });
    }

    // ?????? Layer cache ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????

    // Per-map caches: mapEl ??? { name ??? { layer, def } }
    // Layers are created lazily the first time a checkbox is toggled on.

    var _layerDataPromise = null;

    function fetchLayerData() {
        if (_layerDataPromise) {
            return _layerDataPromise;
        }
        console.log(LOG_PREFIX + "fetching /api/cog-layers ???");
        _layerDataPromise = fetch("/api/cog-layers", { credentials: "same-origin" })
            .then(function (resp) {
                if (!resp.ok) {
                    throw new Error("HTTP " + resp.status);
                }
                return resp.json();
            })
            .then(function (data) {
                var layers = data.layers || [];
                console.log(LOG_PREFIX + layers.length + " layers available");
                return layers;
            })
            .catch(function (err) {
                console.warn(LOG_PREFIX + "fetch failed:", err);
                _layerDataPromise = null; // allow retry
                return [];
            });
        return _layerDataPromise;
    }

    var _vectorLayerDataPromise = null;

    function fetchVectorLayerData() {
        if (_vectorLayerDataPromise) {
            return _vectorLayerDataPromise;
        }
        console.log(LOG_PREFIX + "fetching /api/vector-layers ???");
        _vectorLayerDataPromise = fetch("/api/vector-layers", { credentials: "same-origin" })
            .then(function (resp) {
                if (!resp.ok) throw new Error("HTTP " + resp.status);
                return resp.json();
            })
            .then(function (data) {
                var layers = data.layers || [];
                console.log(LOG_PREFIX + layers.length + " vector layers available");
                return layers;
            })
            .catch(function (err) {
                console.warn(LOG_PREFIX + "vector layers fetch failed:", err);
                _vectorLayerDataPromise = null;
                return [];
            });
        return _vectorLayerDataPromise;
    }

    /**
     * Create a WebGLTile layer for a covariate.  Returns null if
     * GeoTIFF support is missing.
     */
    function createLayer(layerDef) {
        if (!hasGeoTIFFSupport()) {
            console.warn(LOG_PREFIX + "ol.source.GeoTIFF not available");
            return null;
        }

        var style = layerDef.style || {};
        var colorExpr = buildColorExpression(style);
        colorExpr = wrapNodata(colorExpr, style.nodata_value);

        try {
            var source = new ol.source.GeoTIFF({
                sources: [{ url: layerDef.url }],
                normalize: false,
                convertToRGB: false,
                opaque: false,
                transition: 0,
            });

            // Diagnostic: log source state changes and errors
            source.on("change", function () {
                console.log(
                    LOG_PREFIX + "source state [" + layerDef.name + "]: " +
                    source.getState()
                );
            });
            source.on("tileloaderror", function (evt) {
                console.error(
                    LOG_PREFIX + "tile load error [" + layerDef.name + "]",
                    evt
                );
            });

            console.log(
                LOG_PREFIX + "style expr [" + layerDef.name + "]:",
                JSON.stringify(colorExpr)
            );

            var layer = new ol.layer.WebGLTile({
                source: source,
                visible: false,
                opacity: style.opacity != null ? style.opacity : 1.0,
                style: { color: colorExpr },
                properties: { title: layerDef.description || layerDef.name },
            });

            layer.on("error", function (evt) {
                console.error(
                    LOG_PREFIX + "layer render error [" + layerDef.name + "]",
                    evt
                );
            });

            console.log(LOG_PREFIX + "created layer: " + layerDef.name);
            return layer;
        } catch (err) {
            console.error(LOG_PREFIX + "error creating layer " + layerDef.name, err);
            return null;
        }
    }

    // ?????? Layer control panel ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????

    function buildLayerControl(map, mapEl, layers, vectorLayers) {
        // Per-map layer cache stored on the element.
        if (!mapEl._cogLayerCache) {
            mapEl._cogLayerCache = {};
        }
        var cache = mapEl._cogLayerCache;

        // Per-map vector layer cache.
        if (!mapEl._vectorLayerCache) {
            mapEl._vectorLayerCache = {};
        }
        var vecCache = mapEl._vectorLayerCache;

        // Look up a layer def by name.
        var defByName = {};
        for (var i = 0; i < layers.length; i++) {
            defByName[layers[i].name] = layers[i];
        }

        // Group COG layers by category.
        var categories = {};
        for (var k = 0; k < layers.length; k++) {
            var cat = layers[k].category || "other";
            if (!categories[cat]) {
                categories[cat] = [];
            }
            categories[cat].push(layers[k]);
        }

        // Group vector layers by category.
        var vecCategories = {};
        for (var vi = 0; vi < vectorLayers.length; vi++) {
            var vcat = vectorLayers[vi].category || "other";
            if (!vecCategories[vcat]) {
                vecCategories[vcat] = [];
            }
            vecCategories[vcat].push(vectorLayers[vi]);
        }

        // Outer wrapper.
        var panel = document.createElement("div");
        panel.className = "ae-layer-control ol-unselectable ol-control";

        // Toggle button.
        var toggleBtn = document.createElement("button");
        toggleBtn.type = "button";
        toggleBtn.className = "ae-layer-control-toggle";
        toggleBtn.title = "Toggle layers";
        toggleBtn.textContent = "\uD83D\uDDFA"; // 🗺
        panel.appendChild(toggleBtn);

        // Content area (hidden by default).
        var content = document.createElement("div");
        content.className = "ae-layer-control-content";
        content.style.display = "none";

        // --- Vector overlay section ---
        if (vectorLayers.length > 0) {
            var vecHeading = document.createElement("div");
            vecHeading.className = "ae-layer-control-heading";
            vecHeading.textContent = "Overlays";
            content.appendChild(vecHeading);

            var vecCatOrder = Object.keys(vecCategories).sort();
            for (var vci = 0; vci < vecCatOrder.length; vci++) {
                var vcatName = vecCatOrder[vci];
                var vcatLayers = vecCategories[vcatName];

                var vcatDiv = document.createElement("div");
                vcatDiv.className = "ae-layer-cat";

                var vcatLabel = document.createElement("div");
                vcatLabel.className = "ae-layer-cat-label";
                vcatLabel.textContent =
                    vcatName.charAt(0).toUpperCase() + vcatName.slice(1).replace(/_/g, " ");
                vcatDiv.appendChild(vcatLabel);

                for (var vli = 0; vli < vcatLayers.length; vli++) {
                    var vdef = vcatLayers[vli];
                    var vrow = document.createElement("label");
                    vrow.className = "ae-layer-row";

                    var vcb = document.createElement("input");
                    vcb.type = "checkbox";
                    vcb.dataset.vectorLayerName = vdef.name;
                    vrow.appendChild(vcb);

                    var vspan = document.createElement("span");
                    vspan.textContent = " " + (vdef.description || vdef.name);
                    vspan.title = vdef.name;
                    vrow.appendChild(vspan);

                    var vslider = document.createElement("input");
                    vslider.type = "range";
                    vslider.min = "0";
                    vslider.max = "100";
                    vslider.value = "100";
                    vslider.className = "ae-layer-opacity";
                    vslider.dataset.vectorLayerName = vdef.name;
                    vslider.title = "Opacity";
                    vrow.appendChild(vslider);

                    vcatDiv.appendChild(vrow);
                }

                content.appendChild(vcatDiv);
            }
        }

        // --- COG covariate layer section ---
        var heading = document.createElement("div");
        heading.className = "ae-layer-control-heading";
        heading.textContent = "Covariate Layers";
        content.appendChild(heading);

        if (!hasGeoTIFFSupport()) {
            var warn = document.createElement("div");
            warn.className = "ae-layer-cat-label";
            warn.style.color = "#dc3545";
            warn.textContent = "COG rendering not supported in this browser/OL build";
            content.appendChild(warn);
        }

        var catOrder = Object.keys(categories).sort();
        for (var ci = 0; ci < catOrder.length; ci++) {
            var catName = catOrder[ci];
            var catLayers = categories[catName];

            var catDiv = document.createElement("div");
            catDiv.className = "ae-layer-cat";

            var catLabel = document.createElement("div");
            catLabel.className = "ae-layer-cat-label";
            catLabel.textContent =
                catName.charAt(0).toUpperCase() + catName.slice(1).replace(/_/g, " ");
            catDiv.appendChild(catLabel);

            for (var li = 0; li < catLayers.length; li++) {
                var ldef = catLayers[li];
                var row = document.createElement("label");
                row.className = "ae-layer-row";

                var cb = document.createElement("input");
                cb.type = "checkbox";
                cb.dataset.layerName = ldef.name;
                if (!hasGeoTIFFSupport()) {
                    cb.disabled = true;
                }
                row.appendChild(cb);

                var span = document.createElement("span");
                span.textContent = " " + (ldef.description || ldef.name);
                span.title = ldef.name;
                row.appendChild(span);

                var slider = document.createElement("input");
                slider.type = "range";
                slider.min = "0";
                slider.max = "100";
                slider.value = String(
                    Math.round((ldef.style.opacity != null ? ldef.style.opacity : 1.0) * 100)
                );
                slider.className = "ae-layer-opacity";
                slider.dataset.layerName = ldef.name;
                slider.title = "Opacity";
                row.appendChild(slider);

                var legend = buildLegendElement(ldef.style || {});
                legend.dataset.layerName = ldef.name;

                catDiv.appendChild(row);
                catDiv.appendChild(legend);
            }

            content.appendChild(catDiv);
        }

        panel.appendChild(content);

        // Wire events.
        toggleBtn.addEventListener("click", function (e) {
            e.preventDefault();
            e.stopPropagation();
            var isVisible = content.style.display !== "none";
            content.style.display = isVisible ? "none" : "block";
        });

        // --- COG checkbox handler (lazy-create raster layers) ---
        content.addEventListener("change", function (e) {
            var target = e.target;

            // Vector layer checkbox
            if (target.type === "checkbox" && target.dataset.vectorLayerName) {
                var vName = target.dataset.vectorLayerName;
                if (target.checked) {
                    if (!vecCache[vName]) {
                        var colorIdx = Object.keys(vecCache).length;
                        var source = new ol.source.Vector();
                        var vlayer = new ol.layer.Vector({
                            source: source,
                            style: buildVectorStyle(colorIdx),
                            visible: true,
                            properties: { title: vName },
                        });
                        vecCache[vName] = { layer: vlayer, name: vName };
                        // Insert below site polygons (top layer) but above tiles
                        var mapLayers = map.getLayers();
                        mapLayers.insertAt(mapLayers.getLength() - 1, vlayer);
                    }
                    vecCache[vName].layer.setVisible(true);
                    loadVectorFeatures(map, vecCache[vName].layer, vName);
                } else {
                    if (vecCache[vName]) {
                        vecCache[vName].layer.setVisible(false);
                    }
                }
                return;
            }

            // COG layer checkbox
            if (target.type !== "checkbox" || !target.dataset.layerName) {
                return;
            }
            var layerName = target.dataset.layerName;

            if (target.checked) {
                // Lazily create the layer if needed.
                if (!cache[layerName]) {
                    var def = defByName[layerName];
                    if (!def) {
                        return;
                    }
                    var layer = createLayer(def);
                    if (!layer) {
                        target.checked = false;
                        return;
                    }
                    cache[layerName] = { layer: layer, def: def };
                }

                var cached = cache[layerName];
                // Add layer to map if not already present.
                var cogMapLayers = map.getLayers();
                var found = false;
                cogMapLayers.forEach(function (l) {
                    if (l === cached.layer) {
                        found = true;
                    }
                });
                if (!found) {
                    cogMapLayers.insertAt(cogMapLayers.getLength() - 1, cached.layer);
                }
                cached.layer.setVisible(true);
            } else {
                if (cache[layerName]) {
                    cache[layerName].layer.setVisible(false);
                }
            }

            // Toggle legend visibility
            var legendEl = content.querySelector(
                ".ae-layer-legend[data-layer-name=\"" + layerName + "\"]"
            );
            if (legendEl) {
                legendEl.style.display = target.checked ? "block" : "none";
            }
        });

        content.addEventListener("input", function (e) {
            var target = e.target;
            if (target.type === "range" && target.dataset.layerName) {
                var c = cache[target.dataset.layerName];
                if (c) {
                    c.layer.setOpacity(parseInt(target.value, 10) / 100);
                }
            }
            if (target.type === "range" && target.dataset.vectorLayerName) {
                var vc = vecCache[target.dataset.vectorLayerName];
                if (vc) {
                    vc.layer.setOpacity(parseInt(target.value, 10) / 100);
                }
            }
        });

        // Reload visible vector layers when map view changes.
        map.on("moveend", function () {
            Object.keys(vecCache).forEach(function (vn) {
                if (vecCache[vn].layer.getVisible()) {
                    loadVectorFeatures(map, vecCache[vn].layer, vn);
                }
            });
        });

        var control = new ol.control.Control({ element: panel });
        map.addControl(control);
        console.log(LOG_PREFIX + "layer control added to map " + mapEl.id);
    }

    // ?????? Attach to maps ????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????

    function maybeAttachCogLayers(el, map) {
        if (el._cogLayersAttached) {
            return;
        }
        var enabled = el.getAttribute("data-enable-cog-layers");
        console.log(
            LOG_PREFIX + "checking map " + el.id +
            ", data-enable-cog-layers=" + enabled
        );
        if (enabled !== "true") {
            return;
        }
        el._cogLayersAttached = true;

        // Fetch COG and vector layer lists in parallel.
        Promise.all([fetchLayerData(), fetchVectorLayerData()]).then(function (results) {
            var layers = results[0];
            var vectorLayers = results[1];

            if (!layers.length && !vectorLayers.length) {
                console.log(LOG_PREFIX + "no layers to show");
                return;
            }

            // Optionally filter COG layers to only covariates used in this task
            var cogFilter = el.getAttribute("data-cog-filter");
            if (cogFilter) {
                var allowed = cogFilter.split(",").map(function (s) {
                    return s.trim();
                });
                var allowedSet = {};
                allowed.forEach(function (n) { allowedSet[n] = true; });
                layers = layers.filter(function (l) {
                    return allowedSet[l.name];
                });
                console.log(
                    LOG_PREFIX + "filtered to " + layers.length +
                    " layers for covariates: " + cogFilter
                );
            }

            buildLayerControl(map, el, layers, vectorLayers);
        });
    }

    // ?????? Integration hook ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????

    function tryAttachAll() {
        document.querySelectorAll(".ol-sites-map").forEach(function (el) {
            if (el._olMap) {
                maybeAttachCogLayers(el, el._olMap);
            }
        });
    }

    function boot() {
        console.log(
            LOG_PREFIX + "booting, GeoTIFF support: " + hasGeoTIFFSupport()
        );

        // Listen for the custom event dispatched by openlayersSitesMap.js.
        document.body.addEventListener("ol-map-ready", function (e) {
            console.log(LOG_PREFIX + "ol-map-ready event on", e.target.id);
            var el = e.target;
            var map = e.detail && e.detail.map;
            if (el && map) {
                maybeAttachCogLayers(el, map);
            }
        });

        // Also catch any maps that were already created before this script loaded.
        tryAttachAll();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", boot);
    } else {
        boot();
    }
})();
