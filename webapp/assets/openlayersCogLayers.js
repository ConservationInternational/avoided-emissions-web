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
                opacity: style.opacity != null ? style.opacity : 0.7,
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

    function buildLayerControl(map, mapEl, layers) {
        // Per-map layer cache stored on the element.
        if (!mapEl._cogLayerCache) {
            mapEl._cogLayerCache = {};
        }
        var cache = mapEl._cogLayerCache;

        // Look up a layer def by name.
        var defByName = {};
        for (var i = 0; i < layers.length; i++) {
            defByName[layers[i].name] = layers[i];
        }

        // Group layers by category.
        var categories = {};
        for (var k = 0; k < layers.length; k++) {
            var cat = layers[k].category || "other";
            if (!categories[cat]) {
                categories[cat] = [];
            }
            categories[cat].push(layers[k]);
        }

        // Outer wrapper.
        var panel = document.createElement("div");
        panel.className = "ae-layer-control ol-unselectable ol-control";

        // Toggle button.
        var toggleBtn = document.createElement("button");
        toggleBtn.type = "button";
        toggleBtn.className = "ae-layer-control-toggle";
        toggleBtn.title = "Toggle covariate layers";
        toggleBtn.textContent = "\uD83D\uDDFA"; // ????
        panel.appendChild(toggleBtn);

        // Content area (hidden by default).
        var content = document.createElement("div");
        content.className = "ae-layer-control-content";
        content.style.display = "none";

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
                    Math.round((ldef.style.opacity != null ? ldef.style.opacity : 0.7) * 100)
                );
                slider.className = "ae-layer-opacity";
                slider.dataset.layerName = ldef.name;
                slider.title = "Opacity";
                row.appendChild(slider);

                catDiv.appendChild(row);
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

        // Lazy-create layers on first toggle.
        content.addEventListener("change", function (e) {
            var target = e.target;
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
                var mapLayers = map.getLayers();
                var found = false;
                mapLayers.forEach(function (l) {
                    if (l === cached.layer) {
                        found = true;
                    }
                });
                if (!found) {
                    mapLayers.insertAt(mapLayers.getLength() - 1, cached.layer);
                }
                cached.layer.setVisible(true);
            } else {
                if (cache[layerName]) {
                    cache[layerName].layer.setVisible(false);
                }
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

        fetchLayerData().then(function (layers) {
            if (!layers.length) {
                console.log(LOG_PREFIX + "no layers to show");
                return;
            }
            buildLayerControl(map, el, layers);
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
