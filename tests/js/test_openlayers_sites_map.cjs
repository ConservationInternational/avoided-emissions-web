const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

function harness(fetch) {
    const controls = [];
    const document = {
        readyState: "loading",
        addEventListener() {},
        removeEventListener() {},
        querySelectorAll: () => [],
        createElement: () => ({
            style: {},
            appendChild() {},
            setAttribute() {},
            addEventListener() {},
        }),
    };
    const context = {
        window: {},
        document,
        AbortController,
        console,
        fetch,
        setTimeout: (callback) => callback(),
        MutationObserver: class { observe() {} },
        ol: {
            control: { Control: class { constructor(options) { controls.push(options); } } },
            proj: { transformExtent: (extent) => extent },
        },
    };
    const source = fs.readFileSync(
        path.join(__dirname, "../../webapp/assets/openlayersSitesMap.js"), "utf8"
    ).replace(/\}\)\(\);\s*$/, `
        globalThis.subject = { makeMvtTileLoadFunction, zoomToFeature, renderElement,
            renderAll, mountedMaps, focusTableRow, featureStyle };
    })();`);
    context.window.ol = context.ol;
    vm.runInNewContext(source, context);
    return { ...context, controls };
}

function mapElement() {
    return {
        _requestGeneration: 1,
        _tileAborts: new Set(),
        _cleanup: [],
        _olMap: { addControl() {} },
        isConnected: true,
    };
}

async function loadTile(subject, element) {
    const tile = {
        features: null,
        state: null,
        setLoader(loader) { this.loader = loader; },
        setFeatures(features) { this.features = features; },
        setState(state) { this.state = state; },
    };
    subject.makeMvtTileLoadFunction(element)(tile, "/api/sites-tiles/set/1/1/1");
    await tile.loader([], 1, "EPSG:3857");
    return tile;
}

test("valid empty tiles are successes, not errors", async () => {
    const env = harness(async () => ({
        ok: true, status: 200, arrayBuffer: async () => new ArrayBuffer(0),
    }));
    const tile = await loadTile(env.subject, mapElement());
    assert.equal(tile.features.length, 0);
    assert.equal(tile.state, null);
});

for (const status of [429, 503]) {
    test(`HTTP ${status} tiles retry finitely and expose an accessible error`, async () => {
        let requests = 0;
        const env = harness(async () => {
            requests++;
            return { ok: false, status };
        });
        const element = mapElement();
        const tile = await loadTile(env.subject, element);
        assert.equal(requests, 3);
        assert.equal(tile.features, null);
        assert.equal(tile.state, 3);
        assert.match(element._mapStatus.text.textContent, /could not load/);
        assert.equal(element._tileAborts.size, 0);
    });
}

test("expired sessions are visible and never decoded as successful empty tiles", async () => {
    let requests = 0;
    const env = harness(async () => {
        requests++;
        return { ok: true, status: 200, redirected: true };
    });
    const element = mapElement();
    const tile = await loadTile(env.subject, element);
    assert.equal(requests, 1);
    assert.equal(tile.state, 3);
    assert.match(element._mapStatus.text.textContent, /Session expired/);
});

test("stale tile generations cannot publish features", async () => {
    const element = mapElement();
    const env = harness(async () => {
        element._requestGeneration++;
        return { ok: true, status: 200, arrayBuffer: async () => new ArrayBuffer(0) };
    });
    const tile = await loadTile(env.subject, element);
    assert.equal(tile.features, null);
    assert.equal(tile.state, null);
});

test("table selection aborts the older extent lookup and fits only the latest feature", async () => {
    const pending = [];
    const fits = [];
    const env = harness((_url, options) => new Promise(resolve => {
        pending.push({ resolve, signal: options.signal });
    }));
    const element = mapElement();
    element.getAttribute = () => "/api/sites-tiles/set/{z}/{x}/{y}";
    const map = { getView: () => ({ fit: extent => fits.push(extent) }) };
    const oldRequest = env.subject.zoomToFeature(element, map, "old-feature");
    const newRequest = env.subject.zoomToFeature(element, map, "new-feature");
    assert.equal(pending[0].signal.aborted, true);
    for (const [index, request] of pending.entries()) {
        request.resolve({
            ok: true,
            json: async () => ({
                bounds: { west: index, south: index, east: index + 1, north: index + 1 },
            }),
        });
    }
    await Promise.all([oldRequest, newRequest]);
    assert.equal(fits.length, 1);
    assert.deepEqual(Array.from(fits[0]), [1, 1, 2, 2]);
});

test("initial bounds fit immediately without downloading all centroids", () => {
    let requests = 0;
    const env = harness(async () => { requests++; });
    const fits = [];
    const tileUrl = "/api/sites-tiles/set/{z}/{x}/{y}";
    const attributes = {
        "data-tile-url": tileUrl,
        "data-centroids-url": "/api/site-centroids/set",
        "data-bounds": JSON.stringify({ west: 1, south: 2, east: 3, north: 4 }),
    };
    const element = {
        ...mapElement(),
        id: "preview",
        classList: { contains: () => true },
        style: {},
        _lastTileUrl: tileUrl,
        _resizeObserverAttached: true,
        getAttribute: name => attributes[name] || "",
        _olMap: {
            getView: () => ({ fit: extent => fits.push(extent) }),
            updateSize() {},
        },
    };
    env.subject.renderElement(element);
    assert.equal(requests, 0);
    assert.equal(fits.length, 1);
    assert.deepEqual(Array.from(fits[0]), [1, 2, 3, 4]);
});

test("unloaded map selections use server filtering by canonical feature identity", () => {
    let filter;
    const api = {
        getRowNode: () => null,
        deselectAll() {},
        addEventListener() {},
        setFilterModel: value => { filter = value; },
    };
    const env = harness();
    env.window.dash_ag_grid = { getApi: () => api };
    env.subject.focusTableRow("site-preview-table", "canonical-feature");
    assert.equal(filter.feature_id.filter, "canonical-feature");
    assert.equal(filter.feature_id.type, "equals");
});

test("unmount cancels pending requests, removes listeners and disposes the map", () => {
    const env = harness();
    const element = mapElement();
    const lookup = new AbortController();
    const tile = new AbortController();
    let cleaned = 0;
    let disposed = 0;
    element.isConnected = false;
    element._lookupAbort = lookup;
    element._tileAborts.add(tile);
    element._cleanup.push(() => cleaned++);
    element._olMap = { setTarget() {}, dispose: () => disposed++ };
    env.subject.mountedMaps.add(element);
    env.subject.renderAll();
    assert.equal(lookup.signal.aborted, true);
    assert.equal(tile.signal.aborted, true);
    assert.equal(cleaned, 1);
    assert.equal(disposed, 1);
    assert.equal(env.subject.mountedMaps.size, 0);
});

test("unlabelled polygons share styles across site identities and zoom levels", () => {
    const env = harness();
    let styleCount = 0;
    env.ol.style = {
        Fill: class {},
        Stroke: class {},
        Style: class { constructor() { styleCount++; } },
    };
    const element = {
        _selectedSiteId: "",
        _olMap: { getView: () => ({ getZoom: () => 3 }) },
    };
    const style = env.subject.featureStyle(element);
    for (let index = 0; index < 10000; index++) {
        style({
            getGeometry: () => ({ getType: () => "MultiPolygon" }),
            get: key => key === "site_id" ? `feature-${index}` : undefined,
        });
    }
    assert.equal(styleCount, 1);
});
