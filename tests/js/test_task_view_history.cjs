const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

function harness(initial = "/tasks/task?tab=plots&keep=1") {
    const listeners = {};
    const updates = [];
    const entries = [{ url: new URL(initial, "https://example.test"), state: { dash: true } }];
    let index = 0;
    let moveend;
    let viewport = { center: [10, 20], zoom: 4, rotation: 0.3 };
    const events = [];
    const microtasks = [];
    const view = {
        getCenter: () => viewport.center,
        getZoom: () => viewport.zoom,
        getRotation: () => viewport.rotation,
        cancelAnimations() {},
    };
    const map = {
        _selectedSiteId: "",
        _cleanup: [],
        isConnected: true,
        _olMap: {
            getView: () => view,
            on(_name, listener) { moveend = listener; },
            un() {},
        },
        dispatchEvent(event) {
            events.push(event.detail);
            map._selectedSiteId = event.detail.siteId;
            viewport = event.detail.viewport
                ? structuredClone(event.detail.viewport)
                : { center: event.detail.siteId === "a" ? [1, 2] : [3, 4], zoom: 12, rotation: 0 };
            moveend?.();
        },
    };
    const window = {
        location: entries[0].url,
        addEventListener(name, listener) { listeners[name] = listener; },
        dash_clientside: {
            no_update: {},
            set_props(id, props) { updates.push({ id, props }); },
        },
    };
    window.history = {
        get state() { return entries[index].state; },
        replaceState(state, _title, url) {
            entries[index] = { url: new URL(url, window.location), state: structuredClone(state) };
            window.location = entries[index].url;
        },
        pushState(state, _title, url) {
            entries.splice(index + 1);
            entries.push({ url: new URL(url, window.location), state: structuredClone(state) });
            index++;
            window.location = entries[index].url;
        },
    };
    const document = { getElementById: () => map };
    vm.runInNewContext(fs.readFileSync(
        path.join(__dirname, "../../webapp/assets/taskViewHistory.js"), "utf8",
    ), {
        window, document, URL, queueMicrotask: callback => microtasks.push(callback),
        CustomEvent: class { constructor(type, options) { this.type = type; this.detail = options.detail; } },
    });
    const subject = window.dash_clientside.taskViewHistory;
    function go(delta) {
        index += delta;
        window.location = entries[index].url;
        listeners.popstate();
    }
    return {
        subject, window, document, entries, map, updates, events,
        go,
        flushReady() { microtasks.splice(0).forEach(callback => callback()); },
        pan(next) { viewport = structuredClone(next); moveend(); },
        viewport: () => structuredClone(viewport),
    };
}

for (const zoomFirst of [true, false]) {
    test(`Back/Forward restores group, site and exact viewport (zoom first: ${zoomFirst})`, () => {
        const env = harness();
        const { subject, window } = env;
        const group = { field: "region", value: '"North"' };
        subject.sync({}, "tab-plots", "task");
        const allView = { center: [10, 11], zoom: 5, rotation: 0.4 };
        env.pan(allView);
        subject.sync(group, "tab-plots", "task");
        const groupView = { center: [20, 21], zoom: 7, rotation: 0.7 };
        env.pan(groupView);
        const site = { ...group, site: "a" };
        if (zoomFirst) subject.zoomToSite("a");
        subject.sync(site, "tab-plots", "task");
        if (!zoomFirst) subject.zoomToSite("a");
        const siteView = { center: [30, 31], zoom: 10, rotation: 0.2 };
        env.pan(siteView);
        subject.zoomToSite("b");
        subject.sync({ ...group, site: "b" }, "tab-plots", "task");
        assert.equal(env.entries.length, 4);
        env.go(-1);
        assert.equal(env.updates.at(-1).id, "task-view-navigation");
        assert.equal(env.updates.at(-1).props.data.task_id, "task");
        subject.sync(site, "tab-plots", "task");
        subject.zoomToSite("a");
        assert.deepEqual(env.viewport(), siteView);
        env.go(-1);
        subject.sync(group, "tab-plots", "task");
        subject.zoomToSite("");
        assert.deepEqual(env.viewport(), groupView);
        env.go(-1);
        subject.sync({}, "tab-plots", "task");
        assert.deepEqual(env.viewport(), allView);
        env.go(1);
        subject.sync(group, "tab-plots", "task");
        assert.deepEqual(env.viewport(), groupView);
        assert.equal(env.entries.length, 4);
        assert.equal(window.location.search, "?tab=plots&keep=1&field=region&value=%22North%22");
    });
}

test("initialization, duplicate callbacks, partial group edits and panning do not push entries", () => {
    const env = harness("/shared/token?site=a&keep=1");
    const selection = { site: "a" };
    env.subject.sync(selection, "tab-plots", "task");
    env.subject.sync(selection, "tab-plots", "task");
    env.subject.sync({ field: "region" }, "tab-plots", "task");
    assert.equal(env.entries.length, 1);
    assert.equal(env.window.location.pathname, "/shared/token");
    assert.equal(env.window.location.searchParams.get("site"), "a");
    assert.equal(env.window.history.state.dash, true);
    env.subject.sync({ field: "region", value: "false" }, "tab-plots", "task");
    env.subject.zoomToSite("");
    assert.equal(env.entries.length, 2);
    assert.equal(env.window.location.searchParams.get("keep"), "1");
    env.pan({ center: [3, 5], zoom: 6, rotation: 0 });
    assert.equal(env.entries.length, 2);
    env.subject.sync({ field: "country" }, "tab-plots", "task");
    env.subject.sync({ field: "region", value: "false" }, "tab-plots", "task");
    env.pan({ center: [5, 6], zoom: 9, rotation: 0.1 });
    assert.equal(env.window.history.state.aeTaskView.viewport.zoom, 9);
    assert.equal(env.entries.length, 2);
});

test("tab navigation retains selection and ignores stale updates while restoring", () => {
    const env = harness();
    const selection = { site: "a" };
    env.subject.sync(selection, "tab-plots", "task");
    env.subject.sync(selection, "tab-results", "task");
    env.subject.sync(selection, "tab-raw-results", "task");
    env.go(-1);
    env.subject.sync(selection, "tab-plots", "task");
    assert.equal(env.window.location.searchParams.get("tab"), "results");
    assert.equal(env.entries.length, 3);
    env.subject.sync(selection, "tab-results", "task");
    env.subject.sync(selection, "tab-results", "task");
    assert.equal(env.entries.length, 3);
    assert.equal(env.window.location.searchParams.get("site"), "a");
});

test("pending map-ready handlers and old extent requests cannot override newer navigation", () => {
    const env = harness();
    let aborted = 0;
    env.subject.sync({}, "tab-plots", "task");
    env.subject.sync({ site: "a" }, "tab-plots", "task");
    env.map._lookupAbort = { abort() { aborted++; } };
    const map = env.map._olMap;
    const ready = [];
    env.map._olMap = null;
    env.map.addEventListener = (_name, listener) => ready.push(listener);
    env.subject.zoomToSite("a");
    env.go(-1);
    env.subject.sync({}, "tab-plots", "task");
    env.map._olMap = map;
    const before = env.events.length;
    ready[0]();
    env.flushReady();
    assert.equal(env.events.length, before);
    ready[1]();
    assert.equal(env.events.length, before);
    env.flushReady();
    assert.equal(env.events.at(-1).siteId, "");
    assert.equal(aborted, 1);
});

test("popstate on another route is left to Dash routing", () => {
    const env = harness();
    env.subject.sync({}, "tab-plots", "task");
    env.subject.sync({ site: "a" }, "tab-plots", "task");
    env.entries[0].url = new URL("https://example.test/tasks/other");
    env.go(-1);
    assert.equal(env.updates.length, 0);
    env.go(1);
    env.subject.sync({ site: "a" }, "tab-plots", "task");
    assert.equal(env.entries.length, 2);
    assert.equal(env.updates.length, 0);
});
