const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

function harness(pathname = "/docs/setup", hash = "#caliper-width") {
    const listeners = {};
    const elements = {};
    const events = [];
    let update;
    let resize;
    const window = {
        location: { pathname, hash },
        scrollY: 0,
        addEventListener(name, callback) { listeners[name] = callback; },
    };
    const document = {
        documentElement: {},
        getElementById(id) { return elements[id]; },
    };
    vm.runInNewContext(fs.readFileSync(
        path.join(__dirname, "../../webapp/assets/documentation.js"), "utf8",
    ), {
        window, document,
        requestAnimationFrame(callback) { callback(); },
        console: { warn() { events.push("invalid anchor"); } },
        MutationObserver: class {
            constructor(callback) { update = callback; }
            observe() {}
        },
        ResizeObserver: class {
            constructor(callback) { resize = callback; }
            observe() {}
            disconnect() {}
        },
    });
    elements["documentation-content"] = { contains: () => true };
    return {
        events, window, update, resize, listeners,
        mount(id) {
            elements[id] = {
                getBoundingClientRect() { return { top: elements[id].top || 0 }; },
                scrollIntoView() { events.push(`scroll:${id}`); },
                focus() { events.push(`focus:${id}`); },
            };
        },
        move(id, top) { elements[id].top = top; },
    };
}

test("deep links wait for Dash to mount and do not steal focus on later updates", () => {
    const env = harness();
    assert.deepEqual(env.events, []);
    env.mount("caliper-width");
    env.update();
    env.update();
    assert.deepEqual(env.events, ["scroll:caliper-width", "focus:caliper-width"]);
});

test("hash changes and Back/Forward restore documentation sections", () => {
    const env = harness();
    env.mount("caliper-width");
    env.mount("replicates");
    env.update();
    env.window.location.hash = "#replicates";
    env.listeners.hashchange();
    env.window.location.hash = "#caliper-width";
    env.listeners.popstate();
    assert.deepEqual(env.events, [
        "scroll:caliper-width", "focus:caliper-width",
        "scroll:replicates", "focus:replicates",
        "scroll:caliper-width", "focus:caliper-width",
    ]);
});

test("documentation anchor restoration leaves application routes untouched", () => {
    const env = harness("/submit");
    env.mount("caliper-width");
    env.update();
    assert.deepEqual(env.events, []);
});

test("late Markdown layout restores the anchor until the user takes control", () => {
    const env = harness();
    env.mount("caliper-width");
    env.update();
    env.move("caliper-width", 500);
    env.resize();
    assert.deepEqual(env.events, [
        "scroll:caliper-width", "focus:caliper-width", "scroll:caliper-width",
    ]);
    env.listeners.wheel();
    env.move("caliper-width", 700);
    env.resize();
    assert.equal(env.events.length, 3);
});

test("invalid fragment encoding is reported without breaking the page", () => {
    const env = harness("/docs/setup", "#%invalid");
    assert.deepEqual(env.events, ["invalid anchor"]);
});
