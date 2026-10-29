const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { resolve } = require("node:path");
const { test } = require("node:test");
const vm = require("node:vm");

function harness() {
    class Element {
        constructor(id = "") {
            this.id = id;
            this.listeners = {};
            this.style = {};
            this.textContent = "Click to Upload";
        }
        addEventListener(name, callback) {
            (this.listeners[name] ||= []).push(callback);
        }
        setAttribute() {}
        closest(selector) { return selector === `#${this.id}` ? this : null; }
        click() { this.clicked = (this.clicked || 0) + 1; }
    }
    const nodes = Object.fromEntries([
        "upload-sites-stream-btn", "upload-sites-stream-cancel-btn", "upload-status",
    ].map(id => [id, new Element(id)]));
    const listeners = {};
    const requests = [];
    const payloads = [];
    let mutation;
    const document = {
        getElementById: id => nodes[id],
        createElement: () => new Element(),
        addEventListener: (name, callback) => (listeners[name] ||= []).push(callback),
        body: { appendChild: node => { nodes[node.id] = node; } },
        documentElement: {},
    };
    class XMLHttpRequest {
        constructor() { this.upload = {}; requests.push(this); }
        open() {}
        send() {}
        abort() { this.aborted = true; this.onabort(); }
    }
    const context = {
        document, Element, XMLHttpRequest,
        FormData: class { append() {} },
        MutationObserver: class {
            constructor(callback) { mutation = callback; }
            observe() {}
        },
        window: { dash_clientside: { set_props: (_id, props) => payloads.push(props) } },
    };
    vm.runInNewContext(
        readFileSync(resolve(__dirname, "../../webapp/assets/siteUploadStream.js"), "utf8"),
        context,
    );
    function select() {
        const input = nodes["upload-sites-stream-input"];
        input.files = [{ name: "synthetic.geojson", size: 50 }];
        input.listeners.change[0]();
    }
    return { nodes, requests, payloads, listeners, mutation, select };
}

test("repeated page mutations do not accumulate file input listeners", () => {
    const h = harness();
    for (let i = 0; i < 10; i++) h.mutation();
    assert.equal(h.listeners.click.length, 1);
    assert.equal(h.nodes["upload-sites-stream-input"].listeners.change.length, 1);
    h.select();
    h.select();
    assert.equal(h.requests.length, 1);
});

test("preview processing is announced before the server response", () => {
    const h = harness();
    h.select();
    h.requests[0].upload.onload();
    assert.match(h.nodes["upload-status"].textContent, /Preparing the mapping preview/);
    assert.equal(h.nodes["upload-sites-stream-btn"].textContent, "Processing preview...");
    assert.equal(h.payloads.length, 0);
});

test("server validation details remain visible on failed previews", async () => {
    const h = harness();
    h.select();
    const xhr = h.requests[0];
    xhr.status = 400;
    xhr.responseText = JSON.stringify({ ok: false, errors: ["Missing source CRS."] });
    xhr.onload();
    await new Promise(setImmediate);
    assert.match(h.nodes["upload-status"].textContent, /Missing source CRS/);
    assert.equal(h.nodes["upload-sites-stream-btn"].disabled, false);
    assert.equal(h.nodes["upload-sites-stream-input"].value, "");
    assert.equal(JSON.parse(h.payloads[0].value).status, "error");
});

test("cancelling a transfer aborts only the active request", async () => {
    const h = harness();
    h.select();
    h.listeners.click[0]({ target: h.nodes["upload-sites-stream-cancel-btn"] });
    await new Promise(setImmediate);
    assert.equal(h.requests[0].aborted, true);
    assert.equal(h.nodes["upload-status"].textContent, "Upload cancelled.");
    assert.equal(h.nodes["upload-sites-stream-cancel-btn"].disabled, true);
});
