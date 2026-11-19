const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const window = {};
vm.runInNewContext(fs.readFileSync(
    path.join(__dirname, "../../webapp/assets/dashAgGridComponentFunctions.js"), "utf8",
), { window });

test("Elapsed formats hours, minutes, and seconds without wrapping at 24 hours", () => {
    const render = window.dashAgGridComponentFunctions.TaskElapsed;
    for (const [seconds, expected] of [
        [0, "00:00:00"],
        [59, "00:00:59"],
        [60, "00:01:00"],
        [3661, "01:01:01"],
        [90061, "25:01:01"],
        [360001, "100:00:01"],
    ]) {
        assert.equal(render({ value: seconds }), expected);
    }
});

test("Elapsed distinguishes unavailable timestamps from zero elapsed time", () => {
    const render = window.dashAgGridComponentFunctions.TaskElapsed;
    assert.equal(render({ value: null }), "\u2014");
    assert.equal(render({}), "\u2014");
});
