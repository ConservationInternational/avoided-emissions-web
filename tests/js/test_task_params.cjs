const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

test("Params button sends the selected row's parameters without navigating", () => {
    const window = {};
    vm.runInNewContext(fs.readFileSync(
        path.join(__dirname, "../../webapp/assets/dashAgGridComponentFunctions.js"), "utf8",
    ), {
        window,
        React: {
            createElement: (type, props, ...children) => ({ type, props, children }),
        },
    });
    const data = {
        id: "task-1", name: "Analysis",
        params: { caliper_width: 0, nested: { enabled: true } },
    };
    const events = [];
    const render = window.dashAgGridComponentFunctions.TaskParams;
    const button = render({ data, setData: value => events.push(value) });
    let stopped = false;
    button.props.onClick({ stopPropagation() { stopped = true; } });
    assert.equal(stopped, true);
    assert.equal(button.type, "button");
    assert.equal(button.props.disabled, false);
    assert.equal(events[0].action, "params");
    assert.equal(events[0].task_name, data.name);
    assert.equal(events[0].params, data.params);
    assert.equal(render({}).props.disabled, true);
});
