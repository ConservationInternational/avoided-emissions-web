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

test("Logs button matches Params and sends only eligible task events", () => {
    const listeners = new Map();
    const window = {
        addEventListener: (name, callback) => listeners.set(name, callback),
        removeEventListener: name => listeners.delete(name),
    };
    const stateUpdates = [];
    vm.runInNewContext(fs.readFileSync(
        path.join(__dirname, "../../webapp/assets/dashAgGridComponentFunctions.js"), "utf8",
    ), {
        window,
        React: {
            createElement: (type, props, ...children) => ({ type, props, children }),
            useState: value => [value, updated => stateUpdates.push(updated)],
            useEffect: effect => effect(),
        },
    });
    const events = [];
    const render = window.dashAgGridComponentFunctions;
    const data = { id: "task-1", has_logs: true };
    const params = render.TaskParams({ data });
    const logs = render.TaskLogs({ data, setData: value => events.push(value) });
    assert.equal(logs.props.className, params.props.className);
    assert.equal(logs.props.disabled, false);
    let stopped = false;
    logs.props.onClick({ stopPropagation() { stopped = true; } });
    assert.equal(stopped, true);
    assert.equal(events[0].action, "logs");
    assert.equal(events[0].task_id, data.id);

    window.taskListAction.pending = true;
    listeners.get("task-list-action-pending")();
    assert.equal(stateUpdates.at(-1), true);
    logs.props.onClick({ stopPropagation() {} });
    assert.equal(events.length, 1);
    assert.equal(render.TaskLogs({ data }).props.disabled, true);
    window.taskListAction.pending = false;
    assert.equal(render.TaskLogs({ data: { id: "task-2", has_logs: false } }).props.disabled, true);
    assert.equal(render.TaskLogs({ data: { has_logs: true } }).props.disabled, true);
});
