const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

for (const [action, status, canResume, index] of [
    ["cancel", "running", false, 1],
    ["resume", "failed", true, 2],
]) {
    test(`${action} requires confirmation before dispatching and marking pending`, () => {
        const events = [];
        const prompts = [];
        let accepted = false;
        const window = {
            confirm(message) { prompts.push(message); return accepted; },
            dispatchEvent() {},
            addEventListener() {},
            removeEventListener() {},
        };
        const React = {
            createElement: (type, props, ...children) => ({ type, props, children }),
            useState: value => [value, () => {}],
            useEffect() {},
        };
        vm.runInNewContext(fs.readFileSync(
            path.join(__dirname, "../../webapp/assets/dashAgGridComponentFunctions.js"),
            "utf8",
        ), { window, React, document: { getElementById: () => null }, Event: class {} });
        const row = window.dashAgGridComponentFunctions.TaskActions({
            data: { id: "task-1", status, can_resume: canResume },
            setData: value => events.push(value),
        });
        const button = row.children[index];
        button.props.onClick({ stopPropagation() {} });
        assert.equal(prompts.length, 1);
        assert.match(prompts[0], new RegExp(action, "i"));
        assert.equal(events.length, 0);
        assert.equal(window.taskListAction.pending, false);

        accepted = true;
        button.props.onClick({ stopPropagation() {} });
        assert.equal(events.length, 1);
        assert.equal(events[0].action, action);
        assert.equal(events[0].task_id, "task-1");
        assert.equal(window.taskListAction.pending, true);
        button.props.onClick({ stopPropagation() {} });
        assert.equal(prompts.length, 2);
        assert.equal(events.length, 1);
    });
}
