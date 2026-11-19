"""Server-Timing metrics for task page callbacks."""

from contextlib import contextmanager
from functools import wraps
from time import perf_counter

from flask import g, has_request_context


@contextmanager
def task_timing(name):
    start = perf_counter()
    try:
        yield
    finally:
        if has_request_context():
            metrics = g.setdefault("task_timings", {})
            metrics[name] = metrics.get(name, 0) + (perf_counter() - start) * 1000


def timed_task_step(name):
    def decorate(function):
        @wraps(function)
        def timed(*args, **kwargs):
            with task_timing(name):
                return function(*args, **kwargs)

        return timed

    return decorate


def add_task_timing_headers(response):
    metrics = g.get("task_timings")
    if metrics and (start := g.get("dash_request_start")) is not None:
        metrics["dash_request"] = (perf_counter() - start) * 1000
        response.headers.add(
            "Server-Timing",
            ", ".join(f"{name};dur={duration:.2f}" for name, duration in metrics.items()),
        )
    return response
