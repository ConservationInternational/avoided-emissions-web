"""Short-lived, shared cache for derived results; callers must authorize first."""

import hashlib
import json
import logging
from functools import lru_cache

import redis
from config import Config
from redis.backoff import NoBackoff
from redis.retry import Retry
from task_timing import task_timing

logger = logging.getLogger(__name__)
QUALITY_WARNINGS_KEY = "quality_warnings_v1"


@lru_cache(maxsize=1)
def _client(url):
    return redis.Redis.from_url(
        url,
        socket_timeout=0.25,
        socket_connect_timeout=0.25,
        retry=Retry(NoBackoff(), 0),
    )


def cached_task_result(task, kind, compute, *, site_ids=None):
    if task.status != "succeeded" or not Config.TASK_RESULTS_CACHE_URL:
        return compute()
    revision = (task.extra_metadata or {}).get("result_revision")
    if not revision:
        revision = str(task.completed_at)
    identity = [
        str(task.id), revision, task.results_s3_uri, kind,
        sorted({str(site_id) for site_id in site_ids}) if site_ids is not None else None,
    ]
    digest = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
    key = f"avoided-emissions:derived:v1:{digest}"
    with task_timing("task_cache_read"):
        try:
            client = _client(Config.TASK_RESULTS_CACHE_URL)
            if (value := client.get(key)) is not None:
                return json.loads(value)
        except (redis.RedisError, ValueError):
            logger.warning("Task result cache read failed (%s)", kind, exc_info=True)
            return compute()
    value = compute()
    with task_timing("task_cache_write"):
        try:
            payload = json.dumps(value, allow_nan=False)
            if len(payload) <= 512 * 1024:
                client.set(key, payload, ex=900)
        except (redis.RedisError, ValueError, TypeError):
            logger.warning("Task result cache write failed (%s)", kind, exc_info=True)
    return value
