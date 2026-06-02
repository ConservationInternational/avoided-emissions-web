"""Root test configuration.

Sets environment variables *before* any application module is imported.
Config reads from os.environ at class-definition time (module import time),
so these assignments must come before any ``from models import …`` or
``from services import …`` statements run in test files or fixtures.
"""

import os

# ---------------------------------------------------------------------------
# Provide test-safe defaults for every env var the app reads at import time.
# We use setdefault so that a developer's real settings (e.g. a local
# DATABASE_URL pointing to a live dev DB) are not overridden when running
# tests manually.  CI environments will have none of these set.
# ---------------------------------------------------------------------------
_TEST_DEFAULTS = {
    # Database — points to a dedicated test DB; unit tests mock get_db()
    # entirely so no real connection is made during unit tests.
    "DATABASE_URL": "postgresql://ae_user:ae_password@localhost:5432/avoided_emissions_test",
    "POSTGRES_HOST": "localhost",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "avoided_emissions_test",
    "POSTGRES_USER": "ae_user",
    "POSTGRES_PASSWORD": "ae_password",
    # Security
    "SECRET_KEY": "test-secret-key-for-unit-tests-only-minimum-32-chars!",
    "ENCRYPTION_KEY": "test-encryption-key-for-unit-tests!!32c",
    # Storage
    "S3_BUCKET": "avoided-emissions-test-bucket",
    "S3_PREFIX": "test-prefix",
    # Celery — use a separate Redis DB (9) to avoid polluting dev data
    "CELERY_BROKER_URL": "redis://localhost:6379/9",
    "CELERY_RESULT_BACKEND": "redis://localhost:6379/9",
    # trends.earth API
    "TRENDSEARTH_API_URL": "https://api.trends.earth/api/v1",
    "TRENDSEARTH_SCRIPT_ID": "00000000-0000-0000-0000-000000000001",
    "TRENDSEARTH_CLIENT_ID": "test-client-id",
    "TRENDSEARTH_CLIENT_SECRET": "test-client-secret",
    # Misc
    "ROLLBAR_ACCESS_TOKEN": "",
    "ENVIRONMENT": "test",
    "ENABLE_TASK_ADOPTION": "false",
    "AWS_REGION": "us-east-1",
}

for _key, _value in _TEST_DEFAULTS.items():
    os.environ.setdefault(_key, _value)
