"""add submitting to task_status

Revision ID: 0024a1b2c3eb
Revises: 0023a1b2c3ea
Create Date: 2026-06-02 00:00:00.000000

Add ``'submitting'`` to the ``task_status`` enum.  Tasks enter this state
immediately after the user presses Submit, while a Celery worker handles
the slow parts of submission (PostGIS geometry computations, S3 site
upload, and the trends.earth API call).  The task transitions to
``'submitted'`` on success or ``'failed'`` if an error occurs in the worker.
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0024a1b2c3eb"
down_revision: Union[str, None] = "0023a1b2c3ea"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE task_status ADD VALUE IF NOT EXISTS 'submitting'")


def downgrade() -> None:
    # PostgreSQL does not support removing values from an enum type.
    # Downgrade is a no-op; 'submitting' rows should be cleaned up
    # before rolling back the migration.
    pass
