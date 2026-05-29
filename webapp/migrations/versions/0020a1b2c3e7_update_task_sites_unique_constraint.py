"""Update task_sites unique constraint to include sub_site_index

Revision ID: 0020a1b2c3e7
Revises: 0019a1b2c3e6
Create Date: 2026-05-29 16:00:00.000000

Updates the unique constraint on task_sites to include sub_site_index,
allowing multiple sub-sites with the same site_id within a task.
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0020a1b2c3e7"
down_revision = "0019a1b2c3e6"
branch_labels = None
depends_on = None


def upgrade():
    # Drop the old unique constraint (task_id, site_id)
    op.drop_constraint("task_sites_task_id_site_id_key", "task_sites", type_="unique")

    # Create new unique constraint (task_id, site_id, sub_site_index)
    op.create_unique_constraint(
        "task_sites_task_id_site_id_sub_site_index_key",
        "task_sites",
        ["task_id", "site_id", "sub_site_index"],
    )


def downgrade():
    # Drop the new unique constraint
    op.drop_constraint(
        "task_sites_task_id_site_id_sub_site_index_key", "task_sites", type_="unique"
    )

    # Recreate the old unique constraint
    # Note: This will fail if there are multiple sub-sites with the same site_id
    op.create_unique_constraint(
        "task_sites_task_id_site_id_key", "task_sites", ["task_id", "site_id"]
    )
