"""tasks package — re-exports all Celery tasks for backward compatibility.

``celery_app.py`` uses ``imports=["tasks"]`` which causes Celery to import
this package at worker startup, discovering every task decorated with
``@celery_app.task``.

External callers (services, app.py) that reference task objects by name
or that do ``import tasks as webapp_tasks`` continue to work unchanged.
"""

# Import submodules to register all @celery_app.task decorators.
from tasks.analysis import (  # noqa: F401
    generate_match_quality_summary_task,
    poll_batch_tasks,
    submit_analysis_task_worker,
)
from tasks.covariate import (  # noqa: F401
    auto_merge_unmerged,
    poll_gee_exports,
    run_cog_merge,
)
from tasks.reference_layers import (  # noqa: F401
    export_reference_layers_task,
    ingest_sdg_cog_task,
)
from tasks.site_upload import import_user_site_upload_task  # noqa: F401
from tasks.vector_import import (  # noqa: F401
    import_vector_data_task,
    rasterize_vectors_task,
)
