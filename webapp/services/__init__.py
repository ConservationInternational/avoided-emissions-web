"""services package — re-exports the full public API of the monolithic services.py.

External callers (app.py, callbacks.py, layouts.py, tasks.py) continue to use
``from services import <name>`` exactly as before.

Model symbols (UserSiteUpload, get_db, …) that tasks.py currently imports
*through* services are also re-exported here for backward compatibility.
"""

# -- Model symbols re-exported for backward-compat (tasks.py imports them via services) --
from models import (
    AnalysisTask,
    Covariate,
    CovariatePreset,
    MatchingSettingsPreset,
    ReferenceLayerExport,
    TaskResult,
    TaskResultTotal,
    TaskSite,
    User,
    UserSiteSet,
    UserSiteUpload,
    get_db,
)

# -- Analysis task submission + results --
from services.analysis_task import (
    ALLOWED_MATCHING_JOB_QUEUES,
    ANALYSIS_DEFAULTS,
    DEFAULT_MATCHING_JOB_QUEUE,
    _complete_analysis_task_submission,
    adopt_api_execution,
    cancel_task,
    download_results_csv,
    generate_match_quality_summary,
    get_task_detail,
    get_task_list,
    get_task_site_results,
    import_execution_results,
    list_task_s3_files,
    queue_analysis_task,
    submit_analysis_task,
    update_task_info,
)

# -- GEE covariate management --
from services.covariate import (
    force_reexport,
    force_remerge,
    get_covariate_inventory,
    get_ready_covariate_names,
    get_ready_exact_match_names,
    list_export_tiles,
    start_gee_export,
)

# -- Presets --
from services.preset import (
    delete_covariate_preset,
    delete_matching_settings_preset,
    get_covariate_presets,
    get_matching_settings_presets,
    save_covariate_preset,
    save_matching_settings_preset,
)

# -- Reference layers --
from services.reference_layers import (
    compute_exact_match_groups_with_splitting,
    compute_matching_extent,
    compute_sites_exclusion_buffer,
    export_reference_layers_to_s3,
    get_reference_layer_uris,
)

# -- S3 helpers --
from services.s3 import S3_COST_TAGGING, get_s3_client

# -- Share links + resubmit --
from services.sharing import (
    create_share_link,
    get_recompute_config,
    list_share_links,
    resubmit_analysis_task,
    revoke_share_link,
    validate_share_token,
)

# -- Site set management --
from services.site_set import (
    archive_user_site_set,
    delete_user_site_set,
    get_user_site_set_centroids_geojson,
    get_user_site_set_detail,
    get_user_site_set_gdf,
    get_user_site_set_geojson,
    get_user_site_set_preview_rows,
    list_user_site_sets,
    rename_user_site_set,
    upload_sites_parquet_to_s3,
    upload_sites_to_geojson,
    upload_sites_to_s3,
    upload_user_site_set_geojson_to_s3,
)

# -- Site upload --
from services.site_upload import (
    ALL_SITE_FIELDS,
    OPTIONAL_SITE_FIELDS,
    REQUIRED_SITE_FIELDS,
    apply_site_column_mapping,
    cancel_user_site_upload,
    create_user_site_upload,
    delete_user_site_upload,
    discard_staged_site_upload,
    get_site_upload_mapping_preview_from_staged,
    get_staged_site_upload,
    list_user_site_uploads,
    save_user_site_set,
    save_user_site_set_from_staged,
    stage_site_upload,
    stream_stage_site_upload,
    suggest_site_column_mapping,
    update_user_site_upload_status,
    validate_site_upload_mapping,
)

# -- User administration --
from services.user_admin import (
    approve_user,
    change_user_role,
    delete_user,
    get_user_list,
    grant_te_script_access,
    revoke_te_script_access,
)

__all__ = [
    # models re-exported
    "AnalysisTask",
    "Covariate",
    "CovariatePreset",
    "MatchingSettingsPreset",
    "ReferenceLayerExport",
    "TaskResult",
    "TaskResultTotal",
    "TaskSite",
    "User",
    "UserSiteUpload",
    "UserSiteSet",
    "get_db",
    # s3
    "S3_COST_TAGGING",
    "get_s3_client",
    # site_upload
    "ALL_SITE_FIELDS",
    "OPTIONAL_SITE_FIELDS",
    "REQUIRED_SITE_FIELDS",
    "apply_site_column_mapping",
    "cancel_user_site_upload",
    "create_user_site_upload",
    "delete_user_site_upload",
    "discard_staged_site_upload",
    "get_site_upload_mapping_preview_from_staged",
    "get_staged_site_upload",
    "list_user_site_uploads",
    "save_user_site_set",
    "save_user_site_set_from_staged",
    "stage_site_upload",
    "stream_stage_site_upload",
    "suggest_site_column_mapping",
    "update_user_site_upload_status",
    "validate_site_upload_mapping",
    # site_set
    "archive_user_site_set",
    "delete_user_site_set",
    "get_user_site_set_centroids_geojson",
    "get_user_site_set_detail",
    "get_user_site_set_geojson",
    "get_user_site_set_gdf",
    "get_user_site_set_preview_rows",
    "list_user_site_sets",
    "rename_user_site_set",
    "upload_sites_parquet_to_s3",
    "upload_sites_to_geojson",
    "upload_sites_to_s3",
    "upload_user_site_set_geojson_to_s3",
    # reference_layers
    "compute_exact_match_groups_with_splitting",
    "compute_matching_extent",
    "compute_sites_exclusion_buffer",
    "export_reference_layers_to_s3",
    "get_reference_layer_uris",
    # analysis_task
    "ALLOWED_MATCHING_JOB_QUEUES",
    "ANALYSIS_DEFAULTS",
    "DEFAULT_MATCHING_JOB_QUEUE",
    "_complete_analysis_task_submission",
    "adopt_api_execution",
    "cancel_task",
    "download_results_csv",
    "generate_match_quality_summary",
    "get_task_detail",
    "get_task_list",
    "get_task_site_results",
    "import_execution_results",
    "list_task_s3_files",
    "queue_analysis_task",
    "submit_analysis_task",
    "update_task_info",
    # user_admin
    "approve_user",
    "change_user_role",
    "delete_user",
    "get_user_list",
    "grant_te_script_access",
    "revoke_te_script_access",
    # covariate
    "force_reexport",
    "force_remerge",
    "get_covariate_inventory",
    "get_ready_covariate_names",
    "get_ready_exact_match_names",
    "list_export_tiles",
    "start_gee_export",
    # preset
    "delete_covariate_preset",
    "delete_matching_settings_preset",
    "get_covariate_presets",
    "get_matching_settings_presets",
    "save_covariate_preset",
    "save_matching_settings_preset",
    # sharing
    "create_share_link",
    "get_recompute_config",
    "list_share_links",
    "resubmit_analysis_task",
    "revoke_share_link",
    "validate_share_token",
]
