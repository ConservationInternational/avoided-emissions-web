"""CLI for exporting GEE covariate layers to Google Cloud Storage.

Usage:
    python export_covariates.py --bucket BUCKET [--prefix PREFIX] [--covariates NAME ...]
    python export_covariates.py --list
    python export_covariates.py --status
"""

import json
import os
import sys
import time

import click
import ee

from config import COVARIATES, DEFAULT_GCS_PREFIX
from tasks import check_task_status, export_admin_region_key, start_export_task


@click.command()
@click.option(
    "--bucket", type=str, default=None,
    help="GCS bucket name for exports.",
)
@click.option(
    "--prefix", type=str, default=DEFAULT_GCS_PREFIX,
    help="GCS path prefix within the bucket.",
)
@click.option(
    "--covariates", type=str, multiple=True,
    help="Specific covariate names to export. If omitted, exports all.",
)
@click.option(
    "--list", "list_covariates", is_flag=True, default=False,
    help="List all available covariates and exit.",
)
@click.option(
    "--status", "check_status", is_flag=True, default=False,
    help="Check status of running GEE export tasks.",
)
@click.option(
    "--category", type=str, default=None,
    help="Export only covariates in this category.",
)
@click.option(
    "--wait", "wait_for_completion", is_flag=True, default=False,
    help="Wait for all tasks to complete (polls every 60s).",
)
@click.option(
    "--scale", type=float, default=None,
    help="Export scale in meters (default: ~927m / ~1km).",
)
def main(bucket, prefix, covariates, list_covariates, check_status,
         category, wait_for_completion, scale):
    """Export covariate layers from Google Earth Engine to GCS as COGs."""

    if list_covariates:
        _print_covariate_list(category)
        return

    project = os.environ.get("GOOGLE_PROJECT_ID") or None
    opt_url = os.environ.get("GEE_ENDPOINT") or None

    # Authenticate with a service account if credentials are provided
    ee_sa_json = os.environ.get("EE_SERVICE_ACCOUNT_JSON", "")
    if ee_sa_json:
        import base64
        try:
            key_data = base64.b64decode(ee_sa_json).decode("utf-8")
        except Exception:
            key_data = ee_sa_json
        sa_info = json.loads(key_data)
        credentials = ee.ServiceAccountCredentials(
            sa_info["client_email"], key_data=json.dumps(sa_info)
        )
        ee.Initialize(credentials=credentials, project=project, opt_url=opt_url)
    else:
        ee.Initialize(project=project, opt_url=opt_url)

    if check_status:
        _print_task_status()
        return

    if not bucket:
        click.echo("Error: --bucket is required for exports.", err=True)
        sys.exit(1)

    # Determine which covariates to export
    if covariates:
        names = list(covariates)
        invalid = [n for n in names if n not in COVARIATES]
        if invalid:
            click.echo(
                f"Error: unknown covariates: {', '.join(invalid)}", err=True
            )
            sys.exit(1)
    elif category:
        names = [
            k for k, v in COVARIATES.items()
            if v.get("category") == category
        ]
        if not names:
            click.echo(f"Error: no covariates in category '{category}'",
                       err=True)
            sys.exit(1)
    else:
        names = list(COVARIATES.keys())

    click.echo(f"Starting {len(names)} export task(s) to gs://{bucket}/{prefix}/")

    tasks = []
    for name in names:
        click.echo(f"  Starting: {name}")
        task = start_export_task(
            covariate_name=name,
            bucket=bucket,
            prefix=prefix,
            scale=scale,
        )
        tasks.append((name, task))

    click.echo(f"\n{len(tasks)} task(s) submitted to GEE.")

    # If the 'region' covariate was exported, also upload its CSV key
    if "region" in names:
        click.echo("\nUploading region ID key CSV...")
        try:
            csv_path = export_admin_region_key(bucket, prefix)
            click.echo(f"  Region key saved to {csv_path}")
        except Exception as exc:
            click.echo(f"  WARNING: failed to upload region key: {exc}", err=True)

    if wait_for_completion:
        _wait_for_tasks(tasks)


def _print_covariate_list(category_filter=None):
    """Print a formatted list of available covariates."""
    current_category = None
    for name, cfg in sorted(COVARIATES.items(),
                            key=lambda x: (x[1].get("category", ""), x[0])):
        cat = cfg.get("category", "uncategorized")
        if category_filter and cat != category_filter:
            continue
        if cat != current_category:
            current_category = cat
            click.echo(f"\n[{cat}]")
        desc = cfg.get("description", "")
        is_derived = "derived" if cfg.get("derived") else "asset"
        click.echo(f"  {name:30s} ({is_derived}) {desc}")


def _print_task_status():
    """Print status of all active GEE tasks."""
    tasks = ee.batch.Task.list()
    active = [t for t in tasks if t.state in ("READY", "RUNNING")]
    if not active:
        click.echo("No active GEE tasks.")
        return

    click.echo(f"{len(active)} active task(s):")
    for task in active:
        status = task.status()
        click.echo(
            f"  {status.get('description', 'unknown'):40s} "
            f"state={status.get('state')}  "
            f"id={status.get('id', 'N/A')}"
        )


def _wait_for_tasks(tasks, poll_interval=60):
    """Poll GEE tasks until all complete or fail."""
    remaining = dict(tasks)
    while remaining:
        time.sleep(poll_interval)
        completed = []
        for name, task in remaining.items():
            status = check_task_status(task)
            state = status["state"]
            if state == "COMPLETED":
                click.echo(f"  Completed: {name}")
                completed.append(name)
            elif state == "FAILED":
                click.echo(
                    f"  FAILED: {name} - {status.get('error_message', '')}"
                )
                completed.append(name)
            elif state == "CANCELLED":
                click.echo(f"  Cancelled: {name}")
                completed.append(name)

        for name in completed:
            del remaining[name]

        if remaining:
            click.echo(
                f"  {len(remaining)} task(s) still running... "
                f"(polling every {poll_interval}s)"
            )

    click.echo("All tasks finished.")


if __name__ == "__main__":
    main()
