#!/bin/bash
set -euo pipefail

# Entrypoint for the R analysis container. Dispatches to the appropriate
# analysis step based on the first argument.
#
# When EXECUTION_ID is set (Batch mode), batch_runner.py handles S3
# param retrieval and result upload.  The API's monitor_batch_executions
# periodic task manages status transitions.
#
# Usage (standalone / debugging):
#   docker run r-analysis analyze --config /data/config.json
#   docker run r-analysis extract --config /data/config.json
#   docker run r-analysis match --config /data/config.json --site-id SITE_001
#   docker run r-analysis summarize --config /data/config.json

# -- Rollbar fallback --------------------------------------------------------
# If an R script fails and Rollbar reporting from within R did not succeed
# (e.g. the failure happened before utils.R was sourced), this function
# sends a minimal error report via curl as a last resort.
rollbar_report() {
    local message="$1"
    local token="${ROLLBAR_SCRIPT_TOKEN:-}"
    local env="${ROLLBAR_ENVIRONMENT:-${ENVIRONMENT:-development}}"

    if [ -z "$token" ]; then
        return 0
    fi

    curl -s --max-time 10 \
        -H "Content-Type: application/json" \
        -d "{
            \"access_token\": \"${token}\",
            \"data\": {
                \"environment\": \"${env}\",
                \"body\": {
                    \"message\": {
                        \"body\": \"${message}\"
                    }
                },
                \"level\": \"error\",
                \"language\": \"shell\",
                \"framework\": \"entrypoint.sh\",
                \"server\": {
                    \"host\": \"$(hostname)\"
                }
            }
        }" \
        https://api.rollbar.com/api/1/item/ > /dev/null 2>&1 || true
}

run_step() {
    "$@" || {
        local exit_code=$?
        rollbar_report "R analysis failed (exit code ${exit_code}): $*"
        exit $exit_code
    }
}

# -- Batch mode: API-managed lifecycle ----------------------------------------
# When dispatched by the trends.earth API to AWS Batch, EXECUTION_ID is set.
# batch_runner.py handles S3 param retrieval and result upload.  The API's
# monitor_batch_executions periodic task manages status transitions —
# no API credentials or status callbacks are needed in this container.
if [ -n "${EXECUTION_ID:-}" ]; then
    echo "Batch mode: EXECUTION_ID=${EXECUTION_ID}"
    exec python /app/batch_runner.py "$@"
fi

# -- Standalone / debug mode --------------------------------------------------
COMMAND="${1:-help}"
shift || true

case "$COMMAND" in
    analyze)
        # Full pipeline: extract (Python) + match + summarize
        echo "Running full analysis pipeline..."
        run_step python /app/scripts/01_extract_covariates.py "$@"
        run_step Rscript /app/scripts/02_perform_matching.R "$@"
        run_step Rscript /app/scripts/03_summarize_results.R "$@"
        ;;
    extract)
        echo "Extracting covariates (Python)..."
        run_step python /app/scripts/01_extract_covariates.py "$@"
        ;;
    match)
        echo "Running matching for individual site..."
        run_step Rscript /app/scripts/02_perform_matching.R "$@"
        ;;
    summarize)
        echo "Summarizing results..."
        run_step Rscript /app/scripts/03_summarize_results.R "$@"
        ;;
    help|--help|-h)
        echo "Avoided Emissions Analysis Container"
        echo ""
        echo "When launched by the trends.earth API, this container's main.py"
        echo "is placed at gefcore/script/main.py inside the Environment image."
        echo "The entrypoint below is for standalone / debugging use only."
        echo ""
        echo "Commands:"
        echo "  analyze    Run the full pipeline (extract + match + summarize)"
        echo "  extract    Extract covariate values for sites and controls"
        echo "  match      Run propensity score matching for a single site"
        echo "  summarize  Summarize matching results into emissions estimates"
        echo ""
        echo "Options:"
        echo "  --config PATH    Path to the task configuration JSON file"
        echo "  --site-id ID     Site ID to process (for 'match' command)"
        echo "  --data-dir PATH  Base directory for input/output data"
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo "Run with 'help' for usage information."
        exit 1
        ;;
esac
