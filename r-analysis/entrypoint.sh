#!/bin/bash
set -euo pipefail

# Entrypoint for the R analysis container. Dispatches to the appropriate
# analysis step based on the first argument.
#
# Usage:
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
    local token="${ROLLBAR_ACCESS_TOKEN:-}"
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

COMMAND="${1:-help}"
shift || true

# When launched by the trends.earth API (EXECUTION_ID is set and the first
# argument is "api-run"), use the Python wrapper which handles param
# retrieval, status updates, and result posting.
if [ "$COMMAND" = "api-run" ] || [ -n "${EXECUTION_ID:-}" -a "$COMMAND" = "analyze" ]; then
    echo "Running via API wrapper (execution $EXECUTION_ID)..."
    exec python3 /app/api_wrapper.py
fi

case "$COMMAND" in
    analyze)
        # Full pipeline: extract + match + summarize
        echo "Running full analysis pipeline..."
        run_step Rscript /app/scripts/01_extract_covariates.R "$@"
        run_step Rscript /app/scripts/02_perform_matching.R "$@"
        run_step Rscript /app/scripts/03_summarize_results.R "$@"
        ;;
    extract)
        echo "Extracting covariates..."
        run_step Rscript /app/scripts/01_extract_covariates.R "$@"
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
