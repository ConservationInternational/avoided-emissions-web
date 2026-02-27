#!/bin/bash
# ValidateService - health checks to verify the deployment was successful.
# Failure causes CodeDeploy to mark the deployment as failed and optionally rollback.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

log_info "ValidateService hook started"

check_swarm_leader_or_skip

ENVIRONMENT=$(detect_environment)
log_info "Detected environment: $ENVIRONMENT"

APP_DIR=$(get_app_directory "$ENVIRONMENT")
STACK_NAME=$(get_stack_name "$ENVIRONMENT")
APP_PORT=$(get_app_port "$ENVIRONMENT")

cd "$APP_DIR"
log_info "Stack name: $STACK_NAME"
log_info "App port: $APP_PORT"

# -- Service status ----------------------------------------------------------

log_info "Checking service status..."
docker service ls --filter "name=${STACK_NAME}_" \
    --format "table {{.Name}}\t{{.Replicas}}\t{{.Image}}"

WEBAPP_SERVICE="${STACK_NAME}_webapp"
WEBAPP_REPLICAS=$(docker service ls --filter "name=${WEBAPP_SERVICE}" \
    --format "{{.Replicas}}" 2>/dev/null || echo "0/0")

if [ "$WEBAPP_REPLICAS" = "0/0" ] || [ -z "$WEBAPP_REPLICAS" ]; then
    log_error "Webapp service is not running!"
    docker service ps "$WEBAPP_SERVICE" \
        --format "table {{.Name}}\t{{.CurrentState}}\t{{.Error}}" 2>/dev/null || true
    exit 1
fi
log_success "Webapp service status: $WEBAPP_REPLICAS"

# -- Health check - webapp endpoint ------------------------------------------

log_info "Performing health check on port $APP_PORT..."
HEALTH_URL="http://127.0.0.1:${APP_PORT}/"
MAX_ATTEMPTS=30
ATTEMPT=1

while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    log_info "Health check attempt $ATTEMPT/$MAX_ATTEMPTS..."
    HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" "$HEALTH_URL" 2>/dev/null || echo "000")

    if [ "$HTTP_CODE" = "200" ]; then
        log_success "Health check passed (HTTP $HTTP_CODE)"
        break
    else
        log_warning "Health check returned HTTP $HTTP_CODE"

        if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
            log_error "Health check failed after $MAX_ATTEMPTS attempts"
            log_info "Debugging:"
            docker service ps "$WEBAPP_SERVICE" \
                --format "table {{.Name}}\t{{.CurrentState}}\t{{.Error}}" 2>/dev/null || true
            docker service logs --tail 30 "$WEBAPP_SERVICE" 2>/dev/null || true
            docker ps --filter "name=${STACK_NAME}" \
                --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || true
            exit 1
        fi

        sleep 10
        ATTEMPT=$((ATTEMPT + 1))
    fi
done

# -- Deployment summary ------------------------------------------------------

log_info "Deployment validation summary:"
echo "  Stack: $STACK_NAME"
echo "  Webapp Service: $WEBAPP_REPLICAS"
echo "  Port: $APP_PORT"
echo "  Health Check: Passed"

log_success "ValidateService hook completed -- deployment successful!"
exit 0
