#!/bin/bash
# ApplicationStart - deploy Docker stack and wait for services to be healthy.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

log_info "ApplicationStart hook started"

check_swarm_leader_or_skip

ENVIRONMENT=$(detect_environment)
log_info "Detected environment: $ENVIRONMENT"

APP_DIR=$(get_app_directory "$ENVIRONMENT")
STACK_NAME=$(get_stack_name "$ENVIRONMENT")
COMPOSE_FILE=$(get_compose_file "$ENVIRONMENT")

cd "$APP_DIR"
log_info "Working directory: $APP_DIR"
log_info "Stack name: $STACK_NAME"
log_info "Compose file: $COMPOSE_FILE"

# -- Load environment --------------------------------------------------------

ENV_FILE=$(get_env_file "$ENVIRONMENT")
log_info "Loading environment from $ENV_FILE"
if ! safe_source_env "$ENV_FILE"; then
    log_error "Failed to load environment file: $ENV_FILE"
    exit 1
fi

# -- ECR credentials ---------------------------------------------------------

if [ -n "$ECR_REGISTRY" ]; then
    log_info "Refreshing ECR credentials..."
    ecr_login "$ECR_REGISTRY" || { log_error "ECR login failed"; exit 1; }
fi

# -- Verify compose file -----------------------------------------------------

if [ ! -f "$COMPOSE_FILE" ]; then
    log_error "Compose file not found: $COMPOSE_FILE"
    ls -la "$APP_DIR"/deploy/*.yml 2>/dev/null || echo "No compose files found"
    exit 1
fi

# -- Stack health check and recovery -----------------------------------------

log_info "Checking stack health before deployment..."
STACK_EXISTS=$(docker stack ls --format "{{.Name}}" 2>/dev/null | grep -c "^${STACK_NAME}$" || echo "0")

if [ "$STACK_EXISTS" -gt 0 ]; then
    log_info "Stack $STACK_NAME exists, checking health..."
    if ! check_for_stuck_services "$STACK_NAME"; then
        log_warning "Found stuck services, initiating recovery..."
        recover_stack "$STACK_NAME"
    fi
else
    log_info "Stack $STACK_NAME does not exist, will create fresh"
fi

# -- Deploy stack ------------------------------------------------------------

log_info "Deploying stack: $STACK_NAME"

MAX_ATTEMPTS=3
ATTEMPT=1

while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    log_info "Stack deploy attempt $ATTEMPT/$MAX_ATTEMPTS..."
    if docker stack deploy \
        -c "$COMPOSE_FILE" \
        --with-registry-auth \
        --resolve-image always \
        "$STACK_NAME"; then
        log_success "Stack deploy command succeeded"
        break
    else
        log_warning "Stack deploy failed on attempt $ATTEMPT"
        if [ $ATTEMPT -ge $MAX_ATTEMPTS ]; then
            log_error "Stack deploy failed after $MAX_ATTEMPTS attempts"
            exit 1
        fi
        sleep 10
        ATTEMPT=$((ATTEMPT + 1))
    fi
done

# -- Wait for services -------------------------------------------------------

log_info "Waiting for services to initialize..."
sleep 15

log_info "Current service status:"
docker service ls --filter "name=${STACK_NAME}_" --format "table {{.Name}}\t{{.Replicas}}\t{{.Image}}"

is_service_ready() {
    local replicas="$1"
    if echo "$replicas" | grep -qE "^[0-9]+/[0-9]+$"; then
        local current="${replicas%/*}"
        local desired="${replicas#*/}"
        [ "$current" = "$desired" ] && [ "$desired" != "0" ]
    else
        return 1
    fi
}

MAX_WAIT=180
WAIT_TIME=0

while [ $WAIT_TIME -lt $MAX_WAIT ]; do
    NOT_READY=""
    while IFS= read -r line; do
        [ -z "$line" ] && continue
        service_name=$(echo "$line" | awk '{print $1}')
        replicas=$(echo "$line" | awk '{print $2}')
        if ! is_service_ready "$replicas"; then
            NOT_READY="${NOT_READY}${line}\n"
        fi
    done < <(docker service ls --filter "name=${STACK_NAME}_" --format "{{.Name}} {{.Replicas}}" 2>/dev/null)

    if [ -z "$NOT_READY" ]; then
        log_success "All services are running"
        break
    fi

    log_info "Waiting for services... ($WAIT_TIME/$MAX_WAIT seconds)"
    docker service ls --filter "name=${STACK_NAME}_" --format "table {{.Name}}\t{{.Replicas}}"
    sleep 10
    WAIT_TIME=$((WAIT_TIME + 10))
done

if [ $WAIT_TIME -ge $MAX_WAIT ]; then
    log_error "Services did not start within $MAX_WAIT seconds"
    for service in $(docker service ls --filter "name=${STACK_NAME}_" --format "{{.Name}}" 2>/dev/null); do
        REPLICAS=$(docker service ls --filter "name=$service" --format "{{.Replicas}}" 2>/dev/null)
        if ! is_service_ready "$REPLICAS"; then
            log_error "Service $service stuck (Replicas: $REPLICAS)"
            docker service logs --tail 20 "$service" 2>&1 || true
        fi
    done
    exit 1
fi

# -- Final status ------------------------------------------------------------

log_info "Final service status:"
docker service ls --filter "name=${STACK_NAME}_" --format "table {{.Name}}\t{{.Replicas}}\t{{.Image}}"

log_success "ApplicationStart hook completed"
exit 0
