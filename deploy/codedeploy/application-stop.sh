#!/bin/bash
# ApplicationStop - gracefully prepare existing services for redeployment.
# Only affects the specific environment being deployed.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

log_info "ApplicationStop hook started"

check_swarm_leader_or_skip

ENVIRONMENT=$(detect_environment)
log_info "Detected environment: $ENVIRONMENT"

STACK_NAME=$(get_stack_name "$ENVIRONMENT")
log_info "Stack name: $STACK_NAME"

if ! docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null | grep -q "active"; then
    log_info "Docker Swarm is not active, nothing to stop"
    exit 0
fi

if ! docker stack ls --format "{{.Name}}" 2>/dev/null | grep -q "^${STACK_NAME}$"; then
    log_info "Stack $STACK_NAME does not exist, nothing to stop"
    exit 0
fi

log_info "Preparing services for deployment..."

SERVICES=$(docker service ls --filter "name=${STACK_NAME}_" --format "{{.Name}}" 2>/dev/null || echo "")
if [ -n "$SERVICES" ]; then
    log_info "Current services in stack:"
    docker service ls --filter "name=${STACK_NAME}_" --format "table {{.Name}}\t{{.Replicas}}\t{{.Image}}"
    log_success "Services prepared for deployment"
else
    log_info "No services found in stack $STACK_NAME"
fi

log_success "ApplicationStop hook completed"
exit 0
