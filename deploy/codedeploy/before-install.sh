#!/bin/bash
# BeforeInstall - prepare environment, verify Docker and AWS CLI, clean up disk.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

log_info "BeforeInstall hook started"

if ! is_swarm_leader; then
    log_info "Non-leader node -- performing minimal setup only"
    if command -v docker &> /dev/null && ! systemctl is-active --quiet docker; then
        log_info "Starting Docker service..."
        systemctl start docker || true
    fi
    log_success "BeforeInstall hook completed (non-leader node)"
    exit 0
fi

log_info "This node is the Swarm leader -- performing full setup"

ENVIRONMENT=$(detect_environment)
log_info "Detected environment: $ENVIRONMENT"

APP_DIR=$(get_app_directory "$ENVIRONMENT")
log_info "Application directory: $APP_DIR"

if [ ! -d "$APP_DIR" ]; then
    log_info "Creating application directory: $APP_DIR"
    mkdir -p "$APP_DIR"
fi
chown -R ubuntu:ubuntu "$APP_DIR"

# -- Verify Docker ----------------------------------------------------------

log_info "Checking Docker installation..."
if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed."
    exit 1
fi

if ! systemctl is-active --quiet docker; then
    log_info "Starting Docker service..."
    systemctl start docker
fi

if ! docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null | grep -q "active"; then
    log_info "Initializing Docker Swarm..."
    docker swarm init --advertise-addr "$(hostname -I | awk '{print $1}')" || {
        log_warning "Swarm init failed, may already be part of a swarm"
    }
fi

# -- Docker cleanup ----------------------------------------------------------

log_info "Checking Docker disk usage..."
DISK_USAGE=$(df /var/lib/docker 2>/dev/null | tail -1 | awk '{print $5}' | tr -d '%')
log_info "Current Docker disk usage: ${DISK_USAGE:-unknown}%"

docker system df 2>/dev/null || true

# Basic cleanup
docker container prune -f 2>/dev/null || true
docker image prune -f 2>/dev/null || true
docker image prune -a --filter "until=168h" -f 2>/dev/null || true

if [ -n "$DISK_USAGE" ] && [ "$DISK_USAGE" -gt 70 ]; then
    log_warning "Disk usage above 70%, aggressive cleanup..."
    docker image prune -a -f 2>/dev/null || true
fi

if [ -n "$DISK_USAGE" ] && [ "$DISK_USAGE" -gt 85 ]; then
    log_warning "Disk usage above 85%, emergency cleanup..."
    docker system prune -a -f --volumes 2>/dev/null || true
fi

docker system df 2>/dev/null || true

# -- Verify AWS CLI ----------------------------------------------------------

log_info "Verifying AWS CLI installation..."
if ! command -v aws &> /dev/null; then
    log_error "AWS CLI is not installed."
    exit 1
fi

REGION=$(get_aws_region)
if ! aws ecr get-login-password --region "$REGION" > /dev/null 2>&1; then
    log_error "Cannot authenticate to ECR. Ensure EC2 instance has proper IAM role."
    exit 1
fi
log_success "ECR access verified"

log_success "BeforeInstall hook completed"
exit 0
