#!/bin/bash
# Common functions for CodeDeploy lifecycle scripts.
#
# Both staging and production can run on the same EC2 instance:
#   - Production: /opt/avoided-emissions-web-production (port 8050)
#   - Staging:    /opt/avoided-emissions-web-staging    (port 8051)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Detect environment from CodeDeploy deployment group or hostname.
detect_environment() {
    if [ -n "$DEPLOYMENT_GROUP_NAME" ]; then
        if echo "$DEPLOYMENT_GROUP_NAME" | grep -qi "staging"; then
            echo "staging"; return
        elif echo "$DEPLOYMENT_GROUP_NAME" | grep -qi "production\|prod"; then
            echo "production"; return
        fi
    fi

    HOSTNAME=$(hostname)
    if echo "$HOSTNAME" | grep -qi "staging"; then
        echo "staging"; return
    elif echo "$HOSTNAME" | grep -qi "production\|prod"; then
        echo "production"; return
    fi

    if [ -f "/opt/avoided-emissions-web-staging/.env" ]; then
        echo "staging"; return
    elif [ -f "/opt/avoided-emissions-web-production/.env" ]; then
        echo "production"; return
    fi

    log_warning "Could not determine environment, defaulting to staging"
    echo "staging"
}

get_app_directory() {
    local environment="$1"
    echo "/opt/avoided-emissions-web-${environment}"
}

get_compose_file() {
    local environment="$1"
    if [ "$environment" = "staging" ]; then
        echo "deploy/docker-compose.staging.yml"
    else
        echo "deploy/docker-compose.prod.yml"
    fi
}

get_stack_name() {
    local environment="$1"
    echo "avoided-emissions-${environment}"
}

get_app_port() {
    local environment="$1"
    if [ "$environment" = "production" ]; then
        echo "8050"
    else
        echo "8051"
    fi
}

get_env_file() {
    local environment="$1"
    local app_dir
    app_dir=$(get_app_directory "$environment")
    echo "$app_dir/.env"
}

# Read environment file line-by-line to avoid bash interpreting special chars.
safe_source_env() {
    local file="$1"
    if [ ! -f "$file" ]; then
        log_error "Environment file not found: $file"
        return 1
    fi

    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ -z "$line" ]] && continue
        [[ "$line" =~ ^[[:space:]]*# ]] && continue
        [[ "$line" != *"="* ]] && continue

        local key="${line%%=*}"
        local value="${line#*=}"
        [[ -z "$key" ]] && continue

        if [[ "$value" =~ ^\'(.*)\'$ ]]; then
            value="${BASH_REMATCH[1]}"
        elif [[ "$value" =~ ^\"(.*)\"$ ]]; then
            value="${BASH_REMATCH[1]}"
        fi

        export "$key=$value"
    done < "$file"
    return 0
}

wait_for_service() {
    local service_url="$1"
    local max_attempts="${2:-30}"
    local wait_time="${3:-10}"
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if curl -sf "$service_url" > /dev/null 2>&1; then
            return 0
        fi
        log_info "Waiting for $service_url (attempt $attempt/$max_attempts)..."
        sleep $wait_time
        attempt=$((attempt + 1))
    done
    return 1
}

get_aws_region() {
    local TOKEN
    TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || echo "")

    local REGION=""
    if [ -n "$TOKEN" ]; then
        REGION=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
            http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null || echo "")
    else
        REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null || echo "")
    fi

    if [ -z "$REGION" ]; then
        REGION="us-east-1"
    fi
    echo "$REGION"
}

ecr_login() {
    local region
    region=$(get_aws_region)
    local ecr_registry="$1"

    log_info "Logging in to Amazon ECR in region $region..."
    aws ecr get-login-password --region "$region" | \
        docker login --username AWS --password-stdin "$ecr_registry" || {
        log_error "Failed to log in to ECR"
        return 1
    }
    log_success "ECR login successful"
    return 0
}

is_swarm_leader() {
    if ! docker info >/dev/null 2>&1; then
        log_warning "Docker daemon unreachable"
        return 1
    fi

    local manager_status
    manager_status=$(docker node ls --format '{{.Self}} {{.ManagerStatus}}' 2>/dev/null \
        | awk '$1=="true" {print $2}')

    if [ -z "$manager_status" ]; then
        log_warning "Node is not part of the swarm or status unknown"
        return 1
    fi

    if [ "$manager_status" != "Leader" ]; then
        log_info "Node is a swarm manager but not the leader (status: $manager_status)"
        return 1
    fi

    log_success "Node is the active swarm leader"
    return 0
}

check_swarm_leader_or_skip() {
    log_info "Checking swarm manager status..."
    if ! is_swarm_leader; then
        log_info "Skipping deployment on this node (not the swarm leader)"
        exit 0
    fi
}

recover_stack() {
    local stack_name="$1"

    log_warning "Attempting stack recovery for $stack_name..."

    if docker stack ls --format "{{.Name}}" 2>/dev/null | grep -q "^${stack_name}$"; then
        log_info "Removing existing stack..."
        docker stack rm "$stack_name" 2>/dev/null || true

        local wait_count=0
        local max_wait=60
        while [ $wait_count -lt $max_wait ]; do
            local remaining
            remaining=$(docker service ls --filter "name=${stack_name}_" --format "{{.Name}}" 2>/dev/null | wc -l)
            if [ "$remaining" -eq 0 ]; then
                if ! docker network inspect "${stack_name}_backend" >/dev/null 2>&1; then
                    log_success "Stack resources cleaned up"
                    break
                fi
            fi
            sleep 2
            wait_count=$((wait_count + 2))
        done

        if [ $wait_count -ge $max_wait ]; then
            log_warning "Timeout waiting for cleanup, proceeding anyway..."
        fi

        sleep 5
    fi
    return 0
}

check_for_stuck_services() {
    local stack_name="$1"
    local stuck_tasks
    stuck_tasks=$(docker service ls --filter "name=${stack_name}_" --format "{{.Name}} {{.Replicas}}" 2>/dev/null \
        | grep -E "0/[0-9]+" | head -5 || echo "")

    if [ -n "$stuck_tasks" ]; then
        log_warning "Found services with 0 running replicas:"
        echo "$stuck_tasks"
        return 1
    fi
    return 0
}

export -f log_info log_success log_warning log_error
export -f detect_environment get_app_directory get_compose_file get_env_file
export -f get_stack_name get_app_port wait_for_service get_aws_region ecr_login
export -f is_swarm_leader check_swarm_leader_or_skip safe_source_env
export -f recover_stack check_for_stuck_services
