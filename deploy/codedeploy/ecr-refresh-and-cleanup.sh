#!/bin/bash
# Refresh ECR credentials stored in the Docker Swarm raft log.
#
# Docker Swarm distributes registry credentials to worker nodes only at
# deploy time (via --with-registry-auth).  ECR tokens expire after 12 hours,
# so if a node needs to re-pull an image after that (e.g. after a
# "docker system prune -a" or a node restart), the pull fails with
# "No such image".
#
# This script keeps the credentials fresh by:
#   1. Refreshing the ECR login on this node.
#   2. On the Swarm leader only: running "docker service update
#      --with-registry-auth" on every ECR-backed service, which pushes
#      the fresh token into the raft store for all nodes.
#
# Run via systemd timer every 4 hours (well within the 12-hour lifetime)
# on every Swarm manager node.  "docker system prune -a" can safely run
# on any schedule — Swarm will re-pull images using the fresh credentials.

set -euo pipefail

LOG_TAG="ecr-refresh"

log() { echo "[$LOG_TAG] $(date '+%Y-%m-%d %H:%M:%S') $1"; }

# --- Determine AWS region ---------------------------------------------------

get_aws_region() {
    local TOKEN REGION
    TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60" 2>/dev/null || echo "")
    if [ -n "$TOKEN" ]; then
        REGION=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
            http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null || echo "")
    else
        REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null || echo "")
    fi
    echo "${REGION:-us-east-1}"
}

# --- Only run the service-update logic on the Swarm leader -------------------

is_swarm_leader() {
    local status
    status=$(docker node ls --format '{{.Self}} {{.ManagerStatus}}' 2>/dev/null \
        | awk '$1=="true" {print $2}')
    [ "$status" = "Leader" ]
}

# --- ECR login ---------------------------------------------------------------

REGION=$(get_aws_region)

# Detect ECR registry from running services or fall back to env / STS.
ECR_REGISTRY=$(docker service ls --format '{{.Image}}' 2>/dev/null \
    | grep -oP '^\d+\.dkr\.ecr\.[^/]+' | head -1 || echo "")
if [ -z "$ECR_REGISTRY" ]; then
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "")
    if [ -n "$ACCOUNT_ID" ]; then
        ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
    fi
fi

if [ -z "$ECR_REGISTRY" ]; then
    log "ERROR: Could not determine ECR registry — skipping."
    exit 1
fi

log "Refreshing ECR login for $ECR_REGISTRY ..."
aws ecr get-login-password --region "$REGION" \
    | docker login --username AWS --password-stdin "$ECR_REGISTRY" >/dev/null 2>&1
log "ECR login refreshed."

# --- Push fresh credentials into Swarm (leader only) ------------------------

if is_swarm_leader; then
    log "This node is the Swarm leader — pushing fresh credentials to raft store."

    for svc in $(docker service ls --format '{{.Name}}' 2>/dev/null); do
        IMAGE=$(docker service inspect "$svc" --format '{{.Spec.TaskTemplate.ContainerSpec.Image}}' 2>/dev/null || echo "")
        if echo "$IMAGE" | grep -q "dkr.ecr."; then
            log "  Refreshing credentials for $svc ..."
            # --with-registry-auth distributes creds; --image keeps the same
            # image so no container restart occurs.
            docker service update --with-registry-auth --image "$IMAGE" "$svc" >/dev/null 2>&1 || \
                log "  WARNING: failed to update $svc"
        fi
    done
    log "Service credential refresh complete."
else
    log "Not the Swarm leader — skipping service updates."
fi
