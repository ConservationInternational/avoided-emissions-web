#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# tag-existing-resources.sh
#
# Retroactively apply the cost-allocation tag  Project=avoided-emissions  to
# all existing AWS resources used by the avoided-emissions app:
#
#   - ECR repositories
#   - S3 objects in the app bucket (under the configured prefix)
#   - AWS Batch job definitions
#   - AWS Batch job queues / compute environments (if named with "avoided")
#
# Prerequisites:
#   - AWS CLI v2 installed and configured with sufficient IAM permissions
#   - jq installed
#
# Usage:
#   export AWS_REGION=us-east-1          # optional, defaults to us-east-1
#   export S3_BUCKET=my-bucket           # REQUIRED
#   export S3_PREFIX=avoided-emissions   # optional, defaults to avoided-emissions
#   bash deploy/tag-existing-resources.sh
#
# Dry-run mode (prints what would be done without making changes):
#   DRY_RUN=1 bash deploy/tag-existing-resources.sh
# ---------------------------------------------------------------------------
set -euo pipefail

TAG_KEY="Project"
TAG_VALUE="avoided-emissions"
AWS_REGION="${AWS_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-}"
S3_PREFIX="${S3_PREFIX:-avoided-emissions}"
DRY_RUN="${DRY_RUN:-0}"

# ECR repository names used by the app
ECR_REPOS=(
    "avoided-emissions-webapp"
    "avoided-emissions-ranalysis"
)

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }
step() { echo -e "\n${CYAN}=== $* ===${NC}"; }
dry()  { echo -e "${YELLOW}[DRY-RUN]${NC} $*"; }

# ---------------------------------------------------------------------------
# 1. ECR Repositories
# ---------------------------------------------------------------------------
step "Tagging ECR repositories"

for REPO in "${ECR_REPOS[@]}"; do
    REPO_ARN=$(aws ecr describe-repositories \
        --repository-names "$REPO" \
        --region "$AWS_REGION" \
        --query 'repositories[0].repositoryArn' \
        --output text 2>/dev/null || echo "NOT_FOUND")

    if [ "$REPO_ARN" = "NOT_FOUND" ] || [ "$REPO_ARN" = "None" ]; then
        warn "ECR repository '$REPO' not found — skipping"
        continue
    fi

    if [ "$DRY_RUN" = "1" ]; then
        dry "Would tag ECR repo $REPO ($REPO_ARN) with $TAG_KEY=$TAG_VALUE"
    else
        aws ecr tag-resource \
            --resource-arn "$REPO_ARN" \
            --tags "Key=$TAG_KEY,Value=$TAG_VALUE" \
            --region "$AWS_REGION"
        log "Tagged ECR repo: $REPO"
    fi
done

# ---------------------------------------------------------------------------
# 2. S3 Objects
# ---------------------------------------------------------------------------
step "Tagging S3 objects in s3://${S3_BUCKET}/${S3_PREFIX}"

if [ -z "$S3_BUCKET" ]; then
    warn "S3_BUCKET is not set — skipping S3 object tagging"
else
    OBJECT_COUNT=0
    TAGGED_COUNT=0
    SKIPPED_COUNT=0

    # List all objects under the prefix and tag each one
    while IFS= read -r KEY; do
        [ -z "$KEY" ] && continue
        OBJECT_COUNT=$((OBJECT_COUNT + 1))

        # Check if the tag already exists
        EXISTING_TAGS=$(aws s3api get-object-tagging \
            --bucket "$S3_BUCKET" \
            --key "$KEY" \
            --region "$AWS_REGION" \
            --query "TagSet[?Key=='$TAG_KEY'].Value" \
            --output text 2>/dev/null || echo "")

        if [ "$EXISTING_TAGS" = "$TAG_VALUE" ]; then
            SKIPPED_COUNT=$((SKIPPED_COUNT + 1))
            continue
        fi

        # Merge with existing tags so we don't overwrite them
        EXISTING_TAG_SET=$(aws s3api get-object-tagging \
            --bucket "$S3_BUCKET" \
            --key "$KEY" \
            --region "$AWS_REGION" \
            --output json 2>/dev/null || echo '{"TagSet":[]}')

        # Add/replace our tag in the existing tag set
        NEW_TAG_SET=$(echo "$EXISTING_TAG_SET" | jq \
            --arg key "$TAG_KEY" --arg val "$TAG_VALUE" \
            '.TagSet = [.TagSet[] | select(.Key != $key)] + [{"Key": $key, "Value": $val}]')

        if [ "$DRY_RUN" = "1" ]; then
            dry "Would tag s3://$S3_BUCKET/$KEY"
        else
            echo "$NEW_TAG_SET" | aws s3api put-object-tagging \
                --bucket "$S3_BUCKET" \
                --key "$KEY" \
                --tagging file:///dev/stdin \
                --region "$AWS_REGION"
            TAGGED_COUNT=$((TAGGED_COUNT + 1))
        fi

        # Progress indicator every 100 objects
        if [ $((OBJECT_COUNT % 100)) -eq 0 ]; then
            log "  ... processed $OBJECT_COUNT objects so far"
        fi
    done < <(aws s3api list-objects-v2 \
        --bucket "$S3_BUCKET" \
        --prefix "$S3_PREFIX/" \
        --region "$AWS_REGION" \
        --query 'Contents[].Key' \
        --output text 2>/dev/null | tr '\t' '\n')

    log "S3 objects: $OBJECT_COUNT found, $TAGGED_COUNT tagged, $SKIPPED_COUNT already tagged"
fi

# Also tag the S3 bucket itself
if [ -n "$S3_BUCKET" ]; then
    step "Tagging S3 bucket: $S3_BUCKET"

    EXISTING_BUCKET_TAGS=$(aws s3api get-bucket-tagging \
        --bucket "$S3_BUCKET" \
        --region "$AWS_REGION" \
        --output json 2>/dev/null || echo '{"TagSet":[]}')

    NEW_BUCKET_TAGS=$(echo "$EXISTING_BUCKET_TAGS" | jq \
        --arg key "$TAG_KEY" --arg val "$TAG_VALUE" \
        '.TagSet = [.TagSet[] | select(.Key != $key)] + [{"Key": $key, "Value": $val}]')

    if [ "$DRY_RUN" = "1" ]; then
        dry "Would tag S3 bucket $S3_BUCKET with $TAG_KEY=$TAG_VALUE"
    else
        echo "$NEW_BUCKET_TAGS" | aws s3api put-bucket-tagging \
            --bucket "$S3_BUCKET" \
            --tagging file:///dev/stdin \
            --region "$AWS_REGION"
        log "Tagged S3 bucket: $S3_BUCKET"
    fi
fi

# ---------------------------------------------------------------------------
# 3. AWS Batch Job Definitions
# ---------------------------------------------------------------------------
step "Tagging AWS Batch job definitions"

# Find job definitions related to avoided-emissions (by naming convention)
BATCH_JOB_DEF_NAMES=$(aws batch describe-job-definitions \
    --status ACTIVE \
    --region "$AWS_REGION" \
    --query 'jobDefinitions[].jobDefinitionArn' \
    --output text 2>/dev/null | tr '\t' '\n' || echo "")

BATCH_TAGGED=0
for JOB_DEF_ARN in $BATCH_JOB_DEF_NAMES; do
    [ -z "$JOB_DEF_ARN" ] && continue
    # Only tag definitions whose name or image contains "avoided-emissions"
    JOB_DEF_NAME=$(echo "$JOB_DEF_ARN" | grep -oP 'job-definition/\K[^:]+' || echo "")
    JOB_DEF_IMAGE=$(aws batch describe-job-definitions \
        --job-definitions "$JOB_DEF_ARN" \
        --region "$AWS_REGION" \
        --query 'jobDefinitions[0].containerProperties.image' \
        --output text 2>/dev/null || echo "")

    if echo "$JOB_DEF_NAME" | grep -qi "avoided-emissions" || \
       echo "$JOB_DEF_IMAGE" | grep -qi "avoided-emissions"; then

        if [ "$DRY_RUN" = "1" ]; then
            dry "Would tag Batch job definition: $JOB_DEF_ARN"
        else
            aws batch tag-resource \
                --resource-arn "$JOB_DEF_ARN" \
                --tags "$TAG_KEY=$TAG_VALUE" \
                --region "$AWS_REGION" 2>/dev/null || \
                warn "Failed to tag job definition: $JOB_DEF_ARN"
            BATCH_TAGGED=$((BATCH_TAGGED + 1))
        fi
    fi
done
log "Batch job definitions tagged: $BATCH_TAGGED"

# ---------------------------------------------------------------------------
# 4. AWS Batch Job Queues
# ---------------------------------------------------------------------------
step "Tagging AWS Batch job queues"

BATCH_QUEUE_TAGGED=0
QUEUE_ARNS=$(aws batch describe-job-queues \
    --region "$AWS_REGION" \
    --query 'jobQueues[].jobQueueArn' \
    --output text 2>/dev/null | tr '\t' '\n' || echo "")

for QUEUE_ARN in $QUEUE_ARNS; do
    [ -z "$QUEUE_ARN" ] && continue
    QUEUE_NAME=$(echo "$QUEUE_ARN" | grep -oP 'job-queue/\K.*' || echo "")

    # Tag queues used by the app (spot/ondemand fleets)
    if echo "$QUEUE_NAME" | grep -qiE "avoided|spot_fleet|ondemand_fleet"; then
        if [ "$DRY_RUN" = "1" ]; then
            dry "Would tag Batch queue: $QUEUE_NAME ($QUEUE_ARN)"
        else
            aws batch tag-resource \
                --resource-arn "$QUEUE_ARN" \
                --tags "$TAG_KEY=$TAG_VALUE" \
                --region "$AWS_REGION" 2>/dev/null || \
                warn "Failed to tag queue: $QUEUE_ARN"
            BATCH_QUEUE_TAGGED=$((BATCH_QUEUE_TAGGED + 1))
        fi
    fi
done
log "Batch job queues tagged: $BATCH_QUEUE_TAGGED"

# ---------------------------------------------------------------------------
# 5. AWS Batch Compute Environments
# ---------------------------------------------------------------------------
step "Tagging AWS Batch compute environments"

CE_TAGGED=0
CE_ARNS=$(aws batch describe-compute-environments \
    --region "$AWS_REGION" \
    --query 'computeEnvironments[].computeEnvironmentArn' \
    --output text 2>/dev/null | tr '\t' '\n' || echo "")

for CE_ARN in $CE_ARNS; do
    [ -z "$CE_ARN" ] && continue
    CE_NAME=$(echo "$CE_ARN" | grep -oP 'compute-environment/\K.*' || echo "")

    if echo "$CE_NAME" | grep -qiE "avoided|spot_fleet|ondemand_fleet"; then
        if [ "$DRY_RUN" = "1" ]; then
            dry "Would tag compute environment: $CE_NAME ($CE_ARN)"
        else
            aws batch tag-resource \
                --resource-arn "$CE_ARN" \
                --tags "$TAG_KEY=$TAG_VALUE" \
                --region "$AWS_REGION" 2>/dev/null || \
                warn "Failed to tag compute env: $CE_ARN"
            CE_TAGGED=$((CE_TAGGED + 1))
        fi
    fi
done
log "Batch compute environments tagged: $CE_TAGGED"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
step "Tagging complete"
if [ "$DRY_RUN" = "1" ]; then
    warn "This was a DRY RUN — no changes were made. Re-run without DRY_RUN=1 to apply."
else
    log "All existing resources have been tagged with $TAG_KEY=$TAG_VALUE"
fi
echo ""
log "Remember to activate the '$TAG_KEY' tag in AWS Cost Explorer:"
log "  Billing → Cost allocation tags → Activate '$TAG_KEY'"
