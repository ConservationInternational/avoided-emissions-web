#!/bin/bash
# setup-aws.sh -- Provision AWS resources for the avoided-emissions-web stack.
#
# Prerequisites:
#   - AWS CLI v2 configured with credentials that have admin-level access
#   - jq installed
#
# Usage:
#   ./setup-aws.sh                      # use default credentials
#   ./setup-aws.sh --profile my-profile  # use a named AWS CLI profile
#   ./setup-aws.sh --region us-east-1 --account 123456789012 --profile prod

set -euo pipefail

# ============================================================================
# Defaults
# ============================================================================

AWS_REGION="${AWS_REGION:-us-east-1}"
AWS_ACCOUNT_ID=""
AWS_PROFILE_ARG="${AWS_PROFILE:-}"
GITHUB_ORG="conservationinternational"
GITHUB_REPO="avoided-emissions-web"

APP_NAME="avoided-emissions-web"
ECR_WEBAPP="avoided-emissions-webapp"
ECR_RANALYSIS="avoided-emissions-ranalysis"
S3_DEPLOY_PREFIX="avoided-emissions-deployments"
S3_DATA_BUCKET="avoided-emissions-data"
BATCH_COMPUTE_ENV="avoided-emissions-ce"
BATCH_JOB_QUEUE="avoided-emissions-queue"
BATCH_JOB_DEF="avoided-emissions-analysis"

# ============================================================================
# Helper functions
# ============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC} $1"; }
success() { echo -e "${GREEN}[OK]${NC}   $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $1"; }
error()   { echo -e "${RED}[ERR]${NC}  $1"; }

ensure_command() {
    if ! command -v "$1" &>/dev/null; then
        error "$1 is required but not installed."
        exit 1
    fi
}

# ============================================================================
# Parse arguments
# ============================================================================

while [[ $# -gt 0 ]]; do
    case "$1" in
        --region)  AWS_REGION="$2";      shift 2 ;;
        --account) AWS_ACCOUNT_ID="$2";  shift 2 ;;
        --profile) AWS_PROFILE_ARG="$2"; shift 2 ;;
        --org)     GITHUB_ORG="$2";      shift 2 ;;
        --repo)    GITHUB_REPO="$2";     shift 2 ;;
        --help|-h)
            echo "Usage: $0 [--profile PROFILE] [--region REGION] [--account ACCOUNT_ID] [--org GITHUB_ORG] [--repo GITHUB_REPO]"
            exit 0 ;;
        *) error "Unknown argument: $1"; exit 1 ;;
    esac
done

# ============================================================================
# Pre-flight checks
# ============================================================================

ensure_command aws
ensure_command jq

# Export AWS_PROFILE so all aws CLI calls pick it up automatically.
if [ -n "$AWS_PROFILE_ARG" ]; then
    export AWS_PROFILE="$AWS_PROFILE_ARG"
    info "Using AWS CLI profile: $AWS_PROFILE"
fi

if [ -z "$AWS_ACCOUNT_ID" ]; then
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
fi

info "AWS Region:     $AWS_REGION"
info "AWS Account:    $AWS_ACCOUNT_ID"
if [ -n "${AWS_PROFILE:-}" ]; then
    info "AWS Profile:    $AWS_PROFILE"
fi
info "GitHub:         $GITHUB_ORG/$GITHUB_REPO"
echo ""

ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
S3_DEPLOY_BUCKET="${S3_DEPLOY_PREFIX}-${AWS_ACCOUNT_ID}"

# ============================================================================
# 1. ECR Repositories
# ============================================================================

create_ecr_repo() {
    local repo="$1"
    if aws ecr describe-repositories --repository-names "$repo" --region "$AWS_REGION" &>/dev/null; then
        success "ECR repository already exists: $repo"
    else
        info "Creating ECR repository: $repo"
        aws ecr create-repository \
            --repository-name "$repo" \
            --region "$AWS_REGION" \
            --image-scanning-configuration scanOnPush=true \
            --image-tag-mutability MUTABLE \
            --output text --query 'repository.repositoryArn'
        success "Created ECR repository: $repo"
    fi

    # Add lifecycle policy to expire untagged images after 14 days
    aws ecr put-lifecycle-policy \
        --repository-name "$repo" \
        --region "$AWS_REGION" \
        --lifecycle-policy-text '{
            "rules": [{
                "rulePriority": 1,
                "description": "Expire untagged images after 14 days",
                "selection": {
                    "tagStatus": "untagged",
                    "countType": "sinceImagePushed",
                    "countUnit": "days",
                    "countNumber": 14
                },
                "action": { "type": "expire" }
            }]
        }' >/dev/null 2>&1
}

info "--- ECR Repositories ---"
create_ecr_repo "$ECR_WEBAPP"
create_ecr_repo "$ECR_RANALYSIS"
echo ""

# ============================================================================
# 2. S3 Buckets
# ============================================================================

create_s3_bucket() {
    local bucket="$1"
    local desc="$2"
    if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        success "S3 bucket already exists: $bucket ($desc)"
    else
        info "Creating S3 bucket: $bucket ($desc)"
        if [ "$AWS_REGION" = "us-east-1" ]; then
            aws s3api create-bucket --bucket "$bucket" --region "$AWS_REGION"
        else
            aws s3api create-bucket --bucket "$bucket" --region "$AWS_REGION" \
                --create-bucket-configuration LocationConstraint="$AWS_REGION"
        fi

        # Block public access
        aws s3api put-public-access-block --bucket "$bucket" \
            --public-access-block-configuration \
            "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

        # Enable versioning on deployment bucket
        if [[ "$bucket" == *"deployments"* ]]; then
            aws s3api put-bucket-versioning --bucket "$bucket" \
                --versioning-configuration Status=Enabled
        fi

        success "Created S3 bucket: $bucket"
    fi
}

info "--- S3 Buckets ---"
create_s3_bucket "$S3_DEPLOY_BUCKET" "CodeDeploy deployment packages"
create_s3_bucket "$S3_DATA_BUCKET" "analysis data and results"
echo ""

# ============================================================================
# 3. IAM Role for GitHub Actions (OIDC)
# ============================================================================

OIDC_ROLE_NAME="${APP_NAME}-github-actions"
OIDC_PROVIDER_URL="https://token.actions.githubusercontent.com"

info "--- GitHub Actions OIDC ---"

# Create the OIDC provider if it doesn't exist
EXISTING_PROVIDER=$(aws iam list-open-id-connect-providers --query \
    "OpenIDConnectProviderList[?ends_with(Arn, 'token.actions.githubusercontent.com')].Arn" \
    --output text)

if [ -z "$EXISTING_PROVIDER" ] || [ "$EXISTING_PROVIDER" = "None" ]; then
    info "Creating GitHub Actions OIDC provider"
    THUMBPRINT="6938fd4d98bab03faadb97b34396831e3780aea1"
    aws iam create-open-id-connect-provider \
        --url "$OIDC_PROVIDER_URL" \
        --thumbprint-list "$THUMBPRINT" \
        --client-id-list "sts.amazonaws.com" >/dev/null
    success "OIDC provider created"
    OIDC_PROVIDER_ARN="arn:aws:iam::${AWS_ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com"
else
    success "OIDC provider already exists: $EXISTING_PROVIDER"
    OIDC_PROVIDER_ARN="$EXISTING_PROVIDER"
fi

# Create IAM role for GitHub Actions
if aws iam get-role --role-name "$OIDC_ROLE_NAME" &>/dev/null; then
    success "IAM role already exists: $OIDC_ROLE_NAME"
else
    info "Creating IAM role: $OIDC_ROLE_NAME"
    TRUST_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {
            "Federated": "arn:aws:iam::${AWS_ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com"
        },
        "Action": "sts:AssumeRoleWithWebIdentity",
        "Condition": {
            "StringEquals": {
                "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
            },
            "StringLike": {
                "token.actions.githubusercontent.com:sub": "repo:${GITHUB_ORG}/${GITHUB_REPO}:*"
            }
        }
    }]
}
EOF
    )
    aws iam create-role \
        --role-name "$OIDC_ROLE_NAME" \
        --assume-role-policy-document "$TRUST_POLICY" \
        --description "GitHub Actions role for $APP_NAME" >/dev/null
    success "Created IAM role: $OIDC_ROLE_NAME"
fi

# Attach policies to the GitHub Actions role
GH_ACTIONS_POLICY_NAME="${APP_NAME}-github-actions-policy"
GH_ACTIONS_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ECRAuth",
            "Effect": "Allow",
            "Action": "ecr:GetAuthorizationToken",
            "Resource": "*"
        },
        {
            "Sid": "ECRPush",
            "Effect": "Allow",
            "Action": [
                "ecr:BatchCheckLayerAvailability",
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchGetImage",
                "ecr:PutImage",
                "ecr:InitiateLayerUpload",
                "ecr:UploadLayerPart",
                "ecr:CompleteLayerUpload",
                "ecr:DescribeRepositories",
                "ecr:CreateRepository"
            ],
            "Resource": [
                "arn:aws:ecr:${AWS_REGION}:${AWS_ACCOUNT_ID}:repository/${ECR_WEBAPP}",
                "arn:aws:ecr:${AWS_REGION}:${AWS_ACCOUNT_ID}:repository/${ECR_RANALYSIS}"
            ]
        },
        {
            "Sid": "S3Deploy",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_DEPLOY_BUCKET}",
                "arn:aws:s3:::${S3_DEPLOY_BUCKET}/*"
            ]
        },
        {
            "Sid": "CodeDeploy",
            "Effect": "Allow",
            "Action": [
                "codedeploy:CreateDeployment",
                "codedeploy:GetDeployment",
                "codedeploy:GetDeploymentConfig",
                "codedeploy:GetApplicationRevision",
                "codedeploy:RegisterApplicationRevision",
                "codedeploy:ListDeployments",
                "codedeploy:StopDeployment",
                "codedeploy:ListDeploymentTargets",
                "codedeploy:GetDeploymentTarget"
            ],
            "Resource": [
                "arn:aws:codedeploy:${AWS_REGION}:${AWS_ACCOUNT_ID}:application:${APP_NAME}",
                "arn:aws:codedeploy:${AWS_REGION}:${AWS_ACCOUNT_ID}:deploymentgroup:${APP_NAME}/*",
                "arn:aws:codedeploy:${AWS_REGION}:${AWS_ACCOUNT_ID}:deploymentconfig:*"
            ]
        },
        {
            "Sid": "STS",
            "Effect": "Allow",
            "Action": "sts:GetCallerIdentity",
            "Resource": "*"
        }
    ]
}
EOF
)

info "Updating inline policy on $OIDC_ROLE_NAME"
aws iam put-role-policy \
    --role-name "$OIDC_ROLE_NAME" \
    --policy-name "$GH_ACTIONS_POLICY_NAME" \
    --policy-document "$GH_ACTIONS_POLICY" >/dev/null
success "GitHub Actions IAM role configured"

OIDC_ROLE_ARN=$(aws iam get-role --role-name "$OIDC_ROLE_NAME" --query 'Role.Arn' --output text)
echo ""

# ============================================================================
# 4. IAM Role for EC2 Instance (CodeDeploy Agent + App)
# ============================================================================

EC2_ROLE_NAME="${APP_NAME}-ec2"
EC2_INSTANCE_PROFILE="${APP_NAME}-ec2-profile"

info "--- EC2 Instance Role ---"

if aws iam get-role --role-name "$EC2_ROLE_NAME" &>/dev/null; then
    success "EC2 IAM role already exists: $EC2_ROLE_NAME"
else
    info "Creating EC2 IAM role: $EC2_ROLE_NAME"
    aws iam create-role \
        --role-name "$EC2_ROLE_NAME" \
        --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "Service": "ec2.amazonaws.com" },
                "Action": "sts:AssumeRole"
            }]
        }' \
        --description "EC2 instance role for $APP_NAME" >/dev/null
    success "Created EC2 IAM role: $EC2_ROLE_NAME"
fi

# Attach managed policies
for POLICY_ARN in \
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly" \
    "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore" \
    "arn:aws:iam::aws:policy/service-role/AmazonEC2RoleforAWSCodeDeploy"; do
    aws iam attach-role-policy --role-name "$EC2_ROLE_NAME" --policy-arn "$POLICY_ARN" 2>/dev/null || true
done

# Add inline policy for S3 data access and Batch submission
EC2_APP_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3Data",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_DATA_BUCKET}",
                "arn:aws:s3:::${S3_DATA_BUCKET}/*",
                "arn:aws:s3:::${S3_DEPLOY_BUCKET}",
                "arn:aws:s3:::${S3_DEPLOY_BUCKET}/*"
            ]
        },
        {
            "Sid": "BatchSubmit",
            "Effect": "Allow",
            "Action": [
                "batch:SubmitJob",
                "batch:DescribeJobs",
                "batch:ListJobs",
                "batch:TerminateJob",
                "batch:RegisterJobDefinition"
            ],
            "Resource": "*"
        }
    ]
}
EOF
)
aws iam put-role-policy \
    --role-name "$EC2_ROLE_NAME" \
    --policy-name "${APP_NAME}-ec2-app-policy" \
    --policy-document "$EC2_APP_POLICY" >/dev/null

# Create instance profile
if aws iam get-instance-profile --instance-profile-name "$EC2_INSTANCE_PROFILE" &>/dev/null; then
    success "Instance profile already exists: $EC2_INSTANCE_PROFILE"
else
    info "Creating instance profile: $EC2_INSTANCE_PROFILE"
    aws iam create-instance-profile --instance-profile-name "$EC2_INSTANCE_PROFILE" >/dev/null
    aws iam add-role-to-instance-profile \
        --instance-profile-name "$EC2_INSTANCE_PROFILE" \
        --role-name "$EC2_ROLE_NAME" >/dev/null
    success "Created instance profile: $EC2_INSTANCE_PROFILE"
fi

echo ""

# ============================================================================
# 5. IAM Role for AWS Batch Execution
# ============================================================================

BATCH_ROLE_NAME="${APP_NAME}-batch-execution"

info "--- AWS Batch Execution Role ---"

if aws iam get-role --role-name "$BATCH_ROLE_NAME" &>/dev/null; then
    success "Batch execution role already exists: $BATCH_ROLE_NAME"
else
    info "Creating Batch execution role: $BATCH_ROLE_NAME"
    aws iam create-role \
        --role-name "$BATCH_ROLE_NAME" \
        --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "Service": "ecs-tasks.amazonaws.com" },
                "Action": "sts:AssumeRole"
            }]
        }' \
        --description "Batch task execution role for $APP_NAME" >/dev/null
    success "Created Batch execution role"
fi

aws iam attach-role-policy --role-name "$BATCH_ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy" 2>/dev/null || true

BATCH_TASK_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [{
        "Sid": "S3DataAccess",
        "Effect": "Allow",
        "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:ListBucket"
        ],
        "Resource": [
            "arn:aws:s3:::${S3_DATA_BUCKET}",
            "arn:aws:s3:::${S3_DATA_BUCKET}/*"
        ]
    }]
}
EOF
)
aws iam put-role-policy \
    --role-name "$BATCH_ROLE_NAME" \
    --policy-name "${APP_NAME}-batch-s3-policy" \
    --policy-document "$BATCH_TASK_POLICY" >/dev/null
success "Batch execution role configured"
echo ""

# ============================================================================
# 6. CodeDeploy Application and Deployment Groups
# ============================================================================

info "--- CodeDeploy ---"

if aws deploy get-application --application-name "$APP_NAME" --region "$AWS_REGION" &>/dev/null; then
    success "CodeDeploy application already exists: $APP_NAME"
else
    info "Creating CodeDeploy application: $APP_NAME"
    aws deploy create-application \
        --application-name "$APP_NAME" \
        --compute-platform Server \
        --region "$AWS_REGION" >/dev/null
    success "Created CodeDeploy application"
fi

# Create deployment groups (uses EC2 tags to target instances)
create_deployment_group() {
    local group_name="$1"
    local env_tag="$2"

    if aws deploy get-deployment-group \
        --application-name "$APP_NAME" \
        --deployment-group-name "$group_name" \
        --region "$AWS_REGION" &>/dev/null; then
        success "Deployment group already exists: $group_name"
    else
        info "Creating deployment group: $group_name"

        # Create CodeDeploy service role if it doesn't exist
        local cd_role_name="${APP_NAME}-codedeploy-service"
        if ! aws iam get-role --role-name "$cd_role_name" &>/dev/null; then
            aws iam create-role \
                --role-name "$cd_role_name" \
                --assume-role-policy-document '{
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Effect": "Allow",
                        "Principal": { "Service": "codedeploy.amazonaws.com" },
                        "Action": "sts:AssumeRole"
                    }]
                }' >/dev/null
            aws iam attach-role-policy --role-name "$cd_role_name" \
                --policy-arn "arn:aws:iam::aws:policy/service-role/AWSCodeDeployRole" 2>/dev/null || true
        fi
        local cd_role_arn
        cd_role_arn=$(aws iam get-role --role-name "$cd_role_name" --query 'Role.Arn' --output text)

        aws deploy create-deployment-group \
            --application-name "$APP_NAME" \
            --deployment-group-name "$group_name" \
            --service-role-arn "$cd_role_arn" \
            --deployment-config-name CodeDeployDefault.OneAtATime \
            --ec2-tag-filters "Key=DeploymentGroupAvoidedEmissions,Value=${env_tag},Type=KEY_AND_VALUE" \
            --auto-rollback-configuration "enabled=true,events=DEPLOYMENT_FAILURE" \
            --region "$AWS_REGION" >/dev/null
        success "Created deployment group: $group_name"
    fi
}

create_deployment_group "${APP_NAME}-staging" "avoided-emissions-staging"
create_deployment_group "${APP_NAME}-production" "avoided-emissions-production"
echo ""

# ============================================================================
# 7. AWS Batch Resources
# ============================================================================

BATCH_SERVICE_ROLE_NAME="${APP_NAME}-batch-service"

info "--- AWS Batch ---"

# Batch service role
if aws iam get-role --role-name "$BATCH_SERVICE_ROLE_NAME" &>/dev/null; then
    success "Batch service role already exists"
else
    info "Creating Batch service role: $BATCH_SERVICE_ROLE_NAME"
    aws iam create-role \
        --role-name "$BATCH_SERVICE_ROLE_NAME" \
        --assume-role-policy-document '{
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": { "Service": "batch.amazonaws.com" },
                "Action": "sts:AssumeRole"
            }]
        }' >/dev/null
    aws iam attach-role-policy --role-name "$BATCH_SERVICE_ROLE_NAME" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole" 2>/dev/null || true
    success "Created Batch service role"
fi

# Check if compute environment already exists
if aws batch describe-compute-environments \
    --compute-environments "$BATCH_COMPUTE_ENV" \
    --region "$AWS_REGION" \
    --query 'computeEnvironments[0].computeEnvironmentName' \
    --output text 2>/dev/null | grep -q "$BATCH_COMPUTE_ENV"; then
    success "Batch compute environment already exists: $BATCH_COMPUTE_ENV"
else
    warn "Batch compute environment needs manual setup."
    warn "You must configure subnets and security groups for your VPC."
    warn "See deploy/README.md for instructions."
fi

# Check if job queue exists
if aws batch describe-job-queues \
    --job-queues "$BATCH_JOB_QUEUE" \
    --region "$AWS_REGION" \
    --query 'jobQueues[0].jobQueueName' \
    --output text 2>/dev/null | grep -q "$BATCH_JOB_QUEUE"; then
    success "Batch job queue already exists: $BATCH_JOB_QUEUE"
else
    warn "Batch job queue needs the compute environment first."
    warn "See deploy/README.md for instructions."
fi

echo ""

# ============================================================================
# Summary
# ============================================================================

echo "============================================================"
echo -e "${GREEN}AWS Setup Complete${NC}"
echo "============================================================"
echo ""
echo "Resources created/verified:"
echo "  ECR:        ${ECR_REGISTRY}/${ECR_WEBAPP}"
echo "              ${ECR_REGISTRY}/${ECR_RANALYSIS}"
echo "  S3:         ${S3_DEPLOY_BUCKET} (deployments)"
echo "              ${S3_DATA_BUCKET} (analysis data)"
echo "  CodeDeploy: ${APP_NAME} (staging + production groups)"
echo "  IAM Roles:  ${OIDC_ROLE_NAME} (GitHub Actions OIDC)"
echo "              ${EC2_ROLE_NAME} (EC2 instance)"
echo "              ${BATCH_ROLE_NAME} (Batch tasks)"
echo ""
echo "GitHub repository secrets/variables to configure:"
echo "  vars.AWS_OIDC_ROLE_ARN = ${OIDC_ROLE_ARN}"
echo "  vars.S3_BUCKET         = ${S3_DATA_BUCKET}"
echo "  vars.GCS_BUCKET        = (your GCS bucket for GEE exports)"
echo "  vars.GCS_PREFIX        = avoided-emissions/covariates  (default)"
echo "  vars.GOOGLE_PROJECT_ID = (Google Cloud project for Earth Engine)"
echo "  vars.GEE_ENDPOINT      = (optional: high-volume EE endpoint URL)"
echo "  secrets.SECRET_KEY     = (generate with: python -c \"import secrets; print(secrets.token_hex(32))\")"
echo "  secrets.POSTGRES_USER  = (database username)"
echo "  secrets.POSTGRES_PASSWORD = (database password)"
echo "  secrets.POSTGRES_DB    = (database name)"
echo "  secrets.EE_SERVICE_ACCOUNT_JSON = (GEE service account key JSON)"
echo ""
echo "Manual steps remaining:"
echo "  1. Tag your EC2 instance (see deploy/README.md)"
echo "  2. Install CodeDeploy agent on EC2"
echo "  3. Create Batch compute environment with VPC subnets"
echo "  4. Configure ELB target group (see deploy/README.md)"
echo ""
