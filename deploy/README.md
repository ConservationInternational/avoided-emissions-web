# Deployment Guide

This document covers the AWS infrastructure setup for the **avoided-emissions-web**
application, including what the automated setup script provisions and what must be
configured manually.

## Architecture Overview

```
┌─────────────┐       ┌──────┐       ┌─────────────┐       ┌──────────┐
│  CloudFront  │──────▶│  ELB │──────▶│     EC2     │──────▶│ PostGIS  │
│  (optional) │       └──────┘       │ Docker Swarm │       │ (local)  │
└─────────────┘                      └──────┬───────┘       └──────────┘
                                            │
                                    ┌───────▼────────┐
                                    │   AWS Batch    │
                                    │ R Analysis Jobs │
                                    └───────┬────────┘
                                            │
                                    ┌───────▼────────┐
                                    │       S3       │
                                    │  Data + Deploys │
                                    └────────────────┘
```

- **ELB** ─ Application Load Balancer (manually provisioned)
- **EC2** ─ Docker Swarm host running the webapp + PostGIS via `docker-compose`
- **AWS Batch** ─ Runs R analysis containers (extract → match → summarize)
- **ECR** ─ Docker image registry for webapp and R analysis images
- **S3** ─ Deployment packages and analysis data/results
- **CodeDeploy** ─ Deploys new versions from S3 to EC2 via GitHub Actions

---

## Quick Start

### Prerequisites

| Requirement | Notes |
|---|---|
| **AWS CLI v2** | Configured with admin credentials or an appropriately permissioned role |
| **jq** | Used for JSON parsing in the setup script |
| **EC2 instance** | Already launched and accessible (Amazon Linux 2023 or Ubuntu 22.04 recommended) |
| **ELB** | Already provisioned (see [ELB Setup](#elastic-load-balancer-elb-setup) below) |

### Run the setup script

```bash
# From the repository root
chmod +x deploy/setup-aws.sh
./deploy/setup-aws.sh

# Use a named AWS CLI profile
./deploy/setup-aws.sh --profile my-aws-profile

# With explicit parameters
./deploy/setup-aws.sh \
    --profile prod \
    --region us-east-1 \
    --account 123456789012 \
    --org conservationinternational \
    --repo avoided-emissions-web
```

The `--profile` flag sets `AWS_PROFILE` for all CLI calls. You can also export
`AWS_PROFILE` in your shell before running the script.

The script is **idempotent** — safe to run multiple times. It will skip resources that
already exist.

---

## What the Script Creates

### 1. ECR Repositories

| Repository | Purpose |
|---|---|
| `avoided-emissions-webapp` | Python 3.13 Dash/Flask webapp image |
| `avoided-emissions-ranalysis` | R 4.5.2 analysis container image |

Both repos have a lifecycle policy that expires untagged images after 14 days.

### 2. S3 Buckets

| Bucket | Purpose |
|---|---|
| `avoided-emissions-deployments-{ACCOUNT_ID}` | CodeDeploy revision packages |
| `avoided-emissions-data` | Analysis uploads, COGs, and results |

Both buckets have public access blocked and the deployment bucket has versioning
enabled.

### 3. IAM Roles

| Role | Trust Principal | Purpose |
|---|---|---|
| `avoided-emissions-web-github-actions` | GitHub OIDC provider | CI/CD: push to ECR, upload to S3, create CodeDeploy deployments |
| `avoided-emissions-web-ec2` | `ec2.amazonaws.com` | Instance role: ECR pull, S3 data access, Batch job submission, CodeDeploy agent, SSM |
| `avoided-emissions-web-batch-execution` | `ecs-tasks.amazonaws.com` | Batch containers: S3 read/write for analysis data |
| `avoided-emissions-web-codedeploy-service` | `codedeploy.amazonaws.com` | CodeDeploy service role for managing deployments |

A **GitHub Actions OIDC provider** is also created if one does not already exist in
the account.

### 4. CodeDeploy

| Resource | Value |
|---|---|
| Application | `avoided-emissions-web` |
| Staging group | `avoided-emissions-web-staging` |
| Production group | `avoided-emissions-web-production` |

Deployment groups use EC2 tag filters:
- **Staging**: `DeploymentGroupAvoidedEmissions = avoided-emissions-staging`
- **Production**: `DeploymentGroupAvoidedEmissions = avoided-emissions-production`

Auto-rollback is enabled on deployment failure.

---

## Manual Setup Steps

### EC2 Instance Preparation

1. **Attach the instance profile**:
   ```bash
   aws ec2 associate-iam-instance-profile \
       --instance-id i-XXXXXXXXX \
       --iam-instance-profile Name=avoided-emissions-web-ec2-profile
   ```

2. **Tag the instance** for CodeDeploy targeting:
   ```bash
   # For staging
   aws ec2 create-tags --resources i-XXXXXXXXX \
       --tags Key=DeploymentGroupAvoidedEmissions,Value=avoided-emissions-staging

   # For production
   aws ec2 create-tags --resources i-XXXXXXXXX \
       --tags Key=DeploymentGroupAvoidedEmissions,Value=avoided-emissions-production
   ```

3. **Install Docker and initialize Swarm**:
   ```bash
   # Amazon Linux 2023
   sudo yum install -y docker
   sudo systemctl enable --now docker
   sudo usermod -aG docker ec2-user
   docker swarm init
   ```

4. **Install the CodeDeploy agent**:
   ```bash
   # Amazon Linux 2023
   sudo yum install -y ruby wget
   REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/region)
   wget "https://aws-codedeploy-${REGION}.s3.${REGION}.amazonaws.com/latest/install"
   chmod +x install
   sudo ./install auto
   sudo systemctl enable --now codedeploy-agent
   ```

5. **Authenticate to ECR** (also done automatically by CodeDeploy hooks):
   ```bash
   REGION=us-east-1
   ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
   aws ecr get-login-password --region $REGION | \
       docker login --username AWS --password-stdin "${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
   ```

### Elastic Load Balancer (ELB) Setup

The setup script does **not** provision the ELB because it is assumed to already
exist. Here is what the ELB configuration should look like:

#### Target Group

| Setting | Value |
|---|---|
| **Target Type** | Instance |
| **Protocol** | HTTP |
| **Port** | 8050 (production) or 8051 (staging) |
| **VPC** | Same VPC as the EC2 instance |
| **Health check path** | `/health` |
| **Health check interval** | 30 seconds |
| **Healthy threshold** | 2 |
| **Unhealthy threshold** | 3 |

Register the EC2 instance as a target.

#### Listeners

| Listener | Action |
|---|---|
| **HTTPS :443** | Forward to target group |
| **HTTP :80** | Redirect to HTTPS :443 |

#### SSL Certificate

Attach an ACM certificate for your domain to the HTTPS listener. Use ACM Certificate
Manager to request or import a certificate.

#### Security Group (ELB)

| Rule | Port | Source |
|---|---|---|
| Inbound | 80 | `0.0.0.0/0` |
| Inbound | 443 | `0.0.0.0/0` |
| Outbound | 8050-8051 | EC2 security group |

#### Security Group (EC2)

| Rule | Port | Source |
|---|---|---|
| Inbound | 8050-8051 | ELB security group |
| Inbound | 22 | Your IP / bastion SG |
| Outbound | All | `0.0.0.0/0` |

### AWS Batch Setup

The setup script creates the IAM roles for Batch but does **not** create the compute
environment or job queue because those require VPC-specific configuration (subnets,
security groups).

#### Create the Compute Environment

```bash
aws batch create-compute-environment \
    --compute-environment-name avoided-emissions-ce \
    --type MANAGED \
    --compute-resources '{
        "type": "FARGATE",
        "maxvCpus": 16,
        "subnets": ["subnet-XXXXXXXX", "subnet-YYYYYYYY"],
        "securityGroupIds": ["sg-XXXXXXXX"]
    }' \
    --service-role arn:aws:iam::ACCOUNT:role/avoided-emissions-web-batch-service \
    --region us-east-1
```

> **Subnet requirements**: Use private subnets with a NAT Gateway so that Batch
> containers can pull ECR images and access S3. The subnets should be in the same VPC
> as the EC2 instance.

#### Create the Job Queue

```bash
aws batch create-job-queue \
    --job-queue-name avoided-emissions-queue \
    --priority 1 \
    --compute-environment-order '[{
        "computeEnvironment": "avoided-emissions-ce",
        "order": 1
    }]' \
    --region us-east-1
```

#### Job Definitions

Job definitions are registered programmatically by the webapp via `batch_jobs.py`.
The first analysis run will register the definition automatically. Default resources
per job:

| Resource | Value |
|---|---|
| vCPUs | 4 |
| Memory | 16,384 MiB (16 GB) |
| Timeout | 14,400 s (4 hours) |
| Retries | 2 |

---

## GitHub Repository Configuration

After running the setup script, configure these in your GitHub repository settings
under **Settings → Secrets and variables → Actions**.

Variables and secrets are shared across staging and production GitHub environments
unless overridden at the environment level.

### Secrets

| Secret | Description |
|---|---|
| `SECRET_KEY` | Flask secret key. Generate: `python -c "import secrets; print(secrets.token_hex(32))"` |
| `POSTGRES_USER` | Database username |
| `POSTGRES_PASSWORD` | Database password |
| `POSTGRES_DB` | Database name |
| `EE_SERVICE_ACCOUNT_JSON` | Google Earth Engine service account key (JSON string). Used by the webapp to trigger GEE covariate exports. The R analysis container does **not** need GCS credentials — it reads COGs from the public GCS bucket via GDAL `/vsicurl/`. |

### Variables

| Variable | Default | Description |
|---|---|---|
| `AWS_OIDC_ROLE_ARN` | *(none — required)* | IAM role ARN for GitHub Actions OIDC. Output by `setup-aws.sh` |
| `S3_BUCKET` | *(none — required)* | S3 bucket for analysis data (e.g. `avoided-emissions-data`) |
| `S3_PREFIX` | `avoided-emissions` | Key prefix inside the S3 bucket |
| `GCS_BUCKET` | *(none — required)* | GCS bucket for GEE covariate exports. Must allow public read access so the R container can read COGs via `/vsicurl/` |
| `GCS_PREFIX` | `avoided-emissions/covariates` | Key prefix inside the GCS bucket |
| `GEE_PROJECT_ID` | *(none — required for GEE exports)* | Google Cloud project ID registered for Earth Engine. Passed to `ee.Initialize(project=...)` |
| `GEE_ENDPOINT` | *(default EE endpoint)* | Optional Earth Engine API endpoint URL (e.g. `https://earthengine-highvolume.googleapis.com`) |
| `AWS_BATCH_JOB_QUEUE` | `avoided-emissions-queue` | AWS Batch job queue name |
| `AWS_BATCH_JOB_DEFINITION` | `avoided-emissions-analysis` | AWS Batch job definition name |

### Generated `.env` File

During deployment, the workflow generates a `.env` file containing all of the above
plus values derived at build time (ECR registry, image URIs, `DATABASE_URL`,
`AWS_ACCOUNT_ID`, etc.). The CodeDeploy `after-install.sh` hook copies this file to
the application directory where Docker Compose reads it.

See [deploy/.env.example](deploy/.env.example) for the full list of variables with
comments.

---

## Environment Files

In **CI/CD**, environment files are generated automatically by the GitHub Actions
workflow from repository secrets and variables. You do **not** need to create them
manually for staging/production.

For **local development**, copy the template:

```bash
cp deploy/.env.example .env
# Edit .env with your local values
```

The development compose file reads from `../.env` (repository root).

---

## Deployment Flow

```
Developer pushes to main/staging branch
        │
        ▼
GitHub Actions workflow triggered
        │
        ├─ Build & push Docker images to ECR
        ├─ Generate .env from secrets/variables
        ├─ Package deploy/ + .env + appspec.yml → S3
        └─ Create CodeDeploy deployment
                │
                ▼
        CodeDeploy agent on EC2
                │
                ├─ application-stop.sh   → drain Docker Swarm services
                ├─ before-install.sh     → verify Docker/Swarm, disk cleanup
                ├─ after-install.sh      → install .env, pull ECR images
                ├─ application-start.sh  → deploy Docker Swarm stack
                └─ validate-service.sh   → health check /health endpoint
```

---

## Troubleshooting

### CodeDeploy agent not picking up deployments

```bash
# Check agent status
sudo systemctl status codedeploy-agent

# View agent logs
tail -100 /var/log/aws/codedeploy-agent/codedeploy-agent.log

# Verify EC2 tags
aws ec2 describe-instances --instance-ids i-XXXXX \
    --query 'Reservations[].Instances[].Tags'
```

### ECR login expired

ECR tokens expire after 12 hours. The CodeDeploy hooks re-authenticate on each
deployment. For manual access:

```bash
aws ecr get-login-password --region us-east-1 | \
    docker login --username AWS --password-stdin ACCOUNT.dkr.ecr.us-east-1.amazonaws.com
```

### Batch jobs failing

```bash
# Check job status
aws batch describe-jobs --jobs JOB_ID --region us-east-1

# View CloudWatch logs (log group: /aws/batch/job)
aws logs get-log-events \
    --log-group-name /aws/batch/job \
    --log-stream-name "avoided-emissions-analysis/default/TASK_ID"
```

### Docker Swarm stack not starting

```bash
# Check service status
docker stack ls
docker stack services avoided-emissions

# View service logs
docker service logs avoided-emissions_webapp --tail 100
docker service logs avoided-emissions_db --tail 100
```
