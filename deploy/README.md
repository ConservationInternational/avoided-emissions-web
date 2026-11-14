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

### Required Nextflow analysis backend

Nextflow is the sole analysis backend, including single-site analyses. The
[Nextflow implementation and rollout guide](../docs/matching-reliability-plan.md#8-implementation-and-operational-contract)
describes the separate pinned On-Demand driver, child-job policies, IAM, durable
cache/checkpoints, explicit resume, and required AWS canaries. Deploy the
companion API changes and configure its driver before deploying this webapp.
Republish the script policy: it both defaults to and permits only Nextflow,
including submissions made directly through the API. There is no legacy fallback.
The publishing workflow builds both images but does not configure the API's
driver or register its job definition automatically.

### Migrate the Nextflow driver to Fargate

Use [migrate_nextflow_driver.py](migrate_nextflow_driver.py) to move **only the
driver** to AWS Batch Fargate On-Demand. Extraction, matching, and summary jobs
stay on their existing EC2 Spot/On-Demand queues. The migration does not rebuild
images or alter scientific inputs, worker retries, S3 state, or workflow policy.
It clones the current driver's command, roles, secrets, and workflow environment,
resolves its image to an immutable ECR digest, and sets both AWS region variables.
The source must be a compatible single-container job with at least 2 vCPUs /
4 GiB; incompatible host mounts, privileged mode, and other EC2-only settings
cause an explicit error rather than being silently dropped.

Prerequisites:

- Python 3.10+ with `boto3` and `requests` (already in
  [webapp/requirements.txt](../webapp/requirements.txt)).
- AWS credentials with Batch read/create/register/submit/terminate permissions,
  EC2 subnet/security-group lookup, STS identity, ECR image lookup, IAM
  `GetRole`/`GetRolePolicy`/`PutRolePolicy`, and permission to pass the source driver/execution
  roles. Creating the Batch service-linked role may require
  `iam:CreateServiceLinkedRole` if it does not exist.
- An API **admin** JWT in `TRENDSEARTH_API_TOKEN`, or OAuth credentials in
  `TRENDSEARTH_CLIENT_ID` / `TRENDSEARTH_CLIENT_SECRET` for an admin principal.
  Tokens need `script:read` and `script:write` scopes. Select credentials for
  the target API environment; do not put credentials in command-line arguments.
- Existing Nextflow-enabled Script and working driver/execution IAM roles,
  including the S3/tagging and child-job permissions in the rollout guide.
  The existing API role must retain its Batch monitoring/cancellation permissions.
- Explicit subnet/security-group IDs in one VPC. With `--assign-public-ip
  DISABLED`, provide NAT or all required VPC endpoints and routes (including
  ECR, S3, CloudWatch Logs, and Batch). Public-IP mode needs public subnet
  routes to an internet gateway. The image may also need outbound internet
  access for Nextflow plugins. The script does not change networking.

From the repository root, preview the read-only plan (authenticate first using
the environment variables above):

```bash
python3 deploy/migrate_nextflow_driver.py \
  --environment staging --profile my-profile --region us-east-1 \
  --subnets subnet-aaaaaaaa subnet-bbbbbbbb \
  --security-groups sg-aaaaaaaa \
  --assign-public-ip DISABLED
```

Replace the example IDs with actual networking IDs. Re-run the same command with
`--apply --report /secure/existing-directory/staging-fargate-migration.json` to
perform the migration. The report must be a **new file in an existing directory**.
Choose `--environment production` and production credentials/networking to
migrate production separately. `--max-vcpus` defaults to 16 (eight 2-vCPU drivers);
increase it deliberately for higher workflow concurrency.

If the Script currently relies on API fallback settings instead of explicit
driver fields, supply their effective values with `--source-job-definition`
(preferably a revision-pinned ARN) and `--source-job-queue`. Overrides cannot
replace a different nonempty published Script field. Use `--api-role-name` if
the API runs under a role other than `trendsearth-api-docker-swarm-role`.

With `--apply`, the script:

1. Creates or reuses the environment-specific
   `ae-nextflow-driver-{environment}-fargate` compute environment and queue.
   Existing resources with conflicting configuration are rejected, not modified.
   The environment is `FARGATE`, never `FARGATE_SPOT`.
2. Registers/reuses a pinned compatible driver definition with one native
   attempt and timeout headroom for the maximum seven-day workflow budget.
   Per-execution API timeouts still take precedence.
3. Runs a billed canary using that image, networking, and driver role. It checks
   Nextflow startup, AWS region configuration, task-role credentials, and child
   queue lookup. A failure prevents cutover; a timed-out/interrupted canary is
   terminated. Inspect its Batch/CloudWatch logs using the printed job ID.
4. Adds an environment-specific inline policy to the API role for submitting
   and tagging the new driver and passing its roles. Existing policies and
   child-job roles are not replaced; a conflicting same-named inline policy
   causes an error. IAM propagation, permission boundaries,
   and organization-level denies can still affect the API's effective access.
5. Saves effective old driver settings and target ARNs to the rollback report,
   PATCHes only this Script's `batch_job_definition` / `batch_job_queue`, and
   reads them back to verify persistence. The report is written **before**
   attempting the API switch and contains no credentials.

No API redeploy or global `NEXTFLOW_*` variable change is required: the Script
fields take precedence. Subsequent publishing from the current
[configuration.json](../r-analysis/configuration.json) preserves these fields
because it does not declare them. Do not later add EC2 driver fields to that
configuration or overwrite them manually.

The canary is not an end-to-end scientific analysis or an API-role submission
test. After cutover, run a small workflow through the target API and verify
S3 publication, cancellation, and explicit resume before broad production use.
Infrastructure is retained on failure for diagnosis; there is no automatic
destructive rollback. Creation/reuse is repeatable, but each apply needs a fresh
report path; retain the **first** report containing the old EC2 settings.

To roll back the Script after a failed rollout, use a current admin JWT:

```bash
REPORT=/secure/existing-directory/staging-fargate-migration.json
curl --fail-with-body --request PATCH \
  "$(jq -r '.api_url' "$REPORT")/api/v1/script/$(jq -r '.script_id' "$REPORT")/config" \
  --header "Authorization: Bearer $TRENDSEARTH_API_TOKEN" \
  --header 'Content-Type: application/json' \
  --data "$(jq -c '.rollback_patch' "$REPORT")"
```

Read the Script back to confirm its two driver fields match `rollback_patch`.
This restores effective pinned settings, even if the old deployment used API
fallbacks. Rollback affects future submissions, not drivers already submitted
to Fargate. Likewise, migration does not terminate existing EC2 drivers.
**Keep old queues and job-definition revisions enabled/active**: explicit
resume uses infrastructure recorded on the original execution. Do not delete
shared EC2 compute environments or worker queues as part of this migration.

### Valkey broker and session storage

Development, staging, and production use `valkey/valkey:9.1.3-alpine`.
The service remains named `redis`, and `redis://` URLs and the Python `redis`
client are retained for Redis-protocol compatibility. The Python client uses
redis-py 8.1 or later (below 9), within the Celery/Kombu Redis transport's
supported dependency range.

The stacks do not configure persistent storage for this service. Before replacing
an existing Redis server, stop new task submissions and drain queued and active
Celery tasks in a maintenance window. Without a separate data migration, replacing
the server can discard queued tasks, cached results, and sign-in sessions; users
will need to sign in again.

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
| `avoided-emissions-nextflow` | Nextflow driver container image |

All three repos have a lifecycle policy that expires untagged images after 14 days.

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

Deployment groups use **separate** EC2 tag keys so the same instances can serve
both environments simultaneously:
- **Staging**: `AvoidedEmissionsStaging = true`
- **Production**: `AvoidedEmissionsProduction = true`

The CodeDeploy hooks detect which environment is being deployed from the
`DEPLOYMENT_GROUP_NAME` variable and only run the deployment on the Docker
Swarm leader node.

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

2. **Tag the instances** for CodeDeploy targeting. Both tags can be applied to
   the same instances so that staging and production run side-by-side on the
   same Docker Swarm cluster:
   ```bash
   # Tag all swarm instances for both environments
   aws ec2 create-tags --resources i-XXXXXXXXX i-YYYYYYYYY i-ZZZZZZZZZ \
       --tags Key=AvoidedEmissionsStaging,Value=true \
              Key=AvoidedEmissionsProduction,Value=true
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

Job definitions are registered on the **trends.earth API** as part of the
`Script` model configuration.  The webapp does not manage Batch job definitions
directly — it creates an execution on the API, which dispatches to Batch using
the trusted Nextflow driver configured on the API. Nextflow owns all extraction,
matching, recovery, and summary jobs. The webapp sends only approved initial
matching memory/queue preferences; it does not submit pipeline descriptors,
array jobs, commands, timeouts, retry counts, or job definitions.

Resource defaults are process-specific:

| Resource | Value |
|---|---|
| Driver | At least 2 vCPUs / 4 GiB, On-Demand (Fargate after migration), one native attempt |
| Matching | 30 GiB initially by default; up to 120 GiB after confirmed OOM |
| Matching timeout | 4/8/16 hours across Nextflow retries |
| Extraction | 128 GiB initially; up to 512 GiB after confirmed OOM |
| Child retries | One native Batch attempt; at most three Nextflow submissions |

Configure workflow-specific `AE_NEXTFLOW_*` settings on the driver job
definition, not on the API. The API supplies generic `NEXTFLOW_*` lifecycle
variables and leaves the driver command unchanged. Matching memory and initial
queue are workflow inputs validated by this repository's driver.

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
| `OIDC_CLIENT_SECRET` | Secret of the Trends.Earth OIDC client used for user sign-in. **Set per environment**: the staging environment uses the `avoided-emissions-web-staging` client and production the `avoided-emissions-web` client. |
| `TRENDSEARTH_CLIENT_ID` | OAuth2 service-client ID for the trends.earth API. Used for background polling of execution status, result downloads and admin cancellation of other users' tasks (user task submission uses the signed-in user's token). |
| `TRENDSEARTH_CLIENT_SECRET` | OAuth2 service-client secret for the trends.earth API. |
| `ROLLBAR_ACCESS_TOKEN` | *(optional)* Rollbar server-side access token for error tracking. |

#### Publish-Script Workflow Secrets

The `publish-script.yml` workflow (publishes the R analysis script definition
to the trends.earth API) uses its own OAuth2 credentials:

| Secret | Description |
|---|---|
| `TRENDSEARTH_CLIENT_ID` | OAuth2 client ID for the trends.earth CLI (used by the publish workflow only). |
| `TRENDSEARTH_CLIENT_SECRET` | OAuth2 client secret for the trends.earth CLI. |

Publication failures stop the workflow. Verification fetches the published
script's compute type, Batch image, and execution policy and compares them with
the configuration being published; an existing script's successful status alone
does not verify an update.

### Variables

| Variable | Default | Description |
|---|---|---|
| `AWS_OIDC_ROLE_ARN` | *(none — required)* | IAM role ARN for GitHub Actions OIDC. Output by `setup-aws.sh` |
| `S3_BUCKET` | *(none — required)* | S3 bucket for analysis data (e.g. `avoided-emissions-data`) |
| `S3_PREFIX` | `avoided-emissions` | Key prefix inside the S3 bucket |
| `GCS_BUCKET` | *(none — required)* | GCS bucket for GEE covariate exports. Must allow public read access so the R container can read COGs via `/vsicurl/` |
| `GCS_PREFIX` | `avoided-emissions/covariates` | Key prefix inside the GCS bucket |
| `GOOGLE_PROJECT_ID` | *(none — required for GEE exports)* | Google Cloud project ID registered for Earth Engine. Passed to `ee.Initialize(project=...)` |
| `GEE_ENDPOINT` | *(default EE endpoint)* | Optional Earth Engine API endpoint URL (e.g. `https://earthengine-highvolume.googleapis.com`) |
| `TRENDSEARTH_API_URL` | `https://api.trends.earth/api/v1` | trends.earth API v1 endpoint |
| `APP_URL` | *(none — required)* | Public URL of the environment. `APP_URL/auth/callback` (redirect) and `APP_URL/` (post-logout redirect) must be registered on the environment's OIDC client |
| `OIDC_ISSUER` | `https://api.trends.earth` | Trends.Earth OIDC issuer |
| `OIDC_CLIENT_ID` | staging: `avoided-emissions-web-staging`; production: `avoided-emissions-web` | OIDC client ID |
| `OIDC_ACCESS_AUDIENCE` | staging: `avoided-emissions-staging`; production: `avoided-emissions` | Expected access-token audience |
| `OIDC_APP_KEY` | staging: `avoided_emissions_staging`; production: `avoided_emissions` | Trends.Earth app key for access grants and roles |
| `TRENDSEARTH_SCRIPT_ID` | *(none — required)* | UUID of the avoided-emissions R analysis script registered on the trends.earth API |

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
