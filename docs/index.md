# Avoided Emissions Analysis System — Documentation

This directory contains in-depth technical documentation for the
Avoided Emissions Analysis System. For quick-start instructions, environment
variables, and a system overview see the top-level [README](../README.md).

## Contents

| Document | Description |
|---|---|
| [webapp.md](webapp.md) | Web application user guide — pages, workflows, authentication, admin panel |
| [covariates.md](covariates.md) | Full covariate reference — all 40+ variables with sources, units, and year ranges |
| [covariate-pipeline.md](covariate-pipeline.md) | COG processing pipeline — GEE export → tile merge → reference layer rasterization → pixel extraction |
| [matching.md](matching.md) | Propensity score matching — methodology, every parameter explained, fallback behaviour |
| [analysis-outputs.md](analysis-outputs.md) | Analysis output reference — S3 bucket layout, all output files with column schemas |

## System Overview

The system chains three distinct runtimes to produce avoided-deforestation
emissions estimates for a set of conservation sites:

```
┌─────────────────────┐     ┌──────────────────────────────────────┐
│   GEE Covariate     │     │           Web Application            │
│   Export Pipeline   │────▶│  (site upload → task submission →    │
│   (gee_export/)     │     │   monitoring → result visualisation) │
└─────────────────────┘     └─────────────┬────────────────────────┘
                                          │ AWS Batch / trends.earth API
                                          ▼
                             ┌────────────────────────┐
                             │   R Analysis Container │
                             │   (r-analysis/)        │
                             │   Step 1: extract      │
                             │   Step 2: match        │
                             │   Step 3: summarize    │
                             └────────────────────────┘
```

**Data flow in brief:**

1. Covariate rasters (COGs) are exported from Google Earth Engine to S3 once,
   then reused for every analysis.
2. A user uploads site polygons through the web app and configures matching
   parameters.
3. The web app submits a job to AWS Batch via the trends.earth API. The batch
   container runs the three-step pipeline, writing intermediate and final
   outputs to S3.
4. The web app polls for completion, imports the results from S3 into the
   PostgreSQL database, and displays them interactively.
