# SarvSynth: Clinical Intelligence and Health Analytics Platform

SarvSynth is a high-performance clinical analytics infrastructure designed for hospital administrators and medical practitioners. The platform transforms synthetic FHIR-standard patient data into actionable intelligence through a multi-stage ETL pipeline and a specialized medical-grade executive interface.

## Project Overview

The SarvSynth platform provides an integrated perspective on hospital operations, population health dynamics, and longitudinal pathological trends. The system is architected to process complex clinical datasets while maintaining a premium, distraction-free environment optimized for high-stakes healthcare decision-making.

## Core Capabilities

- **Executive Intelligence**: Unified aggregate statistics across all clinical and administrative departments with **All-Time** cumulative benchmarking.
- **Temporal Reconstruction**: Advanced historical trend engine that snapshots population health and operational metrics as they existed at specific points in time (Last Week/Month/Year).
- **Population Health Analytics**: Granular analysis of patient demographics, socioeconomic dependence, and mortality risk modeling.
- **Pathological Tracking**: Longitudinal monitoring of condition incidence, recurrence intervals, and recovery efficacy.
- **Data Operations Center**: Interactive interface for triggering regionalized synthetic data generation with precise control over patient volume and geographic targeting.

## Technical Architecture

- **Frontend**: React (Vite) utilizing Tailwind CSS with Recharts and ECharts for high-density data visualization.
- **Backend**: Asynchronous Flask API utilizing dependency injection and schema-anchored Supabase connectivity.
- **Data Engineering**: PySpark-driven ETL and **Temporal Analytics Pipelines** for high-throughput metric derivation and historical population reconstruction.
- **Data Persistence**: Supabase (PostgreSQL) for structured clinical records and pre-calculated analytical metrics.
- **Deployment**: Native configuration for Vercel Serverless Functions and GitHub Actions for automated regionalized data refreshes.

## Deployment and Installation

### 1. Local Development Environment

**System Requirements**: Python 3.12+, Node.js 18+, Java (required for PySpark execution).

1. **Dependency Installation**:
   ```bash
   # Backend and Analytics Infrastructure
   uv sync
   
   # Frontend Application
   cd synthea-frontend && npm install
   ```

2. **Configuration**:
   Establish a `.env` file in the repository root with the following parameters:
   ```env
   SUPABASE_URL=your_endpoint_url
   SUPABASE_ANON_KEY=your_access_key
   VITE_APP_API_URL=http://127.0.0.1:3001

   # Optional: Deploy Generation Jobs to GitHub Actions
   GITHUB_TOKEN=your_github_personal_access_token
   GITHUB_REPOSITORY=owner/repo_name
   GITHUB_REF=main
   ```

3. **Service Execution**:
   ```bash
   # Execute Flask API (Default Port: 3001)
   python -m api.main
   
   # Execute Vite Development Server
   cd synthea-frontend && npm run dev
   ```

### 2. Analytics Execution

To synchronize dashboard metrics following data ingestion:
```bash
uv run python3 -m workflows.analytics_runner
```

## Cloud Deployment

The repository is pre-configured for the Vercel platform.

- **Build Specification**: `npm run build` (within the `synthea-frontend` directory)
- **Output Specification**: `synthea-frontend/dist`
- **Routing Configuration**: Managed through the `vercel.json` specification in the repository root.

---
*Developed as an enterprise-grade ERP solution for synthetic medical data visualization and analysis.*
