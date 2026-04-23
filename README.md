# SarvSynth

SarvSynth is a high-performance, automated data pipeline designed for the ingestion, transformation, and analysis of synthetic medical records. The system orchestrates a complex workflow involving data generation via Synthea, distributed processing with Apache Spark, and cloud synchronization with Supabase.

## Architecture Overview

The system is built on a decoupled, asynchronous architecture to ensure scalability and reliability:

1. Data Generation: Synthea is utilized to generate realistic, longitudinal patient records in CSV format.
2. ETL Engine: A PySpark-based processing layer handles data cleaning, regex-based field extraction, and ISO-8601 date standardization.
3. Cloud Synchronization: An asynchronous Supabase client manages high-speed batch ingestion into a PostgreSQL backend.
4. Analytics Engine: A secondary Spark pipeline computes advanced clinical and demographic metrics, storing results as optimized JSONB structures in a centralized metrics store.
5. Orchestration: GitHub Actions automates the entire lifecycle, providing scheduled updates and manual trigger capabilities.

## Key Features

- Distributed Processing: Utilizes Apache Spark for efficient transformation of large-scale medical datasets.
- Asynchronous I/O: Implements non-blocking database operations to maximize throughput during cloud ingestion.
- Polymorphic Metrics Store: Uses a JSONB-based schema for analytics, allowing for flexible storage of complex clinical KPIs without schema migrations.
- Automated Lifecycle: Fully integrated CI/CD pipeline for data generation, processing, and analytics.

## Directory Structure

- .github/workflows/: Contains the GitHub Actions CI/CD configuration.
- workflows/etl_pipeline/: Core ETL logic and Pydantic data models.
- workflows/analytics_pipeline/: Spark logic for clinical and demographic metrics.
- workflows/scripts/: Utility scripts for environment initialization and data generation.
- supabase/migrations/: Database schema definitions and permission sets.

## Technical Specifications

- Language: Python 3.10+
- Processing: Apache Spark 3.5+
- Database: Supabase (PostgreSQL)
- Dependency Management: uv
- Infrastructure: GitHub-hosted runners (Ubuntu Latest, 4-vCPU, 16GB RAM)

## Configuration

The system requires the following environment variables for cloud connectivity. These should be defined in a .env file for local use or as Repository Secrets in GitHub Actions:

```bash
SUPABASE_URL=https://your-project-id.supabase.co
SUPABASE_KEY=your-service-role-key
```

## Execution

### Local Execution
To execute the pipeline locally, ensure Java 17+ and Python 3.10+ are installed:

```bash
uv sync
uv run python3 -m workflows.etl_runner
uv run python3 -m workflows.analytics_runner
```

### Manual Trigger
The pipeline can be manually triggered via the GitHub Actions interface or through the GitHub REST API using the workflow_dispatch event.

## License
Distributed under the MIT License. See LICENSE for more information.
