
# Football Data Engineering Platform

## Project goal
Build a production-oriented football data engineering platform using StatsBomb Open Data.

## Architecture
Status: only Bronze is implemented. Silver, Gold and orchestration are planned.
- Bronze (implemented): Original JSON data in local storage and S3-compatible SeaweedFS.
- Silver: PySpark transformations and Parquet datasets.
- Gold: Analytical models using dbt and SQL.
- Orchestration: Apache Airflow.
- Testing: pytest and data quality checks.

## Technology
Python, SQL, PySpark, PostgreSQL, Parquet, SeaweedFS, S3 API, dbt, Airflow, Docker.

## Development rules
- Inspect the existing code before proposing changes.
- Preserve the current architecture unless there is a clear reason to change it.
- Prefer simple, maintainable implementations.
- Do not introduce unnecessary dependencies.
- Write tests for new functionality.
- Never remove existing tests just to make the test suite pass.
- Run ruff check . and pytest -q after code changes.
- Never hardcode credentials or commit .env or s3-config.json. Use .env.example and s3-config.example.json as templates.
- Keep manual scripts in scripts/. Files under tests/ must run offline and must not perform real S3 operations.
- Preserve original Bronze data.
- Make ingestion and transformations reproducible and idempotent.
- Explain important engineering decisions.
- Do not invent data or analytical metrics unsupported by StatsBomb.

## Working style
Before implementing a substantial feature:
1. Inspect relevant files.
2. Explain the proposed approach.
3. Identify files that will change.
4. Implement the feature.
5. Run tests and report actual results.
6. Summarize changes and remaining issues.

