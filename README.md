cat > README.md <<'EOF'
# Football Data Engineering Platform

A portfolio project demonstrating reliable data ingestion, data quality,
incremental processing, analytical data storage, and orchestration using
public football event data.

## Planned Technology Stack

- Python
- pytest
- PostgreSQL
- MinIO / S3
- Parquet
- Apache Spark
- Apache Iceberg
- Trino
- dbt
- Apache Airflow
- Docker

## Data Source

StatsBomb Open Data:
https://github.com/statsbomb/open-data

This project is for educational and portfolio purposes. Data usage and
attribution requirements will be respected.

## Project Status

Initial setup. The first milestone is reliable ingestion of one football
match with source metadata, a SHA-256 checksum, and automated tests.
EOF