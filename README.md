# AeroPulse

AeroPulse is a starter analytics project for flight data pipelines. It combines Dagster for orchestration, dbt for transformations, ClickHouse as the local warehouse, and Metabase for dashboards and BI.

Dagster dependencies are managed with `uv` in `dagster/pyproject.toml`.

## Project Structure

```text
aeropulse/
├── .env.example
├── .gitignore
├── Makefile
├── docker-compose.yml
├── sources/
├── dagster/
│   ├── dagster.yaml
│   ├── Dockerfile
│   ├── workspace.yaml
│   ├── pyproject.toml
│   ├── uv.lock
│   └── app/
│       ├── __init__.py
│       ├── assets/
│       └── resources/
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/
│       └── marts/
└── terraform/
    ├── main.tf
    ├── variables.tf
    ├── outputs.tf
    └── terraform.tfvars.example
```

## Run Locally

```bash
make up
```

The Dagster UI will be available at `http://localhost:3000`.
Metabase will be available at `http://localhost:3001`.
ClickHouse will be available on HTTP port `8123` and native port `9000`.

When adding ClickHouse in Metabase, use the Docker service name and HTTP port:

```text
Database type: ClickHouse
Host: clickhouse
Port: 8123
Username: aeropulse
Password: aeropulse
Database: aeropulse_analytics
SSL: off
```

Useful commands:

```bash
make help
make clean
make ps
make terraform-plan
make terraform-deploy
make dbt-build
make materialize
make verify
```

## Source Ingestion

Dagster reads local CSV files from `sources/` by default:

```text
sources/Airline_Delay_Cause.csv
sources/airports.csv
sources/flight_data_2024.csv
```

`airports.csv` is loaded into `aeropulse_raw.airports`. `flight_data_2024.csv` is streamed into `aeropulse_raw.flights` in batches, and any extra airport codes discovered in the file are added to `aeropulse_raw.airports`. `Airline_Delay_Cause.csv` is loaded into `aeropulse_raw.airline_delay_causes`.

For quick test runs, copy `.env.example` to `.env` and set:

```bash
FLIGHTS_SOURCE_LIMIT=10000
DELAY_CAUSES_SOURCE_LIMIT=10000
```

## API Ingestion

Dagster can fetch raw airport and flight data from HTTP JSON APIs. Copy `.env.example` to `.env`, then set:

```bash
AIRPORTS_API_URL="https://example.com/airports"
FLIGHTS_API_URL="https://example.com/flights"
API_KEY="your-token"
```

If your API wraps records, set a dot path such as:

```bash
AIRPORTS_API_DATA_PATH="data.airports"
FLIGHTS_API_DATA_PATH="data.flights"
```

Expected airport fields include `airport_code` or `iata`, `airport_name` or `name`, `city`, `country`, `latitude`, and `longitude`.
Expected flight fields include `flight_number`, origin/destination airport codes, scheduled departure/arrival timestamps, optional actual timestamps, `status`, `aircraft_type`, and `passenger_count`.

## Pipeline

1. Terraform manages the ClickHouse `aeropulse_raw`, `aeropulse_staging`, and `aeropulse_analytics` schemas.
2. Dagster materializes raw data from `sources/`, configured APIs, or local sample data when neither source is available.
3. dbt builds staging models in `aeropulse_staging`.
4. dbt builds analytics marts in `aeropulse_analytics`.
5. Metabase connects to `aeropulse_analytics` for dashboards and BI.
