# Personal Finance Data Platform (FinDataOps)

A personal finance data engineering portfolio project using synthetic transactions, PostgreSQL, dbt, and Python. Airflow, Great Expectations, DuckDB export, and FastAPI components are included; the complete Docker Compose pipeline has not been verified end to end.

Uses synthetic data generated with `--transactions 100000`.

## Verified scope

- CSV extractors with normalization helpers and nine pytest unit tests.
- Fourteen dbt models, including a USD staging view and thirteen analytical models for cashflow, budgets, recurring payments, anomalies, forecasts, and net worth.
- Three reference seeds and 57 executable dbt data tests.
- A nine-task Airflow DAG with year/month Parquet partitioning code. Task count describes the code, not a verified successful DAG run.
- Three Great Expectations suite builders containing 56 expectation declarations. CI does not execute these suites; the DAG's `run_ge_suites` function currently performs SQL checks rather than invoking GE.
- GitHub Actions configuration for Black, isort, fatal flake8 checks, mypy on ingestion scripts, pytest, dbt build/tests and docs against PostgreSQL 15, and Bandit scans. See [Actions](https://github.com/rohitv117/personal-finance-data-platform/actions).

There are no verified analytical speedup measurements, deployed AWS resources, OFX/QFX extractors, ClickHouse integration, or BI dashboards claimed here.

## Run the verified checks

Use Python 3.11 and PostgreSQL 15. Run from `fndataops/`:

```bash
git clone https://github.com/rohitv117/personal-finance-data-platform.git
cd personal-finance-data-platform/fndataops
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements-ci.txt
black --check .
isort --check-only .
flake8 . --select=E9,F63,F7,F82 --show-source --statistics
mypy ingest/scripts/ --ignore-missing-imports
pytest tests/ --cov=ingest/scripts --cov-report=xml
bandit -r ingest api orchestration dq observability security ci -ll
```

Bandit fails on medium/high severity findings. Narrow `B608` exceptions document SQL identifiers selected exclusively from fixed internal lists; request values use bound parameters. This scan is not an authentication or application security audit.

For dbt, create an **empty disposable** PostgreSQL database called `finops`. The fixture creates schemas and tables and is intended only for that database:

```bash
export DATABASE_URL='postgresql://postgres:postgres@localhost:5432/finops'
export DBT_USER=postgres
export DBT_PASSWORD=postgres
export DBT_PROFILES_DIR="$PWD/dbt"
python ci/load_fixture.py
dbt deps --project-dir dbt
dbt build --project-dir dbt
dbt docs generate --project-dir dbt
```

The fixture loads 48 deterministic synthetic transactions across two accounts and six months. It validates the dbt transformations, not a 100K end-to-end pipeline run. Repeating `dbt build` exercises existing incremental relations. The fixture loader itself requires an empty database.

## Generate the 100K dataset

```bash
python ingest/synthetic_data_generator.py   --transactions 100000 --institutions 5   --start-date 2025-01-01 --end-date 2025-12-31   --output-file /tmp/finops-100k.csv
```

This command was run and the output checked: 100,000 rows and 100,000 distinct transaction IDs. This establishes data generation only, not throughput or query performance. Generated records contain no real customer data. Keep outputs outside Git.

There are two generators: `ingest/synthetic_data_generator.py` provides the CLI above and a richer schema; `ingest/scripts/data_generator.py` is the older generator referenced by the DAG. Their outputs are not interchangeable without schema mapping.

## Architecture in the code

```text
CSV → extractors → PostgreSQL raw → dbt staging → dbt marts
                                                  ↓
                                  Airflow Parquet export → DuckDB

Airflow orchestration | GE suite builders | FastAPI endpoints
```

## Current limitations

- Docker Compose, Airflow execution, GE checkpoints, DuckDB loading, and API endpoints have not been validated by the CI workflow. Existing Compose mount paths, Airflow initialization/dependencies, API/model contracts, and SQLAlchemy runtime compatibility still need integration work before using the stack instructions in the legacy Makefile.
- Staging currently supports USD only; the `supported_currency` dbt test rejects unsupported currencies. FX seeds do not establish tested multi-currency ingestion.
- The existing raw schema has no balances. Net-worth models therefore produce no rows with this fixture; a passing null check on an empty model does not establish net-worth correctness.
- Forecasts and anomalies are illustrative SQL heuristics, not validated financial models. Several older incremental marts use date watermarks and need a full refresh to incorporate late transactions.
- Example local credentials appear in the source. Replace them for any actual deployment. Authentication, compliance, alert delivery, disaster recovery, and production readiness are not established.
- No AWS infrastructure is deployed. AWS work belongs on a separate branch and needs its own Terraform, integration, and CI verification.

## Repository layout

```text
.github/workflows/ci.yml    # Active repository-root workflow
fndataops/
  ci/                      # Disposable synthetic PostgreSQL fixture
  ingest/                  # Generators and CSV extractors
  orchestration/dags/      # Airflow DAG
  dbt/                     # Models, seeds, tests, profiles
  dq/                      # GE suite/checkpoint builders
  api/                     # FastAPI source
  tests/                   # Extractor/generator unit tests
  docker-compose.yml       # Local stack definition (integration pending)
```

See [verification notes](docs/VERIFICATION.md) for validation scope and evidence.
