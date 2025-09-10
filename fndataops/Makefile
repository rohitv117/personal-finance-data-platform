# Personal Finance Data Platform Makefile

.PHONY: help quickstart demo refresh clean build test deploy

# Default target
help:
	@echo "Personal Finance Data Platform - Available Commands:"
	@echo ""
	@echo "Setup & Development:"
	@echo "  quickstart    - Start the complete platform with demo data"
	@echo "  demo          - Generate synthetic data and run demo pipeline"
	@echo "  refresh       - Refresh data and rebuild models"
	@echo "  build         - Build all containers"
	@echo "  clean         - Clean up containers and data"
	@echo ""
	@echo "Testing & Validation:"
	@echo "  test          - Run all tests and validations"
	@echo "  dbt-test      - Run dbt tests"
	@echo "  ge-validate   - Run Great Expectations validation"
	@echo ""
	@echo "Data Operations:"
	@echo "  generate-data - Generate synthetic financial data"
	@echo "  load-data     - Load data into PostgreSQL"
	@echo "  export-parquet- Export marts to Parquet format"
	@echo "  load-duckdb   - Load data into DuckDB"
	@echo ""
	@echo "Monitoring:"
	@echo "  logs          - View all container logs"
	@echo "  status        - Check container status"
	@echo "  metrics       - View performance metrics"

# Quick start - complete platform setup
quickstart: build generate-data
	@echo "🚀 Starting Personal Finance Data Platform..."
	docker-compose up -d
	@echo "⏳ Waiting for services to be ready..."
	@sleep 30
	@echo "📊 Platform is ready!"
	@echo "   - Airflow: http://localhost:8080"
	@echo "   - FastAPI: http://localhost:8000"
	@echo "   - PostgreSQL: localhost:5432"
	@echo "   - DuckDB: Available in warehouse/duckdb/"
	@echo ""
	@echo "Next steps:"
	@echo "1. Open Airflow and trigger the 'fin_dataops' DAG"
	@echo "2. Check the FastAPI docs at http://localhost:8000/docs"
	@echo "3. View generated data in the warehouse/ directory"

# Demo mode - generate data and run pipeline
demo: generate-data
	@echo "🎭 Running demo pipeline..."
	@echo "1. Loading synthetic data..."
	@echo "2. Running dbt models..."
	@echo "3. Exporting to Parquet..."
	@echo "4. Loading into DuckDB..."
	@echo "5. Running analytics..."
	@echo ""
	@echo "Demo data generated in data/ directory"
	@echo "Run 'make quickstart' to start the full platform"

# Refresh data and rebuild models
refresh: clean-data
	@echo "🔄 Refreshing data and models..."
	docker-compose exec airflow-scheduler airflow dags trigger fin_dataops
	@echo "✅ Refresh triggered. Check Airflow for progress."

# Build all containers
build:
	@echo "🔨 Building containers..."
	docker-compose build
	@echo "✅ Build complete!"

# Clean up containers and data
clean:
	@echo "🧹 Cleaning up..."
	docker-compose down -v
	docker system prune -f
	rm -rf warehouse/parquet/* warehouse/duckdb/* data/*.csv
	@echo "✅ Cleanup complete!"

# Clean only data (keep containers)
clean-data:
	@echo "🗑️  Cleaning data..."
	docker-compose exec postgres psql -U finops_user -d finops -c "TRUNCATE raw.transactions CASCADE;"
	docker-compose exec postgres psql -U finops_user -d finops -c "TRUNCATE staging.transactions CASCADE;"
	docker-compose exec postgres psql -U finops_user -d finops -c "TRUNCATE marts.mart_cashflow_daily CASCADE;"
	docker-compose exec postgres psql -U finops_user -d finops -c "TRUNCATE marts.mart_cashflow_monthly CASCADE;"
	docker-compose exec postgres psql -U finops_user -d finops -c "TRUNCATE marts.mart_budget_vs_actual CASCADE;"
	docker-compose exec postgres psql -U finops_user -d finops -c "TRUNCATE marts.mart_recurring CASCADE;"
	docker-compose exec postgres psql -U finops_user -d finops -c "TRUNCATE marts.mart_anomalies CASCADE;"
	docker-compose exec postgres psql -U finops_user -d finops -c "TRUNCATE marts.mart_forecasts CASCADE;"
	rm -rf warehouse/parquet/* warehouse/duckdb/*
	@echo "✅ Data cleaned!"

# Run all tests
test: dbt-test ge-validate
	@echo "✅ All tests completed!"

# Run dbt tests
dbt-test:
	@echo "🧪 Running dbt tests..."
	docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt test"
	@echo "✅ dbt tests completed!"

# Run Great Expectations validation
ge-validate:
	@echo "🔍 Running Great Expectations validation..."
	docker-compose exec airflow-scheduler python -c "
import sys
sys.path.append('/opt/airflow/scripts')
from main import run_ge_suites
run_ge_suites()
"
	@echo "✅ GE validation completed!"

# Generate synthetic financial data
generate-data:
	@echo "📊 Generating synthetic financial data..."
	mkdir -p data
	docker run --rm -v $(PWD)/data:/app/data -v $(PWD)/scripts:/app/scripts \
		python:3.11-slim bash -c "
		cd /app && 
		pip install pandas numpy && 
		python scripts/data_generator.py
		"
	@echo "✅ Synthetic data generated in data/ directory"

# Load data into PostgreSQL
load-data:
	@echo "📥 Loading data into PostgreSQL..."
	docker-compose exec airflow-scheduler python -c "
import sys
sys.path.append('/opt/airflow/scripts')
from main import extract_load_raw
extract_load_raw()
"
	@echo "✅ Data loaded!"

# Export marts to Parquet
export-parquet:
	@echo "📤 Exporting marts to Parquet..."
	docker-compose exec airflow-scheduler python -c "
import sys
sys.path.append('/opt/airflow/scripts')
from main import export_parquet
export_parquet()
"
	@echo "✅ Parquet export complete!"

# Load data into DuckDB
load-duckdb:
	@echo "🦆 Loading data into DuckDB..."
	docker-compose exec airflow-scheduler python -c "
import sys
sys.path.append('/opt/airflow/scripts')
from main import load_columnar
load_columnar()
"
	@echo "✅ DuckDB loaded!"

# View container logs
logs:
	@echo "📋 Container logs:"
	docker-compose logs -f

# Check container status
status:
	@echo "📊 Container status:"
	docker-compose ps
	@echo ""
	@echo "Service URLs:"
	@echo "  Airflow: http://localhost:8080"
	@echo "  FastAPI: http://localhost:8000"
	@echo "  PostgreSQL: localhost:5432"

# View performance metrics
metrics:
	@echo "📈 Performance metrics:"
	@echo "PostgreSQL vs DuckDB comparison:"
	curl -s http://localhost:8000/analytics/performance | jq .
	@echo ""
	@echo "Platform summary:"
	curl -s http://localhost:8000/summary | jq .

# Development helpers
dev-setup:
	@echo "🛠️  Setting up development environment..."
	pip install -r requirements.txt
	@echo "✅ Development environment ready!"

# Database operations
db-reset:
	@echo "🗄️  Resetting database..."
	docker-compose down postgres
	docker volume rm personal_finance_data_platform_postgres_data
	docker-compose up -d postgres
	@echo "⏳ Waiting for database to be ready..."
	@sleep 15
	@echo "✅ Database reset complete!"

# Backup data
backup:
	@echo "💾 Creating backup..."
	mkdir -p backups/$(shell date +%Y%m%d_%H%M%S)
	docker-compose exec postgres pg_dump -U finops_user finops > backups/$(shell date +%Y%m%d_%H%M%S)/finops_backup.sql
	cp -r warehouse backups/$(shell date +%Y%m%d_%H%M%S)/
	@echo "✅ Backup created in backups/ directory"

# Restore from backup
restore:
	@echo "📥 Restoring from backup..."
	@read -p "Enter backup directory name: " backup_dir; \
	docker-compose exec -T postgres psql -U finops_user -d finops < backups/$$backup_dir/finops_backup.sql; \
	cp -r backups/$$backup_dir/warehouse/* warehouse/
	@echo "✅ Restore complete!" 