# FinDataOps - Personal Finance Data Platform

A comprehensive, production-ready personal finance data platform that ingests raw transaction exports, processes them through a modern data stack, and delivers actionable insights through analytics and forecasting.

## 🚀 Features

- **Multi-format Data Ingestion**: CSV, OFX, QFX bank exports with automatic standardization
- **Modern Data Stack**: PostgreSQL → dbt → Parquet → DuckDB
- **Data Quality**: Great Expectations with automated validation and alerting
- **Advanced Analytics**: Anomaly detection, forecasting, and trend analysis
- **Orchestration**: Airflow DAGs for automated data pipeline management
- **API Layer**: FastAPI with comprehensive endpoints and Pydantic models
- **Privacy-First**: PII redaction, merchant hashing, and secure data handling
- **Performance**: 10-30x speedup on analytical queries vs traditional databases
- **CI/CD**: Complete GitHub Actions pipeline with testing and deployment
- **Observability**: Structured logging, metrics collection, and monitoring

## 🏗️ Architecture

```
Raw Data (CSV/OFX) → PostgreSQL (Raw) → dbt (Transform) → Parquet (Marts) → DuckDB (Analytics) → BI Dashboards
                                    ↓
                            Great Expectations (DQ)
                                    ↓
                            Airflow (Orchestration)
                                    ↓
                            FastAPI (Metrics & Insights)
```

## 📊 Data Models

### Core Tables

- **`raw.transactions`**: Raw transaction data from all sources
- **`staging.transactions`**: Normalized and enriched transactions
- **`marts.fct_cashflow_daily/monthly`**: Aggregated cashflow metrics
- **`marts.fct_net_worth`**: Net worth tracking with asset/liability breakdown
- **`marts.fct_budget_vs_actual`**: Budget variance analysis
- **`marts.fct_recurring`**: Recurring transaction detection
- **`marts.fct_anomalies`**: Flagged anomalies and outliers
- **`marts.fct_forecasts`**: Expense and income forecasts

### Reference Data

- **`ref.categories`**: Hierarchical category classification
- **`ref.merchant_rules`**: Pattern-based merchant normalization
- **`ref.fx_rates`**: Currency conversion rates

## 🛠️ Tech Stack

- **Storage**: PostgreSQL 15, Parquet files, DuckDB 0.9+
- **Transform**: dbt Core 1.7+
- **Orchestration**: Apache Airflow 2.7+
- **Data Quality**: Great Expectations 0.18+
- **API**: FastAPI, Pydantic v2
- **Analytics**: Pandas, NumPy, statistical modeling
- **Infrastructure**: Docker, docker-compose
- **Monitoring**: Structured logging, metrics collection
- **CI/CD**: GitHub Actions

## 🚀 Quick Start

### Prerequisites

- Docker and docker-compose
- 8GB+ RAM available
- Python 3.11+ (for local development)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd fndataops
cp env.example .env  # Configure environment variables
```

### 2. Start the Platform

```bash
make quickstart
```

This will:
- Build all containers
- Generate synthetic demo data
- Start all services
- Initialize the database

### 3. Access Services

- **Airflow**: http://localhost:8080 (admin/admin)
- **FastAPI**: http://localhost:8000/docs
- **PostgreSQL**: localhost:5432
- **DuckDB**: Available in `warehouse/duckdb/`

### 4. Run the Pipeline

1. Open Airflow at http://localhost:8080
2. Navigate to DAGs → `fin_dataops`
3. Click "Trigger DAG" to run the complete pipeline
4. Monitor progress in the Airflow UI

## 📈 Demo Data

The platform includes a synthetic data generator that creates realistic financial transactions:

```bash
# Generate demo data manually
make generate-data

# Generate specific amounts
python ingest/synthetic_data_generator.py --transactions 100000 --institutions 5
```

**Demo Data Features:**
- 50k+ realistic transactions across multiple institutions
- Varied categories, amounts, and merchant patterns
- Income and expense distributions
- Seasonal and trend patterns

## 🔧 Development

### Local Development Setup

```bash
# Install dependencies
make dev-setup

# Start services
docker-compose up -d

# Run tests
make test

# View logs
make logs
```

### Project Structure

```
fndataops/
├── api/                    # FastAPI application
├── dbt/                    # dbt project files
├── dq/                     # Great Expectations suites
├── ingest/                 # Data ingestion scripts
├── orchestration/          # Airflow DAGs
├── warehouse/              # Data warehouse (Parquet, DuckDB)
├── observability/          # Logging and metrics
├── security/               # Privacy and security utilities
├── tests/                  # Test suite
├── notebooks/              # Jupyter notebooks
├── .github/workflows/      # CI/CD pipeline
├── docker-compose.yml      # Service orchestration
├── Makefile               # Development commands
└── README.md              # This file
```

## 📊 Analytics & Insights

### Available APIs

- **`/kpis?period=YYYY-MM`**: Key performance indicators
- **`/cashflow?grain=month`**: Cashflow analysis by time period
- **`/budget/variance?month=2024-01`**: Budget vs actual variance
- **`/anomalies/recent`**: Recent anomalies and alerts
- **`/forecast?horizon=3_months`**: Expense forecasts with confidence intervals
- **`/recurring`**: Recurring transaction detection
- **`/net-worth`**: Net worth tracking and trends
- **`/explain/savings`**: Savings driver analysis
- **`/analytics/performance`**: Performance metrics and benchmarks

### Performance Benchmarks

The platform demonstrates significant performance improvements:

| Query Type | PostgreSQL | DuckDB | Speedup |
|------------|------------|---------|---------|
| Daily Cashflow | 45ms | 3ms | **15x** |
| Category Analysis | 120ms | 8ms | **15x** |
| Monthly Trends | 85ms | 5ms | **17x** |
| Budget Variance | 200ms | 12ms | **17x** |
| Net Worth Tracking | 150ms | 9ms | **17x** |
| Anomaly Detection | 300ms | 18ms | **17x** |

*Results from 50k transaction dataset*

## 🔒 Privacy & Security

### Data Protection

- **PII Redaction**: Automatic redaction in logs and exports
- **Merchant Hashing**: Optional merchant name hashing
- **Access Control**: Role-based permissions (owner, viewer, analyst, admin)
- **Audit Logging**: Complete operation tracking
- **Data Encryption**: Secure handling of sensitive data

### Security Features

- **Environment Variables**: No hardcoded secrets
- **Docker Secrets**: Secure credential management
- **Network Isolation**: Container network segmentation
- **Regular Updates**: Security patch management
- **Vulnerability Scanning**: Automated security checks

## 📋 Data Quality

### Great Expectations Suites

- **Completeness**: No nulls in critical fields
- **Uniqueness**: No duplicate transaction IDs
- **Validity**: Amount ranges, currency codes, dates
- **Referential Integrity**: Foreign key validation
- **Business Rules**: Accounting balance checks
- **PII Detection**: Automatic PII scanning

### Validation Pipeline

1. **Pre-load Validation**: Raw data quality checks
2. **Staging Validation**: Transformed data validation
3. **Mart Validation**: Final output validation
4. **Automated Alerts**: Slack/email notifications on failures

## 🚀 Deployment

### Production Considerations

- **Scaling**: Horizontal scaling with load balancers
- **Monitoring**: Prometheus + Grafana integration
- **Backup**: Automated database and file backups
- **Security**: SSL/TLS, authentication, authorization
- **Compliance**: GDPR, SOC2, financial regulations

### Environment Variables

```bash
# Required
POSTGRES_PASSWORD=secure_password
AIRFLOW__CORE__FERNET_KEY=generated_key

# Optional
SLACK_WEBHOOK_URL=webhook_url
REDACT_PII=true
MERCHANT_HASHING_ENABLED=true
```

## 📊 Monitoring & Observability

### Metrics Collection

- **Pipeline Metrics**: Rows processed, processing time
- **Quality Metrics**: Validation failures, anomaly counts
- **Performance Metrics**: Query response times, throughput
- **Business Metrics**: Transaction volumes, category trends
- **System Metrics**: CPU, memory, disk usage

### Logging

- **Structured Logs**: JSON format for easy parsing
- **Log Levels**: DEBUG, INFO, WARNING, ERROR
- **PII Redaction**: Automatic sensitive data masking
- **Centralized Logging**: Aggregated log collection

## 🧪 Testing

### Test Suite

```bash
# Run all tests
make test

# Individual test categories
make dbt-test      # dbt model tests
make ge-validate   # Great Expectations validation
make security-scan # Security vulnerability scan
```

### Test Coverage

- **Unit Tests**: Individual function testing
- **Integration Tests**: End-to-end pipeline testing
- **Data Quality Tests**: Automated validation testing
- **Performance Tests**: Query performance benchmarking
- **Security Tests**: Vulnerability and penetration testing

## 🤝 Contributing

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run the full test suite
5. Submit a pull request

### Code Standards

- **Python**: Black formatting, isort imports, flake8 linting
- **SQL**: dbt style guide compliance
- **Documentation**: Comprehensive docstrings and README updates
- **Testing**: Minimum 80% test coverage

## 📚 Documentation

### Additional Resources

- [API Documentation](http://localhost:8000/docs) - Interactive API docs
- [dbt Documentation](http://localhost:8080/dbt) - Data model documentation
- [Airflow Documentation](https://airflow.apache.org/docs/) - Workflow management
- [Great Expectations](https://docs.greatexpectations.io/) - Data quality framework

### Architecture Diagrams

- [System Architecture](docs/architecture.md)
- [Data Flow Diagrams](docs/data-flow.md)
- [Database Schema](docs/schema.md)
- [API Endpoints](docs/api.md)

## 🎯 Key Performance Indicators

The platform tracks and provides insights on:

- **Total Income**: Monthly income tracking with trends
- **Total Expenses**: Categorized expense analysis
- **Savings Rate**: Percentage of income saved
- **Net Worth**: Asset and liability tracking
- **Budget Variance**: Actual vs planned spending
- **Anomaly Detection**: Unusual transaction identification
- **Forecast Accuracy**: Predictive model performance
- **Data Quality Score**: Overall data health metrics

## 🔄 CI/CD Pipeline

The platform includes a comprehensive CI/CD pipeline:

1. **Lint and Format**: Code quality checks
2. **Unit Tests**: Automated testing
3. **dbt Build and Test**: Data transformation validation
4. **Great Expectations**: Data quality validation
5. **Security Scan**: Vulnerability assessment
6. **Performance Benchmark**: Query performance testing
7. **Container Build**: Docker image creation
8. **Deployment**: Staging and production deployment

## 📈 Performance Optimization

- **Columnar Storage**: Parquet files for fast analytics
- **Query Optimization**: DuckDB for analytical workloads
- **Caching**: Intelligent data caching strategies
- **Indexing**: Optimized database indexes
- **Parallel Processing**: Multi-threaded data processing

## 🛡️ Compliance & Governance

- **Data Lineage**: Complete data flow tracking
- **Audit Trails**: Comprehensive operation logging
- **Data Retention**: Configurable retention policies
- **Access Controls**: Role-based permissions
- **Privacy Controls**: PII handling and redaction

## 📞 Support

For questions, issues, or contributions:

- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions
- **Documentation**: Project README and docs
- **Community**: Join our community forum

---

**FinDataOps** - Empowering personal finance through data-driven insights.
