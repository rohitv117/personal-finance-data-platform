"""
Financial Data Operations DAG
Orchestrates the complete ETL pipeline for personal finance data
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'finops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'fin_dataops',
    default_args=default_args,
    description='Personal Finance Data Operations Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['finance', 'etl', 'analytics'],
)

def extract_load_raw(**context):
    """Extract and load raw transaction data"""
    import os
    import sys
    sys.path.append('/opt/airflow/scripts')
    
    from extractors.csv_extractor import CSVExtractor
    from data_generator import FinancialDataGenerator
    import pandas as pd
    from sqlalchemy import create_engine
    
    # Database connection
    db_url = "postgresql://finops_user:finops_password@postgres:5432/finops"
    engine = create_engine(db_url)
    
    # Check if we need to generate demo data
    data_dir = "/opt/airflow/data"
    if not os.path.exists(data_dir) or not os.listdir(data_dir):
        logger.info("No data files found, generating synthetic data...")
        generator = FinancialDataGenerator()
        generator.generate_multiple_files(data_dir, num_files=4, transactions_per_file=15000)
    
    # Process each CSV file
    total_loaded = 0
    for filename in os.listdir(data_dir):
        if filename.endswith('.csv'):
            filepath = os.path.join(data_dir, filename)
            logger.info(f"Processing {filename}")
            
            # Determine institution from filename
            if 'chase' in filename.lower():
                extractor = CSVExtractor.create_chase_extractor()
            elif 'amex' in filename.lower() or 'american_express' in filename.lower():
                extractor = CSVExtractor.create_amex_extractor()
            elif 'bank_of_america' in filename.lower():
                extractor = CSVExtractor.create_bank_of_america_extractor()
            elif 'wells_fargo' in filename.lower():
                extractor = CSVExtractor.create_wells_fargo_extractor()
            else:
                # Generic extractor
                extractor = CSVExtractor('Unknown', {
                    'Date': 'posted_at',
                    'Description': 'merchant_raw',
                    'Amount': 'amount',
                    'Category': 'category_raw'
                })
            
            try:
                # Extract and transform
                df = extractor.extract(filepath)
                df = extractor.transform(df)
                
                # Load to PostgreSQL
                df.to_sql('transactions', engine, schema='raw', 
                          if_exists='append', index=False, method='multi')
                
                total_loaded += len(df)
                logger.info(f"Loaded {len(df)} transactions from {filename}")
                
            except Exception as e:
                logger.error(f"Error processing {filename}: {e}")
                continue
    
    logger.info(f"Total transactions loaded: {total_loaded}")
    return total_loaded

def run_dbt_tests(**context):
    """Run dbt tests on staging models"""
    import subprocess
    import os
    
    os.chdir('/opt/airflow/dbt')
    
    # Run tests on staging models
    result = subprocess.run([
        'dbt', 'test', '--select', 'staging'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"dbt tests failed: {result.stderr}")
        raise Exception("dbt tests failed")
    
    logger.info("dbt tests passed successfully")
    return "Tests passed"

def run_dbt_build(**context):
    """Run full dbt build"""
    import subprocess
    import os
    
    os.chdir('/opt/airflow/dbt')
    
    # Run full build
    result = subprocess.run([
        'dbt', 'build'
    ], capture_output=True, text=True)
    
    if result.returncode != 0:
        logger.error(f"dbt build failed: {result.stderr}")
        raise Exception("dbt build failed")
    
    logger.info("dbt build completed successfully")
    return "Build completed"

def export_parquet(**context):
    """Export marts to Parquet format"""
    import pandas as pd
    from sqlalchemy import create_engine
    import os
    
    # Database connection
    db_url = "postgresql://finops_user:finops_password@postgres:5432/finops"
    engine = create_engine(db_url)
    
    # Create warehouse directory
    warehouse_dir = "/opt/airflow/warehouse/parquet"
    os.makedirs(warehouse_dir, exist_ok=True)
    
    # Export each mart to Parquet
    marts = [
        'mart_cashflow_daily',
        'mart_cashflow_monthly', 
        'mart_budget_vs_actual',
        'mart_recurring',
        'mart_anomalies',
        'mart_forecasts'
    ]
    
    total_rows = 0
    for mart in marts:
        try:
            # Read from PostgreSQL
            df = pd.read_sql(f"SELECT * FROM marts.{mart}", engine)
            
            if len(df) > 0:
                # Create partitioned directory structure
                if 'date' in df.columns:
                    df['year'] = pd.to_datetime(df['date']).dt.year
                    df['month'] = pd.to_datetime(df['date']).dt.month
                    df.to_parquet(
                        f"{warehouse_dir}/{mart}",
                        partition_cols=['year', 'month'],
                        index=False
                    )
                elif 'month' in df.columns:
                    df['year'] = df['month'].str[:4].astype(int)
                    df['month_num'] = df['month'].str[5:7].astype(int)
                    df.to_parquet(
                        f"{warehouse_dir}/{mart}",
                        partition_cols=['year', 'month_num'],
                        index=False
                    )
                else:
                    df.to_parquet(f"{warehouse_dir}/{mart}.parquet", index=False)
                
                total_rows += len(df)
                logger.info(f"Exported {mart}: {len(df)} rows")
            
        except Exception as e:
            logger.error(f"Error exporting {mart}: {e}")
            continue
    
    logger.info(f"Total rows exported to Parquet: {total_rows}")
    return total_rows

def load_columnar(**context):
    """Load data into DuckDB for fast analytics"""
    import duckdb
    import os
    
    # Create DuckDB connection
    warehouse_dir = "/opt/airflow/warehouse"
    duckdb_path = f"{warehouse_dir}/duckdb/finops.duckdb"
    os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)
    
    con = duckdb.connect(duckdb_path)
    
    # Create views for each mart
    marts = [
        'mart_cashflow_daily',
        'mart_cashflow_monthly',
        'mart_budget_vs_actual', 
        'mart_recurring',
        'mart_anomalies',
        'mart_forecasts'
    ]
    
    for mart in marts:
        parquet_path = f"{warehouse_dir}/parquet/{mart}"
        
        # Check if partitioned or single file
        if os.path.isdir(parquet_path):
            # Partitioned data
            con.execute(f"""
                CREATE OR REPLACE VIEW {mart} AS 
                SELECT * FROM read_parquet('{parquet_path}/**/*.parquet')
            """)
        else:
            # Single file
            if os.path.exists(f"{parquet_path}.parquet"):
                con.execute(f"""
                    CREATE OR REPLACE VIEW {mart} AS 
                    SELECT * FROM read_parquet('{parquet_path}.parquet')
                """)
    
    # Create summary view
    con.execute("""
        CREATE OR REPLACE VIEW summary_stats AS
        SELECT 
            'Daily Cashflow' as metric,
            COUNT(*) as row_count,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM mart_cashflow_daily
        UNION ALL
        SELECT 
            'Monthly Cashflow' as metric,
            COUNT(*) as row_count,
            MIN(month) as min_date,
            MAX(month) as max_date
        FROM mart_cashflow_monthly
        UNION ALL
        SELECT 
            'Budget Variance' as metric,
            COUNT(*) as row_count,
            MIN(month) as min_date,
            MAX(month) as max_date
        FROM mart_budget_vs_actual
    """)
    
    con.close()
    logger.info("DuckDB views created successfully")
    return "DuckDB loaded"

def run_ge_suites(**context):
    """Run Great Expectations data quality suites"""
    import subprocess
    import os
    
    # Run GE validation
    ge_dir = "/opt/airflow/warehouse/great_expectations"
    os.makedirs(ge_dir, exist_ok=True)
    
    # Simple data quality checks using Python
    from sqlalchemy import create_engine
    import pandas as pd
    
    db_url = "postgresql://finops_user:finops_password@postgres:5432/finops"
    engine = create_engine(db_url)
    
    # Run basic DQ checks
    checks_passed = 0
    total_checks = 0
    
    # Check 1: No nulls in critical fields
    try:
        result = engine.execute("""
            SELECT COUNT(*) as null_count 
            FROM raw.transactions 
            WHERE txn_id IS NULL OR posted_at IS NULL OR amount IS NULL
        """).fetchone()
        
        if result[0] == 0:
            checks_passed += 1
            logger.info("✓ Critical fields have no nulls")
        else:
            logger.error(f"✗ Found {result[0]} nulls in critical fields")
        
        total_checks += 1
    except Exception as e:
        logger.error(f"Error in null check: {e}")
    
    # Check 2: No duplicate transaction IDs
    try:
        result = engine.execute("""
            SELECT COUNT(*) as duplicate_count
            FROM (
                SELECT txn_id, COUNT(*) 
                FROM raw.transactions 
                GROUP BY txn_id 
                HAVING COUNT(*) > 1
            ) duplicates
        """).fetchone()
        
        if result[0] == 0:
            checks_passed += 1
            logger.info("✓ No duplicate transaction IDs")
        else:
            logger.error(f"✗ Found {result[0]} duplicate transaction IDs")
        
        total_checks += 1
    except Exception as e:
        logger.error(f"Error in duplicate check: {e}")
    
    # Check 3: Amount validation
    try:
        result = engine.execute("""
            SELECT COUNT(*) as invalid_amounts
            FROM raw.transactions 
            WHERE amount = 0
        "").fetchone()
        
        if result[0] == 0:
            checks_passed += 1
            logger.info("✓ No zero-amount transactions")
        else:
            logger.error(f"✗ Found {result[0]} zero-amount transactions")
        
        total_checks += 1
    except Exception as e:
        logger.error(f"Error in amount check: {e}")
    
    logger.info(f"Data quality checks: {checks_passed}/{total_checks} passed")
    
    if checks_passed < total_checks:
        raise Exception("Data quality checks failed")
    
    return f"{checks_passed}/{total_checks} checks passed"

def run_anomaly_detect(**context):
    """Run anomaly detection and populate mart_anomalies"""
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    
    db_url = "postgresql://finops_user:finops_password@postgres:5432/finops"
    engine = create_engine(db_url)
    
    # Read staging transactions
    df = pd.read_sql("""
        SELECT * FROM staging.transactions 
        WHERE amount < 0  -- Only expenses
        AND posted_at >= CURRENT_DATE - INTERVAL '30 days'
    """, engine)
    
    if len(df) == 0:
        logger.info("No recent transactions for anomaly detection")
        return 0
    
    anomalies = []
    
    # Statistical anomaly detection by category
    for category in df['category_norm'].unique():
        category_data = df[df['category_norm'] == category]
        
        if len(category_data) < 3:
            continue
            
        amounts = category_data['abs_amount'].values
        mean = np.mean(amounts)
        std = np.std(amounts)
        
        if std > 0:
            z_scores = np.abs((amounts - mean) / std)
            anomaly_indices = np.where(z_scores > 2)[0]
            
            for idx in anomaly_indices:
                row = category_data.iloc[idx]
                anomalies.append({
                    'txn_id': row['txn_id'],
                    'anomaly_type': 'Statistical Outlier',
                    'severity': 'High' if z_scores[idx] > 3 else 'Medium',
                    'driver': 'Z-score outlier',
                    'remediation_hint': 'Verify transaction amount and necessity'
                })
    
    # Novel merchant detection
    recent_merchants = df['merchant_norm'].unique()
    all_merchants = pd.read_sql("""
        SELECT DISTINCT merchant_norm 
        FROM staging.transactions 
        WHERE posted_at < CURRENT_DATE - INTERVAL '30 days'
    """, engine)['merchant_norm'].unique()
    
    novel_merchants = set(recent_merchants) - set(all_merchants)
    
    for merchant in novel_merchants:
        merchant_txns = df[df['merchant_norm'] == merchant]
        for _, row in merchant_txns.iterrows():
            anomalies.append({
                'txn_id': row['txn_id'],
                'anomaly_type': 'Novel Merchant',
                'severity': 'Medium',
                'driver': 'Merchant not seen in last 90 days',
                'remediation_hint': 'Review transaction and categorize appropriately'
            })
    
    # Insert anomalies into mart
    if anomalies:
        anomalies_df = pd.DataFrame(anomalies)
        anomalies_df['flagged_at'] = pd.Timestamp.now()
        
        # Get next sequence value for ID
        result = engine.execute("SELECT nextval('marts.mart_anomalies_id_seq')").fetchone()
        start_id = result[0] - len(anomalies) + 1
        
        anomalies_df['id'] = range(start_id, start_id + len(anomalies))
        
        # Insert into mart_anomalies
        anomalies_df.to_sql('mart_anomalies', engine, schema='marts', 
                           if_exists='append', index=False, method='multi')
        
        logger.info(f"Inserted {len(anomalies)} anomalies")
        return len(anomalies)
    
    logger.info("No anomalies detected")
    return 0

def run_forecasts(**context):
    """Run forecasting and populate mart_forecasts"""
    from sqlalchemy import create_engine
    import pandas as pd
    from datetime import datetime, timedelta
    
    db_url = "postgresql://finops_user:finops_password@postgres:5432/finops"
    engine = create_engine(db_url)
    
    # Get monthly category totals for the last 6 months
    df = pd.read_sql("""
        SELECT 
            month,
            category_norm,
            SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as expenses
        FROM staging.transactions 
        WHERE amount < 0
        AND month >= TO_CHAR(CURRENT_DATE - INTERVAL '6 months', 'YYYY-MM')
        GROUP BY month, category_norm
        HAVING COUNT(*) >= 3
    """, engine)
    
    if len(df) == 0:
        logger.info("No data available for forecasting")
        return 0
    
    forecasts = []
    
    # Generate forecasts for next 3 months
    for i in range(1, 4):
        forecast_date = datetime.now() + timedelta(days=30*i)
        forecast_month = forecast_date.strftime('%Y-%m')
        
        for category in df['category_norm'].unique():
            category_data = df[df['category_norm'] == category]
            
            if len(category_data) < 2:
                continue
                
            # Simple linear trend forecasting
            amounts = category_data['expenses'].values
            if len(amounts) > 1:
                trend = (amounts[-1] - amounts[0]) / (len(amounts) - 1)
                forecast_amount = amounts[-1] + (trend * i)
                
                # Add some uncertainty
                volatility = np.std(amounts) if len(amounts) > 1 else amounts[0] * 0.1
                
                forecasts.append({
                    'forecast_date': forecast_date.date(),
                    'category_norm': category,
                    'forecast_amount': max(0, forecast_amount),
                    'lower_bound': max(0, forecast_amount - volatility),
                    'upper_bound': forecast_amount + volatility,
                    'confidence_level': 0.8
                })
    
    # Insert forecasts into mart
    if forecasts:
        forecasts_df = pd.DataFrame(forecasts)
        
        # Get category IDs
        categories = pd.read_sql("SELECT category_id, name FROM ref.categories", engine)
        forecasts_df = forecasts_df.merge(categories, left_on='category_norm', right_on='name')
        
        # Prepare for insertion
        forecasts_df['created_at'] = pd.Timestamp.now()
        forecasts_df = forecasts_df[['forecast_date', 'category_id', 'forecast_amount', 
                                   'lower_bound', 'upper_bound', 'confidence_level', 'created_at']]
        
        # Insert into mart_forecasts
        forecasts_df.to_sql('mart_forecasts', engine, schema='marts', 
                           if_exists='append', index=False, method='multi')
        
        logger.info(f"Inserted {len(forecasts)} forecasts")
        return len(forecasts)
    
    logger.info("No forecasts generated")
    return 0

def notify_summary(**context):
    """Send summary notification"""
    from sqlalchemy import create_engine
    import pandas as pd
    
    db_url = "postgresql://finops_user:finops_password@postgres:5432/finops"
    engine = create_engine(db_url)
    
    # Get summary statistics
    summary = {}
    
    # Transaction counts
    result = engine.execute("SELECT COUNT(*) FROM raw.transactions").fetchone()
    summary['total_transactions'] = result[0]
    
    result = engine.execute("SELECT COUNT(*) FROM marts.mart_anomalies").fetchone()
    summary['total_anomalies'] = result[0]
    
    result = engine.execute("SELECT COUNT(*) FROM marts.mart_forecasts").fetchone()
    summary['total_forecasts'] = result[0]
    
    # Recent activity
    result = engine.execute("""
        SELECT COUNT(*) FROM raw.transactions 
        WHERE created_at >= CURRENT_DATE
    """).fetchone()
    summary['today_transactions'] = result[0]
    
    # Log summary
    logger.info("=== FINANCIAL DATA OPS SUMMARY ===")
    logger.info(f"Total transactions: {summary['total_transactions']}")
    logger.info(f"Total anomalies: {summary['total_anomalies']}")
    logger.info(f"Total forecasts: {summary['total_forecasts']}")
    logger.info(f"Today's transactions: {summary['today_transactions']}")
    logger.info("==================================")
    
    return summary

# Task definitions
extract_load_raw_task = PythonOperator(
    task_id='extract_load_raw',
    python_callable=extract_load_raw,
    dag=dag,
)

dbt_tests_task = PythonOperator(
    task_id='dbt_run_tests',
    python_callable=run_dbt_tests,
    dag=dag,
)

dbt_build_task = PythonOperator(
    task_id='dbt_build',
    python_callable=run_dbt_build,
    dag=dag,
)

export_parquet_task = PythonOperator(
    task_id='export_parquet',
    python_callable=export_parquet,
    dag=dag,
)

load_columnar_task = PythonOperator(
    task_id='load_columnar',
    python_callable=load_columnar,
    dag=dag,
)

run_ge_suites_task = PythonOperator(
    task_id='run_ge_suites',
    python_callable=run_ge_suites,
    dag=dag,
)

run_anomaly_detect_task = PythonOperator(
    task_id='run_anomaly_detect',
    python_callable=run_anomaly_detect,
    dag=dag,
)

run_forecasts_task = PythonOperator(
    task_id='run_forecasts',
    python_callable=run_forecasts,
    dag=dag,
)

notify_task = PythonOperator(
    task_id='notify',
    python_callable=notify_summary,
    dag=dag,
)

# Task dependencies
extract_load_raw_task >> dbt_tests_task >> dbt_build_task >> export_parquet_task >> load_columnar_task

[dbt_build_task, export_parquet_task] >> run_ge_suites_task
[dbt_build_task, export_parquet_task] >> run_anomaly_detect_task
[dbt_build_task, export_parquet_task] >> run_forecasts_task

[run_ge_suites_task, run_anomaly_detect_task, run_forecasts_task] >> notify_task 