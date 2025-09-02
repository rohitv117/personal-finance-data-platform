"""
FastAPI application for Personal Finance Data Platform
Provides APIs for accessing financial data, analytics, and insights
"""
import os
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import pandas as pd
import duckdb
from sqlalchemy import create_engine, text
import hashlib

# Configure logging with PII redaction
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PII redaction function
def redact_merchant(merchant: str) -> str:
    """Redact merchant names in logs for privacy"""
    if not merchant:
        return merchant
    return hashlib.sha256(f"{merchant}finops_salt".encode()).hexdigest()[:16]

# Pydantic models
class BalanceResponse(BaseModel):
    account_id: str
    institution: str
    current_balance: float
    last_updated: datetime
    currency: str

class CashflowResponse(BaseModel):
    period: str
    income: float
    expenses: float
    savings_rate: float
    balance_delta: float
    transaction_count: int

class BudgetVarianceResponse(BaseModel):
    month: str
    category_name: str
    budget: float
    actual: float
    variance: float
    variance_pct: float

class AnomalyResponse(BaseModel):
    id: int
    txn_id: str
    anomaly_type: str
    severity: str
    driver: str
    remediation_hint: str
    flagged_at: datetime

class ForecastResponse(BaseModel):
    forecast_date: date
    category_name: str
    forecast_amount: float
    lower_bound: float
    upper_bound: float
    confidence_level: float

class SummaryResponse(BaseModel):
    total_transactions: int
    total_anomalies: int
    total_forecasts: int
    last_updated: datetime

# FastAPI app
app = FastAPI(
    title="Personal Finance Data Platform API",
    description="API for accessing financial data, analytics, and insights",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connections
def get_postgres_engine():
    """Get PostgreSQL connection"""
    database_url = os.getenv("DATABASE_URL", "postgresql://finops_user:finops_password@postgres:5432/finops")
    return create_engine(database_url)

def get_duckdb_connection():
    """Get DuckDB connection"""
    duckdb_path = os.getenv("DUCKDB_PATH", "/app/warehouse/duckdb/finops.duckdb")
    return duckdb.connect(duckdb_path)

# API endpoints
@app.get("/", tags=["Root"])
async def root():
    """Root endpoint"""
    return {
        "message": "Personal Finance Data Platform API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    try:
        # Check PostgreSQL
        pg_engine = get_postgres_engine()
        with pg_engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            pg_healthy = True
    except Exception as e:
        logger.error(f"PostgreSQL health check failed: {e}")
        pg_healthy = False
    
    try:
        # Check DuckDB
        duck_con = get_duckdb_connection()
        result = duck_con.execute("SELECT 1")
        duck_healthy = True
        duck_con.close()
    except Exception as e:
        logger.error(f"DuckDB health check failed: {e}")
        duck_healthy = False
    
    return {
        "status": "healthy" if pg_healthy and duck_healthy else "unhealthy",
        "postgresql": "healthy" if pg_healthy else "unhealthy",
        "duckdb": "healthy" if duck_healthy else "unhealthy",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/balances", response_model=List[BalanceResponse], tags=["Balances"])
async def get_balances():
    """Get current account balances"""
    try:
        engine = get_postgres_engine()
        
        # Calculate current balances from transactions
        query = """
        SELECT 
            account_id,
            institution,
            SUM(amount) as current_balance,
            MAX(created_at) as last_updated,
            currency
        FROM raw.transactions 
        GROUP BY account_id, institution, currency
        ORDER BY institution, account_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            balances = []
            
            for row in result:
                balances.append(BalanceResponse(
                    account_id=row.account_id,
                    institution=row.institution,
                    current_balance=float(row.current_balance),
                    last_updated=row.last_updated,
                    currency=row.currency
                ))
        
        logger.info(f"Retrieved balances for {len(balances)} accounts")
        return balances
        
    except Exception as e:
        logger.error(f"Error retrieving balances: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve balances")

@app.get("/cashflow", response_model=List[CashflowResponse], tags=["Cashflow"])
async def get_cashflow(
    grain: str = Query("month", description="Time grain: daily, weekly, or monthly"),
    months: int = Query(12, description="Number of months to retrieve")
):
    """Get cashflow data by time period"""
    try:
        engine = get_postgres_engine()
        
        if grain == "daily":
            query = """
            SELECT 
                TO_CHAR(date, 'YYYY-MM-DD') as period,
                income,
                expenses,
                savings_rate,
                balance_delta,
                transaction_count
            FROM marts.mart_cashflow_daily
            WHERE date >= CURRENT_DATE - INTERVAL ':months months'
            ORDER BY date DESC
            LIMIT :limit
            """
            limit = months * 30
        elif grain == "weekly":
            query = """
            SELECT 
                week as period,
                SUM(income) as income,
                SUM(expenses) as expenses,
                AVG(savings_rate) as savings_rate,
                SUM(balance_delta) as balance_delta,
                SUM(transaction_count) as transaction_count
            FROM marts.mart_cashflow_daily
            WHERE date >= CURRENT_DATE - INTERVAL ':months months'
            GROUP BY week
            ORDER BY week DESC
            LIMIT :limit
            """
            limit = months * 4
        else:  # monthly
            query = """
            SELECT 
                month as period,
                income,
                expenses,
                savings_rate,
                balance_delta,
                transaction_count
            FROM marts.mart_cashflow_monthly
            WHERE month >= TO_CHAR(CURRENT_DATE - INTERVAL ':months months', 'YYYY-MM')
            ORDER BY month DESC
            LIMIT :limit
            """
            limit = months
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"months": months, "limit": limit})
            cashflow = []
            
            for row in result:
                cashflow.append(CashflowResponse(
                    period=row.period,
                    income=float(row.income or 0),
                    expenses=float(row.expenses or 0),
                    savings_rate=float(row.savings_rate or 0),
                    balance_delta=float(row.balance_delta or 0),
                    transaction_count=int(row.transaction_count or 0)
                ))
        
        # Redact sensitive info in logs
        logger.info(f"Retrieved {grain} cashflow data for {len(cashflow)} periods")
        return cashflow
        
    except Exception as e:
        logger.error(f"Error retrieving cashflow data: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve cashflow data")

@app.get("/budget/variance", response_model=List[BudgetVarianceResponse], tags=["Budget"])
async def get_budget_variance(
    month: str = Query(..., description="Month in YYYY-MM format")
):
    """Get budget vs actual variance for a specific month"""
    try:
        engine = get_postgres_engine()
        
        query = """
        SELECT 
            month,
            category_name,
            budget,
            actual,
            variance,
            variance_pct
        FROM marts.mart_budget_vs_actual
        WHERE month = :month
        ORDER BY ABS(variance_pct) DESC
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"month": month})
            variances = []
            
            for row in result:
                variances.append(BudgetVarianceResponse(
                    month=row.month,
                    category_name=row.category_name,
                    budget=float(row.budget or 0),
                    actual=float(row.actual or 0),
                    variance=float(row.variance or 0),
                    variance_pct=float(row.variance_pct or 0)
                ))
        
        logger.info(f"Retrieved budget variance for {month}: {len(variances)} categories")
        return variances
        
    except Exception as e:
        logger.error(f"Error retrieving budget variance: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve budget variance")

@app.get("/anomalies/recent", response_model=List[AnomalyResponse], tags=["Anomalies"])
async def get_recent_anomalies(
    limit: int = Query(50, description="Number of anomalies to retrieve")
):
    """Get recent anomalies"""
    try:
        engine = get_postgres_engine()
        
        query = """
        SELECT 
            id,
            txn_id,
            anomaly_type,
            severity,
            driver,
            remediation_hint,
            flagged_at
        FROM marts.mart_anomalies
        ORDER BY flagged_at DESC
        LIMIT :limit
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"limit": limit})
            anomalies = []
            
            for row in result:
                anomalies.append(AnomalyResponse(
                    id=row.id,
                    txn_id=row.txn_id,
                    anomaly_type=row.anomaly_type,
                    severity=row.severity,
                    driver=row.driver,
                    remediation_hint=row.remediation_hint,
                    flagged_at=row.flagged_at
                ))
        
        logger.info(f"Retrieved {len(anomalies)} recent anomalies")
        return anomalies
        
    except Exception as e:
        logger.error(f"Error retrieving anomalies: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve anomalies")

@app.get("/forecast", response_model=List[ForecastResponse], tags=["Forecast"])
async def get_forecasts(
    months: int = Query(3, description="Number of months to forecast")
):
    """Get expense forecasts"""
    try:
        engine = get_postgres_engine()
        
        query = """
        SELECT 
            forecast_date,
            category_name,
            forecast_amount,
            lower_bound,
            upper_bound,
            confidence_level
        FROM marts.mart_forecasts
        WHERE forecast_date <= CURRENT_DATE + INTERVAL ':months months'
        ORDER BY forecast_date, category_name
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"months": months})
            forecasts = []
            
            for row in result:
                forecasts.append(ForecastResponse(
                    forecast_date=row.forecast_date,
                    category_name=row.category_name,
                    forecast_amount=float(row.forecast_amount or 0),
                    lower_bound=float(row.lower_bound or 0),
                    upper_bound=float(row.upper_bound or 0),
                    confidence_level=float(row.confidence_level or 0)
                ))
        
        logger.info(f"Retrieved {len(forecasts)} forecasts for next {months} months")
        return forecasts
        
    except Exception as e:
        logger.error(f"Error retrieving forecasts: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve forecasts")

@app.get("/summary", response_model=SummaryResponse, tags=["Summary"])
async def get_summary():
    """Get platform summary statistics"""
    try:
        engine = get_postgres_engine()
        
        # Get transaction count
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM raw.transactions"))
            total_transactions = result.fetchone()[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM marts.mart_anomalies"))
            total_anomalies = result.fetchone()[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM marts.mart_forecasts"))
            total_forecasts = result.fetchone()[0]
        
        return SummaryResponse(
            total_transactions=total_transactions,
            total_anomalies=total_anomalies,
            total_forecasts=total_forecasts,
            last_updated=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Error retrieving summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve summary")

@app.get("/analytics/performance", tags=["Analytics"])
async def get_performance_metrics():
    """Get performance metrics comparing Postgres vs DuckDB"""
    try:
        # Test Postgres performance
        pg_engine = get_postgres_engine()
        pg_start = datetime.now()
        
        with pg_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT 
                    month,
                    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
                    SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as expenses
                FROM staging.transactions
                WHERE month >= '2024-01'
                GROUP BY month
                ORDER BY month
            """))
            pg_data = result.fetchall()
        
        pg_duration = (datetime.now() - pg_start).total_seconds() * 1000
        
        # Test DuckDB performance
        duck_con = get_duckdb_connection()
        duck_start = datetime.now()
        
        result = duck_con.execute("""
            SELECT 
                month,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
                SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as expenses
            FROM mart_cashflow_monthly
            WHERE month >= '2024-01'
            GROUP BY month
            ORDER BY month
        """)
        duck_data = result.fetchall()
        
        duck_duration = (datetime.now() - duck_start).total_seconds() * 1000
        duck_con.close()
        
        # Calculate speedup
        speedup = pg_duration / duck_duration if duck_duration > 0 else 0
        
        return {
            "postgresql": {
                "duration_ms": round(pg_duration, 2),
                "rows_returned": len(pg_data)
            },
            "duckdb": {
                "duration_ms": round(duck_duration, 2),
                "rows_returned": len(duck_data)
            },
            "performance": {
                "speedup_factor": round(speedup, 2),
                "faster_engine": "DuckDB" if speedup > 1 else "PostgreSQL"
            }
        }
        
    except Exception as e:
        logger.error(f"Error retrieving performance metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve performance metrics")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 