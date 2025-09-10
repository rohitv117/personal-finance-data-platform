"""
FastAPI application for Personal Finance Data Platform
Provides comprehensive APIs for financial data, analytics, and insights
"""

import os
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from fastapi import FastAPI, HTTPException, Depends, Query, Path, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy import create_engine, text
import duckdb
import pandas as pd
import hashlib
import json

from models import (
    BaseResponse, BalanceResponse, BalanceSummaryResponse,
    CashflowResponse, CashflowSummaryResponse,
    BudgetVarianceResponse, BudgetSummaryResponse,
    AnomalyResponse, AnomalySummaryResponse,
    ForecastResponse, ForecastSummaryResponse,
    RecurringResponse, RecurringSummaryResponse,
    NetWorthResponse, NetWorthSummaryResponse,
    KPIResponse, KPISummaryResponse,
    DriverAnalysisResponse, SavingsAnalysisResponse,
    PerformanceMetricsResponse, PerformanceSummaryResponse,
    ErrorResponse, HealthCheckResponse, PlatformSummaryResponse,
    DateRangeRequest, PeriodRequest, CategoryRequest, AccountRequest,
    AnomalyAcknowledgeRequest, ForecastRequest,
    SeverityLevel, ForecastType, ForecastHorizon, BudgetStatus, RecurringType
)

# Configure logging with PII redaction
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PII redaction function
def redact_merchant(merchant: str) -> str:
    """Redact merchant names in logs for privacy"""
    if not merchant:
        return merchant
    return hashlib.sha256(f"{merchant}finops_salt".encode()).hexdigest()[:16]

# FastAPI app
app = FastAPI(
    title="Personal Finance Data Platform API",
    description="Comprehensive API for financial data, analytics, and insights",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer(auto_error=False)

# Database connections
def get_postgres_engine():
    """Get PostgreSQL connection"""
    database_url = os.getenv("DATABASE_URL", "postgresql://finops_user:finops_password@postgres:5432/finops")
    return create_engine(database_url)

def get_duckdb_connection():
    """Get DuckDB connection"""
    duckdb_path = os.getenv("DUCKDB_PATH", "/app/warehouse/duckdb/finops.duckdb")
    return duckdb.connect(duckdb_path)

# Authentication (simplified for demo)
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Simple authentication - in production, implement proper OAuth2"""
    if not credentials:
        raise HTTPException(status_code=401, detail="Authentication required")
    return {"user_id": "demo_user", "role": "owner"}

# Health check endpoint
@app.get("/health", response_model=HealthCheckResponse, tags=["Health"])
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
    
    return HealthCheckResponse(
        status="healthy" if pg_healthy and duck_healthy else "unhealthy",
        postgresql="healthy" if pg_healthy else "unhealthy",
        duckdb="healthy" if duck_healthy else "unhealthy",
        timestamp=datetime.now()
    )

# KPI endpoints
@app.get("/kpis", response_model=KPISummaryResponse, tags=["KPIs"])
async def get_kpis(
    period: str = Query(..., regex=r"^\d{4}-\d{2}$", description="Period in YYYY-MM format"),
    account: Optional[str] = Query(None, description="Account ID filter"),
    category: Optional[str] = Query(None, description="Category filter"),
    user: dict = Depends(get_current_user)
):
    """Get key performance indicators for a specific period"""
    try:
        engine = get_postgres_engine()
        
        # Build query with filters
        where_clause = "WHERE month = :period"
        params = {"period": period}
        
        if account:
            where_clause += " AND account_id = :account"
            params["account"] = account
        
        if category:
            where_clause += " AND category_std = :category"
            params["category"] = category
        
        # Get monthly cashflow data
        query = f"""
        SELECT 
            income,
            expenses,
            savings_rate,
            income_mom_change,
            expenses_mom_change,
            savings_rate_mom_change
        FROM marts.fct_cashflow_monthly
        {where_clause}
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="No data found for the specified period")
            
            # Calculate KPIs
            kpis = [
                KPIResponse(
                    metric_name="Total Income",
                    value=float(row.income or 0),
                    unit="USD",
                    change_pct=float(row.income_mom_change or 0),
                    trend="up" if (row.income_mom_change or 0) > 0 else "down",
                    definition="Total income for the period"
                ),
                KPIResponse(
                    metric_name="Total Expenses",
                    value=float(row.expenses or 0),
                    unit="USD",
                    change_pct=float(row.expenses_mom_change or 0),
                    trend="up" if (row.expenses_mom_change or 0) > 0 else "down",
                    definition="Total expenses for the period"
                ),
                KPIResponse(
                    metric_name="Savings Rate",
                    value=float(row.savings_rate or 0) * 100,
                    unit="%",
                    change_pct=float(row.savings_rate_mom_change or 0) * 100,
                    trend="up" if (row.savings_rate_mom_change or 0) > 0 else "down",
                    definition="Percentage of income saved"
                ),
                KPIResponse(
                    metric_name="Net Cash Flow",
                    value=float(row.income or 0) - float(row.expenses or 0),
                    unit="USD",
                    change_pct=None,
                    trend="positive" if (row.income or 0) > (row.expenses or 0) else "negative",
                    definition="Income minus expenses"
                )
            ]
        
        return KPISummaryResponse(
            data=kpis,
            period=period,
            as_of_time=datetime.now()
        )
        
    except Exception as e:
        logger.error(f"Error retrieving KPIs: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve KPIs")

# Cashflow endpoints
@app.get("/cashflow", response_model=CashflowSummaryResponse, tags=["Cashflow"])
async def get_cashflow(
    grain: str = Query("month", description="Time grain: daily, weekly, or monthly"),
    months: int = Query(12, description="Number of months to retrieve"),
    account: Optional[str] = Query(None, description="Account ID filter"),
    user: dict = Depends(get_current_user)
):
    """Get cashflow data by time period"""
    try:
        engine = get_postgres_engine()
        
        # Build query based on grain
        if grain == "daily":
            table = "marts.fct_cashflow_daily"
            date_col = "date"
            group_by = "date"
            limit = months * 30
        elif grain == "weekly":
            table = "marts.fct_cashflow_daily"
            date_col = "week"
            group_by = "week"
            limit = months * 4
        else:  # monthly
            table = "marts.fct_cashflow_monthly"
            date_col = "month"
            group_by = "month"
            limit = months
        
        # Build where clause
        where_clause = f"WHERE {date_col} >= TO_CHAR(CURRENT_DATE - INTERVAL '{months} months', 'YYYY-MM')"
        params = {"limit": limit}
        
        if account:
            where_clause += " AND account_id = :account"
            params["account"] = account
        
        if grain == "weekly":
            query = f"""
            SELECT 
                {date_col} as period,
                SUM(income) as income,
                SUM(expenses) as expenses,
                AVG(savings_rate) as savings_rate,
                SUM(balance_delta) as balance_delta,
                SUM(transaction_count) as transaction_count
            FROM {table}
            {where_clause}
            GROUP BY {group_by}
            ORDER BY {group_by} DESC
            LIMIT :limit
            """
        else:
            query = f"""
            SELECT 
                {date_col} as period,
                income,
                expenses,
                savings_rate,
                balance_delta,
                transaction_count
            FROM {table}
            {where_clause}
            ORDER BY {date_col} DESC
            LIMIT :limit
            """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            cashflow = []
            total_income = 0
            total_expenses = 0
            
            for row in result:
                income = float(row.income or 0)
                expenses = float(row.expenses or 0)
                total_income += income
                total_expenses += expenses
                
                cashflow.append(CashflowResponse(
                    period=row.period,
                    income=income,
                    expenses=expenses,
                    savings_rate=float(row.savings_rate or 0),
                    balance_delta=float(row.balance_delta or 0),
                    transaction_count=int(row.transaction_count or 0)
                ))
        
        overall_savings_rate = (total_income - total_expenses) / total_income if total_income > 0 else 0
        
        return CashflowSummaryResponse(
            data=cashflow,
            total_income=total_income,
            total_expenses=total_expenses,
            overall_savings_rate=overall_savings_rate
        )
        
    except Exception as e:
        logger.error(f"Error retrieving cashflow data: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve cashflow data")

# Budget endpoints
@app.get("/budget/variance", response_model=BudgetSummaryResponse, tags=["Budget"])
async def get_budget_variance(
    month: str = Query(..., regex=r"^\d{4}-\d{2}$", description="Month in YYYY-MM format"),
    user: dict = Depends(get_current_user)
):
    """Get budget vs actual variance for a specific month"""
    try:
        engine = get_postgres_engine()
        
        query = """
        SELECT 
            month,
            category_name,
            budget_target,
            actual_expenses,
            variance,
            variance_pct,
            budget_status
        FROM marts.fct_budget_vs_actual
        WHERE month = :month
        ORDER BY ABS(variance_pct) DESC
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"month": month})
            variances = []
            total_budget = 0
            total_actual = 0
            
            for row in result:
                budget = float(row.budget_target or 0)
                actual = float(row.actual_expenses or 0)
                total_budget += budget
                total_actual += actual
                
                variances.append(BudgetVarianceResponse(
                    month=row.month,
                    category_name=row.category_name,
                    budget=budget,
                    actual=actual,
                    variance=float(row.variance or 0),
                    variance_pct=float(row.variance_pct or 0),
                    budget_status=BudgetStatus(row.budget_status)
                ))
        
        overall_variance = total_actual - total_budget
        
        return BudgetSummaryResponse(
            data=variances,
            total_budget=total_budget,
            total_actual=total_actual,
            overall_variance=overall_variance
        )
        
    except Exception as e:
        logger.error(f"Error retrieving budget variance: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve budget variance")

# Anomaly endpoints
@app.get("/anomalies/recent", response_model=AnomalySummaryResponse, tags=["Anomalies"])
async def get_recent_anomalies(
    limit: int = Query(50, description="Number of anomalies to retrieve"),
    severity: Optional[SeverityLevel] = Query(None, description="Filter by severity level"),
    user: dict = Depends(get_current_user)
):
    """Get recent anomalies"""
    try:
        engine = get_postgres_engine()
        
        where_clause = ""
        params = {"limit": limit}
        
        if severity:
            where_clause = "WHERE severity = :severity"
            params["severity"] = severity.value
        
        query = f"""
        SELECT 
            id,
            txn_id,
            anomaly_type,
            severity,
            driver,
            remediation_hint,
            flagged_at,
            acknowledged
        FROM marts.fct_anomalies
        {where_clause}
        ORDER BY flagged_at DESC
        LIMIT :limit
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            anomalies = []
            high_severity_count = 0
            unacknowledged_count = 0
            
            for row in result:
                if row.severity == "high":
                    high_severity_count += 1
                if not row.acknowledged:
                    unacknowledged_count += 1
                
                anomalies.append(AnomalyResponse(
                    id=row.id,
                    txn_id=row.txn_id,
                    anomaly_type=row.anomaly_type,
                    severity=SeverityLevel(row.severity),
                    driver=row.driver,
                    remediation_hint=row.remediation_hint,
                    flagged_at=row.flagged_at,
                    acknowledged=row.acknowledged
                ))
        
        return AnomalySummaryResponse(
            data=anomalies,
            total_anomalies=len(anomalies),
            high_severity_count=high_severity_count,
            unacknowledged_count=unacknowledged_count
        )
        
    except Exception as e:
        logger.error(f"Error retrieving anomalies: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve anomalies")

@app.post("/anomalies/acknowledge", response_model=BaseResponse, tags=["Anomalies"])
async def acknowledge_anomaly(
    request: AnomalyAcknowledgeRequest,
    user: dict = Depends(get_current_user)
):
    """Acknowledge an anomaly"""
    try:
        engine = get_postgres_engine()
        
        query = """
        UPDATE marts.fct_anomalies 
        SET acknowledged = :acknowledged, 
            acknowledged_at = CURRENT_TIMESTAMP,
            acknowledged_by = :user_id
        WHERE id = :anomaly_id
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {
                "anomaly_id": request.anomaly_id,
                "acknowledged": request.acknowledged,
                "user_id": user["user_id"]
            })
            conn.commit()
            
            if result.rowcount == 0:
                raise HTTPException(status_code=404, detail="Anomaly not found")
        
        return BaseResponse(
            message=f"Anomaly {request.anomaly_id} acknowledged successfully"
        )
        
    except Exception as e:
        logger.error(f"Error acknowledging anomaly: {e}")
        raise HTTPException(status_code=500, detail="Failed to acknowledge anomaly")

# Forecast endpoints
@app.get("/forecast", response_model=ForecastSummaryResponse, tags=["Forecast"])
async def get_forecasts(
    horizon: ForecastHorizon = Query(ForecastHorizon.THREE_MONTHS, description="Forecast horizon"),
    category: Optional[str] = Query(None, description="Category filter"),
    user: dict = Depends(get_current_user)
):
    """Get financial forecasts"""
    try:
        engine = get_postgres_engine()
        
        where_clause = "WHERE forecast_horizon = :horizon"
        params = {"horizon": horizon.value}
        
        if category:
            where_clause += " AND category_name = :category"
            params["category"] = category
        
        query = f"""
        SELECT 
            forecast_date,
            forecast_type,
            category_name,
            forecast_amount,
            lower_bound,
            upper_bound,
            confidence_level,
            forecast_quality
        FROM marts.fct_forecasts
        {where_clause}
        ORDER BY forecast_date, forecast_type, category_name
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            forecasts = []
            total_forecast_amount = 0
            confidence_sum = 0
            count = 0
            
            for row in result:
                amount = float(row.forecast_amount or 0)
                confidence = float(row.confidence_level or 0)
                total_forecast_amount += amount
                confidence_sum += confidence
                count += 1
                
                forecasts.append(ForecastResponse(
                    forecast_date=row.forecast_date,
                    forecast_type=ForecastType(row.forecast_type),
                    category_name=row.category_name,
                    forecast_amount=amount,
                    lower_bound=float(row.lower_bound or 0),
                    upper_bound=float(row.upper_bound or 0),
                    confidence_level=confidence,
                    forecast_quality=row.forecast_quality
                ))
        
        average_confidence = confidence_sum / count if count > 0 else 0
        
        return ForecastSummaryResponse(
            data=forecasts,
            total_forecast_amount=total_forecast_amount,
            average_confidence=average_confidence
        )
        
    except Exception as e:
        logger.error(f"Error retrieving forecasts: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve forecasts")

# Recurring endpoints
@app.get("/recurring", response_model=RecurringSummaryResponse, tags=["Recurring"])
async def get_recurring_transactions(
    user: dict = Depends(get_current_user)
):
    """Get recurring transactions"""
    try:
        engine = get_postgres_engine()
        
        query = """
        SELECT 
            merchant_name,
            category_name,
            recurring_type,
            confidence_score,
            avg_amount,
            next_expected_date,
            days_until_next,
            status
        FROM marts.fct_recurring
        WHERE is_confirmed_recurring = true
        ORDER BY confidence_score DESC, avg_amount DESC
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query))
            recurring = []
            total_recurring_amount = 0
            confirmed_count = 0
            
            for row in result:
                amount = float(row.avg_amount or 0)
                total_recurring_amount += amount
                confirmed_count += 1
                
                recurring.append(RecurringResponse(
                    merchant_name=row.merchant_name,
                    category_name=row.category_name,
                    recurring_type=RecurringType(row.recurring_type),
                    confidence_score=float(row.confidence_score or 0),
                    avg_amount=amount,
                    next_expected_date=row.next_expected_date,
                    days_until_next=row.days_until_next,
                    status=row.status
                ))
        
        return RecurringSummaryResponse(
            data=recurring,
            total_recurring_amount=total_recurring_amount,
            confirmed_recurring_count=confirmed_count
        )
        
    except Exception as e:
        logger.error(f"Error retrieving recurring transactions: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve recurring transactions")

# Net worth endpoints
@app.get("/net-worth", response_model=NetWorthSummaryResponse, tags=["Net Worth"])
async def get_net_worth(
    days: int = Query(90, description="Number of days to retrieve"),
    user: dict = Depends(get_current_user)
):
    """Get net worth data"""
    try:
        engine = get_postgres_engine()
        
        query = """
        SELECT 
            date,
            net_worth,
            total_assets,
            total_liabilities,
            net_worth_change,
            net_worth_dod_change_pct
        FROM marts.fct_net_worth
        WHERE date >= CURRENT_DATE - INTERVAL ':days days'
        ORDER BY date DESC
        LIMIT :limit
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query), {"days": days, "limit": days})
            net_worth_data = []
            current_net_worth = 0
            change_30d = 0
            change_90d = 0
            
            for i, row in enumerate(result):
                net_worth = float(row.net_worth or 0)
                if i == 0:
                    current_net_worth = net_worth
                if i == 30:
                    change_30d = net_worth
                if i == 90:
                    change_90d = net_worth
                
                net_worth_data.append(NetWorthResponse(
                    date=row.date,
                    net_worth=net_worth,
                    total_assets=float(row.total_assets or 0),
                    total_liabilities=float(row.total_liabilities or 0),
                    net_worth_change=float(row.net_worth_change or 0),
                    net_worth_change_pct=float(row.net_worth_dod_change_pct or 0)
                ))
        
        return NetWorthSummaryResponse(
            data=net_worth_data,
            current_net_worth=current_net_worth,
            net_worth_change_30d=current_net_worth - change_30d if change_30d > 0 else 0,
            net_worth_change_90d=current_net_worth - change_90d if change_90d > 0 else 0
        )
        
    except Exception as e:
        logger.error(f"Error retrieving net worth data: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve net worth data")

# Driver analysis endpoints
@app.get("/explain/savings", response_model=SavingsAnalysisResponse, tags=["Analysis"])
async def explain_savings(
    period: str = Query(..., regex=r"^\d{4}-\d{2}$", description="Period in YYYY-MM format"),
    user: dict = Depends(get_current_user)
):
    """Explain savings drivers for a specific period"""
    try:
        engine = get_postgres_engine()
        
        # Get cashflow data
        cashflow_query = """
        SELECT 
            income,
            expenses,
            savings_rate
        FROM marts.fct_cashflow_monthly
        WHERE month = :period
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(cashflow_query), {"period": period})
            row = result.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="No data found for the specified period")
            
            income = float(row.income or 0)
            expenses = float(row.expenses or 0)
            savings_rate = float(row.savings_rate or 0)
            total_savings = income - expenses
            
            # Get category breakdown
            category_query = """
            SELECT 
                category_name,
                actual_expenses,
                variance_pct
            FROM marts.fct_budget_vs_actual
            WHERE month = :period
            ORDER BY actual_expenses DESC
            LIMIT 10
            """
            
            result = conn.execute(text(category_query), {"period": period})
            drivers = []
            
            for cat_row in result:
                category_expenses = float(cat_row.actual_expenses or 0)
                variance_pct = float(cat_row.variance_pct or 0)
                
                drivers.append(DriverAnalysisResponse(
                    driver_type="expense_category",
                    driver_name=cat_row.category_name,
                    impact=category_expenses,
                    impact_pct=(category_expenses / expenses * 100) if expenses > 0 else 0,
                    description=f"Expenses in {cat_row.category_name}: ${category_expenses:,.2f} ({variance_pct:+.1f}% vs budget)"
                ))
            
            # Generate recommendations
            recommendations = []
            if savings_rate < 0.2:
                recommendations.append("Consider reducing discretionary spending to increase savings rate")
            if expenses > income * 0.8:
                recommendations.append("Review and optimize recurring expenses")
            if any(d.variance_pct > 20 for d in drivers):
                recommendations.append("Address budget overruns in high-variance categories")
            
            return SavingsAnalysisResponse(
                period=period,
                total_savings=total_savings,
                savings_rate=savings_rate,
                drivers=drivers,
                recommendations=recommendations
            )
        
    except Exception as e:
        logger.error(f"Error analyzing savings: {e}")
        raise HTTPException(status_code=500, detail="Failed to analyze savings")

# Performance endpoints
@app.get("/analytics/performance", response_model=PerformanceSummaryResponse, tags=["Analytics"])
async def get_performance_metrics(
    user: dict = Depends(get_current_user)
):
    """Get performance metrics comparing Postgres vs DuckDB"""
    try:
        metrics = []
        
        # Test queries
        test_queries = [
            {
                "name": "Daily Cashflow",
                "postgres_query": """
                    SELECT date, income, expenses 
                    FROM marts.fct_cashflow_daily 
                    WHERE date >= '2024-01-01' 
                    ORDER BY date DESC 
                    LIMIT 100
                """,
                "duckdb_query": """
                    SELECT date, income, expenses 
                    FROM mart_cashflow_daily 
                    WHERE date >= '2024-01-01' 
                    ORDER BY date DESC 
                    LIMIT 100
                """
            },
            {
                "name": "Category Analysis",
                "postgres_query": """
                    SELECT category_name, SUM(actual_expenses) as total
                    FROM marts.fct_budget_vs_actual 
                    WHERE month >= '2024-01' 
                    GROUP BY category_name 
                    ORDER BY total DESC
                """,
                "duckdb_query": """
                    SELECT category_name, SUM(actual_expenses) as total
                    FROM mart_budget_vs_actual 
                    WHERE month >= '2024-01' 
                    GROUP BY category_name 
                    ORDER BY total DESC
                """
            },
            {
                "name": "Monthly Trends",
                "postgres_query": """
                    SELECT month, income, expenses, savings_rate
                    FROM marts.fct_cashflow_monthly 
                    WHERE month >= '2024-01' 
                    ORDER BY month DESC
                """,
                "duckdb_query": """
                    SELECT month, income, expenses, savings_rate
                    FROM mart_cashflow_monthly 
                    WHERE month >= '2024-01' 
                    ORDER BY month DESC
                """
            }
        ]
        
        total_speedup = 0
        count = 0
        
        for test in test_queries:
            # Test Postgres
        pg_engine = get_postgres_engine()
        pg_start = datetime.now()
        
        with pg_engine.connect() as conn:
                result = conn.execute(text(test["postgres_query"]))
            pg_data = result.fetchall()
        
        pg_duration = (datetime.now() - pg_start).total_seconds() * 1000
        
            # Test DuckDB
        duck_con = get_duckdb_connection()
        duck_start = datetime.now()
        
            result = duck_con.execute(test["duckdb_query"])
        duck_data = result.fetchall()
        
        duck_duration = (datetime.now() - duck_start).total_seconds() * 1000
        duck_con.close()
        
        # Calculate speedup
        speedup = pg_duration / duck_duration if duck_duration > 0 else 0
            total_speedup += speedup
            count += 1
            
            metrics.append(PerformanceMetricsResponse(
                query_type=test["name"],
                postgresql_duration_ms=round(pg_duration, 2),
                duckdb_duration_ms=round(duck_duration, 2),
                speedup_factor=round(speedup, 2),
                faster_engine="DuckDB" if speedup > 1 else "PostgreSQL"
            ))
        
        average_speedup = total_speedup / count if count > 0 else 0
        
        return PerformanceSummaryResponse(
            data=metrics,
            average_speedup=round(average_speedup, 2)
        )
        
    except Exception as e:
        logger.error(f"Error retrieving performance metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve performance metrics")

# Summary endpoint
@app.get("/summary", response_model=PlatformSummaryResponse, tags=["Summary"])
async def get_platform_summary(
    user: dict = Depends(get_current_user)
):
    """Get platform summary statistics"""
    try:
        engine = get_postgres_engine()
        
        # Get counts
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM raw.transactions"))
            total_transactions = result.fetchone()[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM marts.fct_anomalies"))
            total_anomalies = result.fetchone()[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM marts.fct_forecasts"))
            total_forecasts = result.fetchone()[0]
            
            # Calculate data quality score (simplified)
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_checks,
                    SUM(CASE WHEN anomaly_score < 50 THEN 1 ELSE 0 END) as passed_checks
                FROM marts.fct_anomalies
            """))
            row = result.fetchone()
            data_quality_score = (row.passed_checks / row.total_checks * 100) if row.total_checks > 0 else 100
        
        return PlatformSummaryResponse(
            total_transactions=total_transactions,
            total_anomalies=total_anomalies,
            total_forecasts=total_forecasts,
            last_updated=datetime.now(),
            data_quality_score=round(data_quality_score, 1),
            system_health="healthy"
        )
        
    except Exception as e:
        logger.error(f"Error retrieving platform summary: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve platform summary")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 