"""
Pydantic models for FastAPI application
"""

from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from enum import Enum


class SeverityLevel(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    MINIMAL = "minimal"


class ForecastType(str, Enum):
    TOTAL = "total"
    CATEGORY = "category"


class ForecastHorizon(str, Enum):
    ONE_MONTH = "1_month"
    TWO_MONTHS = "2_months"
    THREE_MONTHS = "3_months"


class BudgetStatus(str, Enum):
    OVER_BUDGET = "over_budget"
    UNDER_BUDGET = "under_budget"
    ON_BUDGET = "on_budget"


class RecurringType(str, Enum):
    MONTHLY = "monthly"
    WEEKLY = "weekly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"
    LIKELY_MONTHLY = "likely_monthly"
    LIKELY_WEEKLY = "likely_weekly"
    IRREGULAR = "irregular"


# Base response models
class BaseResponse(BaseModel):
    success: bool = True
    message: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)


# Balance models
class BalanceResponse(BaseModel):
    account_id: str
    institution: str
    current_balance: float
    last_updated: datetime
    currency: str


class BalanceSummaryResponse(BaseResponse):
    data: List[BalanceResponse]
    total_balance: float
    currency: str


# Cashflow models
class CashflowResponse(BaseModel):
    period: str
    income: float
    expenses: float
    savings_rate: float
    balance_delta: float
    transaction_count: int


class CashflowSummaryResponse(BaseResponse):
    data: List[CashflowResponse]
    total_income: float
    total_expenses: float
    overall_savings_rate: float


# Budget models
class BudgetVarianceResponse(BaseModel):
    month: str
    category_name: str
    budget: float
    actual: float
    variance: float
    variance_pct: float
    budget_status: BudgetStatus


class BudgetSummaryResponse(BaseResponse):
    data: List[BudgetVarianceResponse]
    total_budget: float
    total_actual: float
    overall_variance: float


# Anomaly models
class AnomalyResponse(BaseModel):
    id: int
    txn_id: str
    anomaly_type: str
    severity: SeverityLevel
    driver: str
    remediation_hint: str
    flagged_at: datetime
    acknowledged: bool = False


class AnomalySummaryResponse(BaseResponse):
    data: List[AnomalyResponse]
    total_anomalies: int
    high_severity_count: int
    unacknowledged_count: int


# Forecast models
class ForecastResponse(BaseModel):
    forecast_date: date
    forecast_type: ForecastType
    category_name: str
    forecast_amount: float
    lower_bound: float
    upper_bound: float
    confidence_level: float
    forecast_quality: str


class ForecastSummaryResponse(BaseResponse):
    data: List[ForecastResponse]
    total_forecast_amount: float
    average_confidence: float


# Recurring models
class RecurringResponse(BaseModel):
    merchant_name: str
    category_name: str
    recurring_type: RecurringType
    confidence_score: float
    avg_amount: float
    next_expected_date: Optional[date]
    days_until_next: Optional[int]
    status: str


class RecurringSummaryResponse(BaseResponse):
    data: List[RecurringResponse]
    total_recurring_amount: float
    confirmed_recurring_count: int


# Net worth models
class NetWorthResponse(BaseModel):
    date: date
    net_worth: float
    total_assets: float
    total_liabilities: float
    net_worth_change: float
    net_worth_change_pct: Optional[float]


class NetWorthSummaryResponse(BaseResponse):
    data: List[NetWorthResponse]
    current_net_worth: float
    net_worth_change_30d: float
    net_worth_change_90d: float


# KPI models
class KPIResponse(BaseModel):
    metric_name: str
    value: float
    unit: str
    change_pct: Optional[float]
    trend: str
    definition: str


class KPISummaryResponse(BaseResponse):
    data: List[KPIResponse]
    period: str
    as_of_time: datetime


# Driver analysis models
class DriverAnalysisResponse(BaseModel):
    driver_type: str
    driver_name: str
    impact: float
    impact_pct: float
    description: str


class SavingsAnalysisResponse(BaseResponse):
    period: str
    total_savings: float
    savings_rate: float
    drivers: List[DriverAnalysisResponse]
    recommendations: List[str]


# Performance models
class PerformanceMetricsResponse(BaseModel):
    query_type: str
    postgresql_duration_ms: float
    duckdb_duration_ms: float
    speedup_factor: float
    faster_engine: str


class PerformanceSummaryResponse(BaseResponse):
    data: List[PerformanceMetricsResponse]
    average_speedup: float


# Error models
class ErrorResponse(BaseModel):
    success: bool = False
    error_code: str
    error_message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)


# Request models
class DateRangeRequest(BaseModel):
    start_date: date
    end_date: date


class PeriodRequest(BaseModel):
    period: str = Field(..., regex=r"^\d{4}-\d{2}$", description="Period in YYYY-MM format")


class CategoryRequest(BaseModel):
    category: str


class AccountRequest(BaseModel):
    account_id: str


class AnomalyAcknowledgeRequest(BaseModel):
    anomaly_id: int
    acknowledged: bool = True
    notes: Optional[str] = None


class ForecastRequest(BaseModel):
    horizon: ForecastHorizon = ForecastHorizon.THREE_MONTHS
    category: Optional[str] = None


# Health check models
class HealthCheckResponse(BaseModel):
    status: str
    postgresql: str
    duckdb: str
    timestamp: datetime


# Summary models
class PlatformSummaryResponse(BaseModel):
    total_transactions: int
    total_anomalies: int
    total_forecasts: int
    last_updated: datetime
    data_quality_score: float
    system_health: str
