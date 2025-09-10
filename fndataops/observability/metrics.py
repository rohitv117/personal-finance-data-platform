"""
Metrics collection and monitoring for FinDataOps platform
"""

import time
import psutil
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
from contextlib import contextmanager
import json


@dataclass
class Metric:
    """Metric data structure"""
    name: str
    value: float
    unit: str
    timestamp: datetime
    labels: Dict[str, str] = None
    metadata: Dict[str, Any] = None


class MetricsCollector:
    """Collects and stores metrics for the platform"""
    
    def __init__(self):
        self.metrics = []
        self.logger = logging.getLogger("metrics")
    
    def add_metric(self, metric: Metric):
        """Add a metric to the collection"""
        self.metrics.append(metric)
        self.logger.info(f"Metric collected: {metric.name}={metric.value}{metric.unit}")
    
    def get_metrics(self, name: Optional[str] = None, 
                   start_time: Optional[datetime] = None,
                   end_time: Optional[datetime] = None) -> list:
        """Get metrics with optional filtering"""
        filtered_metrics = self.metrics
        
        if name:
            filtered_metrics = [m for m in filtered_metrics if m.name == name]
        
        if start_time:
            filtered_metrics = [m for m in filtered_metrics if m.timestamp >= start_time]
        
        if end_time:
            filtered_metrics = [m for m in filtered_metrics if m.timestamp <= end_time]
        
        return filtered_metrics
    
    def get_latest_metric(self, name: str) -> Optional[Metric]:
        """Get the latest metric for a given name"""
        metrics = [m for m in self.metrics if m.name == name]
        return max(metrics, key=lambda m: m.timestamp) if metrics else None
    
    def get_metric_summary(self, name: str, 
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Get summary statistics for a metric"""
        metrics = self.get_metrics(name, start_time, end_time)
        
        if not metrics:
            return {}
        
        values = [m.value for m in metrics]
        
        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "latest": values[-1],
            "first": values[0]
        }


# Global metrics collector
metrics_collector = MetricsCollector()


class PerformanceMonitor:
    """Monitors performance metrics"""
    
    def __init__(self):
        self.logger = logging.getLogger("performance_monitor")
    
    @contextmanager
    def measure_duration(self, operation_name: str, **labels):
        """Context manager to measure operation duration"""
        start_time = time.time()
        start_cpu = psutil.cpu_percent()
        start_memory = psutil.virtual_memory().percent
        
        try:
            yield
        finally:
            end_time = time.time()
            end_cpu = psutil.cpu_percent()
            end_memory = psutil.virtual_memory().percent
            
            duration_ms = (end_time - start_time) * 1000
            
            # Record duration metric
            metrics_collector.add_metric(Metric(
                name=f"{operation_name}_duration_ms",
                value=duration_ms,
                unit="ms",
                timestamp=datetime.now(),
                labels=labels
            ))
            
            # Record CPU usage
            metrics_collector.add_metric(Metric(
                name=f"{operation_name}_cpu_usage_pct",
                value=(start_cpu + end_cpu) / 2,
                unit="%",
                timestamp=datetime.now(),
                labels=labels
            ))
            
            # Record memory usage
            metrics_collector.add_metric(Metric(
                name=f"{operation_name}_memory_usage_pct",
                value=(start_memory + end_memory) / 2,
                unit="%",
                timestamp=datetime.now(),
                labels=labels
            ))
            
            self.logger.info(f"Operation {operation_name} completed in {duration_ms:.2f}ms")
    
    def record_query_performance(self, query_type: str, duration_ms: float, 
                               rows_returned: int, engine: str):
        """Record database query performance"""
        metrics_collector.add_metric(Metric(
            name="query_duration_ms",
            value=duration_ms,
            unit="ms",
            timestamp=datetime.now(),
            labels={
                "query_type": query_type,
                "engine": engine,
                "rows_returned": str(rows_returned)
            }
        ))
    
    def record_data_processing_metrics(self, table_name: str, rows_processed: int, 
                                     processing_time_ms: float):
        """Record data processing metrics"""
        metrics_collector.add_metric(Metric(
            name="data_processing_rows",
            value=rows_processed,
            unit="rows",
            timestamp=datetime.now(),
            labels={"table": table_name}
        ))
        
        metrics_collector.add_metric(Metric(
            name="data_processing_duration_ms",
            value=processing_time_ms,
            unit="ms",
            timestamp=datetime.now(),
            labels={"table": table_name}
        ))
        
        # Calculate throughput
        throughput = rows_processed / (processing_time_ms / 1000) if processing_time_ms > 0 else 0
        metrics_collector.add_metric(Metric(
            name="data_processing_throughput",
            value=throughput,
            unit="rows/sec",
            timestamp=datetime.now(),
            labels={"table": table_name}
        ))


class DataQualityMonitor:
    """Monitors data quality metrics"""
    
    def __init__(self):
        self.logger = logging.getLogger("data_quality_monitor")
    
    def record_quality_check(self, table_name: str, check_name: str, 
                           passed: bool, rows_checked: int, 
                           failed_rows: int = 0):
        """Record data quality check results"""
        quality_score = ((rows_checked - failed_rows) / rows_checked * 100) if rows_checked > 0 else 0
        
        metrics_collector.add_metric(Metric(
            name="data_quality_score",
            value=quality_score,
            unit="%",
            timestamp=datetime.now(),
            labels={
                "table": table_name,
                "check": check_name,
                "passed": str(passed)
            }
        ))
        
        metrics_collector.add_metric(Metric(
            name="data_quality_rows_checked",
            value=rows_checked,
            unit="rows",
            timestamp=datetime.now(),
            labels={
                "table": table_name,
                "check": check_name
            }
        ))
        
        if failed_rows > 0:
            metrics_collector.add_metric(Metric(
                name="data_quality_failed_rows",
                value=failed_rows,
                unit="rows",
                timestamp=datetime.now(),
                labels={
                    "table": table_name,
                    "check": check_name
                }
            ))
    
    def record_anomaly_detection(self, anomalies_detected: int, 
                               high_severity_count: int):
        """Record anomaly detection metrics"""
        metrics_collector.add_metric(Metric(
            name="anomalies_detected",
            value=anomalies_detected,
            unit="count",
            timestamp=datetime.now()
        ))
        
        metrics_collector.add_metric(Metric(
            name="high_severity_anomalies",
            value=high_severity_count,
            unit="count",
            timestamp=datetime.now()
        ))


class BusinessMetricsMonitor:
    """Monitors business metrics"""
    
    def __init__(self):
        self.logger = logging.getLogger("business_metrics_monitor")
    
    def record_transaction_metrics(self, transaction_count: int, 
                                 total_amount: float, currency: str = "USD"):
        """Record transaction-related business metrics"""
        metrics_collector.add_metric(Metric(
            name="transaction_count",
            value=transaction_count,
            unit="count",
            timestamp=datetime.now(),
            labels={"currency": currency}
        ))
        
        metrics_collector.add_metric(Metric(
            name="transaction_volume",
            value=total_amount,
            unit=currency,
            timestamp=datetime.now(),
            labels={"currency": currency}
        ))
    
    def record_cashflow_metrics(self, income: float, expenses: float, 
                              savings_rate: float, period: str):
        """Record cashflow metrics"""
        metrics_collector.add_metric(Metric(
            name="income",
            value=income,
            unit="USD",
            timestamp=datetime.now(),
            labels={"period": period}
        ))
        
        metrics_collector.add_metric(Metric(
            name="expenses",
            value=expenses,
            unit="USD",
            timestamp=datetime.now(),
            labels={"period": period}
        ))
        
        metrics_collector.add_metric(Metric(
            name="savings_rate",
            value=savings_rate * 100,
            unit="%",
            timestamp=datetime.now(),
            labels={"period": period}
        ))
    
    def record_forecast_metrics(self, forecast_accuracy: float, 
                              confidence_level: float, horizon: str):
        """Record forecast metrics"""
        metrics_collector.add_metric(Metric(
            name="forecast_accuracy",
            value=forecast_accuracy,
            unit="%",
            timestamp=datetime.now(),
            labels={"horizon": horizon}
        ))
        
        metrics_collector.add_metric(Metric(
            name="forecast_confidence",
            value=confidence_level * 100,
            unit="%",
            timestamp=datetime.now(),
            labels={"horizon": horizon}
        ))


class SystemMonitor:
    """Monitors system resources"""
    
    def __init__(self):
        self.logger = logging.getLogger("system_monitor")
    
    def record_system_metrics(self):
        """Record current system metrics"""
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        metrics_collector.add_metric(Metric(
            name="system_cpu_usage",
            value=cpu_percent,
            unit="%",
            timestamp=datetime.now()
        ))
        
        # Memory usage
        memory = psutil.virtual_memory()
        metrics_collector.add_metric(Metric(
            name="system_memory_usage",
            value=memory.percent,
            unit="%",
            timestamp=datetime.now()
        ))
        
        # Disk usage
        disk = psutil.disk_usage('/')
        disk_percent = (disk.used / disk.total) * 100
        metrics_collector.add_metric(Metric(
            name="system_disk_usage",
            value=disk_percent,
            unit="%",
            timestamp=datetime.now()
        ))
        
        # Load average
        load_avg = psutil.getloadavg()
        metrics_collector.add_metric(Metric(
            name="system_load_1min",
            value=load_avg[0],
            unit="load",
            timestamp=datetime.now()
        ))


# Global monitor instances
performance_monitor = PerformanceMonitor()
data_quality_monitor = DataQualityMonitor()
business_metrics_monitor = BusinessMetricsMonitor()
system_monitor = SystemMonitor()


def get_metrics_summary() -> Dict[str, Any]:
    """Get a summary of all metrics"""
    now = datetime.now()
    last_hour = now - timedelta(hours=1)
    last_day = now - timedelta(days=1)
    
    summary = {
        "timestamp": now.isoformat(),
        "metrics_count": len(metrics_collector.metrics),
        "last_hour": {},
        "last_day": {},
        "system_health": "healthy"
    }
    
    # Get key metrics for last hour
    key_metrics = [
        "query_duration_ms",
        "data_processing_throughput",
        "data_quality_score",
        "anomalies_detected",
        "system_cpu_usage",
        "system_memory_usage"
    ]
    
    for metric_name in key_metrics:
        last_hour_metrics = metrics_collector.get_metrics(metric_name, last_hour)
        if last_hour_metrics:
            summary["last_hour"][metric_name] = metrics_collector.get_metric_summary(
                metric_name, last_hour
            )
        
        last_day_metrics = metrics_collector.get_metrics(metric_name, last_day)
        if last_day_metrics:
            summary["last_day"][metric_name] = metrics_collector.get_metric_summary(
                metric_name, last_day
            )
    
    return summary


def export_metrics_to_json() -> str:
    """Export all metrics to JSON format"""
    metrics_data = []
    for metric in metrics_collector.metrics:
        metrics_data.append({
            "name": metric.name,
            "value": metric.value,
            "unit": metric.unit,
            "timestamp": metric.timestamp.isoformat(),
            "labels": metric.labels or {},
            "metadata": metric.metadata or {}
        })
    
    return json.dumps(metrics_data, indent=2)
