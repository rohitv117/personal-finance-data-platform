"""
Logging configuration for FinDataOps platform
"""

import logging
import json
import sys
from datetime import datetime
from typing import Dict, Any
import hashlib
import re


class PIIRedactingFormatter(logging.Formatter):
    """Custom formatter that redacts PII from log messages"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Patterns for PII detection
        self.pii_patterns = [
            (r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b', '[CARD_NUMBER]'),  # Credit card numbers
            (r'\b\d{3}-\d{2}-\d{4}\b', '[SSN]'),  # SSN
            (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]'),  # Email
            (r'\b\d{3}-\d{3}-\d{4}\b', '[PHONE]'),  # Phone number
            (r'\b[A-Z][a-z]+ [A-Z][a-z]+\b', '[NAME]'),  # Names (simple pattern)
        ]
    
    def format(self, record):
        # Get the formatted message
        message = super().format(record)
        
        # Redact PII
        for pattern, replacement in self.pii_patterns:
            message = re.sub(pattern, replacement, message)
        
        return message


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    
    def format(self, record):
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno,
            'thread': record.thread,
            'process': record.process
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields
        if hasattr(record, 'user_id'):
            log_entry['user_id'] = record.user_id
        if hasattr(record, 'request_id'):
            log_entry['request_id'] = record.request_id
        if hasattr(record, 'correlation_id'):
            log_entry['correlation_id'] = record.correlation_id
        if hasattr(record, 'batch_id'):
            log_entry['batch_id'] = record.batch_id
        if hasattr(record, 'duration_ms'):
            log_entry['duration_ms'] = record.duration_ms
        if hasattr(record, 'rows_processed'):
            log_entry['rows_processed'] = record.rows_processed
        
        return json.dumps(log_entry)


def setup_logging(log_level: str = "INFO", log_format: str = "json") -> None:
    """Setup logging configuration for the platform"""
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Set formatter
    if log_format == "json":
        formatter = JSONFormatter()
    else:
        formatter = PIIRedactingFormatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Configure specific loggers
    configure_loggers()


def configure_loggers():
    """Configure specific loggers for different components"""
    
    # API logger
    api_logger = logging.getLogger("api")
    api_logger.setLevel(logging.INFO)
    
    # Data processing logger
    data_logger = logging.getLogger("data_processing")
    data_logger.setLevel(logging.INFO)
    
    # dbt logger
    dbt_logger = logging.getLogger("dbt")
    dbt_logger.setLevel(logging.INFO)
    
    # Great Expectations logger
    ge_logger = logging.getLogger("great_expectations")
    ge_logger.setLevel(logging.INFO)
    
    # Airflow logger
    airflow_logger = logging.getLogger("airflow")
    airflow_logger.setLevel(logging.INFO)
    
    # DuckDB logger
    duckdb_logger = logging.getLogger("duckdb")
    duckdb_logger.setLevel(logging.INFO)


class StructuredLogger:
    """Structured logger with context management"""
    
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        self.context = {}
    
    def set_context(self, **kwargs):
        """Set context for all subsequent log messages"""
        self.context.update(kwargs)
    
    def clear_context(self):
        """Clear context"""
        self.context.clear()
    
    def _log(self, level: int, message: str, **kwargs):
        """Internal logging method with context"""
        extra = {**self.context, **kwargs}
        self.logger.log(level, message, extra=extra)
    
    def debug(self, message: str, **kwargs):
        self._log(logging.DEBUG, message, **kwargs)
    
    def info(self, message: str, **kwargs):
        self._log(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self._log(logging.WARNING, message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self._log(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        self._log(logging.CRITICAL, message, **kwargs)


# Global logger instances
api_logger = StructuredLogger("api")
data_logger = StructuredLogger("data_processing")
dbt_logger = StructuredLogger("dbt")
ge_logger = StructuredLogger("great_expectations")
airflow_logger = StructuredLogger("airflow")
duckdb_logger = StructuredLogger("duckdb")


def log_performance_metrics(operation: str, duration_ms: float, **kwargs):
    """Log performance metrics"""
    data_logger.info(
        f"Performance metric: {operation}",
        operation=operation,
        duration_ms=duration_ms,
        **kwargs
    )


def log_data_quality_metrics(table: str, rows_processed: int, quality_score: float, **kwargs):
    """Log data quality metrics"""
    ge_logger.info(
        f"Data quality check: {table}",
        table=table,
        rows_processed=rows_processed,
        quality_score=quality_score,
        **kwargs
    )


def log_business_metrics(metric_name: str, value: float, unit: str, **kwargs):
    """Log business metrics"""
    data_logger.info(
        f"Business metric: {metric_name}",
        metric_name=metric_name,
        value=value,
        unit=unit,
        **kwargs
    )


def log_error_with_context(error: Exception, context: Dict[str, Any]):
    """Log error with additional context"""
    data_logger.error(
        f"Error occurred: {str(error)}",
        error_type=type(error).__name__,
        error_message=str(error),
        **context
    )
