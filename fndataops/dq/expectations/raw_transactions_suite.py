"""
Great Expectations suite for raw transactions data quality validation
"""

from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration

def create_raw_transactions_suite():
    """Create expectation suite for raw transactions table"""
    
    suite = ExpectationSuite(
        expectation_suite_name="raw_transactions_suite",
        data_asset_type="table",
        expectations=[
            # Schema expectations
            ExpectationConfiguration(
                expectation_type="expect_table_columns_to_match_ordered_list",
                kwargs={
                    "column_list": [
                        "txn_id", "source", "account_id", "posted_at", "amount", 
                        "currency", "merchant_raw", "mcc_raw", "description_raw", 
                        "category_raw", "counterparty_raw", "balance_after", 
                        "hash_raw", "ingest_batch_id", "created_at"
                    ]
                }
            ),
            
            # Completeness expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "txn_id"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "source"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "account_id"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "posted_at"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "amount"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "currency"}
            ),
            
            # Uniqueness expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "txn_id"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "hash_raw"}
            ),
            
            # Data type expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_of_type",
                kwargs={"column": "amount", "type_": "float"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_of_type",
                kwargs={"column": "posted_at", "type_": "datetime"}
            ),
            
            # Value range expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "amount",
                    "min_value": -1000000,
                    "max_value": 1000000
                }
            ),
            
            # Accepted values expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "source",
                    "value_set": ["bank", "card", "brokerage"]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "currency",
                    "value_set": ["USD", "EUR", "GBP", "CAD", "AUD"]
                }
            ),
            
            # Format expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "txn_id",
                    "regex": r"^[a-f0-9]{64}$"
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "currency",
                    "regex": r"^[A-Z]{3}$"
                }
            ),
            
            # Business logic expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "balance_after"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "ingest_batch_id"}
            ),
            
            # Data freshness expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_dateutil_parseable",
                kwargs={"column": "posted_at"}
            ),
            
            # PII expectations (ensure no PII in raw data)
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_match_regex",
                kwargs={
                    "column": "merchant_raw",
                    "regex": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"  # Credit card numbers
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_match_regex",
                kwargs={
                    "column": "description_raw",
                    "regex": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"  # Credit card numbers
                }
            ),
            
            # Data consistency expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_type_list",
                kwargs={
                    "column": "amount",
                    "type_list": ["int", "float", "decimal"]
                }
            ),
            
            # Row count expectations
            ExpectationConfiguration(
                expectation_type="expect_table_row_count_to_be_between",
                kwargs={"min_value": 1, "max_value": 10000000}
            )
        ]
    )
    
    return suite


def create_staging_transactions_suite():
    """Create expectation suite for staging transactions table"""
    
    suite = ExpectationSuite(
        expectation_suite_name="staging_transactions_suite",
        data_asset_type="table",
        expectations=[
            # Schema expectations
            ExpectationConfiguration(
                expectation_type="expect_table_columns_to_contain_set",
                kwargs={
                    "column_list": [
                        "txn_id", "amount_usd", "amount_ccy", "sign", "month",
                        "is_income", "is_expense", "is_transfer", "is_investment"
                    ]
                }
            ),
            
            # Completeness expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "txn_id"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "amount_usd"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "amount_ccy"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "sign"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "month"}
            ),
            
            # Uniqueness expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "txn_id"}
            ),
            
            # Value range expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "amount_usd",
                    "min_value": -1000000,
                    "max_value": 1000000
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "sign",
                    "min_value": -1,
                    "max_value": 1
                }
            ),
            
            # Accepted values expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "amount_ccy",
                    "value_set": ["USD"]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "sign",
                    "value_set": [1, -1, 0]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "is_income",
                    "value_set": [True, False]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "is_expense",
                    "value_set": [True, False]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "is_transfer",
                    "value_set": [True, False]
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "is_investment",
                    "value_set": [True, False]
                }
            ),
            
            # Format expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_match_regex",
                kwargs={
                    "column": "month",
                    "regex": r"^\d{4}-\d{2}$"
                }
            ),
            
            # Business logic expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_type_list",
                kwargs={
                    "column": "amount_usd",
                    "type_list": ["int", "float", "decimal"]
                }
            ),
            
            # Data consistency expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "merchant_clean"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "category_std"}
            )
        ]
    )
    
    return suite


def create_marts_suite():
    """Create expectation suite for marts tables"""
    
    suite = ExpectationSuite(
        expectation_suite_name="marts_suite",
        data_asset_type="table",
        expectations=[
            # Cashflow daily expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "date"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "income"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "expenses"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "savings_rate",
                    "min_value": 0,
                    "max_value": 1
                }
            ),
            
            # Net worth expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "net_worth"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "total_assets"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "total_liabilities"}
            ),
            
            # Budget variance expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "budget_target"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "actual_expenses"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "variance"}
            ),
            
            # Anomalies expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "anomaly_score",
                    "min_value": 0,
                    "max_value": 100
                }
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "severity",
                    "value_set": ["high", "medium", "low", "minimal"]
                }
            ),
            
            # Forecasts expectations
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": "forecast_amount"}
            ),
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "confidence_level",
                    "min_value": 0,
                    "max_value": 1
                }
            )
        ]
    )
    
    return suite
