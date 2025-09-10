"""
Great Expectations checkpoint for raw data validation
"""

from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context import DataContext

def create_raw_data_checkpoint(context: DataContext):
    """Create checkpoint for raw data validation"""
    
    checkpoint = SimpleCheckpoint(
        name="raw_data_checkpoint",
        data_context=context,
        validations=[
            {
                "batch_request": BatchRequest(
                    datasource_name="postgres_datasource",
                    data_connector_name="default_inferred_data_connector_name",
                    data_asset_name="raw.transactions",
                ),
                "expectation_suite_name": "raw_transactions_suite",
            }
        ],
    )
    
    return checkpoint


def create_staging_data_checkpoint(context: DataContext):
    """Create checkpoint for staging data validation"""
    
    checkpoint = SimpleCheckpoint(
        name="staging_data_checkpoint",
        data_context=context,
        validations=[
            {
                "batch_request": BatchRequest(
                    datasource_name="postgres_datasource",
                    data_connector_name="default_inferred_data_connector_name",
                    data_asset_name="staging.transactions",
                ),
                "expectation_suite_name": "staging_transactions_suite",
            }
        ],
    )
    
    return checkpoint


def create_marts_data_checkpoint(context: DataContext):
    """Create checkpoint for marts data validation"""
    
    checkpoint = SimpleCheckpoint(
        name="marts_data_checkpoint",
        data_context=context,
        validations=[
            {
                "batch_request": BatchRequest(
                    datasource_name="postgres_datasource",
                    data_connector_name="default_inferred_data_connector_name",
                    data_asset_name="marts.fct_cashflow_daily",
                ),
                "expectation_suite_name": "marts_suite",
            },
            {
                "batch_request": BatchRequest(
                    datasource_name="postgres_datasource",
                    data_connector_name="default_inferred_data_connector_name",
                    data_asset_name="marts.fct_cashflow_monthly",
                ),
                "expectation_suite_name": "marts_suite",
            },
            {
                "batch_request": BatchRequest(
                    datasource_name="postgres_datasource",
                    data_connector_name="default_inferred_data_connector_name",
                    data_asset_name="marts.fct_net_worth",
                ),
                "expectation_suite_name": "marts_suite",
            },
            {
                "batch_request": BatchRequest(
                    datasource_name="postgres_datasource",
                    data_connector_name="default_inferred_data_connector_name",
                    data_asset_name="marts.fct_budget_vs_actual",
                ),
                "expectation_suite_name": "marts_suite",
            },
            {
                "batch_request": BatchRequest(
                    datasource_name="postgres_datasource",
                    data_connector_name="default_inferred_data_connector_name",
                    data_asset_name="marts.fct_anomalies",
                ),
                "expectation_suite_name": "marts_suite",
            },
            {
                "batch_request": BatchRequest(
                    datasource_name="postgres_datasource",
                    data_connector_name="default_inferred_data_connector_name",
                    data_asset_name="marts.fct_forecasts",
                ),
                "expectation_suite_name": "marts_suite",
            }
        ],
    )
    
    return checkpoint
