"""Load deterministic synthetic source rows into an isolated CI database."""
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])
sql = Path("dbt/init.sql").read_text()
# The service already creates finops; skip psql database setup directives.
sql = sql[sql.index("-- Create schemas") : sql.index("-- Reference categories table")]
with engine.begin() as conn:
    conn.exec_driver_sql(sql)

rows = []
for account in range(2):
    for month in range(1, 7):
        for day, amount, merchant, category in [
            (1, 5000, "Salary", "Salary"),
            (5, -75, "Netflix", "Entertainment"),
            (15, -100, "Electric Bill", "Utilities"),
            (20, -40, "Starbucks", "Food & Dining"),
        ]:
            rows.append(
                dict(
                    txn_id=f"{account}-{month}-{day}",
                    institution="Synthetic Bank",
                    account_id=f"checking_{account}",
                    posted_at=datetime(datetime.now().year, month, day),
                    amount=amount,
                    currency="USD",
                    merchant_raw=merchant,
                    category_raw=category,
                    import_batch_id="ci-synthetic",
                    created_at=datetime.now(),
                )
            )
pd.DataFrame(rows).to_sql(
    "transactions", engine, schema="raw", if_exists="append", index=False
)
with engine.begin() as conn:
    conn.execute(
        text(
            """
        INSERT INTO core.accounts (account_id, institution, type, currency)
        SELECT DISTINCT account_id, institution, 'checking', 'USD'
        FROM raw.transactions
    """
        )
    )
print(f"Loaded {len(rows)} synthetic transactions")
