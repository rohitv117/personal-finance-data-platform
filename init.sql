-- Create databases
CREATE DATABASE finops;
CREATE DATABASE airflow;

-- Connect to finops database
\c finops;

-- Create schemas
CREATE SCHEMA raw;
CREATE SCHEMA core;
CREATE SCHEMA ref;
CREATE SCHEMA staging;
CREATE SCHEMA marts;

-- Raw transactions table
CREATE TABLE raw.transactions (
    txn_id VARCHAR(64) PRIMARY KEY,
    institution VARCHAR(100) NOT NULL,
    account_id VARCHAR(100) NOT NULL,
    posted_at TIMESTAMP NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    merchant_raw TEXT,
    mcc VARCHAR(4),
    category_raw VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    channel VARCHAR(20),
    description TEXT,
    import_batch_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Core accounts table
CREATE TABLE core.accounts (
    account_id VARCHAR(100) PRIMARY KEY,
    institution VARCHAR(100) NOT NULL,
    type VARCHAR(20) CHECK (type IN ('checking', 'savings', 'credit', 'brokerage')),
    open_dt DATE,
    close_dt DATE,
    currency VARCHAR(3) DEFAULT 'USD',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reference categories table
CREATE TABLE ref.categories (
    category_id SERIAL PRIMARY KEY,
    parent_id INTEGER REFERENCES ref.categories(category_id),
    name VARCHAR(100) NOT NULL,
    budget_group VARCHAR(50),
    is_income BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reference merchant rules table
CREATE TABLE ref.merchant_rules (
    rule_id SERIAL PRIMARY KEY,
    pattern VARCHAR(255) NOT NULL,
    normalized_merchant VARCHAR(100),
    normalized_category VARCHAR(100),
    priority INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging transactions table
CREATE TABLE staging.transactions (
    txn_id VARCHAR(64) PRIMARY KEY,
    institution VARCHAR(100) NOT NULL,
    account_id VARCHAR(100) NOT NULL,
    posted_at TIMESTAMP NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    merchant_raw TEXT,
    merchant_norm VARCHAR(100),
    mcc VARCHAR(4),
    category_raw VARCHAR(100),
    category_norm VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    channel VARCHAR(20),
    description TEXT,
    import_batch_id VARCHAR(100) NOT NULL,
    is_debit BOOLEAN,
    abs_amount DECIMAL(15,2),
    month VARCHAR(7),
    week VARCHAR(10),
    is_recurring_guess BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Marts tables
CREATE TABLE marts.mart_cashflow_daily (
    date DATE PRIMARY KEY,
    income DECIMAL(15,2) DEFAULT 0,
    expenses DECIMAL(15,2) DEFAULT 0,
    savings_rate DECIMAL(5,4),
    balance_delta DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE marts.mart_cashflow_monthly (
    month VARCHAR(7) PRIMARY KEY,
    income DECIMAL(15,2) DEFAULT 0,
    expenses DECIMAL(15,2) DEFAULT 0,
    savings_rate DECIMAL(5,4),
    balance_delta DECIMAL(15,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE marts.mart_budget_vs_actual (
    id SERIAL PRIMARY KEY,
    month VARCHAR(7) NOT NULL,
    category_id INTEGER REFERENCES ref.categories(category_id),
    budget DECIMAL(15,2),
    actual DECIMAL(15,2),
    variance DECIMAL(15,2),
    variance_pct DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE marts.mart_recurring (
    id SERIAL PRIMARY KEY,
    merchant_norm VARCHAR(100),
    category_norm VARCHAR(100),
    next_due_date DATE,
    average_amount DECIMAL(15,2),
    frequency VARCHAR(20),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE marts.mart_anomalies (
    id SERIAL PRIMARY KEY,
    txn_id VARCHAR(64) REFERENCES raw.transactions(txn_id),
    anomaly_type VARCHAR(50),
    severity VARCHAR(20),
    driver VARCHAR(50),
    remediation_hint TEXT,
    flagged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE marts.mart_forecasts (
    id SERIAL PRIMARY KEY,
    forecast_date DATE,
    category_id INTEGER REFERENCES ref.categories(category_id),
    forecast_amount DECIMAL(15,2),
    lower_bound DECIMAL(15,2),
    upper_bound DECIMAL(15,2),
    confidence_level DECIMAL(3,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_raw_transactions_posted_at ON raw.transactions(posted_at);
CREATE INDEX idx_raw_transactions_account_id ON raw.transactions(account_id);
CREATE INDEX idx_raw_transactions_institution ON raw.transactions(institution);
CREATE INDEX idx_staging_transactions_month ON staging.transactions(month);
CREATE INDEX idx_staging_transactions_category ON staging.transactions(category_norm);
CREATE INDEX idx_marts_cashflow_daily_date ON marts.mart_cashflow_daily(date);
CREATE INDEX idx_marts_cashflow_monthly_month ON marts.mart_cashflow_monthly(month);

-- Insert sample categories
INSERT INTO ref.categories (name, budget_group, is_income) VALUES
('Salary', 'Income', TRUE),
('Freelance', 'Income', TRUE),
('Investment Returns', 'Income', TRUE),
('Food & Dining', 'Essential', FALSE),
('Transportation', 'Essential', FALSE),
('Housing', 'Essential', FALSE),
('Utilities', 'Essential', FALSE),
('Entertainment', 'Discretionary', FALSE),
('Shopping', 'Discretionary', FALSE),
('Healthcare', 'Essential', FALSE);

-- Insert sample merchant rules
INSERT INTO ref.merchant_rules (pattern, normalized_merchant, normalized_category, priority) VALUES
('STARBUCKS', 'Starbucks', 'Food & Dining', 1),
('UBER', 'Uber', 'Transportation', 1),
('AMAZON', 'Amazon', 'Shopping', 1),
('NETFLIX', 'Netflix', 'Entertainment', 1),
('WALMART', 'Walmart', 'Shopping', 1),
('TARGET', 'Target', 'Shopping', 1),
('COSTCO', 'Costco', 'Shopping', 1),
('CHIPOTLE', 'Chipotle', 'Food & Dining', 1),
('MCDONALDS', 'McDonalds', 'Food & Dining', 1),
('SHELL', 'Shell', 'Transportation', 1);

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE finops TO finops_user;
GRANT ALL PRIVILEGES ON SCHEMA raw TO finops_user;
GRANT ALL PRIVILEGES ON SCHEMA core TO finops_user;
GRANT ALL PRIVILEGES ON SCHEMA ref TO finops_user;
GRANT ALL PRIVILEGES ON SCHEMA staging TO finops_user;
GRANT ALL PRIVILEGES ON SCHEMA marts TO finops_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO finops_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA core TO finops_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ref TO finops_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO finops_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts TO finops_user; 