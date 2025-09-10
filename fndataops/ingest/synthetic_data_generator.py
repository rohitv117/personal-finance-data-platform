"""
Synthetic data generator for FinDataOps platform
Creates realistic financial transaction data for testing and development
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional
import hashlib
import json
import argparse
import logging
from faker import Faker
import uuid


class SyntheticDataGenerator:
    """Generates synthetic financial transaction data"""
    
    def __init__(self, seed: int = 42):
        self.fake = Faker()
        Faker.seed(seed)
        np.random.seed(seed)
        random.seed(seed)
        
        self.logger = logging.getLogger("synthetic_data_generator")
        
        # Configuration
        self.institutions = [
            {"name": "Chase Bank", "type": "bank", "abbrev": "CHASE"},
            {"name": "Bank of America", "type": "bank", "abbrev": "BOA"},
            {"name": "Wells Fargo", "type": "bank", "abbrev": "WF"},
            {"name": "Capital One", "type": "card", "abbrev": "CAP1"},
            {"name": "American Express", "type": "card", "abbrev": "AMEX"},
            {"name": "Fidelity", "type": "brokerage", "abbrev": "FID"},
            {"name": "Vanguard", "type": "brokerage", "abbrev": "VANG"},
            {"name": "Charles Schwab", "type": "brokerage", "abbrev": "SCHW"}
        ]
        
        self.categories = [
            {"name": "Food & Dining", "subcategories": ["Groceries", "Restaurants", "Coffee"]},
            {"name": "Transportation", "subcategories": ["Gas", "Public Transit", "Rideshare"]},
            {"name": "Entertainment", "subcategories": ["Streaming", "Movies", "Sports"]},
            {"name": "Technology", "subcategories": ["Software", "Hardware", "Internet"]},
            {"name": "Healthcare", "subcategories": ["Insurance", "Medical", "Pharmacy"]},
            {"name": "Utilities", "subcategories": ["Electric", "Water", "Internet"]},
            {"name": "Online Shopping", "subcategories": ["Amazon", "eBay", "Other"]},
            {"name": "Clothing", "subcategories": ["Apparel", "Shoes", "Accessories"]},
            {"name": "Income", "subcategories": ["Salary", "Bonus", "Interest"]},
            {"name": "Investment", "subcategories": ["Stocks", "Bonds", "Mutual Funds"]},
            {"name": "Transfers", "subcategories": ["Bank Transfer", "Venmo", "PayPal"]},
            {"name": "Other", "subcategories": ["Miscellaneous", "Gifts", "Donations"]}
        ]
        
        self.merchants = [
            # Food & Dining
            "Amazon", "Walmart", "Target", "Costco", "Whole Foods", "Safeway",
            "McDonald's", "Starbucks", "Subway", "Chipotle", "Pizza Hut", "Domino's",
            "Uber Eats", "DoorDash", "Grubhub",
            
            # Transportation
            "Shell", "Exxon", "Chevron", "BP", "Uber", "Lyft", "Metro", "BART",
            
            # Entertainment
            "Netflix", "Spotify", "Disney+", "Hulu", "YouTube Premium", "AMC", "Regal",
            
            # Technology
            "Apple", "Google", "Microsoft", "Adobe", "Salesforce", "AWS", "Azure",
            
            # Healthcare
            "CVS", "Walgreens", "Rite Aid", "Blue Cross", "Aetna", "Cigna",
            
            # Utilities
            "PG&E", "Southern California Edison", "Verizon", "AT&T", "Comcast",
            
            # Online Shopping
            "eBay", "Etsy", "Best Buy", "Home Depot", "Lowe's", "IKEA",
            
            # Clothing
            "Nike", "Adidas", "Zara", "H&M", "Gap", "Old Navy", "Macy's"
        ]
        
        # Income patterns
        self.income_patterns = {
            "salary": {"frequency": "monthly", "amount_range": (3000, 8000), "variability": 0.05},
            "bonus": {"frequency": "quarterly", "amount_range": (1000, 5000), "variability": 0.3},
            "interest": {"frequency": "monthly", "amount_range": (50, 200), "variability": 0.1},
            "dividend": {"frequency": "quarterly", "amount_range": (100, 500), "variability": 0.2}
        }
        
        # Expense patterns
        self.expense_patterns = {
            "groceries": {"frequency": "weekly", "amount_range": (80, 200), "variability": 0.2},
            "restaurants": {"frequency": "weekly", "amount_range": (30, 150), "variability": 0.4},
            "gas": {"frequency": "weekly", "amount_range": (40, 80), "variability": 0.3},
            "utilities": {"frequency": "monthly", "amount_range": (100, 300), "variability": 0.1},
            "entertainment": {"frequency": "weekly", "amount_range": (20, 100), "variability": 0.5},
            "shopping": {"frequency": "monthly", "amount_range": (100, 500), "variability": 0.6}
        }
    
    def generate_transactions(self, 
                           num_transactions: int = 10000,
                           start_date: date = None,
                           end_date: date = None,
                           num_institutions: int = 3) -> pd.DataFrame:
        """Generate synthetic transaction data"""
        
        if start_date is None:
            start_date = date.today() - timedelta(days=365)
        if end_date is None:
            end_date = date.today()
        
        self.logger.info(f"Generating {num_transactions} transactions from {start_date} to {end_date}")
        
        transactions = []
        
        # Generate accounts
        accounts = self._generate_accounts(num_institutions)
        
        # Generate recurring transactions first
        recurring_transactions = self._generate_recurring_transactions(
            accounts, start_date, end_date
        )
        
        # Generate random transactions
        remaining_transactions = num_transactions - len(recurring_transactions)
        random_transactions = self._generate_random_transactions(
            accounts, remaining_transactions, start_date, end_date
        )
        
        # Combine all transactions
        all_transactions = recurring_transactions + random_transactions
        
        # Convert to DataFrame
        df = pd.DataFrame(all_transactions)
        
        # Sort by date
        df = df.sort_values('posted_at').reset_index(drop=True)
        
        # Add derived fields
        df = self._add_derived_fields(df)
        
        self.logger.info(f"Generated {len(df)} transactions across {len(accounts)} accounts")
        
        return df
    
    def _generate_accounts(self, num_institutions: int) -> List[Dict[str, Any]]:
        """Generate synthetic accounts"""
        accounts = []
        
        for i in range(num_institutions):
            institution = random.choice(self.institutions)
            account_id = f"{institution['abbrev']}_{i+1:03d}"
            
            accounts.append({
                "account_id": account_id,
                "institution": institution["name"],
                "institution_type": institution["type"],
                "account_type": self._get_account_type(institution["type"]),
                "currency": "USD",
                "opening_balance": random.uniform(1000, 10000)
            })
        
        return accounts
    
    def _get_account_type(self, institution_type: str) -> str:
        """Get account type based on institution type"""
        if institution_type == "bank":
            return random.choice(["checking", "savings"])
        elif institution_type == "card":
            return "credit"
        elif institution_type == "brokerage":
            return "investment"
        else:
            return "other"
    
    def _generate_recurring_transactions(self, 
                                       accounts: List[Dict[str, Any]],
                                       start_date: date,
                                       end_date: date) -> List[Dict[str, Any]]:
        """Generate recurring transactions (bills, subscriptions, etc.)"""
        transactions = []
        
        for account in accounts:
            # Generate salary (monthly)
            if account["institution_type"] == "bank":
                salary_transactions = self._generate_salary_transactions(
                    account, start_date, end_date
                )
                transactions.extend(salary_transactions)
            
            # Generate recurring bills
            bill_transactions = self._generate_bill_transactions(
                account, start_date, end_date
            )
            transactions.extend(bill_transactions)
            
            # Generate subscriptions
            subscription_transactions = self._generate_subscription_transactions(
                account, start_date, end_date
            )
            transactions.extend(subscription_transactions)
        
        return transactions
    
    def _generate_salary_transactions(self, 
                                    account: Dict[str, Any],
                                    start_date: date,
                                    end_date: date) -> List[Dict[str, Any]]:
        """Generate salary transactions"""
        transactions = []
        
        # Generate monthly salary
        current_date = start_date.replace(day=1)  # First day of month
        salary_amount = random.uniform(4000, 8000)
        
        while current_date <= end_date:
            # Add some variability to salary
            amount = salary_amount * random.uniform(0.95, 1.05)
            
            transactions.append({
                "txn_id": self._generate_txn_id(account["account_id"], current_date, amount),
                "source": account["institution_type"],
                "account_id": account["account_id"],
                "posted_at": current_date,
                "amount": amount,
                "currency": account["currency"],
                "merchant_raw": "SALARY",
                "mcc_raw": "6010",
                "description_raw": f"Salary - {account['institution']}",
                "category_raw": "Income",
                "counterparty_raw": account["institution"],
                "balance_after": None,  # Will be calculated later
                "hash_raw": self._generate_hash(account["account_id"], current_date, amount),
                "ingest_batch_id": f"batch_{current_date.strftime('%Y%m%d')}",
                "created_at": datetime.now()
            })
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        return transactions
    
    def _generate_bill_transactions(self, 
                                  account: Dict[str, Any],
                                  start_date: date,
                                  end_date: date) -> List[Dict[str, Any]]:
        """Generate recurring bill transactions"""
        transactions = []
        
        # Common bills
        bills = [
            {"name": "Rent", "amount_range": (1200, 2500), "day": 1},
            {"name": "Electric Bill", "amount_range": (80, 200), "day": 15},
            {"name": "Internet", "amount_range": (50, 100), "day": 20},
            {"name": "Phone Bill", "amount_range": (60, 120), "day": 25},
            {"name": "Insurance", "amount_range": (100, 300), "day": 10}
        ]
        
        for bill in bills:
            current_date = start_date.replace(day=bill["day"])
            if current_date < start_date:
                # Move to next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
            
            while current_date <= end_date:
                amount = random.uniform(*bill["amount_range"])
                
                transactions.append({
                    "txn_id": self._generate_txn_id(account["account_id"], current_date, -amount),
                    "source": account["institution_type"],
                    "account_id": account["account_id"],
                    "posted_at": current_date,
                    "amount": -amount,
                    "currency": account["currency"],
                    "merchant_raw": bill["name"],
                    "mcc_raw": "6010",
                    "description_raw": f"{bill['name']} Payment",
                    "category_raw": "Utilities",
                    "counterparty_raw": bill["name"],
                    "balance_after": None,
                    "hash_raw": self._generate_hash(account["account_id"], current_date, -amount),
                    "ingest_batch_id": f"batch_{current_date.strftime('%Y%m%d')}",
                    "created_at": datetime.now()
                })
                
                # Move to next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
        
        return transactions
    
    def _generate_subscription_transactions(self, 
                                         account: Dict[str, Any],
                                         start_date: date,
                                         end_date: date) -> List[Dict[str, Any]]:
        """Generate subscription transactions"""
        transactions = []
        
        # Common subscriptions
        subscriptions = [
            {"name": "Netflix", "amount": 15.99, "day": 5},
            {"name": "Spotify", "amount": 9.99, "day": 12},
            {"name": "Amazon Prime", "amount": 14.99, "day": 18},
            {"name": "Adobe Creative", "amount": 52.99, "day": 22},
            {"name": "Gym Membership", "amount": 49.99, "day": 28}
        ]
        
        for subscription in subscriptions:
            current_date = start_date.replace(day=subscription["day"])
            if current_date < start_date:
                # Move to next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
            
            while current_date <= end_date:
                amount = subscription["amount"]
                
                transactions.append({
                    "txn_id": self._generate_txn_id(account["account_id"], current_date, -amount),
                    "source": account["institution_type"],
                    "account_id": account["account_id"],
                    "posted_at": current_date,
                    "amount": -amount,
                    "currency": account["currency"],
                    "merchant_raw": subscription["name"],
                    "mcc_raw": "7841",
                    "description_raw": f"{subscription['name']} Subscription",
                    "category_raw": "Entertainment",
                    "counterparty_raw": subscription["name"],
                    "balance_after": None,
                    "hash_raw": self._generate_hash(account["account_id"], current_date, -amount),
                    "ingest_batch_id": f"batch_{current_date.strftime('%Y%m%d')}",
                    "created_at": datetime.now()
                })
                
                # Move to next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
        
        return transactions
    
    def _generate_random_transactions(self, 
                                    accounts: List[Dict[str, Any]],
                                    num_transactions: int,
                                    start_date: date,
                                    end_date: date) -> List[Dict[str, Any]]:
        """Generate random transactions"""
        transactions = []
        
        for _ in range(num_transactions):
            account = random.choice(accounts)
            transaction_date = self.fake.date_between(start_date, end_date)
            
            # Determine if this is income or expense
            is_income = random.random() < 0.1  # 10% chance of income
            
            if is_income:
                transaction = self._generate_income_transaction(account, transaction_date)
            else:
                transaction = self._generate_expense_transaction(account, transaction_date)
            
            transactions.append(transaction)
        
        return transactions
    
    def _generate_income_transaction(self, 
                                   account: Dict[str, Any],
                                   transaction_date: date) -> Dict[str, Any]:
        """Generate income transaction"""
        income_types = ["Bonus", "Interest", "Dividend", "Refund", "Cashback"]
        income_type = random.choice(income_types)
        
        if income_type == "Bonus":
            amount = random.uniform(1000, 5000)
        elif income_type == "Interest":
            amount = random.uniform(10, 100)
        elif income_type == "Dividend":
            amount = random.uniform(50, 500)
        else:
            amount = random.uniform(20, 200)
        
        return {
            "txn_id": self._generate_txn_id(account["account_id"], transaction_date, amount),
            "source": account["institution_type"],
            "account_id": account["account_id"],
            "posted_at": transaction_date,
            "amount": amount,
            "currency": account["currency"],
            "merchant_raw": income_type,
            "mcc_raw": "6010",
            "description_raw": f"{income_type} Payment",
            "category_raw": "Income",
            "counterparty_raw": account["institution"],
            "balance_after": None,
            "hash_raw": self._generate_hash(account["account_id"], transaction_date, amount),
            "ingest_batch_id": f"batch_{transaction_date.strftime('%Y%m%d')}",
            "created_at": datetime.now()
        }
    
    def _generate_expense_transaction(self, 
                                    account: Dict[str, Any],
                                    transaction_date: date) -> Dict[str, Any]:
        """Generate expense transaction"""
        category = random.choice(self.categories)
        subcategory = random.choice(category["subcategories"])
        merchant = random.choice(self.merchants)
        
        # Generate amount based on category
        if category["name"] == "Food & Dining":
            if subcategory == "Groceries":
                amount = random.uniform(50, 200)
            else:  # Restaurants, Coffee
                amount = random.uniform(10, 80)
        elif category["name"] == "Transportation":
            if subcategory == "Gas":
                amount = random.uniform(30, 80)
            else:  # Public Transit, Rideshare
                amount = random.uniform(5, 30)
        elif category["name"] == "Entertainment":
            amount = random.uniform(10, 100)
        elif category["name"] == "Technology":
            amount = random.uniform(20, 500)
        elif category["name"] == "Healthcare":
            amount = random.uniform(20, 300)
        elif category["name"] == "Utilities":
            amount = random.uniform(50, 200)
        elif category["name"] == "Online Shopping":
            amount = random.uniform(20, 300)
        elif category["name"] == "Clothing":
            amount = random.uniform(30, 200)
        else:
            amount = random.uniform(10, 100)
        
        return {
            "txn_id": self._generate_txn_id(account["account_id"], transaction_date, -amount),
            "source": account["institution_type"],
            "account_id": account["account_id"],
            "posted_at": transaction_date,
            "amount": -amount,
            "currency": account["currency"],
            "merchant_raw": merchant,
            "mcc_raw": self._get_mcc_code(category["name"]),
            "description_raw": f"{merchant} - {subcategory}",
            "category_raw": category["name"],
            "counterparty_raw": merchant,
            "balance_after": None,
            "hash_raw": self._generate_hash(account["account_id"], transaction_date, -amount),
            "ingest_batch_id": f"batch_{transaction_date.strftime('%Y%m%d')}",
            "created_at": datetime.now()
        }
    
    def _get_mcc_code(self, category: str) -> str:
        """Get MCC code for category"""
        mcc_codes = {
            "Food & Dining": "5814",
            "Transportation": "5541",
            "Entertainment": "7841",
            "Technology": "5732",
            "Healthcare": "5912",
            "Utilities": "4900",
            "Online Shopping": "5999",
            "Clothing": "5651",
            "Income": "6010",
            "Investment": "6012",
            "Transfers": "6010",
            "Other": "5999"
        }
        return mcc_codes.get(category, "5999")
    
    def _generate_txn_id(self, account_id: str, date: date, amount: float) -> str:
        """Generate unique transaction ID"""
        data = f"{account_id}_{date}_{amount}_{random.random()}"
        return hashlib.sha256(data.encode()).hexdigest()
    
    def _generate_hash(self, account_id: str, date: date, amount: float) -> str:
        """Generate hash for deduplication"""
        data = f"{account_id}_{date}_{amount}"
        return hashlib.md5(data.encode()).hexdigest()
    
    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add derived fields to the dataframe"""
        # Calculate running balance for each account
        df = df.sort_values(['account_id', 'posted_at'])
        df['balance_after'] = df.groupby('account_id')['amount'].cumsum()
        
        # Add opening balance
        opening_balances = df.groupby('account_id')['balance_after'].first() - df.groupby('account_id')['amount'].first()
        df['balance_after'] = df['balance_after'] + df['account_id'].map(opening_balances)
        
        return df
    
    def save_to_csv(self, df: pd.DataFrame, filepath: str):
        """Save transactions to CSV file"""
        df.to_csv(filepath, index=False)
        self.logger.info(f"Saved {len(df)} transactions to {filepath}")
    
    def save_to_json(self, df: pd.DataFrame, filepath: str):
        """Save transactions to JSON file"""
        df.to_json(filepath, orient='records', date_format='iso')
        self.logger.info(f"Saved {len(df)} transactions to {filepath}")


def main():
    """Main function for command line usage"""
    parser = argparse.ArgumentParser(description='Generate synthetic financial transaction data')
    parser.add_argument('--transactions', type=int, default=10000, help='Number of transactions to generate')
    parser.add_argument('--institutions', type=int, default=3, help='Number of institutions')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output-format', choices=['csv', 'json'], default='csv', help='Output format')
    parser.add_argument('--output-file', type=str, help='Output file path')
    parser.add_argument('--seed', type=int, default=42, help='Random seed')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date() if args.start_date else None
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date() if args.end_date else None
    
    # Generate data
    generator = SyntheticDataGenerator(seed=args.seed)
    df = generator.generate_transactions(
        num_transactions=args.transactions,
        start_date=start_date,
        end_date=end_date,
        num_institutions=args.institutions
    )
    
    # Save data
    if args.output_file:
        if args.output_format == 'csv':
            generator.save_to_csv(df, args.output_file)
        else:
            generator.save_to_json(df, args.output_file)
    else:
        # Default output
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"synthetic_transactions_{timestamp}.{args.output_format}"
        if args.output_format == 'csv':
            generator.save_to_csv(df, filename)
        else:
            generator.save_to_json(df, filename)


if __name__ == "__main__":
    main()
