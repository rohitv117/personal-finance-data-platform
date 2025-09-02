"""
Synthetic financial data generator for demo and testing
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict
import random

class FinancialDataGenerator:
    """Generate synthetic financial transaction data"""
    
    def __init__(self, start_date: datetime = None, end_date: datetime = None):
        self.start_date = start_date or (datetime.now() - timedelta(days=365))
        self.end_date = end_date or datetime.now()
        
        # Sample merchants by category
        self.merchants = {
            'Food & Dining': [
                'Starbucks', 'Chipotle', 'McDonalds', 'Subway', 'Pizza Hut',
                'Dominoes', 'KFC', 'Burger King', 'Taco Bell', 'Wendys',
                'Panera Bread', 'Olive Garden', 'Red Lobster', 'Applebee\'s',
                'Buffalo Wild Wings', 'Five Guys', 'Shake Shack', 'In-N-Out'
            ],
            'Transportation': [
                'Uber', 'Lyft', 'Shell', 'Exxon', 'BP', 'Chevron',
                'Mobil', 'Valero', 'Marathon', 'Costco Gas', 'Sam\'s Club Gas',
                'Public Transit', 'Parking Garage', 'Toll Road', 'Car Wash'
            ],
            'Shopping': [
                'Amazon', 'Walmart', 'Target', 'Costco', 'Sam\'s Club',
                'Best Buy', 'Home Depot', 'Lowes', 'Macy\'s', 'Nordstrom',
                'Kohl\'s', 'JCPenney', 'Sears', 'Marshalls', 'TJ Maxx',
                'Ross', 'Burlington', 'Dollar General', 'Family Dollar'
            ],
            'Entertainment': [
                'Netflix', 'Hulu', 'Disney+', 'HBO Max', 'Amazon Prime',
                'Spotify', 'Apple Music', 'YouTube Premium', 'Movie Theater',
                'Concert Venue', 'Sports Event', 'Museum', 'Zoo', 'Theme Park'
            ],
            'Healthcare': [
                'CVS Pharmacy', 'Walgreens', 'Rite Aid', 'Doctor Office',
                'Dentist', 'Hospital', 'Urgent Care', 'Pharmacy', 'Lab Work'
            ],
            'Utilities': [
                'Electric Company', 'Gas Company', 'Water Company',
                'Internet Provider', 'Phone Company', 'Cable Company'
            ],
            'Housing': [
                'Rent Payment', 'Mortgage Payment', 'Home Insurance',
                'Property Tax', 'HOA Fees', 'Maintenance'
            ]
        }
        
        # Income sources
        self.income_sources = [
            'Salary - Company Inc', 'Freelance - Client A', 'Freelance - Client B',
            'Investment Returns', 'Dividend Payment', 'Interest Payment',
            'Refund - Amazon', 'Refund - Store', 'Cashback - Credit Card'
        ]
        
        # Transaction amounts by category (typical ranges)
        self.amount_ranges = {
            'Food & Dining': (5, 50),
            'Transportation': (20, 100),
            'Shopping': (10, 200),
            'Entertainment': (10, 150),
            'Healthcare': (15, 300),
            'Utilities': (50, 300),
            'Housing': (800, 3000)
        }
    
    def generate_dates(self, num_transactions: int) -> List[datetime]:
        """Generate random dates within the specified range"""
        date_range = (self.end_date - self.start_date).days
        random_days = np.random.randint(0, date_range, num_transactions)
        dates = [self.start_date + timedelta(days=days) for days in random_days]
        return sorted(dates)
    
    def generate_transaction(self, date: datetime, category: str, 
                           is_income: bool = False) -> Dict:
        """Generate a single transaction"""
        if is_income:
            merchant = random.choice(self.income_sources)
            amount = random.uniform(100, 5000)  # Income amounts
        else:
            merchant = random.choice(self.merchants.get(category, ['Unknown']))
            min_amount, max_amount = self.amount_ranges.get(category, (10, 100))
            amount = -random.uniform(min_amount, max_amount)  # Negative for expenses
        
        return {
            'posted_at': date,
            'amount': round(amount, 2),
            'merchant_raw': merchant,
            'category_raw': category,
            'description': f"Transaction at {merchant}",
            'mcc': str(random.randint(1000, 9999)),
            'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']),
            'state': random.choice(['NY', 'CA', 'IL', 'TX', 'AZ']),
            'country': 'US',
            'channel': random.choice(['pos', 'ecom', 'ach', 'zelle']),
            'institution': random.choice(['Chase', 'Bank of America', 'Wells Fargo', 'American Express'])
        }
    
    def generate_dataset(self, num_transactions: int = 10000) -> pd.DataFrame:
        """Generate a complete dataset"""
        print(f"Generating {num_transactions} synthetic transactions...")
        
        # Generate dates
        dates = self.generate_dates(num_transactions)
        
        # Generate transactions
        transactions = []
        
        # Income transactions (about 10% of total)
        num_income = int(num_transactions * 0.1)
        for i in range(num_income):
            date = dates[i]
            category = random.choice(['Salary', 'Freelance', 'Investment Returns'])
            transactions.append(self.generate_transaction(date, category, is_income=True))
        
        # Expense transactions (about 90% of total)
        num_expenses = num_transactions - num_income
        expense_categories = list(self.merchants.keys())
        
        for i in range(num_expenses):
            date = dates[i + num_income]
            category = random.choice(expense_categories)
            transactions.append(self.generate_transaction(date, category, is_income=False))
        
        # Convert to DataFrame and sort by date
        df = pd.DataFrame(transactions)
        df = df.sort_values('posted_at').reset_index(drop=True)
        
        # Add account_id
        df['account_id'] = df['institution'].apply(lambda x: f"{x}_001")
        
        print(f"Generated {len(df)} transactions from {df['posted_at'].min()} to {df['posted_at'].max()}")
        print(f"Income transactions: {len(df[df['amount'] > 0])}")
        print(f"Expense transactions: {len(df[df['amount'] < 0])}")
        
        return df
    
    def save_to_csv(self, df: pd.DataFrame, filepath: str):
        """Save dataset to CSV file"""
        df.to_csv(filepath, index=False)
        print(f"Saved dataset to {filepath}")
    
    def generate_multiple_files(self, output_dir: str = "data", 
                               num_files: int = 5, 
                               transactions_per_file: int = 2000):
        """Generate multiple CSV files for different institutions"""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        institutions = ['Chase', 'Bank of America', 'Wells Fargo', 'American Express']
        
        for i in range(num_files):
            institution = institutions[i % len(institutions)]
            filename = f"{institution.lower().replace(' ', '_')}_transactions.csv"
            filepath = os.path.join(output_dir, filename)
            
            # Generate data for this institution
            df = self.generate_dataset(transactions_per_file)
            df['institution'] = institution
            
            # Save to CSV
            self.save_to_csv(df, filepath)
        
        print(f"Generated {num_files} CSV files in {output_dir}/")

if __name__ == "__main__":
    # Example usage
    generator = FinancialDataGenerator()
    
    # Generate a single large dataset
    df = generator.generate_dataset(50000)
    generator.save_to_csv(df, "data/synthetic_transactions.csv")
    
    # Generate multiple institution-specific files
    generator.generate_multiple_files(num_files=4, transactions_per_file=15000) 