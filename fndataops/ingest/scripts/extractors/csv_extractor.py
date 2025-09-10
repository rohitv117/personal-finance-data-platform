"""
CSV extractor for financial data files
"""
import pandas as pd
from typing import Dict, Any
from .base_extractor import BaseExtractor

class CSVExtractor(BaseExtractor):
    """Extract financial data from CSV files"""
    
    def __init__(self, institution: str, column_mapping: Dict[str, str], 
                 redact_pii: bool = True, is_credit: bool = False):
        super().__init__(institution, redact_pii)
        self.column_mapping = column_mapping
        self.is_credit = is_credit
    
    def extract(self, file_path: str) -> pd.DataFrame:
        """Extract data from CSV file"""
        try:
            # Read CSV with flexible parsing
            df = pd.read_csv(
                file_path,
                parse_dates=True,
                infer_datetime_format=True,
                low_memory=False
            )
            
            # Rename columns according to mapping
            df = df.rename(columns=self.column_mapping)
            
            # Ensure required columns exist
            required_columns = ['posted_at', 'amount', 'merchant_raw', 'description']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns after mapping: {missing_columns}")
            
            # Apply standardizations
            df['posted_at'] = df['posted_at'].apply(self.standardize_date)
            df['amount'] = df['amount'].apply(lambda x: self.standardize_amount(x, self.is_credit))
            
            # Handle optional columns
            if 'mcc' not in df.columns:
                df['mcc'] = None
            if 'category_raw' not in df.columns:
                df['category_raw'] = None
            if 'city' not in df.columns:
                df['city'] = None
            if 'state' not in df.columns:
                df['state'] = None
            if 'country' not in df.columns:
                df['country'] = None
            
            # Clean up merchant names
            df['merchant_raw'] = df['merchant_raw'].fillna('').astype(str).str.strip()
            df['description'] = df['description'].fillna('').astype(str).str.strip()
            
            # Remove rows with invalid dates or amounts
            df = df.dropna(subset=['posted_at', 'amount'])
            df = df[df['amount'] != 0]  # Remove zero-amount transactions
            
            return df
            
        except Exception as e:
            raise Exception(f"Failed to extract CSV data: {e}")
    
    @classmethod
    def create_chase_extractor(cls, redact_pii: bool = True) -> 'CSVExtractor':
        """Create extractor for Chase bank CSV exports"""
        column_mapping = {
            'Transaction Date': 'posted_at',
            'Post Date': 'posted_at',
            'Description': 'merchant_raw',
            'Category': 'category_raw',
            'Type': 'description',
            'Amount': 'amount',
            'Memo': 'description'
        }
        return cls('Chase', column_mapping, redact_pii, is_credit=False)
    
    @classmethod
    def create_amex_extractor(cls, redact_pii: bool = True) -> 'CSVExtractor':
        """Create extractor for American Express CSV exports"""
        column_mapping = {
            'Date': 'posted_at',
            'Description': 'merchant_raw',
            'Category': 'category_raw',
            'Amount': 'amount',
            'Reference': 'description'
        }
        return cls('American Express', column_mapping, redact_pii, is_credit=True)
    
    @classmethod
    def create_bank_of_america_extractor(cls, redact_pii: bool = True) -> 'CSVExtractor':
        """Create extractor for Bank of America CSV exports"""
        column_mapping = {
            'Date': 'posted_at',
            'Description': 'merchant_raw',
            'Amount': 'amount',
            'Type': 'description',
            'Balance': 'description'
        }
        return cls('Bank of America', column_mapping, redact_pii, is_credit=False)
    
    @classmethod
    def create_wells_fargo_extractor(cls, redact_pii: bool = True) -> 'CSVExtractor':
        """Create extractor for Wells Fargo CSV exports"""
        column_mapping = {
            'Date': 'posted_at',
            'Description': 'merchant_raw',
            'Amount': 'amount',
            'Type': 'description',
            'Balance': 'description'
        }
        return cls('Wells Fargo', column_mapping, redact_pii, is_credit=False) 