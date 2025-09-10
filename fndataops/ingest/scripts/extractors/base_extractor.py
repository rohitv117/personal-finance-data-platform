"""
Base extractor class for financial data standardization
"""
import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Any
import pandas as pd

# Configure logging with PII redaction
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """Base class for all financial data extractors"""
    
    def __init__(self, institution: str, redact_pii: bool = True):
        self.institution = institution
        self.redact_pii = redact_pii
        
    def hash_merchant(self, merchant: str, salt: str = "finops_salt") -> str:
        """Hash merchant name for privacy"""
        if not merchant or not self.redact_pii:
            return merchant
        return hashlib.sha256(f"{merchant}{salt}".encode()).hexdigest()[:16]
    
    def redact_log(self, message: str, **kwargs) -> str:
        """Redact PII from log messages"""
        if not self.redact_pii:
            return message
        # Simple PII redaction - replace common patterns
        redacted = message
        for key, value in kwargs.items():
            if isinstance(value, str) and len(value) > 3:
                redacted = redacted.replace(value, f"[REDACTED_{key.upper()}]")
        return redacted
    
    def generate_txn_id(self, posted_at: datetime, amount: float, 
                        merchant: str, description: str) -> str:
        """Generate unique transaction ID"""
        # Create a deterministic hash based on transaction attributes
        hash_input = f"{posted_at.isoformat()}{amount}{merchant}{description}{self.institution}"
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    def standardize_amount(self, amount: Any, is_credit: bool = False) -> float:
        """Standardize amount format"""
        if pd.isna(amount):
            return 0.0
        
        try:
            # Handle string amounts
            if isinstance(amount, str):
                # Remove currency symbols and commas
                amount = amount.replace('$', '').replace(',', '').strip()
            
            amount_float = float(amount)
            
            # For credit cards, negative amounts are typically expenses
            if is_credit and amount_float > 0:
                amount_float = -amount_float
                
            return round(amount_float, 2)
        except (ValueError, TypeError):
            logger.warning(f"Could not parse amount: {amount}")
            return 0.0
    
    def standardize_date(self, date_input: Any) -> Optional[datetime]:
        """Standardize date format"""
        if pd.isna(date_input):
            return None
            
        try:
            if isinstance(date_input, str):
                # Try common date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        return datetime.strptime(date_input, fmt)
                    except ValueError:
                        continue
                # If none work, try pandas parsing
                return pd.to_datetime(date_input)
            elif isinstance(date_input, datetime):
                return date_input
            else:
                return pd.to_datetime(date_input)
        except Exception as e:
            logger.warning(f"Could not parse date: {date_input}, error: {e}")
            return None
    
    def standardize_currency(self, currency: Any) -> str:
        """Standardize currency codes"""
        if pd.isna(currency):
            return 'USD'
        
        currency_str = str(currency).strip().upper()
        
        # Common currency mappings
        currency_map = {
            'US': 'USD', 'USA': 'USD', 'DOLLAR': 'USD', '$': 'USD',
            'EURO': 'EUR', 'EU': 'EUR', '€': 'EUR',
            'POUND': 'GBP', 'UK': 'GBP', '£': 'GBP'
        }
        
        return currency_map.get(currency_str, currency_str)
    
    def detect_channel(self, description: str, mcc: str = None) -> str:
        """Detect transaction channel"""
        if not description:
            return 'unknown'
            
        description_lower = description.lower()
        
        # Channel detection logic
        if any(word in description_lower for word in ['pos', 'point of sale', 'debit']):
            return 'pos'
        elif any(word in description_lower for word in ['online', 'ecommerce', 'web']):
            return 'ecom'
        elif any(word in description_lower for word in ['ach', 'transfer', 'wire']):
            return 'ach'
        elif any(word in description_lower for word in ['zelle', 'venmo', 'paypal']):
            return 'zelle'
        elif any(word in description_lower for word in ['atm', 'withdrawal']):
            return 'atm'
        else:
            return 'unknown'
    
    @abstractmethod
    def extract(self, file_path: str) -> pd.DataFrame:
        """Extract data from file and return standardized DataFrame"""
        pass
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform extracted data to standard format"""
        # Ensure required columns exist
        required_columns = ['posted_at', 'amount', 'merchant_raw', 'description']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Apply standardizations
        df['institution'] = self.institution
        df['currency'] = df.get('currency', 'USD').apply(self.standardize_currency)
        df['channel'] = df.apply(
            lambda row: self.detect_channel(
                row.get('description', ''), 
                row.get('mcc', '')
            ), axis=1
        )
        
        # Generate transaction IDs
        df['txn_id'] = df.apply(
            lambda row: self.generate_txn_id(
                row['posted_at'], 
                row['amount'], 
                row.get('merchant_raw', ''), 
                row.get('description', '')
            ), axis=1
        )
        
        # Add import metadata
        df['import_batch_id'] = f"{self.institution}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        df['created_at'] = datetime.now()
        
        # Redact PII in logs
        logger.info(self.redact_log(
            f"Processed {len(df)} transactions from {self.institution}",
            merchant=df['merchant_raw'].iloc[0] if len(df) > 0 else None
        ))
        
        return df
    
    def load_to_postgres(self, df: pd.DataFrame, connection_string: str) -> bool:
        """Load standardized data to PostgreSQL"""
        try:
            import psycopg2
            from sqlalchemy import create_engine
            
            # Create SQLAlchemy engine
            engine = create_engine(connection_string)
            
            # Load to raw.transactions table
            df.to_sql(
                'transactions', 
                engine, 
                schema='raw', 
                if_exists='append', 
                index=False,
                method='multi'
            )
            
            logger.info(f"Successfully loaded {len(df)} transactions to PostgreSQL")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load data to PostgreSQL: {e}")
            return False 