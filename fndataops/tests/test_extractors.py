"""
Tests for financial data extractors
"""
import pytest
import pandas as pd
from datetime import datetime
import sys
import os

# Add scripts to path for testing
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extractors.base_extractor import BaseExtractor
from extractors.csv_extractor import CSVExtractor

class TestBaseExtractor:
    """Test the base extractor functionality"""
    
    def test_hash_merchant(self):
        """Test merchant hashing for privacy"""
        extractor = BaseExtractor("TestBank", redact_pii=True)
        
        # Test hashing
        hashed = extractor.hash_merchant("Starbucks")
        assert hashed != "Starbucks"
        assert len(hashed) == 16
        
        # Test no hashing when disabled
        extractor.redact_pii = False
        unhashed = extractor.hash_merchant("Starbucks")
        assert unhashed == "Starbucks"
    
    def test_standardize_amount(self):
        """Test amount standardization"""
        extractor = BaseExtractor("TestBank")
        
        # Test various formats
        assert extractor.standardize_amount("100.50") == 100.50
        assert extractor.standardize_amount("$1,000.00") == 1000.00
        assert extractor.standardize_amount(-50.25) == -50.25
        assert extractor.standardize_amount(None) == 0.0
    
    def test_standardize_date(self):
        """Test date standardization"""
        extractor = BaseExtractor("TestBank")
        
        # Test various formats
        date1 = extractor.standardize_date("2024-01-15")
        assert isinstance(date1, datetime)
        assert date1.year == 2024
        assert date1.month == 1
        assert date1.day == 15
        
        # Test invalid date
        assert extractor.standardize_date("invalid") is None
    
    def test_detect_channel(self):
        """Test transaction channel detection"""
        extractor = BaseExtractor("TestBank")
        
        assert extractor.detect_channel("POS transaction") == "pos"
        assert extractor.detect_channel("Online purchase") == "ecom"
        assert extractor.detect_channel("ACH transfer") == "ach"
        assert extractor.detect_channel("Zelle payment") == "zelle"
        assert extractor.detect_channel("Unknown") == "unknown"

class TestCSVExtractor:
    """Test CSV extractor functionality"""
    
    def test_chase_extractor_creation(self):
        """Test Chase extractor factory method"""
        extractor = CSVExtractor.create_chase_extractor()
        assert extractor.institution == "Chase"
        assert extractor.is_credit == False
        assert "Transaction Date" in extractor.column_mapping
    
    def test_amex_extractor_creation(self):
        """Test American Express extractor factory method"""
        extractor = CSVExtractor.create_amex_extractor()
        assert extractor.institution == "American Express"
        assert extractor.is_credit == True
        assert "Date" in extractor.column_mapping
    
    def test_extractor_initialization(self):
        """Test custom extractor initialization"""
        column_mapping = {"Date": "posted_at", "Amount": "amount"}
        extractor = CSVExtractor("CustomBank", column_mapping)
        
        assert extractor.institution == "CustomBank"
        assert extractor.column_mapping == column_mapping
        assert extractor.is_credit == False

class TestDataGenerator:
    """Test synthetic data generation"""
    
    def test_data_generator_import(self):
        """Test that data generator can be imported"""
        try:
            from data_generator import FinancialDataGenerator
            generator = FinancialDataGenerator()
            assert generator is not None
        except ImportError:
            pytest.skip("Data generator not available for testing")
    
    def test_generator_merchants(self):
        """Test merchant category mapping"""
        try:
            from data_generator import FinancialDataGenerator
            generator = FinancialDataGenerator()
            
            # Check that merchants are properly categorized
            assert "Starbucks" in generator.merchants["Food & Dining"]
            assert "Amazon" in generator.merchants["Shopping"]
            assert "Netflix" in generator.merchants["Entertainment"]
            
        except ImportError:
            pytest.skip("Data generator not available for testing")

if __name__ == "__main__":
    pytest.main([__file__]) 