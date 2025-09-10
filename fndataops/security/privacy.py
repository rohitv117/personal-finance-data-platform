"""
Privacy and security utilities for FinDataOps platform
"""

import hashlib
import re
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json


class PIIRedactor:
    """Redacts PII from data and logs"""
    
    def __init__(self):
        self.logger = logging.getLogger("pii_redactor")
        
        # PII patterns
        self.patterns = {
            'credit_card': r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'\b\d{3}-\d{3}-\d{4}\b',
            'account_number': r'\b\d{8,12}\b'
        }
    
    def redact_text(self, text: str) -> str:
        """Redact PII from text"""
        if not text:
            return text
        
        redacted = text
        for pii_type, pattern in self.patterns.items():
            redacted = re.sub(pattern, f'[{pii_type.upper()}]', redacted)
        
        return redacted
    
    def redact_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Redact PII from dictionary"""
        redacted = {}
        for key, value in data.items():
            if isinstance(value, str):
                redacted[key] = self.redact_text(value)
            elif isinstance(value, dict):
                redacted[key] = self.redact_dict(value)
            elif isinstance(value, list):
                redacted[key] = [self.redact_text(str(item)) if isinstance(item, str) else item for item in value]
            else:
                redacted[key] = value
        
        return redacted


class DataEncryption:
    """Handles data encryption and hashing"""
    
    def __init__(self, salt: str = "finops_salt_2024"):
        self.salt = salt
        self.logger = logging.getLogger("data_encryption")
    
    def hash_merchant(self, merchant: str) -> str:
        """Hash merchant name for privacy"""
        if not merchant:
            return merchant
        
        data = f"{merchant}{self.salt}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
    
    def hash_account_id(self, account_id: str) -> str:
        """Hash account ID for privacy"""
        if not account_id:
            return account_id
        
        data = f"{account_id}{self.salt}"
        return hashlib.sha256(data.encode()).hexdigest()[:12]
    
    def generate_audit_hash(self, data: Dict[str, Any]) -> str:
        """Generate audit hash for data integrity"""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()


class AccessControl:
    """Handles access control and permissions"""
    
    def __init__(self):
        self.logger = logging.getLogger("access_control")
        self.permissions = {
            'owner': ['read', 'write', 'delete', 'admin'],
            'viewer': ['read'],
            'analyst': ['read', 'write'],
            'admin': ['read', 'write', 'delete', 'admin']
        }
    
    def check_permission(self, user_role: str, action: str) -> bool:
        """Check if user has permission for action"""
        if user_role not in self.permissions:
            return False
        
        return action in self.permissions[user_role]
    
    def get_allowed_actions(self, user_role: str) -> List[str]:
        """Get list of allowed actions for user role"""
        return self.permissions.get(user_role, [])


class AuditLogger:
    """Handles audit logging for security"""
    
    def __init__(self):
        self.logger = logging.getLogger("audit")
        self.audit_events = []
    
    def log_access(self, user_id: str, resource: str, action: str, success: bool):
        """Log access attempt"""
        event = {
            'timestamp': datetime.now().isoformat(),
            'event_type': 'access',
            'user_id': user_id,
            'resource': resource,
            'action': action,
            'success': success
        }
        
        self.audit_events.append(event)
        self.logger.info(f"Access: {user_id} {action} {resource} - {'SUCCESS' if success else 'FAILED'}")
    
    def log_data_change(self, user_id: str, table: str, operation: str, record_count: int):
        """Log data change"""
        event = {
            'timestamp': datetime.now().isoformat(),
            'event_type': 'data_change',
            'user_id': user_id,
            'table': table,
            'operation': operation,
            'record_count': record_count
        }
        
        self.audit_events.append(event)
        self.logger.info(f"Data Change: {user_id} {operation} {record_count} records in {table}")
    
    def get_audit_trail(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get audit trail"""
        if user_id:
            return [event for event in self.audit_events if event.get('user_id') == user_id]
        return self.audit_events


# Global instances
pii_redactor = PIIRedactor()
data_encryption = DataEncryption()
access_control = AccessControl()
audit_logger = AuditLogger()
