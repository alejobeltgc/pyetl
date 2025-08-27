"""
Business rules for accounts data transformation.
"""

import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

# Import the accounts config directly
_config_path = Path(__file__).parent.parent / "config"
sys.path.insert(0, str(_config_path))
from accounts_config import ACCOUNTS_CONFIG  # noqa: E402


class AccountsBusinessRules:
    """
    Business rules specific to accounts product line.
    """
    
    def __init__(self):
        self.config = ACCOUNTS_CONFIG
    
    def classify_table_type(self, table: Dict[str, Any]) -> str:
        """
        Classify the type of table based on content and structure.
        
        Args:
            table (Dict): Table data with columns and rows
            
        Returns:
            str: Table type classification
        """
        table_name = table.get('table_name', '').lower()
        
        # Get column names from header_row
        header_row = table.get('header_row', {})
        column_names = list(header_row.values()) if header_row else []
        
        # Check for mobile plans table - debe tener las 3 columnas de planes
        plan_columns = [self.config['plan_types'].get(col, col) for col in column_names]
        mobile_plan_keys = ['g_zero', 'puls', 'premier']
        if all(plan in plan_columns for plan in mobile_plan_keys):
            return 'mobile_plans'
        
        # Check for traditional services table - tiene VALOR (Sin IVA)
        if 'VALOR (Sin IVA)' in column_names:
            return 'traditional_services'
        
        # Check data content for classification
        sample_descriptions = []
        if 'data' in table and table['data']:
            sample_descriptions = [
                row.get(self.config['description_field'], '').lower() 
                for row in table['data'][:3] if row.get(self.config['description_field'])  # Check first 3 rows
            ]
        
        # Classify based on patterns
        for table_type, classification in self.config['table_classification'].items():
            if table_type in ['mobile_plans', 'traditional_services']:
                continue  # Already checked above
                
            patterns = classification.get('patterns', [])
            keywords = classification.get('keywords', [])
            
            # Check table name and descriptions
            text_to_check = f"{table_name} {' '.join(sample_descriptions)}"
            
            pattern_match = any(pattern in text_to_check for pattern in patterns)
            keyword_match = any(keyword in text_to_check for keyword in keywords) if keywords else True
            
            if pattern_match and keyword_match:
                return table_type
        
        return 'unknown'
    
    def generate_service_id(self, description: str) -> str:
        """
        Generate a service ID from description text.
        
        Args:
            description (str): Service description
            
        Returns:
            str: Generated service ID
        """
        desc_lower = description.lower()
        
        # Check for specific patterns
        for pattern, service_id in self.config['service_id_patterns'].items():
            if pattern in desc_lower:
                return service_id
        
        # Fallback: create from first words
        words = re.findall(r'\b\w+\b', desc_lower)
        if words:
            # Take first 2-3 meaningful words
            meaningful_words = [w for w in words[:3] if len(w) > 2]
            return '_'.join(meaningful_words[:2]) if meaningful_words else 'unknown_service'
        
        return 'unknown_service'
    
    def normalize_frequency(self, frequency: str) -> str:
        """
        Normalize frequency values.
        
        Args:
            frequency (str): Raw frequency value
            
        Returns:
            str: Normalized frequency
        """
        if not frequency:
            return 'unknown'
        
        # Clean and normalize the frequency value
        cleaned_frequency = frequency.strip() if isinstance(frequency, str) else str(frequency)
        return self.config['frequency_mapping'].get(cleaned_frequency, cleaned_frequency.lower())
    
    def normalize_tax_application(self, tax_value: str) -> bool:
        """
        Normalize tax application values.
        
        Args:
            tax_value (str): Raw tax value
            
        Returns:
            bool: Whether tax applies
        """
        if not tax_value:
            return False
        
        return self.config['tax_mapping'].get(tax_value, False)
    
    def parse_rate_value(self, rate_value: Any) -> Dict[str, Any]:
        """
        Parse rate values, including special cases like conditional rates.
        
        Args:
            rate_value: Raw rate value (number, string, etc.)
            
        Returns:
            Dict: Parsed rate information
        """
        if rate_value is None:
            return {"type": "not_applicable", "value": 0}
        
        # Handle NaN values specifically
        if isinstance(rate_value, float) and pd.isna(rate_value):
            return {"type": "not_applicable", "value": 0}
        
        # Handle numeric values
        if isinstance(rate_value, (int, float)):
            return {"type": "fixed", "value": rate_value}
        
        # Handle string values
        rate_str = str(rate_value).strip()
        
        # Check for "No aplica" or similar
        if rate_str.lower() in ['no aplica', 'no', 'n/a', '']:
            return {"type": "not_applicable", "value": 0}
        
        # Check for conditional rates (e.g., "3 incluidos sin costo. $7.510 por transferencia adicional")
        conditional_pattern = r'(\d+)\s+incluidos?\s+sin\s+costo.*?\$?(\d+(?:[.,]\d+)?)\s*por'
        match = re.search(conditional_pattern, rate_str, re.IGNORECASE)
        
        if match:
            included_count = int(match.group(1))
            additional_cost = self._extract_numeric_value(match.group(2))
            return {
                "type": "conditional",
                "included_free": included_count,
                "additional_cost": additional_cost
            }
        
        # Check for unlimited or similar
        if any(word in rate_str.lower() for word in ['ilimitado', 'unlimited', 'incluido']):
            return {"type": "unlimited", "value": 0}
        
        # Try to extract numeric value
        numeric_value = self._extract_numeric_value(rate_str)
        if numeric_value is not None:
            return {"type": "fixed", "value": numeric_value}
        
        # Fallback: return as text
        return {"type": "text", "value": rate_str}
    
    def _extract_numeric_value(self, text: str) -> Optional[float]:
        """
        Extract numeric value from text.
        Colombian format: 8.990 = 8990 (thousands), 8,50 = 8.5 (decimal)
        
        Args:
            text (str): Text containing a number
            
        Returns:
            Optional[float]: Extracted number or None
        """
        # Remove currency symbols and clean up
        cleaned = text.strip().replace('$', '').strip()
        
        # Handle special cases
        if cleaned.lower() in ['desde 0', '0']:
            return 0.0
        
        # Pattern to match numbers - simplified
        number_pattern = r'(\d+(?:[.,]\d+)*)'
        match = re.search(number_pattern, cleaned)
        
        if match:
            number_str = match.group(1)
            
            # Convert Colombian format to standard format
            if ',' in number_str:
                if '.' in number_str:
                    # Has both: 1.234.567,89 -> remove dots, replace comma with dot
                    parts = number_str.split(',')
                    integer_part = parts[0].replace('.', '')
                    decimal_part = parts[1]
                    result = float(f"{integer_part}.{decimal_part}")
                else:
                    # Only comma: 123,45 -> replace comma with dot
                    result = float(number_str.replace(',', '.'))
            elif '.' in number_str:
                # Check if it's thousands separator (3 digits after dot) or decimal
                parts = number_str.split('.')
                if len(parts) == 2 and len(parts[1]) == 3:
                    # Thousands separator: 8.990 -> 8990
                    result = float(number_str.replace('.', ''))
                else:
                    # Decimal separator: 8.5 -> 8.5
                    result = float(number_str)
            else:
                # Plain integer: 123 -> 123
                result = float(number_str)
            
            return result
            
        return None
