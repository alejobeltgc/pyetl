"""Excel processing domain service."""

from typing import BinaryIO, List, Dict, Any, Optional
from decimal import Decimal
import re
from ..entities import Document, FinancialService, Rate, RateType, ValidationReport


class ExcelProcessorService:
    """
    Domain service for processing Excel files.
    
    Contains the core business logic for extracting financial services
    from Excel files, independent of the specific Excel parsing library.
    """
    
    def __init__(self):
        self.business_line_mapping = {
            'tarifas': 'accounts',
            'limites': 'accounts', 
            'tasas': 'loans'
        }
    
    def process_excel_data(self, file_content: BinaryIO, filename: str, 
                          document_id: str) -> Document:
        """
        Process Excel file content and extract financial services.
        
        Args:
            file_content: Excel file content
            filename: Original filename
            document_id: Unique document identifier
            
        Returns:
            Document with extracted services
        """
        # This method coordinates the processing but delegates
        # the actual Excel parsing to infrastructure adapters
        raise NotImplementedError("This method should be implemented by infrastructure adapters")
    
    def classify_business_line(self, table_type: str) -> str:
        """
        Classify business line based on table type.
        
        Args:
            table_type: The type of table/sheet
            
        Returns:
            Business line classification
        """
        table_lower = table_type.lower()
        for key, business_line in self.business_line_mapping.items():
            if key in table_lower:
                return business_line
        return 'other'
    
    def parse_colombian_number(self, value: str) -> Optional[Decimal]:
        """
        Parse Colombian number format (dots as thousands separators).
        
        Args:
            value: String value to parse
            
        Returns:
            Parsed decimal value or None if invalid
        """
        if not value or value.strip() == '':
            return None
        
        try:
            # Handle Colombian format: 1.234.567,89 or 1.234.567
            value_str = str(value).strip()
            
            # Remove currency symbols and extra spaces
            value_str = re.sub(r'[^\d.,\-]', '', value_str)
            
            if not value_str:
                return None
            
            # If comma is present, it's the decimal separator
            if ',' in value_str:
                parts = value_str.split(',')
                if len(parts) == 2:
                    integer_part = parts[0].replace('.', '')
                    decimal_part = parts[1]
                    value_str = f"{integer_part}.{decimal_part}"
                else:
                    return None
            else:
                # No comma, dots are thousands separators
                # Only treat last dot as decimal if there are 1-2 digits after it
                dot_parts = value_str.split('.')
                if len(dot_parts) > 1:
                    last_part = dot_parts[-1]
                    if len(last_part) <= 2 and len(dot_parts) > 2:
                        # Last dot is decimal separator
                        integer_parts = dot_parts[:-1]
                        decimal_part = last_part
                        value_str = f"{''.join(integer_parts)}.{decimal_part}"
                    else:
                        # All dots are thousands separators
                        value_str = value_str.replace('.', '')
            
            return Decimal(value_str)
            
        except (ValueError, TypeError, ArithmeticError):
            return None
    
    def detect_rate_type(self, value: Any, column_name: str = "") -> RateType:
        """
        Detect the type of rate based on value and context.
        
        Args:
            value: The rate value
            column_name: Column name for context
            
        Returns:
            Detected rate type
        """
        if value is None:
            return RateType.UNLIMITED
        
        value_str = str(value).lower().strip()
        
        if 'unlimited' in value_str or 'ilimitado' in value_str:
            return RateType.UNLIMITED
        
        if '%' in value_str or 'e.a.' in column_name.lower():
            return RateType.PERCENTAGE
        
        if 'gratis' in value_str and '+' in value_str:
            return RateType.CONDITIONAL
        
        # Check if it's a monetary amount
        parsed = self.parse_colombian_number(value_str)
        if parsed is not None:
            if parsed == 0:
                return RateType.UNLIMITED
            elif parsed < 1:
                return RateType.PERCENTAGE
            else:
                return RateType.FIXED
        
        return RateType.FIXED
    
    def create_rate_from_value(self, value: Any, column_name: str = "") -> Rate:
        """
        Create a Rate entity from a raw value.
        
        Args:
            value: Raw value from Excel
            column_name: Column name for context
            
        Returns:
            Rate entity
        """
        rate_type = self.detect_rate_type(value, column_name)
        
        if rate_type == RateType.UNLIMITED:
            return Rate.unlimited()
        
        # Parse numeric value
        parsed_value = self.parse_colombian_number(str(value))
        if parsed_value is None:
            parsed_value = Decimal('0')
        
        if rate_type == RateType.PERCENTAGE:
            return Rate.percentage(parsed_value)
        elif rate_type == RateType.FIXED:
            # Determine currency based on value size
            currency = "COP" if parsed_value > 1000 else None
            return Rate.fixed(parsed_value, currency)
        else:
            # Default to fixed
            return Rate.fixed(parsed_value)
    
    def generate_service_id(self, description: str, table_type: str, 
                          row_index: int) -> str:
        """
        Generate a unique service ID.
        
        Args:
            description: Service description
            table_type: Table type
            row_index: Row index in the table
            
        Returns:
            Unique service ID
        """
        # Clean description for ID
        clean_desc = re.sub(r'[^\w\s]', '', description.lower())
        clean_desc = re.sub(r'\s+', '_', clean_desc.strip())
        
        # Truncate if too long
        if len(clean_desc) > 30:
            clean_desc = clean_desc[:30]
        
        return f"{clean_desc}_{row_index}"
    
    def validate_service_data(self, service: FinancialService) -> List[str]:
        """
        Validate service data and return list of issues.
        
        Args:
            service: Service to validate
            
        Returns:
            List of validation issues
        """
        issues = []
        
        if not service.description or service.description.strip() == '':
            issues.append("Service description is empty")
        
        if not service.service_id:
            issues.append("Service ID is missing")
        
        if not service.business_line:
            issues.append("Business line is not classified")
        
        if not service.has_rates():
            issues.append("Service has no rates defined")
        
        return issues
