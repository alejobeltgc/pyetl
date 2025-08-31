"""Extraction strategy for loans business line."""

import re
from typing import List, Dict, Any, Optional
from decimal import Decimal

from .extraction_strategy import ExtractionStrategy
from ..entities import FinancialService, Rate, RateType
from ..services import ExcelProcessorService


class LoansExtractionStrategy(ExtractionStrategy):
    """
    Extraction strategy for loans/credit business line.
    
    Handles Excel files with loan rates, credit limits, and
    financial products related to lending.
    """
    
    def __init__(self):
        self.excel_processor = ExcelProcessorService()
        
        # Patterns specific to loans business line
        self.sheet_type_patterns = {
            'tasas_credito': [r'tasa.*credito', r'credit.*rate', r'prestamo'],
            'limites_credito': [r'limite.*credito', r'credit.*limit', r'cupo'],
            'productos': [r'producto', r'product', r'credito'],
            'comisiones': [r'comision', r'commission', r'fee']
        }
        
        # Header patterns for loans data
        self.description_patterns = [
            r'producto', r'credito', r'prestamo', r'linea',
            r'product', r'credit', r'loan', r'line'
        ]
        
        self.rate_type_patterns = [
            r'tasa.*efectiva', r'tasa.*nominal', r'ea', r'mv', r'tv'
        ]
    
    @property
    def business_line(self) -> str:
        return 'loans'
    
    @property
    def supported_sheet_patterns(self) -> List[str]:
        patterns = []
        for sheet_patterns in self.sheet_type_patterns.values():
            patterns.extend(sheet_patterns)
        return patterns
    
    def classify_sheet_type(self, sheet_name: str) -> str:
        """Classify sheet type for loans business line."""
        sheet_lower = sheet_name.lower()
        
        for sheet_type, patterns in self.sheet_type_patterns.items():
            for pattern in patterns:
                if re.search(pattern, sheet_lower):
                    return sheet_type
        
        return 'other'
    
    def should_process_sheet(self, sheet_name: str) -> bool:
        """Check if sheet should be processed for loans."""
        sheet_type = self.classify_sheet_type(sheet_name)
        return sheet_type != 'other'
    
    def find_data_start_row(self, sheet_data: List[List[Any]], sheet_name: str) -> Optional[int]:
        """Find where data starts in loans sheets."""
        for row_idx, row in enumerate(sheet_data[:10]):
            if not row:
                continue
                
            row_text = [str(cell).lower().strip() if cell else '' for cell in row]
            
            # Look for loan-specific indicators
            loan_indicators = any(
                any(re.search(pattern, cell) for pattern in self.description_patterns)
                for cell in row_text
                if cell
            )
            
            rate_indicators = any(
                any(re.search(pattern, cell) for pattern in self.rate_type_patterns)
                for cell in row_text
                if cell
            )
            
            if loan_indicators or rate_indicators:
                return row_idx + 1
        
        return 1 if len(sheet_data) > 1 else None
    
    def extract_headers(self, sheet_data: List[List[Any]], header_row: int) -> List[str]:
        """Extract and normalize headers for loans data."""
        if header_row >= len(sheet_data):
            return []
        
        raw_headers = sheet_data[header_row - 1] if header_row > 0 else sheet_data[0]
        headers = []
        
        for i, header in enumerate(raw_headers):
            if header is None:
                normalized = f'column_{i + 1}'
            else:
                normalized = str(header).strip().lower()
                normalized = re.sub(r'[^\w\s\.]', ' ', normalized)
                normalized = re.sub(r'\s+', '_', normalized)
                normalized = normalized.strip('_')
                
                if not normalized:
                    normalized = f'column_{i + 1}'
            
            headers.append(normalized)
        
        return headers
    
    def extract_service_from_row(self, row_data: List[Any], headers: List[str], 
                                sheet_name: str, row_index: int, 
                                document_id: str) -> Optional[FinancialService]:
        """Extract loan service from data row."""
        if not row_data:
            return None
        
        # Create row dictionary
        row_dict = {}
        for i, header in enumerate(headers):
            value = row_data[i] if i < len(row_data) else None
            row_dict[header] = value
        
        # Find loan product description
        description = self._find_loan_description(row_dict)
        if not description:
            return None
        
        # Generate service ID and classify
        sheet_type = self.classify_sheet_type(sheet_name)
        table_type = f"{self.business_line}_{sheet_type}"
        service_id = self.excel_processor.generate_service_id(
            description, table_type, row_index
        )
        
        # Create service
        service = FinancialService(
            service_id=service_id,
            description=description,
            business_line=self.business_line,
            table_type=table_type,
            document_id=document_id,
            source_position={
                'sheet': sheet_name,
                'row': row_index,
                'headers': headers
            }
        )
        
        # Extract rates for loans
        for header, value in row_dict.items():
            if self._is_rate_column(header) and value is not None:
                if str(value).strip():
                    try:
                        rate = self._create_loan_rate(value, header, sheet_type)
                        rate_name = self._normalize_rate_name(header)
                        service.add_rate(rate_name, rate)
                    except Exception:
                        continue
        
        return service
    
    def validate_extracted_data(self, services: List[FinancialService]) -> List[str]:
        """Validate loans-specific business rules."""
        errors = []
        
        # Check for reasonable interest rates
        for service in services:
            for rate_name, rate in service.rates.items():
                if rate.type == RateType.PERCENTAGE:
                    if rate.value > 50:  # Suspiciously high interest rate
                        errors.append(
                            f"High interest rate in {service.service_id}: {rate.value}%"
                        )
                    elif rate.value < 0:
                        errors.append(
                            f"Negative interest rate in {service.service_id}: {rate.value}%"
                        )
        
        return errors
    
    def _find_loan_description(self, row_dict: Dict[str, Any]) -> Optional[str]:
        """Find loan product description."""
        for header, value in row_dict.items():
            if any(re.search(pattern, header) for pattern in self.description_patterns):
                if value and str(value).strip():
                    return str(value).strip()
        
        # Fallback to first text value
        for value in row_dict.values():
            if value and str(value).strip() and not self._looks_like_number(str(value)):
                return str(value).strip()
        
        return None
    
    def _is_rate_column(self, header: str) -> bool:
        """Check if column represents a rate."""
        if any(re.search(pattern, header) for pattern in self.description_patterns):
            return False
        
        # Check for rate indicators
        rate_indicators = [r'tasa', r'rate', r'ea', r'mv', r'tv', r'%']
        return any(re.search(indicator, header) for indicator in rate_indicators)
    
    def _create_loan_rate(self, value: Any, column_name: str, sheet_type: str) -> Rate:
        """Create rate specific to loans business logic."""
        # Parse value
        parsed_value = self.excel_processor.parse_colombian_number(str(value))
        if parsed_value is None:
            parsed_value = Decimal('0')
        
        # Loans typically have percentage rates
        if parsed_value < 1 and parsed_value > 0:
            # Decimal percentage (e.g., 0.15 = 15%)
            return Rate.percentage(parsed_value * 100)
        elif parsed_value <= 100:
            # Direct percentage
            return Rate.percentage(parsed_value)
        else:
            # Large number, might be monetary limit
            return Rate.fixed(parsed_value, "COP")
    
    def _normalize_rate_name(self, header: str) -> str:
        """Normalize rate names for loans."""
        normalized = re.sub(r'[^\w\s]', ' ', header.lower())
        normalized = re.sub(r'\s+', '_', normalized.strip())
        
        # Map common rate types
        rate_mappings = {
            'tasa_efectiva': 'effective_rate',
            'tasa_nominal': 'nominal_rate',
            'ea': 'effective_annual',
            'mv': 'monthly_variable',
            'tv': 'quarterly_variable'
        }
        
        for pattern, mapped_name in rate_mappings.items():
            if pattern in normalized:
                return mapped_name
        
        return normalized
    
    def _looks_like_number(self, value: str) -> bool:
        """Check if string looks like a number."""
        try:
            self.excel_processor.parse_colombian_number(value)
            return True
        except Exception:
            return False
