"""Extraction strategy for accounts business line."""

import re
from typing import List, Dict, Any, Optional
from decimal import Decimal

from .extraction_strategy import ExtractionStrategy
from ..entities import FinancialService, Rate, RateType
from ..services import ExcelProcessorService


class AccountsExtractionStrategy(ExtractionStrategy):
    """
    Extraction strategy for accounts/banking business line.
    
    Handles Excel files with sheets like TARIFAS, LÍMITES, and related
    financial data for banking accounts and services.
    """
    
    def __init__(self):
        self.excel_processor = ExcelProcessorService()
        
        # Patterns specific to accounts business line
        self.sheet_type_patterns = {
            'tarifas': [r'tarifa', r'fee', r'cost', r'costo'],
            'limites': [r'limite', r'limit', r'max', r'tope'],
            'tasas': [r'tasa', r'rate', r'interest', r'interes'],
            'servicios': [r'servicio', r'service']
        }
        
        # Header patterns for different data types
        self.description_patterns = [
            r'descripcion', r'concepto', r'servicio', r'detalle',
            r'description', r'concept', r'service', r'detail'
        ]
        
        self.plan_patterns = [
            r'plan.*zero', r'g.*zero', r'puls', r'premier', r'tradicional',
            r'movil', r'app', r'digital'
        ]
    
    @property
    def business_line(self) -> str:
        return 'accounts'
    
    @property
    def supported_sheet_patterns(self) -> List[str]:
        patterns = []
        for sheet_patterns in self.sheet_type_patterns.values():
            patterns.extend(sheet_patterns)
        return patterns
    
    def classify_sheet_type(self, sheet_name: str) -> str:
        """Classify sheet type for accounts business line."""
        sheet_lower = sheet_name.lower()
        
        for sheet_type, patterns in self.sheet_type_patterns.items():
            for pattern in patterns:
                if re.search(pattern, sheet_lower):
                    return sheet_type
        
        return 'other'
    
    def should_process_sheet(self, sheet_name: str) -> bool:
        """Check if sheet should be processed for accounts."""
        sheet_type = self.classify_sheet_type(sheet_name)
        return sheet_type != 'other'
    
    def find_data_start_row(self, sheet_data: List[List[Any]], sheet_name: str) -> Optional[int]:
        """Find where data starts in accounts sheets."""
        for row_idx, row in enumerate(sheet_data[:15]):  # Check first 15 rows
            if not row:
                continue
                
            # Convert row to lowercase strings for pattern matching
            row_text = [str(cell).lower().strip() if cell else '' for cell in row]
            
            # Look for description column indicators
            description_found = any(
                any(re.search(pattern, cell) for pattern in self.description_patterns)
                for cell in row_text
                if cell
            )
            
            # Look for plan column indicators
            plan_found = any(
                any(re.search(pattern, cell) for pattern in self.plan_patterns)
                for cell in row_text
                if cell
            )
            
            # If we find both description and plan indicators, this is likely header row
            if description_found and plan_found:
                return row_idx + 1  # Data starts after header
            elif description_found:  # At least description column
                return row_idx + 1
        
        # Default to row 2 if no clear pattern found
        return 1 if len(sheet_data) > 1 else None
    
    def extract_headers(self, sheet_data: List[List[Any]], header_row: int) -> List[str]:
        """Extract and normalize headers for accounts data."""
        if header_row >= len(sheet_data):
            return []
        
        raw_headers = sheet_data[header_row - 1] if header_row > 0 else sheet_data[0]
        headers = []
        
        for i, header in enumerate(raw_headers):
            if header is None:
                normalized = f'column_{i + 1}'
            else:
                # Normalize header text
                normalized = str(header).strip().lower()
                normalized = re.sub(r'[^\w\s]', ' ', normalized)
                normalized = re.sub(r'\s+', '_', normalized)
                normalized = normalized.strip('_')
                
                if not normalized:
                    normalized = f'column_{i + 1}'
            
            headers.append(normalized)
        
        return headers
    
    def extract_service_from_row(self, row_data: List[Any], headers: List[str], 
                                sheet_name: str, row_index: int, 
                                document_id: str) -> Optional[FinancialService]:
        """Extract financial service from accounts data row."""
        if not row_data or len(row_data) == 0:
            return None
        
        # Create row dictionary
        row_dict = {}
        for i, header in enumerate(headers):
            value = row_data[i] if i < len(row_data) else None
            row_dict[header] = value
        
        # Find description
        description = self._find_description_in_row(row_dict)
        if not description or description.strip() == '':
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
        
        # Extract rates from other columns
        for header, value in row_dict.items():
            if self._is_rate_column(header) and value is not None:
                if str(value).strip() != '' and header != self._get_description_header(row_dict):
                    try:
                        rate = self._create_rate_for_accounts(value, header, sheet_type)
                        plan_name = self._normalize_plan_name(header)
                        service.add_rate(plan_name, rate)
                    except Exception as e:
                        # Log error but continue processing
                        continue
        
        return service if service.has_rates() else service  # Return even without rates for accounts
    
    def validate_extracted_data(self, services: List[FinancialService]) -> List[str]:
        """Validate accounts-specific business rules."""
        errors = []
        
        # Check for required service types in accounts
        service_types = set(service.table_type for service in services)
        expected_types = ['accounts_tarifas', 'accounts_limites']
        
        for expected_type in expected_types:
            if expected_type not in service_types:
                errors.append(f"Missing expected service type: {expected_type}")
        
        # Validate service descriptions
        descriptions = [service.description for service in services]
        if len(descriptions) != len(set(descriptions)):
            errors.append("Duplicate service descriptions found")
        
        # Validate rate structures for accounts
        for service in services:
            if 'tarifas' in service.table_type:
                # Tarifas should have monetary rates
                for plan, rate in service.rates.items():
                    if rate.type == RateType.PERCENTAGE and rate.value > 50:
                        errors.append(f"Unusually high percentage rate in {service.service_id}: {rate.value}%")
        
        return errors
    
    def _find_description_in_row(self, row_dict: Dict[str, Any]) -> Optional[str]:
        """Find the description field in accounts row data."""
        # Try description patterns first
        for header, value in row_dict.items():
            if any(re.search(pattern, header) for pattern in self.description_patterns):
                if value and str(value).strip():
                    return str(value).strip()
        
        # Fallback to first non-empty value
        for header, value in row_dict.items():
            if value and str(value).strip() and not self._is_numeric_string(str(value)):
                return str(value).strip()
        
        return None
    
    def _get_description_header(self, row_dict: Dict[str, Any]) -> Optional[str]:
        """Get the header that contains the description."""
        for header in row_dict.keys():
            if any(re.search(pattern, header) for pattern in self.description_patterns):
                return header
        return None
    
    def _is_rate_column(self, header: str) -> bool:
        """Check if a column header represents a rate/plan."""
        # Skip description columns
        if any(re.search(pattern, header) for pattern in self.description_patterns):
            return False
        
        # Skip metadata columns
        metadata_patterns = [r'id', r'tipo', r'categoria', r'fecha']
        if any(re.search(pattern, header) for pattern in metadata_patterns):
            return False
        
        return True
    
    def _is_numeric_string(self, value: str) -> bool:
        """Check if string represents a numeric value."""
        try:
            # Try to parse as number (Colombian format)
            self.excel_processor.parse_colombian_number(value)
            return True
        except:
            return False
    
    def _create_rate_for_accounts(self, value: Any, column_name: str, sheet_type: str) -> Rate:
        """Create rate specific to accounts business logic."""
        # For limits, usually large monetary amounts
        if sheet_type == 'limites':
            parsed_value = self.excel_processor.parse_colombian_number(str(value))
            if parsed_value and parsed_value > 10000:  # Large amounts are likely limits in COP
                return Rate.fixed(parsed_value, "COP")
        
        # For tasas, usually percentages
        if sheet_type == 'tasas' or 'tasa' in column_name.lower():
            parsed_value = self.excel_processor.parse_colombian_number(str(value))
            if parsed_value and parsed_value < 100:  # Likely percentage
                return Rate.percentage(parsed_value)
        
        # Default processing
        return self.excel_processor.create_rate_from_value(value, column_name)
    
    def _normalize_plan_name(self, header: str) -> str:
        """Normalize plan names for accounts."""
        # Clean header
        normalized = re.sub(r'[^\w\s]', ' ', header.lower())
        normalized = re.sub(r'\s+', '_', normalized.strip())
        
        # Map common plan names
        plan_mappings = {
            'g_zero': 'plan_g_zero',
            'plan_zero': 'plan_g_zero', 
            'puls': 'plan_puls',
            'premier': 'plan_premier',
            'tradicional': 'plan_tradicional',
            'movil': 'cuenta_movil',
            'app': 'cuenta_app'
        }
        
        for pattern, mapped_name in plan_mappings.items():
            if pattern in normalized:
                return mapped_name
        
        return normalized
