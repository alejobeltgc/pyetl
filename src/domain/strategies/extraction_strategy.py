"""Base extraction strategy interface."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from ..entities import FinancialService


class ExtractionStrategy(ABC):
    """
    Abstract base class for Excel extraction strategies.
    
    Each business line should implement this interface to define
    how Excel files are processed for that specific domain.
    """
    
    @property
    @abstractmethod
    def business_line(self) -> str:
        """Return the business line this strategy handles."""
        pass
    
    @property
    @abstractmethod
    def supported_sheet_patterns(self) -> List[str]:
        """Return list of regex patterns for supported sheet names."""
        pass
    
    @abstractmethod
    def classify_sheet_type(self, sheet_name: str) -> str:
        """
        Classify the type of sheet based on its name.
        
        Args:
            sheet_name: Name of the Excel sheet
            
        Returns:
            Sheet type classification (e.g., 'tarifas', 'limites', 'tasas')
        """
        pass
    
    @abstractmethod
    def should_process_sheet(self, sheet_name: str) -> bool:
        """
        Determine if a sheet should be processed by this strategy.
        
        Args:
            sheet_name: Name of the Excel sheet
            
        Returns:
            True if sheet should be processed, False otherwise
        """
        pass
    
    @abstractmethod
    def find_data_start_row(self, sheet_data: List[List[Any]], sheet_name: str) -> Optional[int]:
        """
        Find the row where actual data starts (after headers).
        
        Args:
            sheet_data: 2D array representing sheet data
            sheet_name: Name of the sheet
            
        Returns:
            Row index where data starts, or None if not found
        """
        pass
    
    @abstractmethod
    def extract_headers(self, sheet_data: List[List[Any]], header_row: int) -> List[str]:
        """
        Extract and normalize column headers.
        
        Args:
            sheet_data: 2D array representing sheet data
            header_row: Row index containing headers
            
        Returns:
            List of normalized header names
        """
        pass
    
    @abstractmethod
    def extract_service_from_row(self, row_data: List[Any], headers: List[str], 
                                sheet_name: str, row_index: int, 
                                document_id: str) -> Optional[FinancialService]:
        """
        Extract a financial service from a data row.
        
        Args:
            row_data: List of cell values for the row
            headers: List of column headers
            sheet_name: Name of the sheet
            row_index: Index of the row in the sheet
            document_id: Document ID for the service
            
        Returns:
            FinancialService if extraction successful, None otherwise
        """
        pass
    
    @abstractmethod
    def validate_extracted_data(self, services: List[FinancialService]) -> List[str]:
        """
        Validate extracted services for this business line.
        
        Args:
            services: List of extracted services
            
        Returns:
            List of validation error messages
        """
        pass
    
    def get_strategy_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about this extraction strategy.
        
        Returns:
            Dictionary with strategy information
        """
        return {
            'business_line': self.business_line,
            'supported_patterns': self.supported_sheet_patterns,
            'strategy_class': self.__class__.__name__
        }
