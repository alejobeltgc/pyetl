"""Factory for creating extraction strategies."""

from typing import Dict, List, Optional
import re

from .extraction_strategy import ExtractionStrategy
from .accounts_strategy import AccountsExtractionStrategy
from .loans_strategy import LoansExtractionStrategy


class ExtractionStrategyFactory:
    """
    Factory for creating appropriate extraction strategies.
    
    Analyzes Excel file content to determine which business line
    strategy should be used for extraction.
    """
    
    def __init__(self):
        # Register available strategies
        self._strategies: Dict[str, ExtractionStrategy] = {
            'accounts': AccountsExtractionStrategy(),
            'loans': LoansExtractionStrategy()
        }
        
        # Patterns to identify business lines from sheet names
        self._business_line_indicators = {
            'accounts': [
                r'cuenta', r'tarifa', r'limite', r'servicio.*bancario',
                r'account', r'fee', r'banking.*service'
            ],
            'loans': [
                r'credito', r'prestamo', r'tasa.*credito', r'cupo',
                r'credit', r'loan', r'lending'
            ]
        }
    
    def detect_business_line(self, sheet_names: List[str], filename: str = "") -> str:
        """
        Detect the primary business line from sheet names and filename.
        
        Args:
            sheet_names: List of sheet names in the Excel file
            filename: Optional filename for additional context
            
        Returns:
            Detected business line identifier
        """
        # Score each business line based on sheet names
        scores = {business_line: 0 for business_line in self._strategies.keys()}
        
        # Analyze sheet names
        for sheet_name in sheet_names:
            sheet_lower = sheet_name.lower()
            
            for business_line, patterns in self._business_line_indicators.items():
                for pattern in patterns:
                    if re.search(pattern, sheet_lower):
                        scores[business_line] += 1
        
        # Analyze filename if provided
        if filename:
            filename_lower = filename.lower()
            for business_line, patterns in self._business_line_indicators.items():
                for pattern in patterns:
                    if re.search(pattern, filename_lower):
                        scores[business_line] += 2  # Filename has higher weight
        
        # Return business line with highest score
        if max(scores.values()) > 0:
            return max(scores, key=scores.get)
        
        # Default to accounts if no clear pattern
        return 'accounts'
    
    def get_strategy(self, business_line: str) -> Optional[ExtractionStrategy]:
        """
        Get extraction strategy for a specific business line.
        
        Args:
            business_line: Business line identifier
            
        Returns:
            Extraction strategy instance or None if not found
        """
        return self._strategies.get(business_line)
    
    def get_strategy_for_file(self, sheet_names: List[str], filename: str = "") -> ExtractionStrategy:
        """
        Get the best extraction strategy for a file.
        
        Args:
            sheet_names: List of sheet names in the Excel file
            filename: Optional filename for additional context
            
        Returns:
            Best matching extraction strategy
        """
        business_line = self.detect_business_line(sheet_names, filename)
        strategy = self.get_strategy(business_line)
        
        if strategy is None:
            # Fallback to accounts strategy
            strategy = self._strategies['accounts']
        
        return strategy
    
    def register_strategy(self, business_line: str, strategy: ExtractionStrategy) -> None:
        """
        Register a new extraction strategy.
        
        Args:
            business_line: Business line identifier
            strategy: Strategy instance
        """
        self._strategies[business_line] = strategy
        
        # Optionally add business line indicators
        if hasattr(strategy, 'business_line_indicators'):
            self._business_line_indicators[business_line] = strategy.business_line_indicators
    
    def get_available_strategies(self) -> List[str]:
        """Get list of available business line strategies."""
        return list(self._strategies.keys())
    
    def get_strategy_info(self) -> Dict[str, Dict]:
        """Get information about all registered strategies."""
        info = {}
        for business_line, strategy in self._strategies.items():
            info[business_line] = {
                'class': strategy.__class__.__name__,
                'supported_patterns': strategy.supported_sheet_patterns,
                'business_line': strategy.business_line
            }
        return info
