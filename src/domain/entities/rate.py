"""Rate entity and related enums."""

from enum import Enum
from dataclasses import dataclass
from typing import Optional
from decimal import Decimal


class RateType(Enum):
    """Types of rates in the financial system."""
    FIXED = "fixed"
    PERCENTAGE = "percentage"
    CONDITIONAL = "conditional"
    UNLIMITED = "unlimited"


@dataclass(frozen=True)
class Rate:
    """
    Represents a financial rate with its type and value.
    
    Immutable entity that represents different types of financial rates
    such as fixed amounts, percentages, conditional rates, etc.
    """
    type: RateType
    value: Decimal
    currency: Optional[str] = None
    included_free: Optional[int] = None
    additional_cost: Optional[Decimal] = None
    
    def __post_init__(self):
        """Validate rate constraints."""
        if self.type == RateType.CONDITIONAL:
            if self.included_free is None or self.additional_cost is None:
                raise ValueError("Conditional rates must have included_free and additional_cost")
        
        if self.value < 0:
            raise ValueError("Rate value cannot be negative")
    
    @classmethod
    def fixed(cls, value: Decimal, currency: Optional[str] = None) -> 'Rate':
        """Create a fixed rate."""
        return cls(RateType.FIXED, value, currency)
    
    @classmethod
    def percentage(cls, value: Decimal) -> 'Rate':
        """Create a percentage rate."""
        return cls(RateType.PERCENTAGE, value)
    
    @classmethod
    def conditional(cls, included_free: int, additional_cost: Decimal, 
                   currency: Optional[str] = None) -> 'Rate':
        """Create a conditional rate."""
        return cls(
            RateType.CONDITIONAL, 
            Decimal('0'), 
            currency, 
            included_free, 
            additional_cost
        )
    
    @classmethod
    def unlimited(cls) -> 'Rate':
        """Create an unlimited rate."""
        return cls(RateType.UNLIMITED, Decimal('0'))
    
    def to_dict(self) -> dict:
        """Convert rate to dictionary for serialization."""
        result = {
            "type": self.type.value,
            "value": float(self.value)
        }
        
        if self.currency:
            result["currency"] = self.currency
        
        if self.type == RateType.CONDITIONAL:
            result["included_free"] = self.included_free
            result["additional_cost"] = float(self.additional_cost)
        
        return result
