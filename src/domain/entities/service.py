"""Financial service entity."""

from dataclasses import dataclass, field
from typing import Dict, Optional
from datetime import datetime
from .rate import Rate


@dataclass
class FinancialService:
    """
    Represents a financial service with its rates and metadata.
    
    Core domain entity that encapsulates a financial service
    with its associated rates, business line, and classification.
    """
    service_id: str
    description: str
    business_line: str
    table_type: str
    rates: Dict[str, Rate] = field(default_factory=dict)
    document_id: Optional[str] = None
    source_position: Dict = field(default_factory=dict)
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Initialize created_at if not provided."""
        if self.created_at is None:
            self.created_at = datetime.now()
    
    def add_rate(self, plan_name: str, rate: Rate) -> None:
        """Add a rate for a specific plan."""
        self.rates[plan_name] = rate
    
    def get_rate(self, plan_name: str) -> Optional[Rate]:
        """Get rate for a specific plan."""
        return self.rates.get(plan_name)
    
    def has_rates(self) -> bool:
        """Check if service has any rates."""
        return len(self.rates) > 0
    
    def to_dict(self) -> dict:
        """Convert service to dictionary for serialization."""
        return {
            "service_id": self.service_id,
            "description": self.description,
            "business_line": self.business_line,
            "table_type": self.table_type,
            "rates": {plan: rate.to_dict() for plan, rate in self.rates.items()},
            "document_id": self.document_id,
            "source_position": self.source_position,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'FinancialService':
        """Create service from dictionary."""
        service = cls(
            service_id=data['service_id'],
            description=data['description'],
            business_line=data['business_line'],
            table_type=data['table_type'],
            document_id=data.get('document_id'),
            source_position=data.get('source_position', {}),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None
        )
        
        # Reconstruct rates
        for plan, rate_data in data.get('rates', {}).items():
            from .rate import RateType
            rate_type = RateType(rate_data['type'])
            if rate_type == RateType.CONDITIONAL:
                rate = Rate.conditional(
                    rate_data['included_free'],
                    rate_data['additional_cost'],
                    rate_data.get('currency')
                )
            elif rate_type == RateType.PERCENTAGE:
                rate = Rate.percentage(rate_data['value'])
            elif rate_type == RateType.UNLIMITED:
                rate = Rate.unlimited()
            else:  # FIXED
                rate = Rate.fixed(rate_data['value'], rate_data.get('currency'))
            
            service.add_rate(plan, rate)
        
        return service
