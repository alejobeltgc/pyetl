"""Document entity representing a processed Excel file."""

from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime
from .service import FinancialService


@dataclass
class Document:
    """
    Represents a processed Excel document with its metadata and services.
    
    Aggregate root that contains all financial services extracted
    from a single Excel file, along with processing metadata.
    """
    document_id: str
    business_line: str
    filename: str
    services: List[FinancialService] = field(default_factory=list)
    processing_metadata: Dict = field(default_factory=dict)
    created_at: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    version: str = "v1"
    
    def __post_init__(self):
        """Initialize timestamps if not provided."""
        now = datetime.now()
        if self.created_at is None:
            self.created_at = now
        if self.last_updated is None:
            self.last_updated = now
    
    def add_service(self, service: FinancialService) -> None:
        """Add a financial service to the document."""
        service.document_id = self.document_id
        self.services.append(service)
        self.last_updated = datetime.now()
    
    def get_services_by_table_type(self, table_type: str) -> List[FinancialService]:
        """Get all services of a specific table type."""
        return [service for service in self.services if service.table_type == table_type]
    
    def get_service_count(self) -> int:
        """Get total number of services."""
        return len(self.services)
    
    def get_service_count_by_table_type(self) -> Dict[str, int]:
        """Get service count grouped by table type."""
        counts = {}
        for service in self.services:
            counts[service.table_type] = counts.get(service.table_type, 0) + 1
        return counts
    
    def to_dict(self) -> dict:
        """Convert document to dictionary for serialization."""
        return {
            "document_id": self.document_id,
            "business_line": self.business_line,
            "filename": self.filename,
            "document_version": self.version,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "processing_metadata": self.processing_metadata,
            "services": [service.to_dict() for service in self.services],
            "summary": {
                "total_services": self.get_service_count(),
                "services_by_type": self.get_service_count_by_table_type()
            }
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Document':
        """Create document from dictionary."""
        document = cls(
            document_id=data['document_id'],
            business_line=data['business_line'],
            filename=data['filename'],
            processing_metadata=data.get('processing_metadata', {}),
            created_at=datetime.fromisoformat(data['created_at']) if data.get('created_at') else None,
            last_updated=datetime.fromisoformat(data['last_updated']) if data.get('last_updated') else None,
            version=data.get('document_version', 'v1')
        )
        
        # Add services
        for service_data in data.get('services', []):
            service = FinancialService.from_dict(service_data)
            document.add_service(service)
        
        return document
