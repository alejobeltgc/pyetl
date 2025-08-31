"""Validation report entity."""

from dataclasses import dataclass, field
from typing import List, Dict, Any
from datetime import datetime
from enum import Enum


class ValidationLevel(Enum):
    """Validation severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """Represents a single validation issue."""
    level: ValidationLevel
    message: str
    field: str = ""
    service_id: str = ""
    table_type: str = ""
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "level": self.level.value,
            "message": self.message,
            "field": self.field,
            "service_id": self.service_id,
            "table_type": self.table_type
        }


@dataclass
class ValidationReport:
    """
    Represents the validation results for a document processing.
    
    Contains all validation issues found during processing,
    along with summary statistics and processing metadata.
    """
    document_id: str
    issues: List[ValidationIssue] = field(default_factory=list)
    processing_stats: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    
    def add_error(self, message: str, field: str = "", service_id: str = "", 
                  table_type: str = "") -> None:
        """Add an error to the report."""
        self.issues.append(ValidationIssue(
            ValidationLevel.ERROR, message, field, service_id, table_type
        ))
    
    def add_warning(self, message: str, field: str = "", service_id: str = "", 
                   table_type: str = "") -> None:
        """Add a warning to the report."""
        self.issues.append(ValidationIssue(
            ValidationLevel.WARNING, message, field, service_id, table_type
        ))
    
    def add_info(self, message: str, field: str = "", service_id: str = "", 
                table_type: str = "") -> None:
        """Add an info message to the report."""
        self.issues.append(ValidationIssue(
            ValidationLevel.INFO, message, field, service_id, table_type
        ))
    
    def has_errors(self) -> bool:
        """Check if report has any errors."""
        return any(issue.level == ValidationLevel.ERROR for issue in self.issues)
    
    def has_warnings(self) -> bool:
        """Check if report has any warnings."""
        return any(issue.level == ValidationLevel.WARNING for issue in self.issues)
    
    def get_errors(self) -> List[ValidationIssue]:
        """Get all error issues."""
        return [issue for issue in self.issues if issue.level == ValidationLevel.ERROR]
    
    def get_warnings(self) -> List[ValidationIssue]:
        """Get all warning issues."""
        return [issue for issue in self.issues if issue.level == ValidationLevel.WARNING]
    
    def get_summary(self) -> Dict[str, int]:
        """Get issue count summary."""
        summary = {"errors": 0, "warnings": 0, "info": 0}
        for issue in self.issues:
            if issue.level == ValidationLevel.ERROR:
                summary["errors"] += 1
            elif issue.level == ValidationLevel.WARNING:
                summary["warnings"] += 1
            else:
                summary["info"] += 1
        return summary
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "document_id": self.document_id,
            "created_at": self.created_at.isoformat(),
            "summary": self.get_summary(),
            "processing_stats": self.processing_stats,
            "issues": [issue.to_dict() for issue in self.issues]
        }
