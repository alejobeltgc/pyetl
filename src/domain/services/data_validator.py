"""Data validation domain service."""

from typing import List
from ..entities import Document, FinancialService, ValidationReport, ValidationLevel


class DataValidatorService:
    """
    Domain service for validating processed data.
    
    Contains business rules for validating financial services
    and generating validation reports.
    """
    
    def validate_document(self, document: Document) -> ValidationReport:
        """
        Validate a complete document and its services.
        
        Args:
            document: Document to validate
            
        Returns:
            Validation report with all issues found
        """
        report = ValidationReport(document_id=document.document_id)
        
        # Document-level validations
        self._validate_document_metadata(document, report)
        
        # Service-level validations
        for service in document.services:
            self._validate_service(service, report)
        
        # Business rule validations
        self._validate_business_rules(document, report)
        
        return report
    
    def _validate_document_metadata(self, document: Document, report: ValidationReport) -> None:
        """Validate document metadata."""
        if not document.filename:
            report.add_error("Document filename is missing")
        
        if not document.business_line:
            report.add_error("Document business line is missing")
        
        if document.get_service_count() == 0:
            report.add_warning("Document contains no services")
        
        if not document.document_id:
            report.add_error("Document ID is missing")
    
    def _validate_service(self, service: FinancialService, report: ValidationReport) -> None:
        """Validate individual service."""
        if not service.description or service.description.strip() == '':
            report.add_error(
                "Service description is empty", 
                service_id=service.service_id,
                table_type=service.table_type
            )
        
        if not service.service_id:
            report.add_error(
                "Service ID is missing",
                table_type=service.table_type
            )
        
        if not service.business_line:
            report.add_warning(
                "Business line is not classified",
                service_id=service.service_id,
                table_type=service.table_type
            )
        
        if not service.has_rates():
            report.add_warning(
                "Service has no rates defined",
                service_id=service.service_id,
                table_type=service.table_type
            )
        
        # Validate individual rates
        for plan_name, rate in service.rates.items():
            self._validate_rate(rate, plan_name, service, report)
    
    def _validate_rate(self, rate, plan_name: str, service: FinancialService, 
                      report: ValidationReport) -> None:
        """Validate individual rate."""
        if rate.value < 0:
            report.add_error(
                f"Rate value cannot be negative for plan '{plan_name}'",
                field="rate_value",
                service_id=service.service_id,
                table_type=service.table_type
            )
        
        # Validate conditional rate completeness
        if rate.type.value == "conditional":
            if rate.included_free is None:
                report.add_error(
                    f"Conditional rate missing 'included_free' for plan '{plan_name}'",
                    field="included_free",
                    service_id=service.service_id,
                    table_type=service.table_type
                )
            
            if rate.additional_cost is None:
                report.add_error(
                    f"Conditional rate missing 'additional_cost' for plan '{plan_name}'",
                    field="additional_cost",
                    service_id=service.service_id,
                    table_type=service.table_type
                )
        
        # Validate percentage ranges
        if rate.type.value == "percentage" and rate.value > 100:
            report.add_warning(
                f"Percentage rate seems high ({rate.value}%) for plan '{plan_name}'",
                field="rate_value",
                service_id=service.service_id,
                table_type=service.table_type
            )
    
    def _validate_business_rules(self, document: Document, report: ValidationReport) -> None:
        """Validate business-specific rules."""
        # Check for duplicate service descriptions within same table type
        seen_descriptions = {}
        for service in document.services:
            key = (service.table_type, service.description.lower().strip())
            if key in seen_descriptions:
                report.add_warning(
                    f"Duplicate service description '{service.description}' in {service.table_type}",
                    service_id=service.service_id,
                    table_type=service.table_type
                )
            else:
                seen_descriptions[key] = service.service_id
        
        # Validate business line consistency
        table_business_lines = {}
        for service in document.services:
            if service.table_type not in table_business_lines:
                table_business_lines[service.table_type] = service.business_line
            elif table_business_lines[service.table_type] != service.business_line:
                report.add_warning(
                    f"Inconsistent business line for table {service.table_type}",
                    service_id=service.service_id,
                    table_type=service.table_type
                )
        
        # Check for reasonable service distribution
        service_counts = document.get_service_count_by_table_type()
        for table_type, count in service_counts.items():
            if count == 0:
                report.add_info(f"No services found in table type: {table_type}")
            elif count > 50:
                report.add_warning(f"Large number of services ({count}) in {table_type}")
    
    def get_critical_issues(self, report: ValidationReport) -> List[str]:
        """
        Get list of critical issues that prevent processing.
        
        Args:
            report: Validation report
            
        Returns:
            List of critical issue messages
        """
        critical_issues = []
        for issue in report.get_errors():
            if any(keyword in issue.message.lower() for keyword in ['missing', 'empty', 'invalid']):
                critical_issues.append(issue.message)
        
        return critical_issues
