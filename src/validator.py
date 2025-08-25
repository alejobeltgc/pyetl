"""
Data Validation Module
"""
from typing import Dict, Any

class Validator:
    """
    Validates the transformed data against a set of rules.
    """
    def __init__(self, rules: Dict[str, Any]):
        """
        Initialize the Validator with a set of validation rules.
        
        Args:
            rules (Dict[str, Any]): Dictionary of validation rules.
        """
        self.rules = rules
        self.errors = []
        self.warnings = []

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run all validation checks on the transformed data.
        
        Args:
            data (Dict[str, Any]): The transformed data to validate.
            
        Returns:
            Dict[str, Any]: A validation report.
        """
        self.errors = []
        self.warnings = []

        if not data or "tables" not in data:
            self.errors.append({"type": "critical", "message": "No tables found in transformed data."})
            return self._generate_report()

        for table_type, table_data in data["tables"].items():
            self._validate_table(table_type, table_data)

        return self._generate_report()

    def _validate_table(self, table_type: str, table_data: Dict[str, Any]):
        """Validate a single table."""
        if table_type not in self.rules["valid_table_types"]:
            self.warnings.append({
                "type": "invalid_table_type",
                "table_type": table_type,
                "message": f"Unrecognized table type: {table_type}"
            })

        for service in table_data.get("services", []):
            self._validate_service(service)

    def _validate_service(self, service: Dict[str, Any]):
        """Validate a single service record."""
        # Check for required fields
        for field in self.rules["required_fields"]:
            if field not in service or not service[field]:
                self.errors.append({
                    "type": "missing_required_field",
                    "service_id": service.get('service_id', 'N/A'),
                    "field": field,
                    "message": f"Service is missing required field: {field}"
                })

        # Check description length
        if len(service.get("description", "")) > self.rules["max_description_length"]:
            self.warnings.append({
                "type": "description_too_long",
                "service_id": service.get('service_id'),
                "length": len(service["description"]),
                "max_length": self.rules["max_description_length"]
            })

        # Check frequency value
        if service.get("frequency") not in self.rules["valid_frequencies"]:
            self.errors.append({
                "type": "invalid_frequency",
                "service_id": service.get('service_id'),
                "frequency": service.get("frequency"),
                "message": "Invalid frequency value."
            })

    def _generate_report(self) -> Dict[str, Any]:
        """Generate the final validation report."""
        status = "passed"
        if self.errors:
            status = "failed"
        elif self.warnings:
            status = "passed_with_warnings"

        return {
            "status": status,
            "summary": {
                "total_errors": len(self.errors),
                "total_warnings": len(self.warnings)
            },
            "errors": self.errors,
            "warnings": self.warnings
        }
