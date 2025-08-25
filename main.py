#!/usr/bin/env python3
"""
Main ETL Pipeline for Financial Rates and Fees Processing

This             self._save_json(validation_report, config.OUTPUT_FILES["validation_report"])
            print(f"   ✅ Validation report saved to: {config.OUTPUT_FILES['validation_report'].name}")ript orchestrates the complete ETL process:
1. Extract tables from Excel files
2. Transform data according to business rules
3. Validate the output
4. Save results in structured format
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

# Import global config from root
import sys
config_path = Path(__file__).parent / "config.py"
sys.path.insert(0, str(Path(__file__).parent))
import config

# Add src to Python path for business logic components  
sys.path.insert(0, str(Path(__file__).parent / "src"))
from excel_parser import ExcelParser
from business_rules.accounts_rules import AccountsBusinessRules
from datetime import timezone


class ETLPipeline:
    """
    Main ETL Pipeline orchestrator.
    """
    
    def __init__(self, business_line: str = "accounts"):
        """
        Initialize the ETL pipeline.
        
        Args:
            business_line (str): Business line to process
        """
        self.business_line = business_line
        self.config = config.PROCESSING_CONFIG.copy()
        self.config["business_line"] = business_line
        
        # Initialize components
        self.parser = None
        self.business_rules = None
        self._init_components()
    
    def _init_components(self):
        """Initialize pipeline components."""
        if self.business_line == "accounts":
            self.business_rules = AccountsBusinessRules()
        else:
            raise ValueError(f"Business line '{self.business_line}' not supported")
    
    def process_file(self, input_file: Path, save_intermediate: bool = True) -> dict:
        """
        Process a single Excel file through the complete ETL pipeline.
        
        Args:
            input_file (Path): Path to Excel file
            save_intermediate (bool): Whether to save intermediate results
            
        Returns:
            dict: Final transformed data
        """
        print(f"\n🚀 Starting ETL Pipeline for: {input_file.name}")
        print(f"📊 Business Line: {self.business_line}")
        print("=" * 60)
        
        # Step 1: Extract
        print("\n📥 STEP 1: Extracting tables from Excel...")
        raw_data = self._extract_tables(input_file)
        
        if save_intermediate:
            self._save_json(raw_data, config.OUTPUT_FILES["raw_extracted"])
            print(f"   ✅ Raw data saved to: {config.OUTPUT_FILES['raw_extracted'].name}")
        
        # Step 2: Transform
        print("\n🔄 STEP 2: Transforming data with business rules...")
        transformed_data = self._transform_data(raw_data)
        
        if save_intermediate:
            self._save_json(transformed_data, config.OUTPUT_FILES["transformed"])
            print(f"   ✅ Transformed data saved to: {config.OUTPUT_FILES['transformed'].name}")
        
        # Step 3: Validate
        print("\n✅ STEP 3: Validating output...")
        validation_report = self._validate_data(transformed_data)
        
        if save_intermediate:
            self._save_json(validation_report, config.OUTPUT_FILES["validation_report"])
            print(f"   ✅ Validation report saved to: {config.OUTPUT_FILES['validation_report'].name}")
        
        # Summary
        self._print_summary(transformed_data, validation_report)
        
        return {
            "raw_data": raw_data,
            "transformed_data": transformed_data,
            "validation_report": validation_report
        }
    
    def _extract_tables(self, input_file: Path) -> dict:
        """Extract tables from Excel file."""
        self.parser = ExcelParser(str(input_file))
        
        if not self.parser.load_excel_file():
            raise RuntimeError(f"Failed to load Excel file: {input_file}")
        
        return self.parser.extract_all_tables()
    
    def _transform_data(self, raw_data: dict) -> dict:
        """Transform raw data using business rules."""
        transformed_data = {
            "business_line": self.business_line,
            "document_type": "rates_and_fees",
            "document_version": self.config["document_version"],
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "source_sheets": list(raw_data.keys()),
            "tables": {}
        }
        
        # Process each sheet
        for sheet_name, sheet_tables in raw_data.items():
            if not sheet_tables:
                continue
                
            print(f"   Processing sheet: {sheet_name}")
            
            # Process each table
            for table in sheet_tables:
                table_type = self.business_rules.classify_table_type(table)
                print(f"     - '{table['table_name']}' → {table_type}")
                
                if table_type == "unknown":
                    print("       ⚠️  Skipping unknown table type")
                    continue
                
                # Transform table
                transformed_table = self._transform_table_by_type(table, table_type)
                
                if transformed_table:
                    self._merge_table_results(table_type, transformed_table, transformed_data)
        
        return transformed_data
    
    def _transform_table_by_type(self, table: dict, table_type: str) -> dict:
        """Transform a single table based on its type."""
        transform_methods = {
            "mobile_plans": self._transform_mobile_plans,
            "transfers": self._transform_standard_plans,
            "withdrawals": self._transform_standard_plans,
            "traditional_services": self._transform_traditional_services
        }
        
        method = transform_methods.get(table_type)
        if not method:
            return {}
        
        return method(table, table_type)
    
    def _transform_mobile_plans(self, table: dict, table_type: str) -> dict:
        """Transform mobile plans table."""
        return self._transform_standard_plans(table, table_type)
    
    def _transform_standard_plans(self, table: dict, table_type: str) -> dict:
        """Transform standard plans table (mobile, transfers, withdrawals)."""
        services = []
        
        for row in table.get('data', []):
            description = row.get('Descripción', '').strip()
            if not description:
                continue
            
            service = self._create_service_record(row, description, "rates")
            if service:
                service["category"] = table_type
                services.append(service)
        
        return self._create_table_result(table, table_type, services)
    
    def _transform_traditional_services(self, table: dict, table_type: str) -> dict:
        """Transform traditional services table."""
        services = []
        
        for row in table.get('data', []):
            description = row.get('Descripción', '').strip()
            if not description:
                continue
            
            service = self._create_service_record(row, description, "rate")
            if service:
                service["category"] = table_type
                services.append(service)
        
        return self._create_table_result(table, table_type, services)
    
    def _create_service_record(self, row: dict, description: str, rate_type: str) -> dict:
        """Create a standardized service record."""
        service_id = self.business_rules.generate_service_id(description)
        
        service = {
            "service_id": service_id,
            "description": description,
            "applies_tax": self.business_rules.normalize_tax_application(row.get('Aplica Iva')),
            "frequency": self.business_rules.normalize_frequency(row.get('Frecuencia'))
        }
        
        # Add rates or rate based on type
        if rate_type == "rates":
            # Multiple plans (mobile, transfers, withdrawals)
            rates = {}
            for original_col, plan_key in self.business_rules.config['plan_types'].items():
                if original_col in row:
                    rate_info = self.business_rules.parse_rate_value(row[original_col])
                    rates[plan_key] = rate_info
            service["rates"] = rates
        else:
            # Single rate (traditional services)
            value = row.get('Valor sin iva')
            service["rate"] = self.business_rules.parse_rate_value(value)
        
        # Add disclaimer if present
        disclaimer = row.get('Disclaimer')
        if disclaimer and not (isinstance(disclaimer, float) and str(disclaimer).lower() == 'nan') and isinstance(disclaimer, str) and disclaimer.strip():
            service["disclaimer"] = disclaimer.strip()
        
        return service
    
    def _create_table_result(self, table: dict, table_type: str, services: list) -> dict:
        """Create standardized table result."""
        return {
            "table_type": table_type,
            "table_name": table.get('table_name'),
            "source_position": {
                "sheet": table.get('sheet_name'),
                "start_row": table.get('start_row'),
                "end_row": table.get('end_row')
            },
            "services": services
        }
    
    def _merge_table_results(self, table_type: str, transformed_table: dict, 
                           transformed_data: dict) -> None:
        """Merge table results into main data structure."""
        if table_type not in transformed_data["tables"]:
            transformed_data["tables"][table_type] = transformed_table
        else:
            # Merge services
            existing = transformed_data["tables"][table_type].get("services", [])
            new_services = transformed_table.get("services", [])
            transformed_data["tables"][table_type]["services"] = existing + new_services
    
    def _validate_data(self, transformed_data: dict) -> dict:
        """Validate transformed data."""
        from config import VALIDATION_RULES
        
        validation_report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "passed",
            "errors": [],
            "warnings": [],
            "stats": {}
        }
        
        # Validate overall structure
        if "tables" not in transformed_data:
            validation_report["errors"].append("Missing 'tables' key in transformed data")
            validation_report["status"] = "failed"
            return validation_report
        
        # Validate each table
        total_services = 0
        for table_type, table_data in transformed_data["tables"].items():
            
            if table_type not in VALIDATION_RULES["valid_table_types"]:
                validation_report["warnings"].append(f"Unknown table type: {table_type}")
            
            services = table_data.get("services", [])
            total_services += len(services)
            
            # Validate services
            for i, service in enumerate(services):
                service_errors = self._validate_service(service, i)
                validation_report["errors"].extend(service_errors)
        
        # Update status based on errors
        if validation_report["errors"]:
            validation_report["status"] = "failed"
        elif validation_report["warnings"]:
            validation_report["status"] = "passed_with_warnings"
        
        # Add stats
        validation_report["stats"] = {
            "total_tables": len(transformed_data["tables"]),
            "total_services": total_services,
            "error_count": len(validation_report["errors"]),
            "warning_count": len(validation_report["warnings"])
        }
        
        return validation_report
    
    def _validate_service(self, service: dict, index: int) -> list:
        """Validate a single service record."""
        from config import VALIDATION_RULES
        
        errors = []
        service_id = service.get("service_id", f"service_{index}")
        
        # Check required fields
        for field in VALIDATION_RULES["required_fields"]:
            if field not in service:
                errors.append(f"Service '{service_id}': Missing required field '{field}'")
        
        # Check rate fields
        has_rates = "rates" in service
        has_rate = "rate" in service
        
        if not has_rates and not has_rate:
            errors.append(f"Service '{service_id}': Missing rate information (needs 'rates' or 'rate')")
        
        # Validate frequency
        frequency = service.get("frequency")
        if frequency and frequency not in VALIDATION_RULES["valid_frequencies"]:
            errors.append(f"Service '{service_id}': Invalid frequency '{frequency}'")
        
        # Validate description length
        description = service.get("description", "")
        if len(description) > VALIDATION_RULES["max_description_length"]:
            errors.append(f"Service '{service_id}': Description too long ({len(description)} chars)")
        
        return errors
    
    def _save_json(self, data: dict, file_path: Path) -> None:
        """Save data to JSON file."""
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def _print_summary(self, transformed_data: dict, validation_report: dict) -> None:
        """Print processing summary."""
        print("\n" + "=" * 60)
        print("🎯 PROCESSING SUMMARY")
        print("=" * 60)
        
        print(f"📋 Business Line: {transformed_data['business_line']}")
        print(f"📅 Processed: {transformed_data['last_updated']}")
        print(f"📊 Tables: {len(transformed_data['tables'])}")
        
        total_services = sum(len(table.get('services', [])) for table in transformed_data['tables'].values())
        print(f"🔧 Services: {total_services}")
        
        # Validation status
        status = validation_report['status']
        status_emoji = {"passed": "✅", "passed_with_warnings": "⚠️", "failed": "❌"}
        print(f"✅ Validation: {status_emoji.get(status, '❓')} {status.upper()}")
        
        if validation_report['errors']:
            print(f"❌ Errors: {len(validation_report['errors'])}")
        
        if validation_report['warnings']:
            print(f"⚠️  Warnings: {len(validation_report['warnings'])}")


def main():
    """Main CLI interface."""
    parser = argparse.ArgumentParser(
        description="ETL Pipeline for Financial Rates and Fees",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                           # Process default file
  python main.py --file data/rates.xlsx   # Process specific file
  python main.py --business-line credit   # Process for credit line
        """
    )
    
    parser.add_argument(
        "--file", 
        type=Path,
        default=config.DEFAULT_INPUT_FILE,
        help=f"Excel file to process (default: {config.DEFAULT_INPUT_FILE})"
    )
    
    parser.add_argument(
        "--business-line",
        choices=["accounts", "credit"],
        default="accounts",
        help="Business line to process (default: accounts)"
    )
    
    parser.add_argument(
        "--no-intermediate",
        action="store_true",
        help="Don't save intermediate files"
    )
    
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=config.OUTPUT_DIR,
        help=f"Output directory (default: {config.OUTPUT_DIR})"
    )
    
    args = parser.parse_args()
    
    # Validate input file
    if not args.file.exists():
        print(f"❌ Error: Input file not found: {args.file}")
        sys.exit(1)
    
    # Update output paths if custom output dir
    if args.output_dir != config.OUTPUT_DIR:
        for key, path in config.OUTPUT_FILES.items():
            config.OUTPUT_FILES[key] = args.output_dir / path.name
    
    try:
        # Run ETL pipeline
        pipeline = ETLPipeline(args.business_line)
        results = pipeline.process_file(
            args.file, 
            save_intermediate=not args.no_intermediate
        )
        
        # Exit with appropriate code
        validation_status = results["validation_report"]["status"]
        if validation_status == "failed":
            sys.exit(1)
        elif validation_status == "passed_with_warnings":
            sys.exit(2)
        else:
            sys.exit(0)
            
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
