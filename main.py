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
from transformer import Transformer
from validator import Validator
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
        self.transformer = None
        self.validator = None
        self._init_components()
    
    def _init_components(self):
        """Initialize pipeline components."""
        if self.business_line == "accounts":
            self.business_rules = AccountsBusinessRules()
            self.transformer = Transformer(self.business_rules, self.config)
            self.validator = Validator(config.VALIDATION_RULES)
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
        transformed_data = self.transformer.transform(raw_data)
        
        if save_intermediate:
            self._save_json(transformed_data, config.OUTPUT_FILES["transformed"])
            print(f"   ✅ Transformed data saved to: {config.OUTPUT_FILES['transformed'].name}")
        
        # Step 3: Validate
        print("\n✅ STEP 3: Validating output...")
        validation_report = self.validator.validate(transformed_data)
        
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
        
        print(f"📋 Business Line: {transformed_data.get('business_line', 'N/A')}")
        print(f"📅 Processed: {transformed_data.get('last_updated', 'N/A')}")
        
        tables = transformed_data.get('tables', {})
        print(f"📊 Tables: {len(tables)}")
        
        total_services = sum(len(table.get('services', [])) for table in tables.values())
        print(f"🔧 Services: {total_services}")
        
        # Validation status
        status = validation_report.get('status', 'unknown')
        status_emoji = {"passed": "✅", "passed_with_warnings": "⚠️", "failed": "❌"}
        print(f"✅ Validation: {status_emoji.get(status, '❓')} {status.upper()}")
        
        errors = validation_report.get('errors', [])
        if errors:
            print(f"❌ Errors: {len(errors)}")
        
        warnings = validation_report.get('warnings', [])
        if warnings:
            print(f"⚠️  Warnings: {len(warnings)}")


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
