"""
Global configuration for the pyetl project.
"""

from pathlib import Path

# Project structure
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
SRC_DIR = PROJECT_ROOT / "src"

# File paths
DEFAULT_INPUT_FILE = DATA_DIR / "tasas-y-tarifas.xlsx"

# Output file naming
OUTPUT_FILES = {
    "raw_extracted": OUTPUT_DIR / "01_raw_extracted.json",
    "transformed": OUTPUT_DIR / "02_transformed.json",
    "validation_report": OUTPUT_DIR / "03_validation_report.json"
}

# Processing configuration
PROCESSING_CONFIG = {
    "business_line": "accounts",
    "document_version": "v1",
    "min_table_rows": 4,  # Minimum rows for a valid table
    "enable_validation": True,
    "save_intermediate_files": True
}

# Validation rules
VALIDATION_RULES = {
    "required_fields": ["service_id", "description", "frequency"],
    "rate_fields": ["rates", "rate"],  # Either rates (mobile) or rate (traditional)
    "max_description_length": 200,
    "valid_frequencies": ["monthly", "per_transaction", "one_time", "yearly", "unknown"],
    "valid_table_types": ["mobile_plans", "transfers", "withdrawals", "traditional_services"]
}
