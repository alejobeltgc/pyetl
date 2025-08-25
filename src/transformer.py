"""
Financial Data Transformer

This module transforms raw extracted table data into structured business data
ready for DynamoDB storage and API consumption.
"""

import json
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from .business_rules.accounts_rules import AccountsBusinessRules

# Constants
DESCRIPTION_FIELD = 'Descripción'
TAX_FIELD = 'Aplica Iva'
FREQUENCY_FIELD = 'Frecuencia'
DISCLAIMER_FIELD = 'Disclaimer'


class FinancialDataTransformer:
    """
    Main transformer class for financial rates and fees data.
    """
    
    def __init__(self, business_line: str = "accounts"):
        """
        Initialize the transformer.
        
        Args:
            business_line (str): Business line to transform (accounts, credit, etc.)
        """
        self.business_line = business_line
        
        # Initialize business rules based on business line
        if business_line == "accounts":
            self.business_rules = AccountsBusinessRules()
        else:
            raise ValueError(f"Business line '{business_line}' not supported yet")
    
    def transform_rates_and_fees(self, raw_tables_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw tables data into structured business data.
        
        Args:
            raw_tables_data (Dict): Raw data from excel_parser
            
        Returns:
            Dict: Transformed data ready for DynamoDB
        """
        transformed_data = {
            "business_line": self.business_line,
            "document_type": "rates_and_fees",
            "document_version": "v1",
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "source_sheets": list(raw_tables_data.keys()),
            "tables": {}
        }
        
        # Process each sheet
        for sheet_name, sheet_tables in raw_tables_data.items():
            if sheet_tables:  # Only process non-empty sheets
                self._process_sheet_tables(sheet_name, sheet_tables, transformed_data)
        
        return transformed_data
    
    def _process_sheet_tables(self, sheet_name: str, sheet_tables: List[Dict], 
                            transformed_data: Dict[str, Any]) -> None:
        """
        Process all tables in a sheet.
        
        Args:
            sheet_name (str): Name of the sheet
            sheet_tables (List[Dict]): List of tables in the sheet
            transformed_data (Dict): Main transformed data structure to update
        """
        print(f"Transforming sheet: {sheet_name}")
        
        # Process each table in the sheet
        for table in sheet_tables:
            table_type = self.business_rules.classify_table_type(table)
            print(f"  - Table '{table['table_name']}' classified as: {table_type}")
            
            if table_type == "unknown":
                print("    Warning: Unknown table type, skipping...")
                continue
            
            # Transform based on table type
            transformed_table = self._transform_table_by_type(table, table_type)
            
            if transformed_table:
                self._merge_table_into_results(table_type, transformed_table, transformed_data)
    
    def _merge_table_into_results(self, table_type: str, transformed_table: Dict[str, Any], 
                                transformed_data: Dict[str, Any]) -> None:
        """
        Merge a transformed table into the main results.
        
        Args:
            table_type (str): Type of the table
            transformed_table (Dict): Transformed table data
            transformed_data (Dict): Main transformed data structure to update
        """
        if table_type not in transformed_data["tables"]:
            transformed_data["tables"][table_type] = transformed_table
        else:
            # Merge services if same table type exists
            existing_services = transformed_data["tables"][table_type].get("services", [])
            new_services = transformed_table.get("services", [])
            transformed_data["tables"][table_type]["services"] = existing_services + new_services
    
    def _transform_table_by_type(self, table: Dict[str, Any], table_type: str) -> Optional[Dict[str, Any]]:
        """
        Transform a table based on its classified type.
        
        Args:
            table (Dict): Raw table data
            table_type (str): Classified table type
            
        Returns:
            Optional[Dict]: Transformed table or None
        """
        transform_methods = {
            "mobile_plans": self._transform_mobile_plans_table,
            "transfers": self._transform_transfers_table,
            "withdrawals": self._transform_withdrawals_table,
            "traditional_services": self._transform_traditional_services_table
        }
        
        transform_method = transform_methods.get(table_type)
        if not transform_method:
            print(f"    No transform method for table type: {table_type}")
            return None
        
        return transform_method(table)
    
    def _transform_mobile_plans_table(self, table: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform mobile plans table.
        
        Args:
            table (Dict): Raw table data
            
        Returns:
            Dict: Transformed table
        """
        services = []
        
        for row in table.get('data', []):
            description = row.get(DESCRIPTION_FIELD, '')
            if not description:
                continue
            
            service_id = self.business_rules.generate_service_id(description)
            
            # Extract rates for each plan
            rates = {}
            for original_col, plan_key in self.business_rules.config['plan_types'].items():
                if original_col in row:
                    rate_info = self.business_rules.parse_rate_value(row[original_col])
                    rates[plan_key] = rate_info
            
            # Extract other fields
            applies_tax = self.business_rules.normalize_tax_application(
                row.get(TAX_FIELD)
            )
            frequency = self.business_rules.normalize_frequency(
                row.get(FREQUENCY_FIELD)
            )
            disclaimer = row.get(DISCLAIMER_FIELD)
            
            service = {
                "service_id": service_id,
                "description": description,
                "rates": rates,
                "applies_tax": applies_tax,
                "frequency": frequency
            }
            
            if disclaimer:
                service["disclaimer"] = disclaimer
            
            services.append(service)
        
        return {
            "table_type": "mobile_plans",
            "table_name": table.get('table_name'),
            "source_position": {
                "sheet": table.get('sheet_name'),
                "start_row": table.get('start_row'),
                "end_row": table.get('end_row')
            },
            "services": services
        }
    
    def _transform_transfers_table(self, table: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform transfers table.
        
        Args:
            table (Dict): Raw table data
            
        Returns:
            Dict: Transformed table
        """
        services = []
        
        for row in table.get('data', []):
            description = row.get(DESCRIPTION_FIELD, '')
            if not description:
                continue
            
            service_id = self.business_rules.generate_service_id(description)
            
            # Extract rates for each plan (same structure as mobile plans)
            rates = {}
            for original_col, plan_key in self.business_rules.config['plan_types'].items():
                if original_col in row:
                    rate_info = self.business_rules.parse_rate_value(row[original_col])
                    rates[plan_key] = rate_info
            
            applies_tax = self.business_rules.normalize_tax_application(
                row.get(TAX_FIELD)
            )
            frequency = self.business_rules.normalize_frequency(
                row.get(FREQUENCY_FIELD)
            )
            
            service = {
                "service_id": service_id,
                "description": description,
                "rates": rates,
                "applies_tax": applies_tax,
                "frequency": frequency,
                "category": "transfers"
            }
            
            services.append(service)
        
        return {
            "table_type": "transfers",
            "table_name": table.get('table_name'),
            "source_position": {
                "sheet": table.get('sheet_name'),
                "start_row": table.get('start_row'),
                "end_row": table.get('end_row')
            },
            "services": services
        }
    
    def _transform_withdrawals_table(self, table: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform withdrawals table.
        
        Args:
            table (Dict): Raw table data
            
        Returns:
            Dict: Transformed table
        """
        services = []
        
        for row in table.get('data', []):
            description = row.get(DESCRIPTION_FIELD, '')
            if not description:
                continue
            
            service_id = self.business_rules.generate_service_id(description)
            
            # Extract rates for each plan
            rates = {}
            for original_col, plan_key in self.business_rules.config['plan_types'].items():
                if original_col in row:
                    rate_info = self.business_rules.parse_rate_value(row[original_col])
                    rates[plan_key] = rate_info
            
            applies_tax = self.business_rules.normalize_tax_application(
                row.get(TAX_FIELD)
            )
            frequency = self.business_rules.normalize_frequency(
                row.get(FREQUENCY_FIELD)
            )
            
            service = {
                "service_id": service_id,
                "description": description,
                "rates": rates,
                "applies_tax": applies_tax,
                "frequency": frequency,
                "category": "withdrawals"
            }
            
            services.append(service)
        
        return {
            "table_type": "withdrawals",
            "table_name": table.get('table_name'),
            "source_position": {
                "sheet": table.get('sheet_name'),
                "start_row": table.get('start_row'),
                "end_row": table.get('end_row')
            },
            "services": services
        }
    
    def _transform_traditional_services_table(self, table: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform traditional services table (different structure).
        
        Args:
            table (Dict): Raw table data
            
        Returns:
            Dict: Transformed table
        """
        services = []
        
        for row in table.get('data', []):
            description = row.get(DESCRIPTION_FIELD, '')
            if not description:
                continue
            
            service_id = self.business_rules.generate_service_id(description)
            
            # Traditional services have different column structure
            value = row.get('Valor sin iva')
            applies_tax = self.business_rules.normalize_tax_application(
                row.get(TAX_FIELD)
            )
            frequency = self.business_rules.normalize_frequency(
                row.get(FREQUENCY_FIELD)
            )
            disclaimer = row.get(DISCLAIMER_FIELD)
            
            # Parse the value
            rate_info = self.business_rules.parse_rate_value(value)
            
            service = {
                "service_id": service_id,
                "description": description,
                "rate": rate_info,
                "applies_tax": applies_tax,
                "frequency": frequency,
                "category": "traditional_services"
            }
            
            if disclaimer:
                service["disclaimer"] = disclaimer
            
            services.append(service)
        
        return {
            "table_type": "traditional_services",
            "table_name": table.get('table_name'),
            "source_position": {
                "sheet": table.get('sheet_name'),
                "start_row": table.get('start_row'),
                "end_row": table.get('end_row')
            },
            "services": services
        }
    
    def save_transformed_data(self, transformed_data: Dict[str, Any], output_path: str) -> None:
        """
        Save transformed data to JSON file.
        
        Args:
            transformed_data (Dict): Transformed data
            output_path (str): Output file path
        """
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(transformed_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Transformed data saved to: {output_path}")


# This transformer is now deprecated - use main.py ETLPipeline instead
