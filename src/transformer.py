from datetime import datetime, timezone

class Transformer:
    def __init__(self, business_rules, config):
        self.business_rules = business_rules
        self.config = config

    def transform(self, raw_data: dict) -> dict:
        """Transform raw data using business rules."""
        transformed_data = {
            "business_line": self.config["business_line"],
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
