"""
Configuration for accounts business line transformation rules.
"""

ACCOUNTS_CONFIG = {
    "business_line": "accounts",
    "document_type": "rates_and_fees",
    
    # Field mappings
    "tax_field": "Aplica Iva",
    "frequency_field": "Frecuencia",
    "description_field": "Descripción",
    "disclaimer_field": "Disclaimer",
    
    # Plan types mapping
    "plan_types": {
        "Tarifa Plan G-Zero para cuenta movil": "g_zero",
        "Tarifa Plan Puls para cuenta movil": "puls", 
        "Tarifa Plan Premier para cuenta movil": "premier"
    },
    
    # Frequency mapping
    "frequency_mapping": {
        "Mensual": "monthly",
        "Por transaccion": "per_transaction",
        "Unica vez": "one_time",
        "Anual": "yearly"
    },
    
    # Tax mapping
    "tax_mapping": {
        "Si": True,
        "No": False,
        "No aplica": False
    },
    
    # Table type classification patterns
    "table_classification": {
        "mobile_plans": {
            "patterns": ["planes", "app", "movil"],
            "required_columns": ["g_zero", "puls", "premier"]
        },
        "transfers": {
            "patterns": ["enviar", "transferencia", "ach", "transfiya", "llaves"],
            "keywords": ["dinero", "cuentas", "bancos"]
        },
        "withdrawals": {
            "patterns": ["retiro", "cajero", "oficina", "corresponsal"],
            "keywords": ["debito", "tarjeta", "medio"]
        },
        "traditional_services": {
            "patterns": ["tradicional", "certificacion", "extracto", "consulta"],
            "structure": "different_columns"
        }
    },
    
    # Service ID generation patterns
    "service_id_patterns": {
        "app": "app_opening",
        "debito digital": "digital_debit_card",
        "retiro": "withdrawal",
        "talonario": "checkbook",
        "cheque": "cashier_check",
        "transferencia": "transfer",
        "ach": "ach_transfer",
        "transfiya": "transfiya_transfer",
        "llaves": "keys_transfer",
        "cajero": "atm",
        "corresponsal": "correspondent",
        "oficina": "branch"
    }
}
