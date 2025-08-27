"""
Configuration for accounts business line transformation rules.
"""

ACCOUNTS_CONFIG = {
    "business_line": "accounts",
    "document_type": "rates_and_fees",
    
    # Field mappings
    "tax_field": "APLICA IVA",
    "frequency_field": "FRECUENCIA",
    "description_field": "DESCRIPCIÓN",
    "disclaimer_field": "DISCLAIMER",
    
    # Plan types mapping
    "plan_types": {
        "PLAN G - ZERO PARA CUENTA MOVIL": "g_zero",
        "PLAN PLUS PARA CUENTA MOVIL": "puls", 
        "PLAN PREMIER PARA CUENTA MÓVIL": "premier",
        "VALOR (Sin IVA)": "standard_rate"
    },
    
    # Frequency mapping
    "frequency_mapping": {
        "Mensual": "monthly",
        "Por transacción": "per_transaction",
        "Por transaccion": "per_transaction",
        "Unica vez": "one_time",
        "Unica Vez": "one_time",
        "A demanda": "on_demand",
        "Anual": "yearly"
    },
    
    # Tax mapping
    "tax_mapping": {
        "Si": True,
        "No": False,
        "No Aplica": False,
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
