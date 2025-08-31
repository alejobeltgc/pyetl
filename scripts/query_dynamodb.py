#!/usr/bin/env python3
"""
Script para consultar datos de DynamoDB de forma legible.
"""

import boto3
import json
from datetime import datetime


def query_dynamodb_data():
    """Consulta y muestra datos de DynamoDB de manera legible"""
    
    table_name = "pyetl-rates-rates-demo"
    
    # Inicializar DynamoDB
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')
    
    print(f"� Consultando tabla: {table_name}")
    
    try:
        # Escanear todos los elementos
        response = dynamodb.scan(TableName=table_name)
        items = response['Items']
        
        print(f"📋 Encontrados {len(items)} elementos:")
        
        for item in items:
            print(f"📄 Documento: {item.get('document_id')}")
            print(f"   🏢 Línea de negocio: {item.get('business_line')}")
            print(f"   Servicios: {item.get('total_services', 'Desconocido')}")
            print(f"   Creado: {item.get('created_at', 'Desconocido')}")
            
    except Exception as e:
        print(f"❌ Error consultando DynamoDB: {str(e)}")


if __name__ == "__main__":
    query_dynamodb_data()
