#!/usr/bin/env python3
"""
Script para consultar datos de DynamoDB de forma legible
"""
import boto3
import json
from boto3.dynamodb.types import TypeDeserializer

def query_dynamodb_data():
    # Cliente DynamoDB
    dynamodb = boto3.client('dynamodb', region_name='us-east-1')
    
    table_name = 'pyetl-rates-rates-demo'
    
    try:
        # Escanear todos los items
        response = dynamodb.scan(TableName=table_name)
        
        # Deserializar datos de DynamoDB
        deserializer = TypeDeserializer()
        items = []
        
        for item in response['Items']:
            deserialized_item = {k: deserializer.deserialize(v) for k, v in item.items()}
            items.append(deserialized_item)
        
        print("🗄️ Datos en DynamoDB:")
        print("=" * 50)
        
        # Agrupar por documento
        documents = {}
        for item in items:
            doc_id = item.get('document_id')
            if doc_id not in documents:
                documents[doc_id] = {'metadata': None, 'validation': None, 'services': []}
            
            if item.get('sk') == 'metadata':
                documents[doc_id]['metadata'] = item
            elif item.get('sk') == 'validation':
                documents[doc_id]['validation'] = item
            elif item.get('sk', '').startswith('service#'):
                documents[doc_id]['services'].append(item)
        
        # Mostrar cada documento
        for doc_id, data in documents.items():
            print(f"\n📄 Documento: {doc_id}")
            
            if data['metadata']:
                meta = data['metadata']
                print(f"   📁 Archivo: {meta.get('source_file')}")
                print(f"   🏢 Business Line: {meta.get('business_line')}")
                print(f"   📅 Procesado: {meta.get('processed_at')}")
                print(f"   📊 Stage: {meta.get('stage')}")
            
            if data['validation']:
                val = data['validation']
                print(f"   ✅ Validación: {val.get('validation_status')}")
                print(f"   🚨 Errores: {val.get('total_errors', 0)}")
                print(f"   ⚠️ Warnings: {val.get('total_warnings', 0)}")
            
            if data['services']:
                print(f"   🔧 Servicios ({len(data['services'])}):")
                for service in data['services']:
                    print(f"      - {service.get('service_id')}: {service.get('table_type')}")
        
        print(f"\n📊 Total de registros en DynamoDB: {len(items)}")
        
    except Exception as e:
        print(f"❌ Error consultando DynamoDB: {e}")

if __name__ == "__main__":
    query_dynamodb_data()
