#!/usr/bin/env python3
"""
Script para probar el pipeline subiendo archivos a S3
"""
import boto3
import sys
import os
from pathlib import Path


def upload_file_to_s3(file_path: str, stage: str = 'dev'):
    """Sube un archivo al bucket S3 configurado"""
    
    # Configuración
    bucket_name = f'pyetl-input-{stage}'
    
    # Verificar que el archivo existe
    if not Path(file_path).exists():
        print(f"❌ Archivo no encontrado: {file_path}")
        return False
    
    try:
        # Cliente S3
        s3 = boto3.client('s3')
        
        # Nombre del archivo en S3
        file_name = Path(file_path).name
        s3_key = f"test-uploads/{file_name}"
        
        print(f"📤 Subiendo {file_path} a s3://{bucket_name}/{s3_key}")
        
        # Subir archivo
        s3.upload_file(file_path, bucket_name, s3_key)
        
        print(f"✅ Archivo subido exitosamente!")
        print(f"🔗 S3 URI: s3://{bucket_name}/{s3_key}")
        
        # Mostrar información de monitoring
        print(f"\n📊 Para monitorear el procesamiento:")
        print(f"  CloudWatch Logs: aws logs tail /aws/lambda/pyetl-{stage}-processRatesFile --follow")
        print(f"  DynamoDB Table: pyetl-rates-{stage}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error subiendo archivo: {e}")
        return False


def list_available_files():
    """Lista archivos disponibles en el directorio data/"""
    data_dir = Path('data')
    if not data_dir.exists():
        print("❌ Directorio 'data/' no encontrado")
        return []
    
    xlsx_files = list(data_dir.glob('*.xlsx'))
    if not xlsx_files:
        print("❌ No se encontraron archivos .xlsx en el directorio 'data/'")
        return []
    
    print("📁 Archivos disponibles:")
    for i, file_path in enumerate(xlsx_files, 1):
        print(f"  {i}. {file_path}")
    
    return xlsx_files


def main():
    """Función principal"""
    print("🧪 PyETL - Test File Upload")
    print("=" * 40)
    
    # Verificar argumentos
    if len(sys.argv) < 2:
        print("Uso: python scripts/test_upload.py <archivo.xlsx> [stage]")
        print("\nEjemplo: python scripts/test_upload.py data/sample.xlsx dev")
        print("\nArchivos disponibles:")
        list_available_files()
        return
    
    file_path = sys.argv[1]
    stage = sys.argv[2] if len(sys.argv) > 2 else 'dev'
    
    # Verificar credenciales AWS
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"🔐 AWS Account: {identity['Account']}")
        print(f"🌍 AWS Region: {boto3.Session().region_name or 'us-east-1'}")
        print(f"🏷️ Stage: {stage}")
        print()
    except Exception as e:
        print(f"❌ Error con credenciales AWS: {e}")
        print("Por favor configura tus credenciales con 'aws configure'")
        return
    
    # Subir archivo
    if upload_file_to_s3(file_path, stage):
        print(f"\n🎉 ¡Listo! El pipeline debería procesar el archivo automáticamente.")
    else:
        print(f"\n❌ Error subiendo el archivo.")


if __name__ == "__main__":
    main()
