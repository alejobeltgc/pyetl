#!/usr/bin/env python3
"""
Script para probar el ETL PyETL en local sin AWS
"""
import sys
import os
import json
import asyncio
from pathlib import Path
from datetime import datetime

# Agregar el directorio raíz al path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.infrastructure.adapters.excel_adapter import OpenpyxlExcelAdapter


def print_banner():
    """Imprime banner del script"""
    print("=" * 60)
    print("🧪 PyETL Local Testing Script")
    print("=" * 60)


def print_results(document):
    """Imprime resultados del procesamiento de forma bonita"""
    print(f"\n📋 RESULTADOS DEL PROCESAMIENTO")
    print(f"{'=' * 50}")
    
    print(f"📄 Documento: {document.filename}")
    print(f"🏢 Línea de Negocio: {document.business_line}")
    print(f"🆔 Document ID: {document.document_id}")
    print(f"⏰ Procesado: {document.last_updated}")
    
    # Metadata de procesamiento
    metadata = document.processing_metadata
    print(f"\n🔧 METADATA DE PROCESAMIENTO")
    print(f"📊 Sheets encontrados: {', '.join(metadata.get('sheets_found', []))}")
    print(f"🎯 Estrategia usada: {metadata.get('strategy_used', 'N/A')}")
    
    # Servicios extraídos
    print(f"\n💰 SERVICIOS EXTRAÍDOS ({document.get_service_count()} total)")
    print(f"{'=' * 50}")
    
    services_by_type = document.get_service_count_by_table_type()
    for table_type, count in services_by_type.items():
        print(f"  📊 {table_type}: {count} servicios")
    
    # Detalles de servicios
    if document.services:
        print(f"\n🔍 DETALLE DE SERVICIOS:")
        for i, service in enumerate(document.services[:5]):  # Mostrar solo los primeros 5
            print(f"\n  {i+1}. {service.service_id}")
            print(f"     📝 Descripción: {service.description[:80]}{'...' if len(service.description) > 80 else ''}")
            print(f"     📊 Tipo: {service.table_type}")
            print(f"     💳 Planes: {', '.join(service.rates.keys())}")
        
        if len(document.services) > 5:
            print(f"     ... y {len(document.services) - 5} servicios más")
    
    # Validación
    validation = metadata.get('validation_report', {})
    if validation:
        issues = validation.get('issues', [])
        summary = validation.get('summary', {})
        
        print(f"\n⚠️ VALIDACIÓN")
        print(f"{'=' * 30}")
        print(f"❌ Errores: {summary.get('errors', 0)}")
        print(f"⚠️ Warnings: {summary.get('warnings', 0)}")
        print(f"ℹ️ Info: {summary.get('info', 0)}")
        
        if issues:
            print(f"\n🔍 Primeros Issues:")
            for issue in issues[:3]:
                level_icon = {"error": "❌", "warning": "⚠️", "info": "ℹ️"}
                icon = level_icon.get(issue.get('level', 'info'), 'ℹ️')
                print(f"  {icon} {issue.get('message', 'N/A')}")


async def test_excel_file(file_path: str, document_id: str = None):
    """
    Prueba el procesamiento de un archivo Excel local
    
    Args:
        file_path: Ruta al archivo Excel
        document_id: ID opcional para el documento
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        print(f"❌ Archivo no encontrado: {file_path}")
        return None
    
    if not document_id:
        document_id = f"test-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    
    print(f"📂 Procesando archivo: {file_path.name}")
    print(f"🆔 Document ID: {document_id}")
    
    try:
        # Crear adapter
        adapter = OpenpyxlExcelAdapter()
        
        # Leer archivo
        with open(file_path, 'rb') as f:
            file_content = f
            
            # Procesar archivo
            print(f"⚙️ Iniciando procesamiento...")
            document = await adapter.process_excel_file(
                file_content=file_content,
                filename=file_path.name,
                document_id=document_id
            )
            
            print(f"✅ Procesamiento completado!")
            return document
            
    except Exception as e:
        print(f"❌ Error procesando archivo: {e}")
        import traceback
        traceback.print_exc()
        return None


def save_results_json(document, output_dir: str = "output"):
    """Guarda los resultados en JSON para inspección"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # Generar nombre de archivo
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"test_results_{timestamp}.json"
    file_path = output_path / filename
    
    # Convertir a dict y guardar
    try:
        result_data = document.to_dict()
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, ensure_ascii=False, indent=2)
        
        print(f"💾 Resultados guardados en: {file_path}")
        return file_path
    except Exception as e:
        print(f"❌ Error guardando resultados: {e}")
        return None


async def main():
    """Función principal"""
    print_banner()
    
    # Verificar argumentos
    if len(sys.argv) < 2:
        print("📝 Uso: python test_local.py <archivo_excel> [document_id]")
        print("\nEjemplos:")
        print("  python scripts/test_local.py data/tasas-y-tarifas.xlsx")
        print("  python scripts/test_local.py data/accounts-rates.xlsx mi-test-123")
        
        # Mostrar archivos disponibles
        data_dir = Path("data")
        if data_dir.exists():
            excel_files = list(data_dir.glob("*.xlsx"))
            if excel_files:
                print(f"\n📁 Archivos Excel disponibles en data/:")
                for file in excel_files:
                    print(f"  - {file.name}")
        
        return
    
    file_path = sys.argv[1]
    document_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Procesar archivo
    document = await test_excel_file(file_path, document_id)
    
    if document:
        # Mostrar resultados
        print_results(document)
        
        # Guardar en JSON
        output_file = save_results_json(document)
        
        print(f"\n🎉 ¡Testing completado exitosamente!")
        print(f"📊 Servicios procesados: {document.get_service_count()}")
        
        # Sugerir próximos pasos
        print(f"\n🔮 Próximos pasos sugeridos:")
        print(f"  1. Revisar el JSON generado: {output_file}")
        print(f"  2. Validar que los servicios extraídos sean correctos")
        print(f"  3. Probar con diferentes archivos Excel")
        print(f"  4. Crear nuevas strategies si es necesario")
        
    else:
        print(f"\n❌ El testing falló. Revisa los errores arriba.")
        sys.exit(1)


if __name__ == "__main__":
    # Ejecutar con asyncio
    asyncio.run(main())
