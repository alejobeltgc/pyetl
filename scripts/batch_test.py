#!/usr/bin/env python3
"""
Script para hacer testing batch de múltiples archivos
"""
import sys
import asyncio
import json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.infrastructure.adapters.excel_adapter import OpenpyxlExcelAdapter

async def test_all_files():
    """Prueba todos los archivos Excel en el directorio data/"""
    data_dir = Path("data")
    excel_files = list(data_dir.glob("*.xlsx"))
    
    if not excel_files:
        print("❌ No se encontraron archivos Excel en data/")
        return
    
    print(f"🧪 Testing {len(excel_files)} archivos Excel")
    print("=" * 60)
    
    results = []
    adapter = OpenpyxlExcelAdapter()
    
    for file_path in excel_files:
        print(f"\n📂 Procesando: {file_path.name}")
        
        try:
            with open(file_path, 'rb') as f:
                document = await adapter.process_excel_file(
                    file_content=f,
                    filename=file_path.name,
                    document_id=f"batch-test-{file_path.stem}"
                )
            
            result = {
                "file": file_path.name,
                "status": "success",
                "business_line": document.business_line,
                "strategy": document.processing_metadata.get('strategy_used'),
                "services_count": document.get_service_count(),
                "services_by_type": document.get_service_count_by_table_type(),
                "sheets": document.processing_metadata.get('sheets_found', [])
            }
            
            print(f"  ✅ {document.get_service_count()} servicios | {document.business_line} | {result['strategy']}")
            
        except Exception as e:
            result = {
                "file": file_path.name,
                "status": "error", 
                "error": str(e)
            }
            print(f"  ❌ Error: {e}")
        
        results.append(result)
    
    # Guardar resumen
    summary_file = Path("output") / f"batch_test_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary_file.parent.mkdir(exist_ok=True)
    
    with open(summary_file, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\n📊 RESUMEN:")
    print(f"{'=' * 40}")
    
    successful = [r for r in results if r.get('status') == 'success']
    failed = [r for r in results if r.get('status') == 'error']
    
    print(f"✅ Exitosos: {len(successful)}")
    print(f"❌ Fallidos: {len(failed)}")
    
    if successful:
        total_services = sum(r.get('services_count', 0) for r in successful)
        print(f"💰 Total servicios extraídos: {total_services}")
        
        # Agrupar por business line
        by_business_line = {}
        for r in successful:
            bl = r.get('business_line', 'unknown')
            if bl not in by_business_line:
                by_business_line[bl] = 0
            by_business_line[bl] += r.get('services_count', 0)
        
        print(f"📊 Por línea de negocio:")
        for bl, count in by_business_line.items():
            print(f"  - {bl}: {count} servicios")
    
    if failed:
        print(f"\n❌ Archivos fallidos:")
        for r in failed:
            print(f"  - {r['file']}: {r.get('error', 'Error desconocido')}")
    
    print(f"\n💾 Resumen guardado en: {summary_file}")

if __name__ == "__main__":
    asyncio.run(test_all_files())
