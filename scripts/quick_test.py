#!/usr/bin/env python3
"""
Script simple para testing rápido del ETL - Sin AWS
"""
import sys
import asyncio
from pathlib import Path

# Agregar path para imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.infrastructure.adapters.excel_adapter import OpenpyxlExcelAdapter

async def quick_test(file_path: str):
    """Test rápido sin mucho output"""
    print(f"🧪 Testing: {Path(file_path).name}")
    
    try:
        adapter = OpenpyxlExcelAdapter()
        
        with open(file_path, 'rb') as f:
            document = await adapter.process_excel_file(
                file_content=f,
                filename=Path(file_path).name,
                document_id="quick-test"
            )
        
        print(f"✅ Éxito! {document.get_service_count()} servicios extraídos")
        print(f"📊 Business line: {document.business_line}")
        print(f"🎯 Strategy: {document.processing_metadata.get('strategy_used')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python quick_test.py <archivo_excel>")
        sys.exit(1)
    
    success = asyncio.run(quick_test(sys.argv[1]))
    sys.exit(0 if success else 1)
