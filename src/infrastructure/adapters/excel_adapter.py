"""Excel processing adapter using openpyxl with strategy pattern."""

import logging
from typing import BinaryIO, List, Dict, Any, Optional

try:
    from openpyxl import load_workbook
    EXCEL_PROCESSING_AVAILABLE = True
except ImportError:
    EXCEL_PROCESSING_AVAILABLE = False

from ...domain.entities import Document, FinancialService
from ...domain.strategies import ExtractionStrategyFactory

logger = logging.getLogger(__name__)


class OpenpyxlExcelAdapter:
    """
    Openpyxl implementation for Excel processing using Strategy pattern.
    
    Uses business-specific extraction strategies to handle different
    types of Excel files based on their content and structure.
    """
    
    def __init__(self):
        if not EXCEL_PROCESSING_AVAILABLE:
            raise ExcelAdapterError("openpyxl is not available")
        
        self.strategy_factory = ExtractionStrategyFactory()
    
    async def process_excel_file(self, file_content: BinaryIO, filename: str, 
                                document_id: str) -> Document:
        """
        Process Excel file content using appropriate strategy.
        
        Args:
            file_content: Excel file content
            filename: Original filename
            document_id: Unique document identifier
            
        Returns:
            Document with extracted services
        """
        try:
            # Load workbook
            workbook = load_workbook(file_content, data_only=True)
            sheet_names = workbook.sheetnames
            
            logger.info(f"📋 Found {len(sheet_names)} sheets: {sheet_names}")
            
            # Determine extraction strategy
            strategy = self.strategy_factory.get_strategy_for_file(sheet_names, filename)
            business_line = strategy.business_line
            
            logger.info(f"🎯 Using {strategy.__class__.__name__} for business line: {business_line}")
            
            # Create document
            document = Document(
                document_id=document_id,
                business_line=business_line,
                filename=filename
            )
            
            # Add processing metadata
            document.processing_metadata = {
                'sheets_found': sheet_names,
                'processing_method': 'openpyxl_with_strategy',
                'excel_format': 'xlsx',
                'strategy_used': strategy.__class__.__name__,
                'strategy_metadata': strategy.get_strategy_metadata()
            }
            
            # Process sheets using strategy
            processed_sheets = 0
            for sheet_name in sheet_names:
                if strategy.should_process_sheet(sheet_name):
                    try:
                        sheet = workbook[sheet_name]
                        services = await self._extract_services_using_strategy(
                            sheet, sheet_name, strategy, document_id
                        )
                        
                        for service in services:
                            document.add_service(service)
                        
                        logger.info(f"✅ Extracted {len(services)} services from {sheet_name}")
                        processed_sheets += 1
                        
                    except Exception as e:
                        logger.error(f"❌ Error processing sheet {sheet_name}: {str(e)}")
                        document.processing_metadata[f'error_{sheet_name}'] = str(e)
                else:
                    logger.info(f"⏭️ Skipping sheet {sheet_name} (not relevant for {business_line})")
            
            # Validate extracted data using strategy
            validation_errors = strategy.validate_extracted_data(document.services)
            if validation_errors:
                document.processing_metadata['strategy_validation_errors'] = validation_errors
                logger.warning(f"⚠️ Strategy validation found {len(validation_errors)} issues")
            
            logger.info(f"📊 Total services extracted: {document.get_service_count()} from {processed_sheets} sheets")
            
            return document
            
        except Exception as e:
            raise ExcelAdapterError(f"Failed to process Excel file {filename}: {str(e)}") from e
    
    async def _extract_services_using_strategy(self, sheet, sheet_name: str, 
                                              strategy, document_id: str) -> List[FinancialService]:
        """Extract services from sheet using the provided strategy."""
        services = []
        
        # Convert sheet to 2D array
        sheet_data = self._sheet_to_array(sheet)
        
        if not sheet_data:
            logger.warning(f"Sheet {sheet_name} appears to be empty")
            return services
        
        logger.info(f"Processing sheet {sheet_name}: {len(sheet_data)} rows")
        
        # Find data start row using strategy
        data_start_row = strategy.find_data_start_row(sheet_data, sheet_name)
        if data_start_row is None:
            logger.warning(f"No data start row found in sheet {sheet_name}")
            return services
        
        # Extract headers using strategy
        headers = strategy.extract_headers(sheet_data, data_start_row)
        logger.info(f"Headers found: {headers}")
        
        # Extract services from data rows
        for row_idx in range(data_start_row, len(sheet_data)):
            try:
                row_data = sheet_data[row_idx]
                service = strategy.extract_service_from_row(
                    row_data, headers, sheet_name, row_idx, document_id
                )
                
                if service:
                    services.append(service)
                    
            except Exception as e:
                logger.warning(f"Error processing row {row_idx} in {sheet_name}: {str(e)}")
        
        return services
    
    def _sheet_to_array(self, sheet) -> List[List[Any]]:
        """Convert openpyxl sheet to 2D array."""
        data = []
        
        max_row = sheet.max_row or 0
        max_col = sheet.max_column or 0
        
        for row_idx in range(1, max_row + 1):
            row_data = []
            for col_idx in range(1, max_col + 1):
                cell_value = sheet.cell(row=row_idx, column=col_idx).value
                row_data.append(cell_value)
            data.append(row_data)
        
        return data


class ExcelAdapterError(Exception):
    """Raised when Excel adapter operations fail."""
    pass
