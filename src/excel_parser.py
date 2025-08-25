"""
Excel Parser Module

This module contains functions for reading and parsing Excel files to extract
tables from multiple sheets. It's designed to handle Excel files with multiple
sheets containing various data tables for financial rates and fees.
"""

import pandas as pd
import json
from typing import Dict, List, Any, Optional
from pathlib import Path


class ExcelParser:
    """
    A class to parse Excel files and extract tables from multiple sheets.
    """
    
    def __init__(self, file_path: str):
        """
        Initialize the ExcelParser with a file path.
        
        Args:
            file_path (str): Path to the Excel file
        """
        self.file_path = Path(file_path)
        self.excel_file = None
        self.sheet_names = []
        
    def load_excel_file(self) -> bool:
        """
        Load the Excel file and get sheet names.
        
        Returns:
            bool: True if file loaded successfully, False otherwise
        """
        try:
            self.excel_file = pd.ExcelFile(self.file_path)
            self.sheet_names = self.excel_file.sheet_names
            print(f"Successfully loaded Excel file: {self.file_path}")
            print(f"Found {len(self.sheet_names)} sheets: {self.sheet_names}")
            return True
        except Exception as e:
            print(f"Error loading Excel file: {e}")
            return False
    
    def get_sheet_data(self, sheet_name: str) -> Optional[pd.DataFrame]:
        """
        Get raw data from a specific sheet.
        
        Args:
            sheet_name (str): Name of the sheet to read
            
        Returns:
            Optional[pd.DataFrame]: DataFrame with sheet data or None if error
        """
        try:
            df = pd.read_excel(self.excel_file, sheet_name=sheet_name)
            return df
        except Exception as e:
            print(f"Error reading sheet '{sheet_name}': {e}")
            return None
    
    def _is_header_row(self, row: pd.Series, prev_row: pd.Series = None) -> bool:
        """
        Determine if a row is likely a header row for a NEW table.
        
        Args:
            row (pd.Series): Row to analyze
            prev_row (pd.Series): Previous row for context
            
        Returns:
            bool: True if row appears to be a header for a new table
        """
        # Convert row to string values
        row_values = [str(val).lower().strip() if pd.notna(val) else '' for val in row.values]
        
        # Check if the first column value is "Descripción" - this is a strong indicator
        is_description_header = row_values[0] == 'descripción'
        
        # If it's a "Descripción" header, check if it has the typical header structure
        if is_description_header:
            # Look for typical header patterns in the rest of the row
            header_patterns = [
                'tarifa', 'plan', 'valor', 'aplica', 'frecuencia', 'disclaimer'
            ]
            
            # Check if at least 2 other columns contain header-like terms
            header_count = sum(1 for val in row_values[1:] if any(pattern in val for pattern in header_patterns))
            
            # Also check for specific patterns like "Plan G-Zero", "Plan Puls", etc.
            plan_patterns = ['plan g-zero', 'plan puls', 'plan premier']
            has_plan_patterns = any(any(pattern in val for pattern in plan_patterns) for val in row_values)
            
            return header_count >= 2 or has_plan_patterns
        
        return False
    
    def _find_header_positions(self, df: pd.DataFrame) -> List[int]:
        """
        Find positions of header rows in the dataframe.
        
        Args:
            df (pd.DataFrame): Cleaned dataframe to analyze
            
        Returns:
            List[int]: List of row indices that appear to be headers for new tables
        """
        potential_headers = []
        
        for idx, row in df.iterrows():
            prev_row = df.iloc[idx-1] if idx > 0 else None
            if self._is_header_row(row, prev_row):
                potential_headers.append(idx)
        
        return potential_headers
    
    def _detect_table_boundaries(self, df_section: pd.DataFrame) -> pd.DataFrame:
        """
        Detect the actual boundaries of a table by finding the rightmost column with data.
        
        Args:
            df_section (pd.DataFrame): Section of dataframe containing the table
            
        Returns:
            pd.DataFrame: Trimmed dataframe with only relevant columns
        """
        if df_section.empty:
            return df_section
        
        # Find the rightmost column that has non-null data in the header row
        header_row = df_section.iloc[0]
        last_col_with_data = 0
        
        for i, val in enumerate(header_row.values):
            if pd.notna(val) and str(val).strip():
                last_col_with_data = i
        
        # Include one extra column in case there's data without header
        max_col = min(last_col_with_data + 1, len(df_section.columns) - 1)
        
        # Also check if there's any data in subsequent rows that extends beyond the header
        for _, row in df_section.iloc[1:].iterrows():
            for i, val in enumerate(row.values):
                if i <= max_col and pd.notna(val) and str(val).strip():
                    last_col_with_data = max(last_col_with_data, i)
        
        # Final column range (ensure we have at least the columns with data)
        final_col = min(last_col_with_data + 1, len(df_section.columns))
        
        return df_section.iloc[:, :final_col + 1]
    
    def _create_table_from_section(self, df_section: pd.DataFrame, sheet_name: str, 
                                 table_index: int, start_row: int, end_row: int) -> Dict[str, Any]:
        """
        Create a table dictionary from a section of the dataframe.
        
        Args:
            df_section (pd.DataFrame): Section of dataframe containing the table
            sheet_name (str): Name of the sheet
            table_index (int): Index of this table within the sheet
            start_row (int): Starting row index
            end_row (int): Ending row index
            
        Returns:
            Dict[str, Any]: Table information dictionary or None if table is too small
        """
        # Clean up the table - remove empty rows
        table_data = df_section.dropna(how='all').reset_index(drop=True)
        
        # Require at least header + 3 data rows for a meaningful table
        if len(table_data) < 4:
            return None
        
        # Detect the actual boundaries of this table
        table_data = self._detect_table_boundaries(table_data)
        
        if table_data.empty:
            return None
        
        # Try to use the header row values as column names
        header_row = table_data.iloc[0]
        data_rows = table_data.iloc[1:].copy()
        
        # Create meaningful column names from header row
        new_columns = []
        for j, col_val in enumerate(header_row.values):
            if pd.notna(col_val) and str(col_val).strip():
                new_columns.append(str(col_val).strip())
            else:
                # Only add generic column names if there's actual data in this column
                has_data = data_rows.iloc[:, j].notna().any()
                if has_data:
                    new_columns.append(f"column_{j}")
                else:
                    # Don't include this column if it has no data
                    break
        
        # Trim the data to match the number of meaningful columns
        if len(new_columns) < len(data_rows.columns):
            data_rows = data_rows.iloc[:, :len(new_columns)]
        
        # Ensure column names are unique
        unique_columns = self._make_columns_unique(new_columns)
        data_rows.columns = unique_columns
        
        # Generate table name
        table_name = self._generate_table_name(sheet_name, table_index, header_row)
        
        return {
            'sheet_name': sheet_name,
            'table_index': table_index,
            'start_row': start_row,
            'end_row': end_row,
            'start_col': 0,
            'end_col': len(new_columns),
            'data': data_rows.to_dict('records'),  # Convert DataFrame to list of dicts
            'table_name': table_name,
            'header_row': {k: v for k, v in header_row.to_dict().items() if k in unique_columns or 
                          list(header_row.index).index(k) < len(new_columns)}
        }
    
    def _make_columns_unique(self, columns: List[str]) -> List[str]:
        """
        Ensure column names are unique by adding suffixes.
        
        Args:
            columns (List[str]): List of column names
            
        Returns:
            List[str]: List of unique column names
        """
        seen = set()
        unique_columns = []
        for col in columns:
            if col in seen:
                counter = 1
                while f"{col}_{counter}" in seen:
                    counter += 1
                col = f"{col}_{counter}"
            seen.add(col)
            unique_columns.append(col)
        return unique_columns
    
    def _generate_table_name(self, sheet_name: str, table_index: int, header_row: pd.Series) -> str:
        """
        Generate a meaningful name for the table.
        
        Args:
            sheet_name (str): Name of the sheet
            table_index (int): Index of the table
            header_row (pd.Series): Header row of the table
            
        Returns:
            str: Generated table name
        """
        table_name = f"{sheet_name}_table_{table_index}"
        
        if pd.notna(header_row.iloc[0]) and str(header_row.iloc[0]).strip():
            safe_name = str(header_row.iloc[0]).strip()[:30]  # Limit length
            # Remove special characters for safe naming
            safe_name = ''.join(c if c.isalnum() or c in '_-' else '_' for c in safe_name)
            table_name = f"{sheet_name}_{safe_name}"
        
        return table_name

    def find_tables_in_sheet(self, sheet_name: str) -> List[Dict[str, Any]]:
        """
        Find and extract individual tables from a sheet.
        This method identifies table boundaries by looking for header rows.
        
        Args:
            sheet_name (str): Name of the sheet to analyze
            
        Returns:
            List[Dict[str, Any]]: List of dictionaries containing table information
        """
        df = self.get_sheet_data(sheet_name)
        if df is None:
            return []
        
        # Remove completely empty rows and columns
        df_cleaned = df.dropna(how='all').dropna(axis=1, how='all')
        
        if df_cleaned.empty:
            return []
        
        # Reset index after cleaning
        df_cleaned = df_cleaned.reset_index(drop=True)
        
        # Find header positions
        potential_headers = self._find_header_positions(df_cleaned)
        print(f"   Found {len(potential_headers)} potential header rows at positions: {potential_headers}")
        
        tables = []
        
        # If no headers found, treat entire sheet as one table
        if not potential_headers:
            table_info = self._create_table_from_section(
                df_cleaned, sheet_name, 0, 0, len(df_cleaned)
            )
            if table_info:
                tables.append(table_info)
            return tables
        
        # Create tables based on header positions
        for i, header_idx in enumerate(potential_headers):
            # Determine end of this table
            end_idx = potential_headers[i + 1] if i + 1 < len(potential_headers) else len(df_cleaned)
            
            # Extract table data
            table_section = df_cleaned.iloc[header_idx:end_idx].copy()
            
            # Create table info
            table_info = self._create_table_from_section(
                table_section, sheet_name, i, header_idx, end_idx
            )
            
            if table_info:
                tables.append(table_info)
                print(f"   - Table {i}: '{table_info['table_name']}' ({len(table_info['data'])} rows)")
        
        return tables
    
    def extract_all_tables(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract all tables from all sheets in the Excel file.
        
        Returns:
            Dict[str, List[Dict[str, Any]]]: Dictionary with sheet names as keys
            and list of tables as values
        """
        if not self.excel_file:
            print("Excel file not loaded. Call load_excel_file() first.")
            return {}
        
        all_tables = {}
        
        for sheet_name in self.sheet_names:
            print(f"\nProcessing sheet: {sheet_name}")
            tables = self.find_tables_in_sheet(sheet_name)
            all_tables[sheet_name] = tables
            print(f"Found {len(tables)} table(s) in sheet '{sheet_name}'")
        
        return all_tables
    
    def tables_to_json(self, tables: Dict[str, List[Dict[str, Any]]], 
                      include_data: bool = True) -> str:
        """
        Convert extracted tables to JSON format.
        
        Args:
            tables (Dict): Dictionary of tables from extract_all_tables()
            include_data (bool): Whether to include actual data or just metadata
            
        Returns:
            str: JSON string representation of the tables
        """
        json_data = {}
        
        for sheet_name, sheet_tables in tables.items():
            json_data[sheet_name] = []
            
            for table in sheet_tables:
                table_json = {
                    'table_index': table['table_index'],
                    'table_name': table.get('table_name', f"{sheet_name}_table_{table['table_index']}"),
                    'dimensions': {
                        'rows': len(table['data']),
                        'columns': len(table['data'].columns) if not table['data'].empty else 0
                    },
                    'position': {
                        'start_row': table['start_row'],
                        'end_row': table['end_row'],
                        'start_col': table['start_col'],
                        'end_col': table['end_col']
                    }
                }
                
                # Include header information if available
                if 'header_row' in table:
                    table_json['header_info'] = table['header_row']
                
                if include_data and not table['data'].empty:
                    # Convert DataFrame to dict, handling NaN values
                    df = table['data']
                    # Replace NaN with None for JSON serialization
                    df_clean = df.where(pd.notnull(df), None)
                    table_json['data'] = df_clean.to_dict('records')
                    table_json['columns'] = list(df.columns)
                
                json_data[sheet_name].append(table_json)
        
        return json.dumps(json_data, indent=2, ensure_ascii=False)


def main():
    """
    Main function to demonstrate the Excel parser functionality.
    """
    excel_file_path = "tasas-y-tarifas.xlsx"
    
    # Create parser instance
    parser = ExcelParser(excel_file_path)
    
    # Load the Excel file
    if not parser.load_excel_file():
        return
    
    # Extract all tables
    print("\n" + "="*50)
    print("EXTRACTING TABLES FROM EXCEL FILE")
    print("="*50)
    
    tables = parser.extract_all_tables()
    
    # Convert to JSON and print
    print("\n" + "="*50)
    print("JSON OUTPUT")
    print("="*50)
    
    json_output = parser.tables_to_json(tables, include_data=True)
    print(json_output)
    
    # Save to file for inspection
    with open('output.json', 'w', encoding='utf-8') as f:
        f.write(json_output)
    
    print("\n\nJSON output also saved to 'output.json'")


if __name__ == "__main__":
    main()
