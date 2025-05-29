"""
File analysis module for CSV inspection
"""
import os
import csv
import chardet
from typing import Dict, Any, List, Optional
from pathlib import Path


class FileAnalyzer:
    """Analyzes CSV files for import optimization"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
    def analyze_file(self, file_path: str) -> Dict[str, Any]:
        """Analyze a CSV file and return metadata"""
        try:
            file_stats = os.stat(file_path)
            file_size = file_stats.st_size
            
            # Detect encoding
            encoding = self._detect_encoding(file_path)
            
            # Quick scan for structure
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                # Read header
                header_line = f.readline()
                columns = self._parse_csv_header(header_line)
                
                # Count rows (sample-based for large files)
                if file_size > 100 * 1024 * 1024:  # Over 100MB
                    row_count = self._estimate_row_count(f, file_size, len(header_line))
                else:
                    row_count = sum(1 for _ in f)
                
                # Read sample rows
                f.seek(0)
                next(f)  # Skip header
                sample_rows = []
                for i, line in enumerate(f):
                    if i >= 5:  # Sample first 5 rows
                        break
                    try:
                        row = next(csv.reader([line]))
                        sample_rows.append(row)
                    except:
                        pass
            
            # Calculate metrics
            size_mb = file_size / (1024 * 1024)
            chunk_size_mb = self.config['import']['chunk_size_mb']
            estimated_chunks = max(1, int(size_mb / chunk_size_mb))
            
            return {
                'valid': True,
                'file_path': file_path,
                'size_bytes': file_size,
                'size_mb': size_mb,
                'encoding': encoding,
                'row_count': row_count,
                'column_count': len(columns),
                'columns': columns,
                'estimated_chunks': estimated_chunks,
                'sample_rows': sample_rows
            }
            
        except Exception as e:
            return {
                'valid': False,
                'error': str(e),
                'file_path': file_path
            }
    
    def _detect_encoding(self, file_path: str) -> str:
        """Detect file encoding"""
        if not self.config.get('file_handling', {}).get('encoding_detection', True):
            return self.config.get('file_handling', {}).get('default_encoding', 'utf-8')
        
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(min(1024 * 1024, os.path.getsize(file_path)))  # Read up to 1MB
                result = chardet.detect(raw_data)
                return result['encoding'] or 'utf-8'
        except:
            return 'utf-8'
    
    def _parse_csv_header(self, header_line: str) -> List[str]:
        """Parse CSV header line"""
        try:
            # Try standard CSV parsing first
            reader = csv.reader([header_line.strip()])
            columns = next(reader)
            return [col.strip() for col in columns]
        except:
            # Fallback to simple split
            return [col.strip().strip('"') for col in header_line.strip().split(',')]
    
    def _estimate_row_count(self, file_handle, file_size: int, header_size: int) -> int:
        """Estimate row count for large files"""
        # Sample middle of file
        sample_size = 1024 * 1024  # 1MB sample
        file_handle.seek(file_size // 2)
        sample = file_handle.read(sample_size)
        
        # Count newlines in sample
        newline_count = sample.count('\n')
        
        # Estimate total rows
        if newline_count > 0:
            avg_line_size = sample_size / newline_count
            estimated_rows = int(file_size / avg_line_size)
            return estimated_rows
        else:
            # Fallback: assume average line size
            return int(file_size / 1000)  # Rough estimate
