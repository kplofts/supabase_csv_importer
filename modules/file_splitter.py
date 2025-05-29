"""
File splitting module for large CSV files
"""
import os
import csv
from typing import List, Optional, Dict, Any
from pathlib import Path


class FileSplitter:
    """Splits large CSV files into manageable chunks"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
    def split_file(self, file_path: str, chunk_size_mb: int = 100, 
                   output_dir: Optional[str] = None) -> List[str]:
        """Split a CSV file into chunks"""
        if output_dir is None:
            output_dir = self.config['directories']['temp_directory']
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Calculate chunk size in bytes
        chunk_size_bytes = chunk_size_mb * 1024 * 1024
        
        # Detect encoding
        from .utils import detect_encoding
        encoding = detect_encoding(file_path)
        
        chunk_files = []
        base_name = Path(file_path).stem
        
        with open(file_path, 'r', encoding=encoding, errors='replace') as infile:
            # Read header
            header = infile.readline()
            
            chunk_num = 0
            current_chunk_size = 0
            current_chunk_file = None
            current_chunk_writer = None
            
            for line_num, line in enumerate(infile, start=1):
                # Start new chunk if needed
                if current_chunk_file is None or current_chunk_size >= chunk_size_bytes:
                    # Close previous chunk
                    if current_chunk_file:
                        current_chunk_file.close()
                    
                    # Create new chunk
                    chunk_num += 1
                    chunk_filename = os.path.join(output_dir, f"{base_name}_chunk_{chunk_num:04d}.csv")
                    chunk_files.append(chunk_filename)
                    
                    current_chunk_file = open(chunk_filename, 'w', encoding='utf-8', newline='')
                    current_chunk_file.write(header)
                    current_chunk_size = len(header.encode('utf-8'))
                
                # Write line to current chunk
                current_chunk_file.write(line)
                current_chunk_size += len(line.encode('utf-8'))
                
                # Progress indicator
                if line_num % 100000 == 0:
                    print(f"  Processed {line_num:,} rows, created {chunk_num} chunks")
            
            # Close last chunk
            if current_chunk_file:
                current_chunk_file.close()
        
        print(f"  Split into {len(chunk_files)} chunks")
        return chunk_files
