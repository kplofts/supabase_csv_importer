"""
Utility functions for the importer
"""
import os
import sys
import chardet
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path


def setup_logging(log_dir: str, prefix: str) -> str:
    """Setup logging configuration"""
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{prefix}_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return log_file


def log_message(log_file: Optional[str], message: str, print_to_console: bool = True) -> None:
    """Log a message to file and optionally console"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    
    if log_file:
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry + '\n')
        except:
            pass
    
    if print_to_console:
        print(log_entry)


def format_bytes(bytes_count: int) -> str:
    """Format bytes into human-readable string"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024.0:
            return f"{bytes_count:.2f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.2f} PB"


def format_duration(seconds: float) -> str:
    """Format seconds into human-readable duration"""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"


def detect_encoding(file_path: str) -> str:
    """Detect file encoding using chardet"""
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(min(1024 * 1024, os.path.getsize(file_path)))
            result = chardet.detect(raw_data)
            return result['encoding'] or 'utf-8'
    except:
        return 'utf-8'


def print_banner() -> None:
    """Print application banner"""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║             Supabase CSV Importer - Version 1.0               ║
║                 Optimized for Large Files                     ║
╚═══════════════════════════════════════════════════════════════╝
    """)


def print_summary(stats: Dict[str, Any]) -> None:
    """Print import summary"""
    print("\n" + "="*60)
    print("IMPORT SUMMARY")
    print("="*60)
    print(f"Total rows imported: {stats['total_rows']:,}")
    print(f"Data processed: {format_bytes(stats['bytes_processed'])}")
    print(f"Time elapsed: {format_duration(stats['elapsed_time'])}")
    print(f"Average speed: {stats['rows_per_second']:,.0f} rows/sec")
    print("="*60)


def validate_environment() -> bool:
    """Validate the environment is properly configured"""
    try:
        import psycopg2
        import yaml
        import pandas
        return True
    except ImportError as e:
        print(f"❌ Missing required dependency: {e}")
        print("Please run: pip install -r requirements.txt")
        return False
