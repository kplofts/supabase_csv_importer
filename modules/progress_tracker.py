"""
Progress tracking module with ETA and metrics
"""
import time
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, Any


class ProgressTracker:
    """Tracks import progress with thread-safe updates"""
    
    def __init__(self):
        self.start_time = time.time()
        self.total_rows = 0
        self.total_files = 0
        self.current_file = 0
        self.bytes_processed = 0
        self.current_status = "Initializing..."
        self._lock = threading.Lock()
        
    def add_rows(self, count: int) -> None:
        """Add processed rows to counter"""
        with self._lock:
            self.total_rows += count
    
    def add_bytes(self, count: int) -> None:
        """Add processed bytes to counter"""
        with self._lock:
            self.bytes_processed += count
    
    def update_status(self, status: str) -> None:
        """Update current status message"""
        with self._lock:
            self.current_status = status
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current progress statistics"""
        with self._lock:
            elapsed = time.time() - self.start_time
            rows_per_sec = self.total_rows / elapsed if elapsed > 0 else 0
            bytes_per_sec = self.bytes_processed / elapsed if elapsed > 0 else 0
            
            return {
                'elapsed_time': elapsed,
                'total_rows': self.total_rows,
                'bytes_processed': self.bytes_processed,
                'rows_per_second': rows_per_sec,
                'bytes_per_second': bytes_per_sec,
                'current_status': self.current_status
            }
